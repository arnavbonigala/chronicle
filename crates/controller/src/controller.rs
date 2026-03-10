use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::Raft;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::state_machine::StateMachineStore;
use crate::types::{
    BrokerStatus, ClusterState, MetadataChange, MetadataRequest, MetadataResponse, TypeConfig,
};

pub struct Controller {
    raft: Raft<TypeConfig>,
    sm: Arc<StateMachineStore>,
    pub broker_id: u32,
}

impl Controller {
    pub fn new(raft: Raft<TypeConfig>, sm: Arc<StateMachineStore>, broker_id: u32) -> Self {
        Self {
            raft,
            sm,
            broker_id,
        }
    }

    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    pub async fn propose(&self, cmd: MetadataRequest) -> Result<MetadataResponse, String> {
        let resp = self
            .raft
            .client_write(cmd)
            .await
            .map_err(|e| e.to_string())?;
        Ok(resp.data)
    }

    pub async fn cluster_state(&self) -> ClusterState {
        self.sm.cluster_state().await
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MetadataChange> {
        self.sm.subscribe()
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.current_leader().await == Some(self.broker_id as u64)
    }

    /// Checks all brokers' heartbeats. For any broker whose last heartbeat
    /// exceeds `timeout_ms`, proposes MarkBrokerDead and triggers failover.
    /// Should only be called on the Raft leader.
    pub async fn check_heartbeats(&self, timeout_ms: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let state = self.sm.cluster_state().await;

        for (id, broker) in &state.brokers {
            if *id == self.broker_id {
                continue;
            }
            if broker.status == BrokerStatus::Dead {
                continue;
            }
            let elapsed = now_ms.saturating_sub(broker.last_heartbeat_ms);
            if elapsed > timeout_ms {
                warn!(
                    broker_id = id,
                    elapsed_ms = elapsed,
                    "broker heartbeat timeout, marking dead"
                );
                if let Err(e) = self
                    .propose(MetadataRequest::MarkBrokerDead { broker_id: *id })
                    .await
                {
                    warn!(broker_id = id, error = %e, "failed to mark broker dead");
                    continue;
                }
                self.failover_broker(*id, &state).await;
            }
        }
    }

    /// Checks all consumer group members' heartbeats. For any member whose
    /// last heartbeat exceeds its session timeout, proposes RemoveExpiredMember.
    /// Should only be called on the Raft leader.
    pub async fn check_consumer_heartbeats(&self, default_timeout_ms: u64) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let state = self.sm.cluster_state().await;

        for group in state.consumer_groups.values() {
            for member in group.members.values() {
                let timeout = if member.session_timeout_ms > 0 {
                    member.session_timeout_ms
                } else {
                    default_timeout_ms
                };
                if now_ms.saturating_sub(member.last_heartbeat_ms) > timeout {
                    warn!(
                        group_id = %group.group_id,
                        member_id = %member.member_id,
                        "consumer session expired, removing member"
                    );
                    if let Err(e) = self
                        .propose(MetadataRequest::RemoveExpiredMember {
                            group_id: group.group_id.clone(),
                            member_id: member.member_id.clone(),
                        })
                        .await
                    {
                        warn!(
                            group_id = %group.group_id,
                            member_id = %member.member_id,
                            error = %e,
                            "failed to remove expired member"
                        );
                    }
                }
            }
        }
    }

    /// For all partitions where `failed_broker` is the current leader,
    /// select a new leader from the ISR and propose an UpdateLeader.
    async fn failover_broker(&self, failed_broker: u32, state: &ClusterState) {
        for topic in state.topics.values() {
            for assignment in &topic.assignments {
                if assignment.leader != failed_broker {
                    continue;
                }
                let new_leader = assignment
                    .isr
                    .iter()
                    .find(|&&id| id != failed_broker)
                    .copied();
                match new_leader {
                    Some(leader) => {
                        let new_epoch = assignment.leader_epoch + 1;
                        info!(
                            topic = %topic.name,
                            partition = assignment.partition_id,
                            new_leader = leader,
                            epoch = new_epoch,
                            "failing over partition"
                        );
                        if let Err(e) = self
                            .propose(MetadataRequest::UpdateLeader {
                                topic: topic.name.clone(),
                                partition: assignment.partition_id,
                                new_leader: leader,
                                epoch: new_epoch,
                            })
                            .await
                        {
                            warn!(
                                topic = %topic.name,
                                partition = assignment.partition_id,
                                error = %e,
                                "failed to propose leader update"
                            );
                        }
                    }
                    None => {
                        warn!(
                            topic = %topic.name,
                            partition = assignment.partition_id,
                            "no ISR candidate for failover"
                        );
                    }
                }
            }
        }
    }
}

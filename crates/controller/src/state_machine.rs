use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use tokio::sync::{broadcast, RwLock};

use chronicle_replication::assignment::compute_assignments;

use crate::types::{
    BrokerRegistration, BrokerStatus, ClusterState, MetadataChange, MetadataRequest,
    MetadataResponse, NodeId, PartitionAssignmentMeta, TopicMetadata, TypeConfig,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: StoredMembership<NodeId, BasicNode>,
    pub cluster_state: ClusterState,
}

#[derive(Debug)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

pub struct StateMachineStore {
    sm: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    change_tx: broadcast::Sender<MetadataChange>,
}

impl StateMachineStore {
    pub fn new() -> (Arc<Self>, broadcast::Receiver<MetadataChange>) {
        let (tx, rx) = broadcast::channel(256);
        let store = Arc::new(Self {
            sm: RwLock::new(StateMachineData::default()),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            change_tx: tx,
        });
        (store, rx)
    }

    pub async fn cluster_state(&self) -> ClusterState {
        self.sm.read().await.cluster_state.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<MetadataChange> {
        self.change_tx.subscribe()
    }

    fn apply_command(&self, sm: &mut StateMachineData, req: &MetadataRequest) -> MetadataResponse {
        match req {
            MetadataRequest::RegisterBroker { id, addr } => {
                sm.cluster_state.brokers.insert(
                    *id,
                    BrokerRegistration {
                        id: *id,
                        addr: addr.clone(),
                        last_heartbeat_ms: current_time_ms(),
                        status: BrokerStatus::Live,
                    },
                );
                self.change_tx
                    .send(MetadataChange::BrokerRegistered {
                        id: *id,
                        addr: addr.clone(),
                    })
                    .ok();
                MetadataResponse::Ok
            }
            MetadataRequest::Heartbeat {
                broker_id,
                timestamp_ms,
            } => {
                if let Some(broker) = sm.cluster_state.brokers.get_mut(broker_id) {
                    broker.last_heartbeat_ms = *timestamp_ms;
                    broker.status = BrokerStatus::Live;
                }
                MetadataResponse::Ok
            }
            MetadataRequest::CreateTopic {
                name,
                partition_count,
                replication_factor,
            } => {
                if sm.cluster_state.topics.contains_key(name) {
                    return MetadataResponse::Error(format!("topic already exists: {name}"));
                }
                let live_ids = sm.cluster_state.live_broker_ids();
                if live_ids.is_empty() {
                    return MetadataResponse::Error("no live brokers".to_string());
                }
                let raw = compute_assignments(*partition_count, *replication_factor, &live_ids);
                let assignments: Vec<PartitionAssignmentMeta> = raw
                    .iter()
                    .map(|a| PartitionAssignmentMeta {
                        partition_id: a.partition_id,
                        leader: a.replicas[0],
                        leader_epoch: 1,
                        replicas: a.replicas.clone(),
                        isr: a.replicas.clone(),
                    })
                    .collect();
                sm.cluster_state.topics.insert(
                    name.clone(),
                    TopicMetadata {
                        name: name.clone(),
                        partition_count: *partition_count,
                        replication_factor: *replication_factor,
                        assignments: assignments.clone(),
                    },
                );
                self.change_tx
                    .send(MetadataChange::TopicCreated {
                        name: name.clone(),
                        assignments: assignments.clone(),
                    })
                    .ok();
                MetadataResponse::TopicCreated { assignments }
            }
            MetadataRequest::DeleteTopic { name } => {
                sm.cluster_state.topics.remove(name);
                self.change_tx
                    .send(MetadataChange::TopicDeleted { name: name.clone() })
                    .ok();
                MetadataResponse::Ok
            }
            MetadataRequest::UpdateLeader {
                topic,
                partition,
                new_leader,
                epoch,
            } => {
                if let Some(t) = sm.cluster_state.topics.get_mut(topic) {
                    if let Some(a) = t
                        .assignments
                        .iter_mut()
                        .find(|a| a.partition_id == *partition)
                    {
                        a.leader = *new_leader;
                        a.leader_epoch = *epoch;
                    }
                }
                self.change_tx
                    .send(MetadataChange::LeaderChanged {
                        topic: topic.clone(),
                        partition: *partition,
                        new_leader: *new_leader,
                        epoch: *epoch,
                    })
                    .ok();
                MetadataResponse::Ok
            }
            MetadataRequest::UpdateISR {
                topic,
                partition,
                isr,
            } => {
                if let Some(t) = sm.cluster_state.topics.get_mut(topic) {
                    if let Some(a) = t
                        .assignments
                        .iter_mut()
                        .find(|a| a.partition_id == *partition)
                    {
                        a.isr = isr.clone();
                    }
                }
                self.change_tx
                    .send(MetadataChange::ISRChanged {
                        topic: topic.clone(),
                        partition: *partition,
                        isr: isr.clone(),
                    })
                    .ok();
                MetadataResponse::Ok
            }
            MetadataRequest::MarkBrokerDead { broker_id } => {
                if let Some(broker) = sm.cluster_state.brokers.get_mut(broker_id) {
                    broker.status = BrokerStatus::Dead;
                }
                self.change_tx
                    .send(MetadataChange::BrokerDead { id: *broker_id })
                    .ok();
                MetadataResponse::Ok
            }
            MetadataRequest::JoinGroup { .. }
            | MetadataRequest::LeaveGroup { .. }
            | MetadataRequest::ConsumerHeartbeat { .. }
            | MetadataRequest::CommitOffset { .. }
            | MetadataRequest::RemoveExpiredMember { .. } => {
                MetadataResponse::Error("consumer group operations not yet implemented".into())
            }
        }
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm = self.sm.read().await;
        let data = serde_json::to_vec(&*sm).map_err(|e| StorageIOError::read_state_machine(&e))?;
        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(sm);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let sm = self.sm.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetadataResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut res = Vec::new();
        let mut sm = self.sm.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => res.push(MetadataResponse::Ok),
                EntryPayload::Normal(ref req) => {
                    let response = self.apply_command(&mut sm, req);
                    res.push(response);
                }
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(MetadataResponse::Ok);
                }
            }
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let new_sm: StateMachineData = serde_json::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;

        let mut sm = self.sm.write().await;
        *sm = new_sm;
        let mut current_snapshot = self.current_snapshot.write().await;
        drop(sm);
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_broker() {
        let (store, mut rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "http://127.0.0.1:9092".into(),
            },
        );
        drop(sm);

        let state = store.cluster_state().await;
        assert_eq!(state.brokers.len(), 1);
        assert_eq!(state.brokers[&1].status, BrokerStatus::Live);

        let change = rx.try_recv().unwrap();
        assert!(matches!(
            change,
            MetadataChange::BrokerRegistered { id: 1, .. }
        ));
    }

    #[tokio::test]
    async fn create_topic() {
        let (store, mut rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "a".into(),
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 2,
                addr: "b".into(),
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::CreateTopic {
                name: "orders".into(),
                partition_count: 2,
                replication_factor: 2,
            },
        );
        drop(sm);

        match resp {
            MetadataResponse::TopicCreated { assignments } => {
                assert_eq!(assignments.len(), 2);
                assert_eq!(assignments[0].leader_epoch, 1);
                assert_eq!(assignments[0].replicas.len(), 2);
            }
            other => panic!("expected TopicCreated, got {other:?}"),
        }

        let state = store.cluster_state().await;
        assert!(state.topics.contains_key("orders"));

        // Drain broker registration changes
        let _ = rx.try_recv();
        let _ = rx.try_recv();
        let change = rx.try_recv().unwrap();
        assert!(matches!(change, MetadataChange::TopicCreated { .. }));
    }

    #[tokio::test]
    async fn update_leader() {
        let (store, mut rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "a".into(),
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 2,
                addr: "b".into(),
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::CreateTopic {
                name: "t".into(),
                partition_count: 1,
                replication_factor: 2,
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::UpdateLeader {
                topic: "t".into(),
                partition: 0,
                new_leader: 2,
                epoch: 2,
            },
        );
        drop(sm);

        let state = store.cluster_state().await;
        let a = &state.topics["t"].assignments[0];
        assert_eq!(a.leader, 2);
        assert_eq!(a.leader_epoch, 2);

        // Drain prior changes
        while rx.try_recv().is_ok() {}
    }

    #[tokio::test]
    async fn mark_broker_dead() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "a".into(),
            },
        );
        store.apply_command(&mut sm, &MetadataRequest::MarkBrokerDead { broker_id: 1 });
        drop(sm);

        let state = store.cluster_state().await;
        assert_eq!(state.brokers[&1].status, BrokerStatus::Dead);
        assert!(state.live_broker_ids().is_empty());
    }

    #[tokio::test]
    async fn duplicate_topic_returns_error() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "a".into(),
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::CreateTopic {
                name: "t".into(),
                partition_count: 1,
                replication_factor: 1,
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::CreateTopic {
                name: "t".into(),
                partition_count: 1,
                replication_factor: 1,
            },
        );
        assert!(matches!(resp, MetadataResponse::Error(_)));
    }
}

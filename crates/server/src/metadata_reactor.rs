#![allow(dead_code)]
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chronicle_controller::types::{MetadataChange, PartitionAssignmentMeta};
use chronicle_replication::{ClusterConfig, FollowerFetcher, ReplicaManager};
use chronicle_storage::{PartitionAssignment, TopicStore};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub struct MetadataReactor {
    broker_id: u32,
    store: Arc<TopicStore>,
    replica_manager: Arc<ReplicaManager>,
    cluster: ClusterConfig,
    fetcher_handles: HashMap<(String, u32), CancellationToken>,
}

impl MetadataReactor {
    pub fn new(
        broker_id: u32,
        store: Arc<TopicStore>,
        replica_manager: Arc<ReplicaManager>,
        cluster: ClusterConfig,
    ) -> Self {
        Self {
            broker_id,
            store,
            replica_manager,
            cluster,
            fetcher_handles: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut rx: broadcast::Receiver<MetadataChange>) {
        loop {
            match rx.recv().await {
                Ok(change) => self.handle_change(change),
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(skipped = n, "metadata reactor lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::info!("metadata change channel closed, reactor exiting");
                    return;
                }
            }
        }
    }

    fn handle_change(&mut self, change: MetadataChange) {
        match change {
            MetadataChange::TopicCreated { name, assignments } => {
                self.handle_topic_created(&name, &assignments);
            }
            MetadataChange::TopicDeleted { name } => {
                self.handle_topic_deleted(&name);
            }
            MetadataChange::LeaderChanged {
                topic,
                partition,
                new_leader,
                epoch,
            } => {
                self.handle_leader_changed(&topic, partition, new_leader, epoch);
            }
            MetadataChange::BrokerDead { id } => {
                tracing::info!(broker_id = id, "broker marked dead");
            }
            MetadataChange::ISRChanged {
                topic,
                partition,
                isr,
            } => {
                tracing::debug!(topic = %topic, partition, ?isr, "ISR changed");
            }
            MetadataChange::BrokerRegistered { id, addr } => {
                tracing::info!(broker_id = id, addr = %addr, "broker registered");
            }
            MetadataChange::TransactionCompleted {
                producer_id,
                committed,
                ..
            } => {
                tracing::info!(producer_id, committed, "transaction completed");
            }
        }
    }

    fn handle_topic_created(&mut self, name: &str, assignments: &[PartitionAssignmentMeta]) {
        let local_partitions: Vec<u32> = assignments
            .iter()
            .filter(|a| a.replicas.contains(&self.broker_id))
            .map(|a| a.partition_id)
            .collect();

        if local_partitions.is_empty() {
            return;
        }

        let storage_assignments: Vec<PartitionAssignment> = assignments
            .iter()
            .map(|a| PartitionAssignment {
                partition_id: a.partition_id,
                replicas: a.replicas.clone(),
            })
            .collect();

        let partition_count = assignments.len() as u32;
        let replication_factor = assignments
            .first()
            .map(|a| a.replicas.len() as u32)
            .unwrap_or(1);

        if let Err(e) = self.store.create_topic(
            name,
            partition_count,
            replication_factor,
            &storage_assignments,
            &local_partitions,
        ) {
            tracing::warn!(topic = name, error = %e, "failed to create topic locally");
            return;
        }

        self.replica_manager
            .register_topic(name, &storage_assignments);

        for a in assignments {
            if !a.replicas.contains(&self.broker_id) {
                continue;
            }
            if a.leader != self.broker_id {
                self.spawn_fetcher(name, a.partition_id, a.leader);
            }
        }
    }

    fn handle_topic_deleted(&mut self, name: &str) {
        let keys_to_remove: Vec<(String, u32)> = self
            .fetcher_handles
            .keys()
            .filter(|(t, _)| t == name)
            .cloned()
            .collect();
        for key in keys_to_remove {
            if let Some(token) = self.fetcher_handles.remove(&key) {
                token.cancel();
            }
        }
        if let Err(e) = self.store.delete_topic(name) {
            tracing::warn!(topic = name, error = %e, "failed to delete topic locally");
        }
    }

    fn handle_leader_changed(&mut self, topic: &str, partition: u32, new_leader: u32, epoch: u64) {
        let key = (topic.to_string(), partition);

        if let Some(token) = self.fetcher_handles.remove(&key) {
            token.cancel();
        }

        let has_partition = self
            .replica_manager
            .registered_partitions(topic)
            .iter()
            .any(|(pid, _)| *pid == partition);
        if !has_partition {
            return;
        }

        if new_leader == self.broker_id {
            let replicas = self.get_replicas_for(topic, partition);
            let leo = if let Some(t) = self.store.topic(topic) {
                t.partition(partition)
                    .map(|lock| lock.read().unwrap().latest_offset())
                    .unwrap_or(0)
            } else {
                0
            };
            self.replica_manager
                .promote_to_leader_with_leo(topic, partition, epoch, &replicas, leo);
            tracing::info!(topic, partition, epoch, leo, "promoted to leader");
        } else {
            if self.replica_manager.is_leader(topic, partition) {
                self.replica_manager
                    .demote_to_follower(topic, partition, new_leader, epoch);
                tracing::info!(topic, partition, new_leader, epoch, "demoted to follower");
            } else {
                self.replica_manager
                    .update_leader(topic, partition, new_leader, epoch);
            }
            self.spawn_fetcher(topic, partition, new_leader);
        }
    }

    fn spawn_fetcher(&mut self, topic: &str, partition: u32, leader_id: u32) {
        let addr = match self.cluster.broker_addr(leader_id) {
            Some(a) => a.to_string(),
            None => {
                tracing::warn!(
                    leader_id,
                    "no address for leader broker, cannot spawn fetcher"
                );
                return;
            }
        };

        let cancel = CancellationToken::new();
        let key = (topic.to_string(), partition);
        self.fetcher_handles.insert(key, cancel.clone());

        FollowerFetcher {
            broker_id: self.broker_id,
            topic: topic.to_string(),
            partition,
            leader_addr: addr,
            store: self.store.clone(),
            replica_manager: self.replica_manager.clone(),
            fetch_interval: Duration::from_millis(100),
            cancel,
        }
        .spawn();
    }

    fn get_replicas_for(&self, _topic: &str, _partition: u32) -> Vec<u32> {
        self.cluster.broker_ids()
    }
}

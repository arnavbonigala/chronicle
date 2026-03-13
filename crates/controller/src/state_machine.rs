use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use tokio::sync::{RwLock, broadcast};

use chronicle_replication::assignment::compute_assignments;

use std::collections::BTreeSet;

use crate::types::{
    BrokerRegistration, BrokerStatus, ClusterState, CommittedOffset, ConsumerGroupState,
    GroupMember, MetadataChange, MetadataRequest, MetadataResponse, NodeId,
    PartitionAssignmentMeta, StreamJobMeta, StreamJobStatus, TopicMetadata, TopicPartitionKey,
    TransactionState, TransactionStatus, TxnOffsetCommits, TypeConfig,
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
                if let Some(t) = sm.cluster_state.topics.get_mut(topic)
                    && let Some(a) = t
                        .assignments
                        .iter_mut()
                        .find(|a| a.partition_id == *partition)
                {
                    a.leader = *new_leader;
                    a.leader_epoch = *epoch;
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
                if let Some(t) = sm.cluster_state.topics.get_mut(topic)
                    && let Some(a) = t
                        .assignments
                        .iter_mut()
                        .find(|a| a.partition_id == *partition)
                {
                    a.isr = isr.clone();
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
            MetadataRequest::JoinGroup {
                group_id,
                member_id,
                topics,
                session_timeout_ms,
            } => {
                let group = sm
                    .cluster_state
                    .consumer_groups
                    .entry(group_id.clone())
                    .or_insert_with(|| ConsumerGroupState {
                        group_id: group_id.clone(),
                        ..Default::default()
                    });
                group.members.insert(
                    member_id.clone(),
                    GroupMember {
                        member_id: member_id.clone(),
                        subscriptions: topics.clone(),
                        session_timeout_ms: *session_timeout_ms,
                        last_heartbeat_ms: current_time_ms(),
                    },
                );
                group.generation_id += 1;
                group.assignments =
                    compute_group_assignments(&group.members, &sm.cluster_state.topics);
                let member_assignments = group
                    .assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or_default();
                let generation = group.generation_id;
                MetadataResponse::GroupJoined {
                    generation_id: generation,
                    member_id: member_id.clone(),
                    assignments: member_assignments
                        .into_iter()
                        .map(|tp| (tp.topic, tp.partition))
                        .collect(),
                }
            }
            MetadataRequest::LeaveGroup {
                group_id,
                member_id,
            } => {
                if let Some(group) = sm.cluster_state.consumer_groups.get_mut(group_id) {
                    group.members.remove(member_id);
                    if group.members.is_empty() {
                        sm.cluster_state.consumer_groups.remove(group_id);
                    } else {
                        group.generation_id += 1;
                        group.assignments =
                            compute_group_assignments(&group.members, &sm.cluster_state.topics);
                    }
                }
                MetadataResponse::Ok
            }
            MetadataRequest::ConsumerHeartbeat {
                group_id,
                member_id,
            } => {
                if let Some(group) = sm.cluster_state.consumer_groups.get_mut(group_id) {
                    if let Some(member) = group.members.get_mut(member_id) {
                        member.last_heartbeat_ms = current_time_ms();
                        return MetadataResponse::GroupState {
                            generation_id: group.generation_id,
                        };
                    }
                    return MetadataResponse::Error(format!("member not found: {member_id}"));
                }
                MetadataResponse::Error(format!("consumer group not found: {group_id}"))
            }
            MetadataRequest::CommitOffset { group_id, offsets } => {
                if let Some(group) = sm.cluster_state.consumer_groups.get_mut(group_id) {
                    let now = current_time_ms();
                    for (topic, partition, offset) in offsets {
                        group.offsets.insert(
                            TopicPartitionKey {
                                topic: topic.clone(),
                                partition: *partition,
                            },
                            CommittedOffset {
                                offset: *offset,
                                timestamp_ms: now,
                            },
                        );
                    }
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!("consumer group not found: {group_id}"))
                }
            }
            MetadataRequest::RemoveExpiredMember {
                group_id,
                member_id,
            } => {
                if let Some(group) = sm.cluster_state.consumer_groups.get_mut(group_id)
                    && group.members.remove(member_id).is_some()
                {
                    if group.members.is_empty() {
                        sm.cluster_state.consumer_groups.remove(group_id);
                    } else {
                        group.generation_id += 1;
                        group.assignments =
                            compute_group_assignments(&group.members, &sm.cluster_state.topics);
                    }
                }
                MetadataResponse::Ok
            }
            MetadataRequest::AllocateProducerId { transactional_id } => match transactional_id {
                None => {
                    let id = sm.cluster_state.next_producer_id;
                    sm.cluster_state.next_producer_id += 1;
                    MetadataResponse::ProducerIdAllocated {
                        producer_id: id,
                        producer_epoch: 0,
                    }
                }
                Some(tid) => {
                    use crate::types::TransactionalIdMapping;
                    if let Some(mapping) = sm.cluster_state.transactional_ids.get_mut(tid) {
                        mapping.producer_epoch += 1;
                        MetadataResponse::ProducerIdAllocated {
                            producer_id: mapping.producer_id,
                            producer_epoch: mapping.producer_epoch,
                        }
                    } else {
                        let id = sm.cluster_state.next_producer_id;
                        sm.cluster_state.next_producer_id += 1;
                        sm.cluster_state.transactional_ids.insert(
                            tid.clone(),
                            TransactionalIdMapping {
                                producer_id: id,
                                producer_epoch: 0,
                            },
                        );
                        MetadataResponse::ProducerIdAllocated {
                            producer_id: id,
                            producer_epoch: 0,
                        }
                    }
                }
            },
            MetadataRequest::BeginTransaction { producer_id } => {
                if sm.cluster_state.transactions.contains_key(producer_id) {
                    return MetadataResponse::Error(format!(
                        "transaction already ongoing for producer {producer_id}"
                    ));
                }
                let (txn_id, epoch) = sm
                    .cluster_state
                    .transactional_ids
                    .iter()
                    .find(|(_, m)| m.producer_id == *producer_id)
                    .map(|(tid, m)| (tid.clone(), m.producer_epoch))
                    .unwrap_or_default();
                sm.cluster_state.transactions.insert(
                    *producer_id,
                    TransactionState {
                        transactional_id: txn_id,
                        producer_id: *producer_id,
                        producer_epoch: epoch,
                        status: TransactionStatus::Ongoing,
                        partitions: std::collections::HashSet::new(),
                        offset_commits: None,
                        start_time_ms: current_time_ms(),
                    },
                );
                MetadataResponse::Ok
            }
            MetadataRequest::AddPartitionsToTxn {
                producer_id,
                partitions,
            } => {
                if let Some(txn) = sm.cluster_state.transactions.get_mut(producer_id) {
                    if txn.status != TransactionStatus::Ongoing {
                        return MetadataResponse::Error("transaction not in Ongoing state".into());
                    }
                    for (topic, partition) in partitions {
                        txn.partitions.insert(TopicPartitionKey {
                            topic: topic.clone(),
                            partition: *partition,
                        });
                    }
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!(
                        "transaction not found for producer {producer_id}"
                    ))
                }
            }
            MetadataRequest::AddOffsetsToTxn {
                producer_id,
                group_id,
            } => {
                if let Some(txn) = sm.cluster_state.transactions.get_mut(producer_id) {
                    if txn.status != TransactionStatus::Ongoing {
                        return MetadataResponse::Error("transaction not in Ongoing state".into());
                    }
                    if txn.offset_commits.is_none() {
                        txn.offset_commits = Some(TxnOffsetCommits {
                            group_id: group_id.clone(),
                            offsets: Vec::new(),
                        });
                    }
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!(
                        "transaction not found for producer {producer_id}"
                    ))
                }
            }
            MetadataRequest::TxnOffsetCommit {
                producer_id,
                group_id,
                offsets,
            } => {
                if let Some(txn) = sm.cluster_state.transactions.get_mut(producer_id) {
                    if txn.status != TransactionStatus::Ongoing {
                        return MetadataResponse::Error("transaction not in Ongoing state".into());
                    }
                    txn.offset_commits = Some(TxnOffsetCommits {
                        group_id: group_id.clone(),
                        offsets: offsets.clone(),
                    });
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!(
                        "transaction not found for producer {producer_id}"
                    ))
                }
            }
            MetadataRequest::EndTransaction {
                producer_id,
                commit,
            } => {
                if let Some(txn) = sm.cluster_state.transactions.get_mut(producer_id) {
                    if txn.status != TransactionStatus::Ongoing {
                        return MetadataResponse::Error("transaction not in Ongoing state".into());
                    }
                    txn.status = if *commit {
                        TransactionStatus::PrepareCommit
                    } else {
                        TransactionStatus::PrepareAbort
                    };
                    let partitions: Vec<(String, u32)> = txn
                        .partitions
                        .iter()
                        .map(|tp| (tp.topic.clone(), tp.partition))
                        .collect();
                    MetadataResponse::TxnPartitions { partitions }
                } else {
                    MetadataResponse::Error(format!(
                        "transaction not found for producer {producer_id}"
                    ))
                }
            }
            MetadataRequest::WriteTxnMarkerComplete { producer_id } => {
                if let Some(txn) = sm.cluster_state.transactions.remove(producer_id) {
                    let committed = txn.status == TransactionStatus::PrepareCommit;
                    if committed && let Some(oc) = &txn.offset_commits {
                        let group = sm
                            .cluster_state
                            .consumer_groups
                            .entry(oc.group_id.clone())
                            .or_insert_with(|| ConsumerGroupState {
                                group_id: oc.group_id.clone(),
                                ..Default::default()
                            });
                        let now = current_time_ms();
                        for (topic, partition, offset) in &oc.offsets {
                            group.offsets.insert(
                                TopicPartitionKey {
                                    topic: topic.clone(),
                                    partition: *partition,
                                },
                                CommittedOffset {
                                    offset: *offset,
                                    timestamp_ms: now,
                                },
                            );
                        }
                    }
                    let partitions: Vec<(String, u32)> = txn
                        .partitions
                        .iter()
                        .map(|tp| (tp.topic.clone(), tp.partition))
                        .collect();
                    self.change_tx
                        .send(MetadataChange::TransactionCompleted {
                            producer_id: *producer_id,
                            producer_epoch: txn.producer_epoch,
                            committed,
                            partitions,
                        })
                        .ok();
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!(
                        "transaction not found for producer {producer_id}"
                    ))
                }
            }
            MetadataRequest::CreateStreamJob {
                job_name,
                input_topic,
                input_partition,
                output_topic,
                output_partition,
                operator_chain,
            } => {
                if sm.cluster_state.stream_jobs.contains_key(job_name) {
                    return MetadataResponse::Error(format!(
                        "stream job already exists: {job_name}"
                    ));
                }
                sm.cluster_state.stream_jobs.insert(
                    job_name.clone(),
                    StreamJobMeta {
                        job_name: job_name.clone(),
                        input_topic: input_topic.clone(),
                        input_partition: *input_partition,
                        output_topic: output_topic.clone(),
                        output_partition: *output_partition,
                        operator_chain: operator_chain.clone(),
                        status: StreamJobStatus::Created,
                    },
                );
                MetadataResponse::StreamJobCreated {
                    job_name: job_name.clone(),
                }
            }
            MetadataRequest::DeleteStreamJob { job_name } => {
                if sm.cluster_state.stream_jobs.remove(job_name).is_some() {
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!("stream job not found: {job_name}"))
                }
            }
            MetadataRequest::UpdateStreamJobStatus { job_name, status } => {
                if let Some(job) = sm.cluster_state.stream_jobs.get_mut(job_name) {
                    job.status = status.clone();
                    MetadataResponse::Ok
                } else {
                    MetadataResponse::Error(format!("stream job not found: {job_name}"))
                }
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

fn compute_group_assignments(
    members: &std::collections::HashMap<String, GroupMember>,
    topics: &std::collections::HashMap<String, TopicMetadata>,
) -> std::collections::HashMap<String, Vec<TopicPartitionKey>> {
    let mut subscribed: BTreeSet<&str> = BTreeSet::new();
    for member in members.values() {
        for t in &member.subscriptions {
            subscribed.insert(t);
        }
    }
    let mut all_partitions = Vec::new();
    for topic_name in &subscribed {
        if let Some(meta) = topics.get(*topic_name) {
            for p in 0..meta.partition_count {
                all_partitions.push(TopicPartitionKey {
                    topic: topic_name.to_string(),
                    partition: p,
                });
            }
        }
    }
    let mut member_ids: Vec<&String> = members.keys().collect();
    member_ids.sort();
    let mut assignments: std::collections::HashMap<String, Vec<TopicPartitionKey>> = member_ids
        .iter()
        .map(|id| ((*id).clone(), Vec::new()))
        .collect();
    if member_ids.is_empty() {
        return assignments;
    }
    for (i, tp) in all_partitions.iter().enumerate() {
        let mid = member_ids[i % member_ids.len()];
        assignments.get_mut(mid).unwrap().push(tp.clone());
    }
    assignments
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

    fn setup_with_topic(
        store: &StateMachineStore,
        sm: &mut StateMachineData,
        topic: &str,
        partitions: u32,
    ) {
        store.apply_command(
            sm,
            &MetadataRequest::RegisterBroker {
                id: 1,
                addr: "a".into(),
            },
        );
        store.apply_command(
            sm,
            &MetadataRequest::CreateTopic {
                name: topic.into(),
                partition_count: partitions,
                replication_factor: 1,
            },
        );
    }

    #[tokio::test]
    async fn join_group_single_member() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 4);

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        match resp {
            MetadataResponse::GroupJoined {
                generation_id,
                member_id,
                assignments,
            } => {
                assert_eq!(generation_id, 1);
                assert_eq!(member_id, "c1");
                assert_eq!(assignments.len(), 4);
            }
            other => panic!("expected GroupJoined, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn join_group_two_members_splits_partitions() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 4);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c2".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        match resp {
            MetadataResponse::GroupJoined {
                generation_id,
                assignments,
                ..
            } => {
                assert_eq!(generation_id, 2);
                assert_eq!(assignments.len(), 2);
            }
            other => panic!("expected GroupJoined, got {other:?}"),
        }

        let state = &sm.cluster_state;
        let group = &state.consumer_groups["g1"];
        let total: usize = group.assignments.values().map(|v| v.len()).sum();
        assert_eq!(total, 4);
        assert_eq!(group.assignments["c1"].len(), 2);
        assert_eq!(group.assignments["c2"].len(), 2);
    }

    #[tokio::test]
    async fn leave_group_reassigns() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 4);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c2".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        store.apply_command(
            &mut sm,
            &MetadataRequest::LeaveGroup {
                group_id: "g1".into(),
                member_id: "c2".into(),
            },
        );

        let group = &sm.cluster_state.consumer_groups["g1"];
        assert_eq!(group.generation_id, 3);
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.assignments["c1"].len(), 4);
    }

    #[tokio::test]
    async fn leave_group_last_member_removes_group() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 2);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::LeaveGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
            },
        );

        assert!(!sm.cluster_state.consumer_groups.contains_key("g1"));
    }

    #[tokio::test]
    async fn commit_and_overwrite_offsets() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 2);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        store.apply_command(
            &mut sm,
            &MetadataRequest::CommitOffset {
                group_id: "g1".into(),
                offsets: vec![("orders".into(), 0, 10), ("orders".into(), 1, 20)],
            },
        );

        let group = &sm.cluster_state.consumer_groups["g1"];
        let key0 = TopicPartitionKey {
            topic: "orders".into(),
            partition: 0,
        };
        let key1 = TopicPartitionKey {
            topic: "orders".into(),
            partition: 1,
        };
        assert_eq!(group.offsets[&key0].offset, 10);
        assert_eq!(group.offsets[&key1].offset, 20);

        store.apply_command(
            &mut sm,
            &MetadataRequest::CommitOffset {
                group_id: "g1".into(),
                offsets: vec![("orders".into(), 0, 50)],
            },
        );

        let group = &sm.cluster_state.consumer_groups["g1"];
        assert_eq!(group.offsets[&key0].offset, 50);
        assert_eq!(group.offsets[&key1].offset, 20);
    }

    #[tokio::test]
    async fn remove_expired_member() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 4);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c2".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        store.apply_command(
            &mut sm,
            &MetadataRequest::RemoveExpiredMember {
                group_id: "g1".into(),
                member_id: "c2".into(),
            },
        );

        let group = &sm.cluster_state.consumer_groups["g1"];
        assert_eq!(group.generation_id, 3);
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.assignments["c1"].len(), 4);
    }

    #[tokio::test]
    async fn consumer_heartbeat_returns_generation() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 2);

        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g1".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10000,
            },
        );

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::ConsumerHeartbeat {
                group_id: "g1".into(),
                member_id: "c1".into(),
            },
        );

        match resp {
            MetadataResponse::GroupState { generation_id } => {
                assert_eq!(generation_id, 1);
            }
            other => panic!("expected GroupState, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn assignment_is_deterministic() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        setup_with_topic(&store, &mut sm, "orders", 4);

        for _ in 0..3 {
            sm.cluster_state.consumer_groups.clear();
            store.apply_command(
                &mut sm,
                &MetadataRequest::JoinGroup {
                    group_id: "g1".into(),
                    member_id: "c1".into(),
                    topics: vec!["orders".into()],
                    session_timeout_ms: 10000,
                },
            );
            store.apply_command(
                &mut sm,
                &MetadataRequest::JoinGroup {
                    group_id: "g1".into(),
                    member_id: "c2".into(),
                    topics: vec!["orders".into()],
                    session_timeout_ms: 10000,
                },
            );
        }

        let group = &sm.cluster_state.consumer_groups["g1"];
        assert_eq!(group.assignments["c1"].len(), 2);
        assert_eq!(group.assignments["c2"].len(), 2);
        assert_eq!(group.assignments["c1"][0].partition, 0);
        assert_eq!(group.assignments["c1"][1].partition, 2);
        assert_eq!(group.assignments["c2"][0].partition, 1);
        assert_eq!(group.assignments["c2"][1].partition, 3);
    }

    #[tokio::test]
    async fn allocate_producer_id_increments() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        let resp1 = store.apply_command(
            &mut sm,
            &MetadataRequest::AllocateProducerId {
                transactional_id: None,
            },
        );
        match resp1 {
            MetadataResponse::ProducerIdAllocated {
                producer_id,
                producer_epoch,
            } => {
                assert_eq!(producer_id, 0);
                assert_eq!(producer_epoch, 0);
            }
            other => panic!("expected ProducerIdAllocated, got {other:?}"),
        }

        let resp2 = store.apply_command(
            &mut sm,
            &MetadataRequest::AllocateProducerId {
                transactional_id: None,
            },
        );
        match resp2 {
            MetadataResponse::ProducerIdAllocated {
                producer_id,
                producer_epoch,
            } => {
                assert_eq!(producer_id, 1);
                assert_eq!(producer_epoch, 0);
            }
            other => panic!("expected ProducerIdAllocated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn allocate_producer_id_transactional_bumps_epoch() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        let resp1 = store.apply_command(
            &mut sm,
            &MetadataRequest::AllocateProducerId {
                transactional_id: Some("txn-1".into()),
            },
        );
        match resp1 {
            MetadataResponse::ProducerIdAllocated {
                producer_id,
                producer_epoch,
            } => {
                assert_eq!(producer_id, 0);
                assert_eq!(producer_epoch, 0);
            }
            other => panic!("expected ProducerIdAllocated, got {other:?}"),
        }

        let resp2 = store.apply_command(
            &mut sm,
            &MetadataRequest::AllocateProducerId {
                transactional_id: Some("txn-1".into()),
            },
        );
        match resp2 {
            MetadataResponse::ProducerIdAllocated {
                producer_id,
                producer_epoch,
            } => {
                assert_eq!(producer_id, 0);
                assert_eq!(producer_epoch, 1);
            }
            other => panic!("expected ProducerIdAllocated, got {other:?}"),
        }
    }

    fn allocate_producer(store: &StateMachineStore, sm: &mut StateMachineData) -> u64 {
        match store.apply_command(
            sm,
            &MetadataRequest::AllocateProducerId {
                transactional_id: Some("txn-app".into()),
            },
        ) {
            MetadataResponse::ProducerIdAllocated { producer_id, .. } => producer_id,
            other => panic!("expected ProducerIdAllocated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn begin_transaction() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        assert!(matches!(resp, MetadataResponse::Ok));
        assert!(sm.cluster_state.transactions.contains_key(&pid));
        assert_eq!(
            sm.cluster_state.transactions[&pid].status,
            TransactionStatus::Ongoing
        );
    }

    #[tokio::test]
    async fn begin_transaction_duplicate_errors() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);

        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        assert!(matches!(resp, MetadataResponse::Error(_)));
    }

    #[tokio::test]
    async fn add_partitions_to_txn() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);
        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::AddPartitionsToTxn {
                producer_id: pid,
                partitions: vec![("orders".into(), 0), ("orders".into(), 1)],
            },
        );
        assert!(matches!(resp, MetadataResponse::Ok));
        assert_eq!(sm.cluster_state.transactions[&pid].partitions.len(), 2);
    }

    #[tokio::test]
    async fn add_partitions_idempotent() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);
        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );

        for _ in 0..3 {
            store.apply_command(
                &mut sm,
                &MetadataRequest::AddPartitionsToTxn {
                    producer_id: pid,
                    partitions: vec![("orders".into(), 0)],
                },
            );
        }
        assert_eq!(sm.cluster_state.transactions[&pid].partitions.len(), 1);
    }

    #[tokio::test]
    async fn end_transaction_commit_returns_partitions() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);
        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::AddPartitionsToTxn {
                producer_id: pid,
                partitions: vec![("orders".into(), 0)],
            },
        );

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::EndTransaction {
                producer_id: pid,
                commit: true,
            },
        );

        match resp {
            MetadataResponse::TxnPartitions { partitions } => {
                assert_eq!(partitions.len(), 1);
                assert_eq!(partitions[0], ("orders".to_string(), 0));
            }
            other => panic!("expected TxnPartitions, got {other:?}"),
        }
        assert_eq!(
            sm.cluster_state.transactions[&pid].status,
            TransactionStatus::PrepareCommit
        );
    }

    #[tokio::test]
    async fn end_transaction_abort() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);
        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::EndTransaction {
                producer_id: pid,
                commit: false,
            },
        );
        assert!(matches!(resp, MetadataResponse::TxnPartitions { .. }));
        assert_eq!(
            sm.cluster_state.transactions[&pid].status,
            TransactionStatus::PrepareAbort
        );
    }

    #[tokio::test]
    async fn write_txn_marker_complete_commit_applies_offsets() {
        let (store, mut rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);

        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::AddPartitionsToTxn {
                producer_id: pid,
                partitions: vec![("orders".into(), 0)],
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::AddOffsetsToTxn {
                producer_id: pid,
                group_id: "g1".into(),
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::TxnOffsetCommit {
                producer_id: pid,
                group_id: "g1".into(),
                offsets: vec![("orders".into(), 0, 42)],
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::EndTransaction {
                producer_id: pid,
                commit: true,
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::WriteTxnMarkerComplete { producer_id: pid },
        );
        assert!(matches!(resp, MetadataResponse::Ok));
        assert!(!sm.cluster_state.transactions.contains_key(&pid));

        let group = &sm.cluster_state.consumer_groups["g1"];
        let key = TopicPartitionKey {
            topic: "orders".into(),
            partition: 0,
        };
        assert_eq!(group.offsets[&key].offset, 42);

        while let Ok(change) = rx.try_recv() {
            if matches!(change, MetadataChange::TransactionCompleted { .. }) {
                return;
            }
        }
        panic!("expected TransactionCompleted change");
    }

    #[tokio::test]
    async fn write_txn_marker_complete_abort_discards_offsets() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;
        let pid = allocate_producer(&store, &mut sm);

        store.apply_command(
            &mut sm,
            &MetadataRequest::BeginTransaction { producer_id: pid },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::TxnOffsetCommit {
                producer_id: pid,
                group_id: "g1".into(),
                offsets: vec![("orders".into(), 0, 42)],
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::EndTransaction {
                producer_id: pid,
                commit: false,
            },
        );
        store.apply_command(
            &mut sm,
            &MetadataRequest::WriteTxnMarkerComplete { producer_id: pid },
        );

        assert!(!sm.cluster_state.consumer_groups.contains_key("g1"));
    }

    #[tokio::test]
    async fn create_stream_job() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::CreateStreamJob {
                job_name: "filter-job".into(),
                input_topic: "orders".into(),
                input_partition: 0,
                output_topic: "large_orders".into(),
                output_partition: 0,
                operator_chain: vec!["filter(amount > 1000)".into()],
            },
        );
        assert!(matches!(resp, MetadataResponse::StreamJobCreated { .. }));
        assert!(sm.cluster_state.stream_jobs.contains_key("filter-job"));
        assert_eq!(
            sm.cluster_state.stream_jobs["filter-job"].status,
            StreamJobStatus::Created
        );
    }

    #[tokio::test]
    async fn create_duplicate_stream_job_errors() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        store.apply_command(
            &mut sm,
            &MetadataRequest::CreateStreamJob {
                job_name: "j".into(),
                input_topic: "a".into(),
                input_partition: 0,
                output_topic: "b".into(),
                output_partition: 0,
                operator_chain: vec![],
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::CreateStreamJob {
                job_name: "j".into(),
                input_topic: "a".into(),
                input_partition: 0,
                output_topic: "b".into(),
                output_partition: 0,
                operator_chain: vec![],
            },
        );
        assert!(matches!(resp, MetadataResponse::Error(_)));
    }

    #[tokio::test]
    async fn delete_stream_job() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        store.apply_command(
            &mut sm,
            &MetadataRequest::CreateStreamJob {
                job_name: "j".into(),
                input_topic: "a".into(),
                input_partition: 0,
                output_topic: "b".into(),
                output_partition: 0,
                operator_chain: vec![],
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::DeleteStreamJob {
                job_name: "j".into(),
            },
        );
        assert!(matches!(resp, MetadataResponse::Ok));
        assert!(!sm.cluster_state.stream_jobs.contains_key("j"));
    }

    #[tokio::test]
    async fn update_stream_job_status() {
        let (store, _rx) = StateMachineStore::new();
        let mut sm = store.sm.write().await;

        store.apply_command(
            &mut sm,
            &MetadataRequest::CreateStreamJob {
                job_name: "j".into(),
                input_topic: "a".into(),
                input_partition: 0,
                output_topic: "b".into(),
                output_partition: 0,
                operator_chain: vec![],
            },
        );
        let resp = store.apply_command(
            &mut sm,
            &MetadataRequest::UpdateStreamJobStatus {
                job_name: "j".into(),
                status: StreamJobStatus::Running,
            },
        );
        assert!(matches!(resp, MetadataResponse::Ok));
        assert_eq!(
            sm.cluster_state.stream_jobs["j"].status,
            StreamJobStatus::Running
        );
    }
}

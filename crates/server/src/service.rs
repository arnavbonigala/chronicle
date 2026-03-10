use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chronicle_controller::{Controller, MetadataRequest, MetadataResponse};
use chronicle_replication::assignment::{compute_assignments, local_partitions};
use chronicle_replication::{ClusterConfig, FollowerFetcher, ReplicaManager};
use chronicle_storage::{PartitionAssignment, StorageError, TopicStore};
use tonic::{Request, Response, Status};

use crate::proto;

pub struct ChronicleService {
    store: Arc<TopicStore>,
    replica_manager: Arc<ReplicaManager>,
    cluster: ClusterConfig,
    round_robin: AtomicU32,
    controller: Option<Arc<Controller>>,
}

impl ChronicleService {
    pub fn new(
        store: Arc<TopicStore>,
        replica_manager: Arc<ReplicaManager>,
        cluster: ClusterConfig,
        controller: Option<Arc<Controller>>,
    ) -> Self {
        Self {
            store,
            replica_manager,
            cluster,
            round_robin: AtomicU32::new(0),
            controller,
        }
    }
}

#[tonic::async_trait]
impl proto::chronicle_server::Chronicle for ChronicleService {
    async fn produce(
        &self,
        request: Request<proto::ProduceRequest>,
    ) -> Result<Response<proto::ProduceResponse>, Status> {
        let req = request.into_inner();

        if req.topic.is_empty() {
            return Ok(Response::new(proto::ProduceResponse {
                offset: 0,
                partition: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::InvalidRequest.into(),
                    message: "topic is required".into(),
                }),
                leader_broker_id: None,
            }));
        }

        let topic = match self.store.topic(&req.topic) {
            Some(t) => t,
            None => {
                return Ok(Response::new(proto::ProduceResponse {
                    offset: 0,
                    partition: 0,
                    error: Some(proto::Error {
                        code: proto::ErrorCode::UnknownTopic.into(),
                        message: format!("unknown topic: {}", req.topic),
                    }),
                    leader_broker_id: None,
                }));
            }
        };

        let pid = match req.partition {
            Some(p) => {
                if p >= topic.partition_count() {
                    return Ok(Response::new(proto::ProduceResponse {
                        offset: 0,
                        partition: 0,
                        error: Some(proto::Error {
                            code: proto::ErrorCode::UnknownPartition.into(),
                            message: format!(
                                "partition {} out of range for topic {} (count: {})",
                                p,
                                req.topic,
                                topic.partition_count()
                            ),
                        }),
                        leader_broker_id: None,
                    }));
                }
                p
            }
            None => route_partition(&req.key, topic.partition_count(), &self.round_robin),
        };

        if !self.cluster.is_single_broker() && !self.replica_manager.is_leader(&req.topic, pid) {
            let leader_id = self.replica_manager.leader_for(&req.topic, pid);
            return Ok(Response::new(proto::ProduceResponse {
                offset: 0,
                partition: pid,
                error: Some(proto::Error {
                    code: proto::ErrorCode::NotLeaderForPartition.into(),
                    message: format!(
                        "broker {} is not leader for {}/{}",
                        self.cluster.broker_id, req.topic, pid
                    ),
                }),
                leader_broker_id: leader_id,
            }));
        }

        let offset = {
            let log_lock = match topic.partition(pid) {
                Some(l) => l,
                None => {
                    return Ok(Response::new(proto::ProduceResponse {
                        offset: 0,
                        partition: pid,
                        error: Some(proto::Error {
                            code: proto::ErrorCode::UnknownPartition.into(),
                            message: format!("partition {} not available locally", pid),
                        }),
                        leader_broker_id: None,
                    }));
                }
            };
            let mut log = log_lock.write().unwrap();
            match log.append(&req.key, &req.value) {
                Ok(o) => o,
                Err(e) => {
                    return Ok(Response::new(proto::ProduceResponse {
                        offset: 0,
                        partition: pid,
                        error: Some(storage_err_to_proto(&e)),
                        leader_broker_id: None,
                    }));
                }
            }
        };

        let acks = proto::Acks::try_from(req.acks).unwrap_or(proto::Acks::Leader);
        if acks == proto::Acks::All && !self.cluster.is_single_broker() {
            if let Some(mut rx) = self.replica_manager.hwm_receiver(&req.topic, pid) {
                let timeout = tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        if *rx.borrow() > offset {
                            break;
                        }
                        if rx.changed().await.is_err() {
                            break;
                        }
                    }
                })
                .await;
                if timeout.is_err() {
                    return Ok(Response::new(proto::ProduceResponse {
                        offset,
                        partition: pid,
                        error: Some(proto::Error {
                            code: proto::ErrorCode::InternalError.into(),
                            message: "timeout waiting for replication".into(),
                        }),
                        leader_broker_id: None,
                    }));
                }
            }
        }

        Ok(Response::new(proto::ProduceResponse {
            offset,
            partition: pid,
            error: None,
            leader_broker_id: None,
        }))
    }

    async fn fetch(
        &self,
        request: Request<proto::FetchRequest>,
    ) -> Result<Response<proto::FetchResponse>, Status> {
        let req = request.into_inner();

        if req.topic.is_empty() {
            return Ok(Response::new(proto::FetchResponse {
                records: vec![],
                high_watermark: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::InvalidRequest.into(),
                    message: "topic is required".into(),
                }),
                leader_broker_id: None,
            }));
        }

        let topic = match self.store.topic(&req.topic) {
            Some(t) => t,
            None => {
                return Ok(Response::new(proto::FetchResponse {
                    records: vec![],
                    high_watermark: 0,
                    error: Some(proto::Error {
                        code: proto::ErrorCode::UnknownTopic.into(),
                        message: format!("unknown topic: {}", req.topic),
                    }),
                    leader_broker_id: None,
                }));
            }
        };

        if req.partition >= topic.partition_count() {
            return Ok(Response::new(proto::FetchResponse {
                records: vec![],
                high_watermark: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::UnknownPartition.into(),
                    message: format!(
                        "partition {} out of range for topic {} (count: {})",
                        req.partition,
                        req.topic,
                        topic.partition_count()
                    ),
                }),
                leader_broker_id: None,
            }));
        }

        if !self.cluster.is_single_broker()
            && !self.replica_manager.is_leader(&req.topic, req.partition)
        {
            let leader_id = self.replica_manager.leader_for(&req.topic, req.partition);
            return Ok(Response::new(proto::FetchResponse {
                records: vec![],
                high_watermark: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::NotLeaderForPartition.into(),
                    message: format!(
                        "broker {} is not leader for {}/{}",
                        self.cluster.broker_id, req.topic, req.partition
                    ),
                }),
                leader_broker_id: leader_id,
            }));
        }

        let max_records = if req.max_records == 0 {
            100
        } else {
            req.max_records
        };

        let log_lock = topic.partition(req.partition).unwrap();
        let log = log_lock.read().unwrap();

        let hwm = if self.cluster.is_single_broker() {
            log.latest_offset()
        } else {
            self.replica_manager
                .high_watermark(&req.topic, req.partition)
        };

        match log.read(req.offset, max_records) {
            Ok(records) => {
                let proto_records: Vec<proto::Record> = records
                    .into_iter()
                    .filter(|r| r.offset < hwm)
                    .map(|r| proto::Record {
                        offset: r.offset,
                        timestamp_ms: r.timestamp_ms,
                        key: r.key.to_vec(),
                        value: r.value.to_vec(),
                    })
                    .collect();
                Ok(Response::new(proto::FetchResponse {
                    records: proto_records,
                    high_watermark: hwm,
                    error: None,
                    leader_broker_id: None,
                }))
            }
            Err(e) => Ok(Response::new(proto::FetchResponse {
                records: vec![],
                high_watermark: hwm,
                error: Some(storage_err_to_proto(&e)),
                leader_broker_id: None,
            })),
        }
    }

    async fn create_topic(
        &self,
        request: Request<proto::CreateTopicRequest>,
    ) -> Result<Response<proto::CreateTopicResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() || req.partition_count == 0 {
            return Ok(Response::new(proto::CreateTopicResponse {
                error: Some(proto::Error {
                    code: proto::ErrorCode::InvalidRequest.into(),
                    message: "name and partition_count > 0 are required".into(),
                }),
            }));
        }

        let replication_factor = if req.replication_factor == 0 {
            1
        } else {
            req.replication_factor
        };

        if let Some(ref ctrl) = self.controller {
            let resp = ctrl
                .propose(MetadataRequest::CreateTopic {
                    name: req.name.clone(),
                    partition_count: req.partition_count,
                    replication_factor,
                })
                .await;
            return match resp {
                Ok(MetadataResponse::TopicCreated { .. }) | Ok(MetadataResponse::Ok) => {
                    Ok(Response::new(proto::CreateTopicResponse { error: None }))
                }
                Ok(MetadataResponse::Error(msg)) => Ok(Response::new(proto::CreateTopicResponse {
                    error: Some(proto::Error {
                        code: proto::ErrorCode::TopicAlreadyExists.into(),
                        message: msg,
                    }),
                })),
                Ok(_) => Ok(Response::new(proto::CreateTopicResponse {
                    error: Some(proto::Error {
                        code: proto::ErrorCode::InternalError.into(),
                        message: "unexpected response".into(),
                    }),
                })),
                Err(e) => Ok(Response::new(proto::CreateTopicResponse {
                    error: Some(proto::Error {
                        code: proto::ErrorCode::InternalError.into(),
                        message: e,
                    }),
                })),
            };
        }

        let broker_ids = self.cluster.broker_ids();
        let assignments = if broker_ids.len() > 1 {
            compute_assignments(req.partition_count, replication_factor, &broker_ids)
        } else {
            vec![]
        };

        let local_pids = if assignments.is_empty() {
            vec![]
        } else {
            local_partitions(&assignments, self.cluster.broker_id)
        };

        let storage_assignments: Vec<PartitionAssignment> = assignments
            .iter()
            .map(|a| PartitionAssignment {
                partition_id: a.partition_id,
                replicas: a.replicas.clone(),
            })
            .collect();

        match self.store.create_topic(
            &req.name,
            req.partition_count,
            replication_factor,
            &storage_assignments,
            &local_pids,
        ) {
            Ok(()) => {}
            Err(e) => {
                return Ok(Response::new(proto::CreateTopicResponse {
                    error: Some(storage_err_to_proto(&e)),
                }));
            }
        }

        if !assignments.is_empty() {
            self.replica_manager
                .register_topic(&req.name, &storage_assignments);

            self.spawn_follower_fetchers(&req.name, &storage_assignments);

            for peer in self.cluster.peer_brokers() {
                let peer_local = local_partitions(&assignments, peer.id);
                let proto_assignments: Vec<proto::ProtoPartitionAssignment> = assignments
                    .iter()
                    .map(|a| proto::ProtoPartitionAssignment {
                        partition_id: a.partition_id,
                        replica_ids: a.replicas.clone(),
                    })
                    .collect();

                let addr = peer.addr.clone();
                let topic_name = req.name.clone();
                let pc = req.partition_count;
                let rf = replication_factor;
                tokio::spawn(async move {
                    match proto::chronicle_client::ChronicleClient::connect(addr.clone()).await {
                        Ok(mut client) => {
                            let resp = client
                                .create_replica(proto::CreateReplicaRequest {
                                    topic: topic_name.clone(),
                                    partition_count: pc,
                                    replication_factor: rf,
                                    local_partitions: peer_local,
                                    assignments: proto_assignments,
                                })
                                .await;
                            if let Err(e) = resp {
                                tracing::warn!(
                                    peer = %addr,
                                    topic = %topic_name,
                                    error = %e,
                                    "failed to create replica on peer"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                peer = %addr,
                                error = %e,
                                "failed to connect to peer"
                            );
                        }
                    }
                });
            }
        }

        Ok(Response::new(proto::CreateTopicResponse { error: None }))
    }

    async fn delete_topic(
        &self,
        request: Request<proto::DeleteTopicRequest>,
    ) -> Result<Response<proto::DeleteTopicResponse>, Status> {
        let req = request.into_inner();

        if req.name.is_empty() {
            return Ok(Response::new(proto::DeleteTopicResponse {
                error: Some(proto::Error {
                    code: proto::ErrorCode::InvalidRequest.into(),
                    message: "name is required".into(),
                }),
            }));
        }

        if let Some(ref ctrl) = self.controller {
            return match ctrl
                .propose(MetadataRequest::DeleteTopic {
                    name: req.name.clone(),
                })
                .await
            {
                Ok(_) => Ok(Response::new(proto::DeleteTopicResponse { error: None })),
                Err(e) => Ok(Response::new(proto::DeleteTopicResponse {
                    error: Some(proto::Error {
                        code: proto::ErrorCode::InternalError.into(),
                        message: e,
                    }),
                })),
            };
        }

        match self.store.delete_topic(&req.name) {
            Ok(()) => Ok(Response::new(proto::DeleteTopicResponse { error: None })),
            Err(e) => Ok(Response::new(proto::DeleteTopicResponse {
                error: Some(storage_err_to_proto(&e)),
            })),
        }
    }

    async fn get_metadata(
        &self,
        request: Request<proto::GetMetadataRequest>,
    ) -> Result<Response<proto::GetMetadataResponse>, Status> {
        let req = request.into_inner();

        if let Some(ref ctrl) = self.controller {
            let state = ctrl.cluster_state().await;
            let topics: Vec<proto::TopicInfo> = state
                .topics
                .values()
                .filter(|t| req.topics.is_empty() || req.topics.contains(&t.name))
                .map(|t| {
                    let partitions = t
                        .assignments
                        .iter()
                        .map(|a| proto::PartitionInfo {
                            partition_id: a.partition_id,
                            leader_broker_id: a.leader,
                            replica_broker_ids: a.replicas.clone(),
                            isr_broker_ids: a.isr.clone(),
                            high_watermark: self
                                .replica_manager
                                .high_watermark(&t.name, a.partition_id),
                            log_end_offset: self
                                .store
                                .topic(&t.name)
                                .and_then(|tp| {
                                    tp.partition(a.partition_id)
                                        .map(|l| l.read().unwrap().latest_offset())
                                })
                                .unwrap_or(0),
                            leader_epoch: a.leader_epoch,
                        })
                        .collect();
                    proto::TopicInfo {
                        name: t.name.clone(),
                        partition_count: t.partition_count,
                        replication_factor: t.replication_factor,
                        partitions,
                    }
                })
                .collect();

            return Ok(Response::new(proto::GetMetadataResponse {
                topics,
                error: None,
            }));
        }

        let all = self.store.list_topics();
        let topic_list: Vec<&chronicle_storage::TopicMeta> = if req.topics.is_empty() {
            all.iter().collect()
        } else {
            all.iter()
                .filter(|m| req.topics.contains(&m.name))
                .collect()
        };

        let topics: Vec<proto::TopicInfo> = topic_list
            .into_iter()
            .map(|m| {
                let partitions = (0..m.partition_count)
                    .map(|pid| {
                        let info = self.replica_manager.partition_info(&m.name, pid);
                        let leo = self
                            .store
                            .topic(&m.name)
                            .and_then(|t| {
                                t.partition(pid).map(|l| l.read().unwrap().latest_offset())
                            })
                            .unwrap_or(0);
                        let epoch = self.replica_manager.leader_epoch(&m.name, pid);
                        match info {
                            Some(pi) => proto::PartitionInfo {
                                partition_id: pid,
                                leader_broker_id: pi.leader_id,
                                replica_broker_ids: pi.replicas,
                                isr_broker_ids: pi.isr,
                                high_watermark: pi.high_watermark,
                                log_end_offset: leo,
                                leader_epoch: epoch,
                            },
                            None => proto::PartitionInfo {
                                partition_id: pid,
                                leader_broker_id: self.cluster.broker_id,
                                replica_broker_ids: vec![self.cluster.broker_id],
                                isr_broker_ids: vec![self.cluster.broker_id],
                                high_watermark: leo,
                                log_end_offset: leo,
                                leader_epoch: epoch,
                            },
                        }
                    })
                    .collect();
                proto::TopicInfo {
                    name: m.name.clone(),
                    partition_count: m.partition_count,
                    replication_factor: m.replication_factor,
                    partitions,
                }
            })
            .collect();

        Ok(Response::new(proto::GetMetadataResponse {
            topics,
            error: None,
        }))
    }

    async fn replicate_fetch(
        &self,
        request: Request<proto::ReplicateFetchRequest>,
    ) -> Result<Response<proto::ReplicateFetchResponse>, Status> {
        let req = request.into_inner();
        let current_epoch = self.replica_manager.leader_epoch(&req.topic, req.partition);

        if !self.replica_manager.is_leader(&req.topic, req.partition) {
            return Ok(Response::new(proto::ReplicateFetchResponse {
                records: vec![],
                leader_leo: 0,
                high_watermark: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::NotLeaderForPartition.into(),
                    message: "not leader for this partition".into(),
                }),
                leader_epoch: current_epoch,
            }));
        }

        if req.leader_epoch != 0 && req.leader_epoch != current_epoch {
            return Ok(Response::new(proto::ReplicateFetchResponse {
                records: vec![],
                leader_leo: 0,
                high_watermark: 0,
                error: Some(proto::Error {
                    code: proto::ErrorCode::NotLeaderForPartition.into(),
                    message: format!(
                        "epoch mismatch: request={} current={}",
                        req.leader_epoch, current_epoch
                    ),
                }),
                leader_epoch: current_epoch,
            }));
        }

        let topic = match self.store.topic(&req.topic) {
            Some(t) => t,
            None => {
                return Ok(Response::new(proto::ReplicateFetchResponse {
                    records: vec![],
                    leader_leo: 0,
                    high_watermark: 0,
                    error: Some(proto::Error {
                        code: proto::ErrorCode::UnknownTopic.into(),
                        message: format!("unknown topic: {}", req.topic),
                    }),
                    leader_epoch: current_epoch,
                }));
            }
        };

        let log_lock = match topic.partition(req.partition) {
            Some(l) => l,
            None => {
                return Ok(Response::new(proto::ReplicateFetchResponse {
                    records: vec![],
                    leader_leo: 0,
                    high_watermark: 0,
                    error: Some(proto::Error {
                        code: proto::ErrorCode::UnknownPartition.into(),
                        message: format!("partition {} not found", req.partition),
                    }),
                    leader_epoch: current_epoch,
                }));
            }
        };

        let log = log_lock.read().unwrap();
        let leader_leo = log.latest_offset();
        let records: Vec<chronicle_storage::Record> =
            log.read(req.fetch_offset, 1000).unwrap_or_default();
        drop(log);

        self.replica_manager.update_follower_progress(
            &req.topic,
            req.partition,
            req.broker_id,
            req.fetch_offset,
        );

        let hwm = self
            .replica_manager
            .high_watermark(&req.topic, req.partition);
        let proto_records = records
            .into_iter()
            .map(|r| proto::Record {
                offset: r.offset,
                timestamp_ms: r.timestamp_ms,
                key: r.key.to_vec(),
                value: r.value.to_vec(),
            })
            .collect();

        Ok(Response::new(proto::ReplicateFetchResponse {
            records: proto_records,
            leader_leo,
            high_watermark: hwm,
            error: None,
            leader_epoch: current_epoch,
        }))
    }

    async fn create_replica(
        &self,
        request: Request<proto::CreateReplicaRequest>,
    ) -> Result<Response<proto::CreateReplicaResponse>, Status> {
        let req = request.into_inner();

        let storage_assignments: Vec<PartitionAssignment> = req
            .assignments
            .iter()
            .map(|a| PartitionAssignment {
                partition_id: a.partition_id,
                replicas: a.replica_ids.clone(),
            })
            .collect();

        match self.store.create_topic(
            &req.topic,
            req.partition_count,
            req.replication_factor,
            &storage_assignments,
            &req.local_partitions,
        ) {
            Ok(()) => {}
            Err(e) => {
                return Ok(Response::new(proto::CreateReplicaResponse {
                    error: Some(storage_err_to_proto(&e)),
                }));
            }
        }

        self.replica_manager
            .register_topic(&req.topic, &storage_assignments);

        self.spawn_follower_fetchers(&req.topic, &storage_assignments);

        Ok(Response::new(proto::CreateReplicaResponse { error: None }))
    }

    async fn join_group(
        &self,
        _request: Request<proto::JoinGroupRequest>,
    ) -> Result<Response<proto::JoinGroupResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn leave_group(
        &self,
        _request: Request<proto::LeaveGroupRequest>,
    ) -> Result<Response<proto::LeaveGroupResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn consumer_heartbeat(
        &self,
        _request: Request<proto::ConsumerHeartbeatRequest>,
    ) -> Result<Response<proto::ConsumerHeartbeatResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn commit_offset(
        &self,
        _request: Request<proto::CommitOffsetRequest>,
    ) -> Result<Response<proto::CommitOffsetResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn fetch_offsets(
        &self,
        _request: Request<proto::FetchOffsetsRequest>,
    ) -> Result<Response<proto::FetchOffsetsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn list_groups(
        &self,
        _request: Request<proto::ListGroupsRequest>,
    ) -> Result<Response<proto::ListGroupsResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }

    async fn describe_group(
        &self,
        _request: Request<proto::DescribeGroupRequest>,
    ) -> Result<Response<proto::DescribeGroupResponse>, Status> {
        Err(Status::unimplemented("not yet implemented"))
    }
}

impl ChronicleService {
    fn spawn_follower_fetchers(&self, topic: &str, assignments: &[PartitionAssignment]) {
        for a in assignments {
            if a.replicas.is_empty() {
                continue;
            }
            let leader_id = a.replicas[0];
            if leader_id == self.cluster.broker_id {
                continue;
            }
            if !a.replicas.contains(&self.cluster.broker_id) {
                continue;
            }
            if let Some(addr) = self.cluster.broker_addr(leader_id) {
                FollowerFetcher {
                    broker_id: self.cluster.broker_id,
                    topic: topic.to_string(),
                    partition: a.partition_id,
                    leader_addr: addr.to_string(),
                    store: self.store.clone(),
                    replica_manager: self.replica_manager.clone(),
                    fetch_interval: Duration::from_millis(100),
                    cancel: tokio_util::sync::CancellationToken::new(),
                }
                .spawn();
            }
        }
    }
}

fn route_partition(key: &[u8], partition_count: u32, counter: &AtomicU32) -> u32 {
    if key.is_empty() {
        counter.fetch_add(1, Ordering::Relaxed) % partition_count
    } else {
        crc32fast::hash(key) % partition_count
    }
}

fn storage_err_to_proto(e: &StorageError) -> proto::Error {
    match e {
        StorageError::OffsetOutOfRange { .. } => proto::Error {
            code: proto::ErrorCode::OffsetOutOfRange.into(),
            message: e.to_string(),
        },
        StorageError::UnknownTopic { .. } => proto::Error {
            code: proto::ErrorCode::UnknownTopic.into(),
            message: e.to_string(),
        },
        StorageError::UnknownPartition { .. } => proto::Error {
            code: proto::ErrorCode::UnknownPartition.into(),
            message: e.to_string(),
        },
        StorageError::TopicAlreadyExists { .. } => proto::Error {
            code: proto::ErrorCode::TopicAlreadyExists.into(),
            message: e.to_string(),
        },
        _ => proto::Error {
            code: proto::ErrorCode::InternalError.into(),
            message: e.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_routing_is_deterministic() {
        let counter = AtomicU32::new(0);
        let a = route_partition(b"order-123", 4, &counter);
        let b = route_partition(b"order-123", 4, &counter);
        assert_eq!(a, b);
        assert!(a < 4);
    }

    #[test]
    fn empty_key_round_robins() {
        let counter = AtomicU32::new(0);
        let partitions: Vec<u32> = (0..4).map(|_| route_partition(b"", 4, &counter)).collect();
        assert_eq!(partitions, vec![0, 1, 2, 3]);
    }

    #[test]
    fn key_routing_respects_partition_count() {
        let counter = AtomicU32::new(0);
        for key in [b"a".as_slice(), b"b", b"c", b"xyz", b"hello"] {
            let p = route_partition(key, 3, &counter);
            assert!(p < 3, "partition {p} out of range for count 3");
        }
    }
}

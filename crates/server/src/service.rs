use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use chronicle_storage::{StorageError, TopicStore};
use tonic::{Request, Response, Status};

use crate::proto;

pub struct ChronicleService {
    store: Arc<TopicStore>,
    round_robin: AtomicU32,
}

impl ChronicleService {
    pub fn new(store: Arc<TopicStore>) -> Self {
        Self {
            store,
            round_robin: AtomicU32::new(0),
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
                    }));
                }
                p
            }
            None => route_partition(&req.key, topic.partition_count(), &self.round_robin),
        };

        let log_lock = topic.partition(pid).unwrap();
        let mut log = log_lock.write().unwrap();
        match log.append(&req.key, &req.value) {
            Ok(offset) => Ok(Response::new(proto::ProduceResponse {
                offset,
                partition: pid,
                error: None,
            })),
            Err(e) => Ok(Response::new(proto::ProduceResponse {
                offset: 0,
                partition: pid,
                error: Some(storage_err_to_proto(&e)),
            })),
        }
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
            }));
        }

        let max_records = if req.max_records == 0 {
            100
        } else {
            req.max_records
        };

        let log_lock = topic.partition(req.partition).unwrap();
        let log = log_lock.read().unwrap();
        match log.read(req.offset, max_records) {
            Ok(records) => {
                let high_watermark = log.latest_offset();
                let proto_records = records
                    .into_iter()
                    .map(|r| proto::Record {
                        offset: r.offset,
                        timestamp_ms: r.timestamp_ms,
                        key: r.key.to_vec(),
                        value: r.value.to_vec(),
                    })
                    .collect();
                Ok(Response::new(proto::FetchResponse {
                    records: proto_records,
                    high_watermark,
                    error: None,
                }))
            }
            Err(e) => Ok(Response::new(proto::FetchResponse {
                records: vec![],
                high_watermark: log.latest_offset(),
                error: Some(storage_err_to_proto(&e)),
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

        match self
            .store
            .create_topic(&req.name, req.partition_count, replication_factor)
        {
            Ok(()) => Ok(Response::new(proto::CreateTopicResponse { error: None })),
            Err(e) => Ok(Response::new(proto::CreateTopicResponse {
                error: Some(storage_err_to_proto(&e)),
            })),
        }
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
        let all = self.store.list_topics();

        let topics: Vec<proto::TopicInfo> = if req.topics.is_empty() {
            all.into_iter()
                .map(|m| proto::TopicInfo {
                    name: m.name,
                    partition_count: m.partition_count,
                    replication_factor: m.replication_factor,
                })
                .collect()
        } else {
            all.into_iter()
                .filter(|m| req.topics.contains(&m.name))
                .map(|m| proto::TopicInfo {
                    name: m.name,
                    partition_count: m.partition_count,
                    replication_factor: m.replication_factor,
                })
                .collect()
        };

        Ok(Response::new(proto::GetMetadataResponse {
            topics,
            error: None,
        }))
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

use std::sync::Mutex;

use chronicle_storage::{Log, StorageError};
use tonic::{Request, Response, Status};

use crate::proto;

pub struct ChronicleService {
    log: Mutex<Log>,
}

impl ChronicleService {
    pub fn new(log: Log) -> Self {
        Self {
            log: Mutex::new(log),
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
        let mut log = self.log.lock().unwrap();

        match log.append(&req.key, &req.value) {
            Ok(offset) => Ok(Response::new(proto::ProduceResponse {
                offset,
                error: None,
            })),
            Err(e) => Ok(Response::new(proto::ProduceResponse {
                offset: 0,
                error: Some(storage_err_to_proto(&e)),
            })),
        }
    }

    async fn fetch(
        &self,
        request: Request<proto::FetchRequest>,
    ) -> Result<Response<proto::FetchResponse>, Status> {
        let req = request.into_inner();
        let max_records = if req.max_records == 0 { 100 } else { req.max_records };
        let log = self.log.lock().unwrap();

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
}

fn storage_err_to_proto(e: &StorageError) -> proto::Error {
    match e {
        StorageError::OffsetOutOfRange { .. } => proto::Error {
            code: proto::ErrorCode::OffsetOutOfRange.into(),
            message: e.to_string(),
        },
        _ => proto::Error {
            code: proto::ErrorCode::InternalError.into(),
            message: e.to_string(),
        },
    }
}

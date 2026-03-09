use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::Raft;
use tonic::{Request, Response, Status};

use crate::state_machine::StateMachineStore;
use crate::types::{MetadataRequest, TypeConfig};

mod proto {
    tonic::include_proto!("chronicle");
}

use proto::controller_service_server::ControllerService;
pub use proto::controller_service_server::ControllerServiceServer;

pub struct ControllerGrpcService {
    raft: Raft<TypeConfig>,
    sm: Arc<StateMachineStore>,
}

impl ControllerGrpcService {
    pub fn new(raft: Raft<TypeConfig>, sm: Arc<StateMachineStore>) -> Self {
        Self { raft, sm }
    }
}

#[tonic::async_trait]
impl ControllerService for ControllerGrpcService {
    async fn raft_append_entries(
        &self,
        request: Request<proto::RaftPayload>,
    ) -> Result<Response<proto::RaftPayload>, Status> {
        let req = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .append_entries(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftPayload { data }))
    }

    async fn raft_vote(
        &self,
        request: Request<proto::RaftPayload>,
    ) -> Result<Response<proto::RaftPayload>, Status> {
        let req = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .vote(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftPayload { data }))
    }

    async fn raft_install_snapshot(
        &self,
        request: Request<proto::RaftPayload>,
    ) -> Result<Response<proto::RaftPayload>, Status> {
        let req = serde_json::from_slice(&request.into_inner().data)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let resp = self
            .raft
            .install_snapshot(req)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let data = serde_json::to_vec(&resp).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::RaftPayload { data }))
    }

    async fn register_broker(
        &self,
        request: Request<proto::RegisterBrokerRequest>,
    ) -> Result<Response<proto::RegisterBrokerResponse>, Status> {
        let req = request.into_inner();
        let cmd = MetadataRequest::RegisterBroker {
            id: req.broker_id,
            addr: req.addr,
        };
        match self.raft.client_write(cmd).await {
            Ok(_) => Ok(Response::new(proto::RegisterBrokerResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(proto::RegisterBrokerResponse {
                success: false,
                error: Some(proto::Error {
                    code: proto::ErrorCode::InternalError.into(),
                    message: e.to_string(),
                }),
            })),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let cmd = MetadataRequest::Heartbeat {
            broker_id: req.broker_id,
            timestamp_ms: now_ms,
        };
        match self.raft.client_write(cmd).await {
            Ok(_) => Ok(Response::new(proto::HeartbeatResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(proto::HeartbeatResponse {
                success: false,
                error: Some(proto::Error {
                    code: proto::ErrorCode::InternalError.into(),
                    message: e.to_string(),
                }),
            })),
        }
    }

    async fn propose_command(
        &self,
        request: Request<proto::ProposeCommandRequest>,
    ) -> Result<Response<proto::ProposeCommandResponse>, Status> {
        let req = request.into_inner();
        let cmd: MetadataRequest = serde_json::from_slice(&req.command)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        match self.raft.client_write(cmd).await {
            Ok(resp) => {
                let data =
                    serde_json::to_vec(&resp.data).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(proto::ProposeCommandResponse {
                    response: data,
                    error: None,
                }))
            }
            Err(e) => Ok(Response::new(proto::ProposeCommandResponse {
                response: vec![],
                error: Some(proto::Error {
                    code: proto::ErrorCode::InternalError.into(),
                    message: e.to_string(),
                }),
            })),
        }
    }

    async fn fetch_cluster_state(
        &self,
        _request: Request<proto::FetchClusterStateRequest>,
    ) -> Result<Response<proto::FetchClusterStateResponse>, Status> {
        let state = self.sm.cluster_state().await;
        let data = serde_json::to_vec(&state).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(proto::FetchClusterStateResponse {
            state: data,
            error: None,
        }))
    }
}

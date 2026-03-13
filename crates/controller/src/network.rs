use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::types::{NodeId, TypeConfig};

mod proto {
    tonic::include_proto!("chronicle");
}

use proto::controller_service_client::ControllerServiceClient;

pub struct ControllerNetwork;

impl RaftNetworkFactory<TypeConfig> for ControllerNetwork {
    type Network = ControllerNetworkConn;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        ControllerNetworkConn {
            addr: node.addr.clone(),
        }
    }
}

pub struct ControllerNetworkConn {
    addr: String,
}

impl ControllerNetworkConn {
    async fn client(
        &self,
    ) -> Result<
        ControllerServiceClient<tonic::transport::Channel>,
        RPCError<NodeId, BasicNode, RaftError<NodeId>>,
    > {
        ControllerServiceClient::connect(self.addr.clone())
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))
    }
}

impl RaftNetwork<TypeConfig> for ControllerNetworkConn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let data =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = self.client().await?;
        let resp = client
            .raft_append_entries(proto::RaftPayload { data })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        serde_json::from_slice(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let data =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = ControllerServiceClient::connect(self.addr.clone())
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = client
            .raft_install_snapshot(proto::RaftPayload { data })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        serde_json::from_slice(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let data =
            serde_json::to_vec(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        let mut client = self.client().await?;
        let resp = client
            .raft_vote(proto::RaftPayload { data })
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        serde_json::from_slice(&resp.into_inner().data)
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

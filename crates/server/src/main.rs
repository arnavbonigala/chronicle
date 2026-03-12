mod metadata_reactor;
mod service;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chronicle_controller::{
    BasicNode, Controller, ControllerGrpcService, ControllerNetwork, ControllerServiceServer,
    LogStore, MetadataRequest, Raft, RaftConfig, StateMachineStore, TypeConfig,
};
use chronicle_replication::{BrokerInfo, ClusterConfig, ReplicaManager};
use chronicle_storage::{StorageConfig, TopicStore};
use clap::Parser;
use tonic::transport::Server;

pub mod proto {
    tonic::include_proto!("chronicle");
}

#[derive(Parser)]
#[command(name = "chronicle-server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:9092")]
    listen_addr: String,

    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    segment_max_bytes: u64,

    #[arg(long, default_value_t = 0)]
    broker_id: u32,

    /// Peers in format "id=addr", e.g. "2=http://127.0.0.1:9093,3=http://127.0.0.1:9094"
    #[arg(long, value_delimiter = ',')]
    peers: Vec<String>,
}

fn parse_cluster_config(broker_id: u32, listen_addr: &str, peers: &[String]) -> ClusterConfig {
    let mut brokers = vec![BrokerInfo {
        id: broker_id,
        addr: format!("http://{listen_addr}"),
    }];
    for peer in peers {
        if let Some((id_str, addr)) = peer.split_once('=') {
            if let Ok(id) = id_str.parse::<u32>() {
                let addr = if addr.starts_with("http") {
                    addr.to_string()
                } else {
                    format!("http://{addr}")
                };
                brokers.push(BrokerInfo { id, addr });
            }
        }
    }
    ClusterConfig { broker_id, brokers }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let args = Args::parse();

    let config = StorageConfig {
        data_dir: args.data_dir.clone(),
        segment_max_bytes: args.segment_max_bytes,
    };

    let cluster = parse_cluster_config(args.broker_id, &args.listen_addr, &args.peers);
    let store = Arc::new(TopicStore::open(config)?);
    let replica_manager = Arc::new(ReplicaManager::new(args.broker_id, store.clone()));

    if cluster.is_single_broker() {
        return run_single_broker(args, cluster, store, replica_manager).await;
    }

    run_multi_broker(args, cluster, store, replica_manager).await
}

async fn run_single_broker(
    args: Args,
    cluster: ClusterConfig,
    store: Arc<TopicStore>,
    replica_manager: Arc<ReplicaManager>,
) -> anyhow::Result<()> {
    for meta in store.list_topics() {
        let topic = store.topic(&meta.name).unwrap();
        let assignments = topic.assignments();
        if !assignments.is_empty() {
            replica_manager.register_topic(&meta.name, assignments);
        }
    }

    let rm = replica_manager.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            rm.check_isr_expiry(Duration::from_secs(10));
        }
    });

    tracing::info!(
        addr = %args.listen_addr,
        broker_id = args.broker_id,
        "starting chronicle server (single-broker mode)"
    );

    let addr = args.listen_addr.parse()?;
    let svc = service::ChronicleService::new(store, replica_manager, cluster, None);

    Server::builder()
        .add_service(proto::chronicle_server::ChronicleServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

async fn run_multi_broker(
    args: Args,
    cluster: ClusterConfig,
    store: Arc<TopicStore>,
    replica_manager: Arc<ReplicaManager>,
) -> anyhow::Result<()> {
    let node_id = args.broker_id as u64;

    let raft_config = Arc::new(
        RaftConfig {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            ..RaftConfig::default()
        }
        .validate()?,
    );

    let (sm_store, change_rx) = StateMachineStore::new();
    let log_store = LogStore::new();
    let network = ControllerNetwork;

    let raft =
        Raft::<TypeConfig>::new(node_id, raft_config, network, log_store, sm_store.clone()).await?;

    let broker_ids = cluster.broker_ids();
    let lowest_id = *broker_ids.first().unwrap();
    if args.broker_id == lowest_id {
        let members: BTreeMap<u64, BasicNode> = cluster
            .brokers
            .iter()
            .map(|b| {
                (
                    b.id as u64,
                    BasicNode {
                        addr: b.addr.clone(),
                    },
                )
            })
            .collect();
        tracing::info!("initializing raft cluster");
        raft.initialize(members).await?;
    }

    let controller = Arc::new(Controller::new(
        raft.clone(),
        sm_store.clone(),
        args.broker_id,
    ));

    let controller_grpc = ControllerGrpcService::new(raft.clone(), sm_store.clone());

    tracing::info!(
        addr = %args.listen_addr,
        broker_id = args.broker_id,
        peers = args.peers.len(),
        "starting chronicle server (multi-broker mode)"
    );

    let addr = args.listen_addr.parse()?;
    let svc = service::ChronicleService::new(
        store.clone(),
        replica_manager.clone(),
        cluster.clone(),
        Some(controller.clone()),
    );

    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(proto::chronicle_server::ChronicleServer::new(svc))
            .add_service(ControllerServiceServer::new(controller_grpc))
            .serve(addr)
            .await
    });

    tracing::info!("waiting for raft leader election...");
    loop {
        if raft.current_leader().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    tracing::info!(leader = ?raft.current_leader().await, "raft leader elected");

    let self_addr = cluster
        .broker_addr(args.broker_id)
        .unwrap_or_default()
        .to_string();

    if controller.is_leader().await {
        controller
            .propose(MetadataRequest::RegisterBroker {
                id: args.broker_id,
                addr: self_addr,
            })
            .await
            .map_err(|e| anyhow::anyhow!("failed to register broker: {e}"))?;
    } else {
        let leader_id = raft.current_leader().await.unwrap() as u32;
        let leader_addr = cluster
            .broker_addr(leader_id)
            .ok_or_else(|| anyhow::anyhow!("leader {leader_id} addr not in cluster config"))?;
        let mut ctrl_client =
            proto::controller_service_client::ControllerServiceClient::connect(leader_addr.to_string())
                .await
                .map_err(|e| anyhow::anyhow!("connect to leader for registration: {e}"))?;
        let resp = ctrl_client
            .register_broker(proto::RegisterBrokerRequest {
                broker_id: args.broker_id,
                addr: self_addr,
            })
            .await
            .map_err(|e| anyhow::anyhow!("register_broker rpc: {e}"))?
            .into_inner();
        if !resp.success {
            anyhow::bail!("leader rejected registration: {:?}", resp.error);
        }
    }

    let reactor = metadata_reactor::MetadataReactor::new(
        args.broker_id,
        store.clone(),
        replica_manager.clone(),
        cluster.clone(),
    );
    tokio::spawn(reactor.run(change_rx));

    let hb_controller = controller.clone();
    let hb_broker_id = args.broker_id;
    let hb_cluster = cluster.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            if hb_controller.is_leader().await {
                if let Err(e) = hb_controller
                    .propose(MetadataRequest::Heartbeat {
                        broker_id: hb_broker_id,
                        timestamp_ms: now_ms,
                    })
                    .await
                {
                    tracing::debug!(error = %e, "heartbeat propose failed");
                }
            } else if let Some(leader_id) = hb_controller.raft().current_leader().await {
                let leader_id = leader_id as u32;
                if let Some(addr) = hb_cluster.broker_addr(leader_id) {
                    match proto::controller_service_client::ControllerServiceClient::connect(
                        addr.to_string(),
                    )
                    .await
                    {
                        Ok(mut client) => {
                            if let Err(e) = client
                                .heartbeat(proto::HeartbeatRequest {
                                    broker_id: hb_broker_id,
                                })
                                .await
                            {
                                tracing::debug!(error = %e, "heartbeat rpc failed");
                            }
                        }
                        Err(e) => {
                            tracing::debug!(error = %e, "connect to leader for heartbeat failed");
                        }
                    }
                }
            }
        }
    });

    let checker_controller = controller.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            if checker_controller.is_leader().await {
                checker_controller.check_heartbeats(15_000).await;
                checker_controller.check_consumer_heartbeats(10_000).await;
            }
        }
    });

    let rm = replica_manager.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            rm.check_isr_expiry(Duration::from_secs(10));
        }
    });

    server_handle
        .await
        .map_err(|e| anyhow::anyhow!("server task failed: {e}"))??;

    Ok(())
}

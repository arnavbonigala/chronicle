mod service;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chronicle_replication::{BrokerInfo, ClusterConfig, FollowerFetcher, ReplicaManager};
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
        data_dir: args.data_dir,
        segment_max_bytes: args.segment_max_bytes,
    };

    let cluster = parse_cluster_config(args.broker_id, &args.listen_addr, &args.peers);
    let store = Arc::new(TopicStore::open(config)?);
    let replica_manager = Arc::new(ReplicaManager::new(args.broker_id, store.clone()));

    for meta in store.list_topics() {
        let topic = store.topic(&meta.name).unwrap();
        let assignments = topic.assignments();
        if !assignments.is_empty() {
            replica_manager.register_topic(&meta.name, assignments);
        }
    }

    for (topic, partition, leader_id) in replica_manager.followed_partitions() {
        if let Some(addr) = cluster.broker_addr(leader_id) {
            FollowerFetcher {
                broker_id: args.broker_id,
                topic,
                partition,
                leader_addr: addr.to_string(),
                store: store.clone(),
                replica_manager: replica_manager.clone(),
                fetch_interval: Duration::from_millis(100),
            }
            .spawn();
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
        peers = args.peers.len(),
        "starting chronicle server"
    );

    let addr = args.listen_addr.parse()?;
    let svc = service::ChronicleService::new(store, replica_manager, cluster);

    Server::builder()
        .add_service(proto::chronicle_server::ChronicleServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

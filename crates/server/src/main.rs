mod service;

use std::path::PathBuf;
use std::sync::Arc;

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

    let store = Arc::new(TopicStore::open(config)?);
    tracing::info!(addr = %args.listen_addr, "starting chronicle server");

    let addr = args.listen_addr.parse()?;
    let svc = service::ChronicleService::new(store);

    Server::builder()
        .add_service(proto::chronicle_server::ChronicleServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

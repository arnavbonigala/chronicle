use clap::{Parser, Subcommand};
use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("chronicle");
}

use proto::chronicle_client::ChronicleClient;

#[derive(Parser)]
#[command(name = "chronicle-cli")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Produce {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: Option<u32>,
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long)]
        value: String,
    },
    Consume {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: u32,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        max_records: u32,
        #[arg(long, default_value_t = false)]
        follow: bool,
    },
    CreateTopic {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        partitions: u32,
        #[arg(long, default_value_t = 1)]
        replication_factor: u32,
    },
    DeleteTopic {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        name: String,
    },
    ListTopics {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Produce {
            server,
            topic,
            partition,
            key,
            value,
        } => {
            let mut client = connect(&server).await?;
            let resp = client
                .produce(proto::ProduceRequest {
                    topic,
                    partition,
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    acks: proto::Acks::Leader.into(),
                })
                .await?
                .into_inner();

            check_error(&resp.error);
            println!("partition={} offset={}", resp.partition, resp.offset);
        }
        Command::Consume {
            server,
            topic,
            partition,
            offset,
            max_records,
            follow,
        } => {
            let mut client = connect(&server).await?;
            let mut current_offset = offset;

            loop {
                let resp = client
                    .fetch(proto::FetchRequest {
                        topic: topic.clone(),
                        partition,
                        offset: current_offset,
                        max_records,
                    })
                    .await?
                    .into_inner();

                check_error(&resp.error);

                for record in &resp.records {
                    let key_str = String::from_utf8_lossy(&record.key);
                    let value_str = String::from_utf8_lossy(&record.value);
                    println!(
                        "offset={} timestamp={} key={} value={}",
                        record.offset, record.timestamp_ms, key_str, value_str
                    );
                    current_offset = record.offset + 1;
                }

                if !follow {
                    break;
                }

                if resp.records.is_empty() {
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        Command::CreateTopic {
            server,
            name,
            partitions,
            replication_factor,
        } => {
            let mut client = connect(&server).await?;
            let resp = client
                .create_topic(proto::CreateTopicRequest {
                    name: name.clone(),
                    partition_count: partitions,
                    replication_factor,
                })
                .await?
                .into_inner();

            check_error(&resp.error);
            println!("created topic {} with {} partition(s)", name, partitions);
        }
        Command::DeleteTopic { server, name } => {
            let mut client = connect(&server).await?;
            let resp = client
                .delete_topic(proto::DeleteTopicRequest { name: name.clone() })
                .await?
                .into_inner();

            check_error(&resp.error);
            println!("deleted topic {}", name);
        }
        Command::ListTopics { server } => {
            let mut client = connect(&server).await?;
            let resp = client
                .get_metadata(proto::GetMetadataRequest { topics: vec![] })
                .await?
                .into_inner();

            check_error(&resp.error);

            if resp.topics.is_empty() {
                println!("no topics");
            } else {
                println!("{:<20} {:>10} {:>12}", "TOPIC", "PARTITIONS", "REPLICATION");
                for t in &resp.topics {
                    println!(
                        "{:<20} {:>10} {:>12}",
                        t.name, t.partition_count, t.replication_factor
                    );
                }
            }
        }
    }

    Ok(())
}

fn check_error(error: &Option<proto::Error>) {
    if let Some(err) = error {
        if err.code != proto::ErrorCode::None as i32 {
            eprintln!("error: {}", err.message);
            std::process::exit(1);
        }
    }
}

async fn connect(addr: &str) -> Result<ChronicleClient<Channel>, tonic::transport::Error> {
    ChronicleClient::connect(addr.to_string()).await
}

use clap::{Parser, Subcommand, ValueEnum};
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

#[derive(Clone, ValueEnum)]
enum AcksArg {
    None,
    Leader,
    All,
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
        #[arg(long, default_value = "leader")]
        acks: AcksArg,
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

fn acks_to_proto(a: &AcksArg) -> i32 {
    match a {
        AcksArg::None => proto::Acks::None.into(),
        AcksArg::Leader => proto::Acks::Leader.into(),
        AcksArg::All => proto::Acks::All.into(),
    }
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
            acks,
        } => {
            let mut client = connect(&server).await?;
            let resp = client
                .produce(proto::ProduceRequest {
                    topic,
                    partition,
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    acks: acks_to_proto(&acks),
                })
                .await?
                .into_inner();

            check_error(&resp.error, resp.leader_broker_id);
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

                check_error(&resp.error, resp.leader_broker_id);

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

            check_error(&resp.error, None);
            println!(
                "created topic {} with {} partition(s), rf={}",
                name, partitions, replication_factor
            );

            let meta = client
                .get_metadata(proto::GetMetadataRequest { topics: vec![name] })
                .await?
                .into_inner();
            for t in &meta.topics {
                print_partition_table(t);
            }
        }
        Command::DeleteTopic { server, name } => {
            let mut client = connect(&server).await?;
            let resp = client
                .delete_topic(proto::DeleteTopicRequest { name: name.clone() })
                .await?
                .into_inner();

            check_error(&resp.error, None);
            println!("deleted topic {}", name);
        }
        Command::ListTopics { server } => {
            let mut client = connect(&server).await?;
            let resp = client
                .get_metadata(proto::GetMetadataRequest { topics: vec![] })
                .await?
                .into_inner();

            check_error(&resp.error, None);

            if resp.topics.is_empty() {
                println!("no topics");
            } else {
                for t in &resp.topics {
                    println!(
                        "topic={} partitions={} rf={}",
                        t.name, t.partition_count, t.replication_factor
                    );
                    print_partition_table(t);
                    println!();
                }
            }
        }
    }

    Ok(())
}

fn print_partition_table(t: &proto::TopicInfo) {
    if t.partitions.is_empty() {
        return;
    }
    println!(
        "  {:<10} {:>8} {:>20} {:>20} {:>6} {:>6}",
        "PARTITION", "LEADER", "REPLICAS", "ISR", "HWM", "LEO"
    );
    for p in &t.partitions {
        let replicas: Vec<String> = p.replica_broker_ids.iter().map(|r| r.to_string()).collect();
        let isr: Vec<String> = p.isr_broker_ids.iter().map(|r| r.to_string()).collect();
        println!(
            "  {:<10} {:>8} {:>20} {:>20} {:>6} {:>6}",
            p.partition_id,
            p.leader_broker_id,
            replicas.join(","),
            isr.join(","),
            p.high_watermark,
            p.log_end_offset,
        );
    }
}

fn check_error(error: &Option<proto::Error>, leader_hint: Option<u32>) {
    if let Some(err) = error {
        if err.code != proto::ErrorCode::None as i32 {
            eprintln!("error: {}", err.message);
            if err.code == proto::ErrorCode::NotLeaderForPartition as i32 {
                if let Some(leader_id) = leader_hint {
                    eprintln!("hint: leader is broker {}", leader_id);
                }
            }
            std::process::exit(1);
        }
    }
}

async fn connect(addr: &str) -> Result<ChronicleClient<Channel>, tonic::transport::Error> {
    ChronicleClient::connect(addr.to_string()).await
}

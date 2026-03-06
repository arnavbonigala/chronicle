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
        #[arg(long, default_value = "")]
        key: String,
        #[arg(long)]
        value: String,
    },
    Consume {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 100)]
        max_records: u32,
        #[arg(long, default_value_t = false)]
        follow: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Produce { server, key, value } => {
            let mut client = connect(&server).await?;
            let resp = client
                .produce(proto::ProduceRequest {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                })
                .await?
                .into_inner();

            if let Some(err) = resp.error {
                if err.code != proto::ErrorCode::None as i32 {
                    eprintln!("error: {}", err.message);
                    std::process::exit(1);
                }
            }
            println!("offset={}", resp.offset);
        }
        Command::Consume {
            server,
            offset,
            max_records,
            follow,
        } => {
            let mut client = connect(&server).await?;
            let mut current_offset = offset;

            loop {
                let resp = client
                    .fetch(proto::FetchRequest {
                        offset: current_offset,
                        max_records,
                    })
                    .await?
                    .into_inner();

                if let Some(err) = &resp.error {
                    if err.code != proto::ErrorCode::None as i32 {
                        eprintln!("error: {}", err.message);
                        std::process::exit(1);
                    }
                }

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
    }

    Ok(())
}

async fn connect(addr: &str) -> Result<ChronicleClient<Channel>, tonic::transport::Error> {
    ChronicleClient::connect(addr.to_string()).await
}

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
        #[arg(long)]
        from_timestamp: Option<u64>,
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
    ConsumeGroup {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        group: String,
        #[arg(long, value_delimiter = ',')]
        topics: Vec<String>,
        #[arg(long)]
        member_id: Option<String>,
        #[arg(long, default_value_t = 10000)]
        session_timeout_ms: u32,
        #[arg(long, default_value_t = 5000)]
        auto_commit_interval_ms: u64,
    },
    CommitOffset {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        offset: u64,
    },
    FetchOffsets {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        group: String,
    },
    ListGroups {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
    },
    DescribeGroup {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        group: String,
    },
    OffsetForTimestamp {
        #[arg(long, default_value = "http://127.0.0.1:9092")]
        server: String,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        timestamp: u64,
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
                    producer_id: 0,
                    producer_epoch: 0,
                    first_sequence: 0,
                    headers: vec![],
                    is_transactional: false,
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
            from_timestamp,
        } => {
            let mut client = connect(&server).await?;
            let mut current_offset = if let Some(ts) = from_timestamp {
                let resp = client
                    .offset_for_timestamp(proto::OffsetForTimestampRequest {
                        topic: topic.clone(),
                        partition,
                        timestamp_ms: ts,
                    })
                    .await?
                    .into_inner();
                check_error(&resp.error, None);
                if resp.found {
                    println!("resolved timestamp {} to offset {}", ts, resp.offset);
                    resp.offset
                } else {
                    eprintln!("no records found at or after timestamp {}", ts);
                    return Ok(());
                }
            } else {
                offset
            };

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
        Command::ConsumeGroup {
            server,
            group,
            topics,
            member_id,
            session_timeout_ms,
            auto_commit_interval_ms,
        } => {
            run_consume_group(
                &server,
                &group,
                &topics,
                member_id,
                session_timeout_ms,
                auto_commit_interval_ms,
            )
            .await?;
        }
        Command::CommitOffset {
            server,
            group,
            topic,
            partition,
            offset,
        } => {
            let mut client = connect(&server).await?;
            let resp = client
                .commit_offset(proto::CommitOffsetRequest {
                    group_id: group,
                    offsets: vec![proto::TopicPartitionOffset {
                        topic,
                        partition,
                        offset,
                    }],
                })
                .await?
                .into_inner();
            check_error(&resp.error, None);
            println!("offset committed");
        }
        Command::FetchOffsets { server, group } => {
            let mut client = connect(&server).await?;
            let resp = client
                .fetch_offsets(proto::FetchOffsetsRequest {
                    group_id: group,
                    partitions: vec![],
                })
                .await?
                .into_inner();
            check_error(&resp.error, None);
            if resp.offsets.is_empty() {
                println!("no committed offsets");
            } else {
                println!("{:<20} {:>10} {:>10}", "TOPIC", "PARTITION", "OFFSET");
                for o in &resp.offsets {
                    println!("{:<20} {:>10} {:>10}", o.topic, o.partition, o.offset);
                }
            }
        }
        Command::ListGroups { server } => {
            let mut client = connect(&server).await?;
            let resp = client
                .list_groups(proto::ListGroupsRequest {})
                .await?
                .into_inner();
            check_error(&resp.error, None);
            if resp.group_ids.is_empty() {
                println!("no consumer groups");
            } else {
                for gid in &resp.group_ids {
                    println!("{gid}");
                }
            }
        }
        Command::DescribeGroup { server, group } => {
            let mut client = connect(&server).await?;
            let resp = client
                .describe_group(proto::DescribeGroupRequest { group_id: group })
                .await?
                .into_inner();
            check_error(&resp.error, None);
            println!("group={} generation={}", resp.group_id, resp.generation_id);
            if !resp.members.is_empty() {
                println!("\nMembers:");
                for m in &resp.members {
                    let subs = m.subscriptions.join(",");
                    let assigns: Vec<String> = m
                        .assignments
                        .iter()
                        .map(|a| format!("{}/{}", a.topic, a.partition))
                        .collect();
                    println!(
                        "  member={} subscriptions=[{}] assignments=[{}]",
                        m.member_id,
                        subs,
                        assigns.join(", ")
                    );
                }
            }
            if !resp.committed_offsets.is_empty() {
                println!("\nCommitted offsets:");
                println!("  {:<20} {:>10} {:>10}", "TOPIC", "PARTITION", "OFFSET");
                for o in &resp.committed_offsets {
                    println!("  {:<20} {:>10} {:>10}", o.topic, o.partition, o.offset);
                }
            }
        }
        Command::OffsetForTimestamp {
            server,
            topic,
            partition,
            timestamp,
        } => {
            let mut client = connect(&server).await?;
            let resp = client
                .offset_for_timestamp(proto::OffsetForTimestampRequest {
                    topic,
                    partition,
                    timestamp_ms: timestamp,
                })
                .await?
                .into_inner();
            check_error(&resp.error, None);
            if resp.found {
                println!("offset={}", resp.offset);
            } else {
                println!("no offset found for timestamp {}", timestamp);
            }
        }
    }

    Ok(())
}

async fn run_consume_group(
    server: &str,
    group: &str,
    topics: &[String],
    member_id_arg: Option<String>,
    session_timeout_ms: u32,
    auto_commit_interval_ms: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let member_id = member_id_arg.unwrap_or_else(|| {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        format!("consumer-{}-{}", std::process::id(), ts)
    });
    let mut client = connect(server).await?;
    let heartbeat_interval = Duration::from_millis(session_timeout_ms as u64 / 3);
    let commit_interval = Duration::from_millis(auto_commit_interval_ms);

    println!("member_id={member_id}");

    loop {
        let join_resp = client
            .join_group(proto::JoinGroupRequest {
                group_id: group.to_string(),
                member_id: member_id.clone(),
                topics: topics.to_vec(),
                session_timeout_ms,
            })
            .await?
            .into_inner();
        check_error(&join_resp.error, None);

        let generation_id = join_resp.generation_id;
        let assignments = join_resp.assignments;
        println!(
            "joined group={group} generation={generation_id} assigned={} partition(s)",
            assignments.len()
        );
        for a in &assignments {
            println!("  {}/{}", a.topic, a.partition);
        }

        if assignments.is_empty() {
            println!("no partitions assigned, waiting...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        let fetch_offsets_resp = client
            .fetch_offsets(proto::FetchOffsetsRequest {
                group_id: group.to_string(),
                partitions: assignments.clone(),
            })
            .await?
            .into_inner();

        let mut offsets: HashMap<(String, u32), u64> = HashMap::new();
        for o in &fetch_offsets_resp.offsets {
            offsets.insert((o.topic.clone(), o.partition), o.offset);
        }
        for a in &assignments {
            offsets.entry((a.topic.clone(), a.partition)).or_insert(0);
        }

        let mut last_heartbeat = Instant::now();
        let mut last_commit = Instant::now();
        let mut rebalance = false;

        while !rebalance {
            let mut any_records = false;

            for a in &assignments {
                let key = (a.topic.clone(), a.partition);
                let current_offset = *offsets.get(&key).unwrap_or(&0);

                let fetch_resp = client
                    .fetch(proto::FetchRequest {
                        topic: a.topic.clone(),
                        partition: a.partition,
                        offset: current_offset,
                        max_records: 100,
                    })
                    .await?
                    .into_inner();

                if let Some(ref err) = fetch_resp.error {
                    if err.code == proto::ErrorCode::NotLeaderForPartition as i32 {
                        eprintln!(
                            "not leader for {}/{}, triggering rejoin",
                            a.topic, a.partition
                        );
                        rebalance = true;
                        break;
                    }
                }

                for record in &fetch_resp.records {
                    let key_str = String::from_utf8_lossy(&record.key);
                    let value_str = String::from_utf8_lossy(&record.value);
                    println!(
                        "[{}/{}] offset={} timestamp={} key={} value={}",
                        a.topic,
                        a.partition,
                        record.offset,
                        record.timestamp_ms,
                        key_str,
                        value_str
                    );
                    offsets.insert((a.topic.clone(), a.partition), record.offset + 1);
                    any_records = true;
                }
            }

            if last_heartbeat.elapsed() >= heartbeat_interval {
                let hb_resp = client
                    .consumer_heartbeat(proto::ConsumerHeartbeatRequest {
                        group_id: group.to_string(),
                        member_id: member_id.clone(),
                        generation_id,
                    })
                    .await?
                    .into_inner();
                if let Some(ref err) = hb_resp.error {
                    if err.code != proto::ErrorCode::None as i32 {
                        eprintln!("heartbeat error: {}, rejoining", err.message);
                        rebalance = true;
                        continue;
                    }
                }
                if hb_resp.rebalance_required {
                    println!("rebalance required, rejoining group");
                    rebalance = true;
                    continue;
                }
                last_heartbeat = Instant::now();
            }

            if last_commit.elapsed() >= commit_interval {
                let commit_offsets: Vec<proto::TopicPartitionOffset> = offsets
                    .iter()
                    .map(|((t, p), o)| proto::TopicPartitionOffset {
                        topic: t.clone(),
                        partition: *p,
                        offset: *o,
                    })
                    .collect();
                if !commit_offsets.is_empty() {
                    let _ = client
                        .commit_offset(proto::CommitOffsetRequest {
                            group_id: group.to_string(),
                            offsets: commit_offsets,
                        })
                        .await;
                }
                last_commit = Instant::now();
            }

            if !any_records && !rebalance {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        let commit_offsets: Vec<proto::TopicPartitionOffset> = offsets
            .iter()
            .map(|((t, p), o)| proto::TopicPartitionOffset {
                topic: t.clone(),
                partition: *p,
                offset: *o,
            })
            .collect();
        if !commit_offsets.is_empty() {
            let _ = client
                .commit_offset(proto::CommitOffsetRequest {
                    group_id: group.to_string(),
                    offsets: commit_offsets,
                })
                .await;
        }
    }
}

fn print_partition_table(t: &proto::TopicInfo) {
    if t.partitions.is_empty() {
        return;
    }
    println!(
        "  {:<10} {:>8} {:>6} {:>20} {:>20} {:>6} {:>6}",
        "PARTITION", "LEADER", "EPOCH", "REPLICAS", "ISR", "HWM", "LEO"
    );
    for p in &t.partitions {
        let replicas: Vec<String> = p.replica_broker_ids.iter().map(|r| r.to_string()).collect();
        let isr: Vec<String> = p.isr_broker_ids.iter().map(|r| r.to_string()).collect();
        println!(
            "  {:<10} {:>8} {:>6} {:>20} {:>20} {:>6} {:>6}",
            p.partition_id,
            p.leader_broker_id,
            p.leader_epoch,
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

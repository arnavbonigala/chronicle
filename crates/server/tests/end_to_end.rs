use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;
use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("chronicle");
}

type Client = proto::chronicle_client::ChronicleClient<Channel>;

struct ServerProcess {
    addr: String,
    data_dir: PathBuf,
    child: Child,
}

impl ServerProcess {
    fn start_single(broker_id: u32) -> Self {
        let addr = alloc_addr();
        let data_dir = temp_data_dir(broker_id);

        let bin = server_bin();
        let child = Command::new(bin)
            .arg("--listen-addr")
            .arg(&addr)
            .arg("--broker-id")
            .arg(broker_id.to_string())
            .arg("--data-dir")
            .arg(data_dir.to_str().unwrap())
            .spawn()
            .expect("failed to spawn chronicle-server");

        wait_for_server(&addr, Duration::from_secs(10))
            .unwrap_or_else(|| panic!("server on {addr} did not become ready"));
        thread::sleep(Duration::from_millis(200));

        ServerProcess {
            addr,
            data_dir,
            child,
        }
    }

    fn addr(&self) -> &str {
        &self.addr
    }

    fn take_data_dir(&mut self) -> PathBuf {
        std::mem::replace(&mut self.data_dir, PathBuf::new())
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if !self.data_dir.as_os_str().is_empty() {
            let _ = std::fs::remove_dir_all(&self.data_dir);
        }
    }
}

fn server_bin() -> String {
    std::env::var("CARGO_BIN_EXE_chronicle-server").expect("CARGO_BIN_EXE_chronicle-server not set")
}

fn alloc_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr.to_string()
}

fn temp_data_dir(broker_id: u32) -> PathBuf {
    let mut base = std::env::temp_dir();
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos();
    base.push(format!(
        "chronicle-e2e-b{}-{}-{}",
        broker_id,
        ts,
        std::process::id()
    ));
    std::fs::create_dir_all(&base).expect("create temp data dir");
    base
}

fn wait_for_server(addr: &str, timeout: Duration) -> Option<()> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return Some(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    None
}

async fn client_for(server_addr: &str) -> Client {
    Client::connect(format!("http://{}", server_addr))
        .await
        .expect("connect client")
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "{}-{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis(),
        std::process::id(),
    )
}

// ---------------------------------------------------------------------------
// Test 1: Single-broker produce/consume roundtrip
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn single_broker_produce_and_consume_roundtrip() {
    let server = ServerProcess::start_single(1);
    let mut client = client_for(server.addr()).await;

    let topic = unique_topic("e2e-roundtrip");

    let create_resp = client
        .create_topic(proto::CreateTopicRequest {
            name: topic.clone(),
            partition_count: 1,
            replication_factor: 1,
        })
        .await
        .expect("create_topic rpc")
        .into_inner();
    assert!(
        create_resp.error.is_none(),
        "create_topic error: {:?}",
        create_resp.error
    );

    let values: Vec<&str> = vec!["one", "two", "three"];
    for (i, v) in values.iter().enumerate() {
        let resp = client
            .produce(proto::ProduceRequest {
                key: b"key".to_vec(),
                value: v.as_bytes().to_vec(),
                topic: topic.clone(),
                partition: Some(0),
                acks: 0,
                producer_id: 0,
                producer_epoch: 0,
                first_sequence: i as u32,
                headers: vec![],
                is_transactional: false,
            })
            .await
            .expect("produce rpc")
            .into_inner();
        assert!(resp.error.is_none(), "produce error: {:?}", resp.error);
        assert_eq!(resp.partition, 0);
        assert_eq!(resp.offset, i as u64);
    }

    let fetch_resp = client
        .fetch(proto::FetchRequest {
            offset: 0,
            max_records: 10,
            topic: topic.clone(),
            partition: 0,
            isolation_level: proto::IsolationLevel::ReadUncommitted as i32,
        })
        .await
        .expect("fetch rpc")
        .into_inner();
    assert!(
        fetch_resp.error.is_none(),
        "fetch error: {:?}",
        fetch_resp.error
    );
    assert_eq!(fetch_resp.records.len(), values.len());

    for (i, rec) in fetch_resp.records.iter().enumerate() {
        assert_eq!(rec.offset, i as u64);
        assert_eq!(String::from_utf8_lossy(&rec.value), values[i]);
    }
}

// ---------------------------------------------------------------------------
// Test 2: Single-broker durability across restart
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn single_broker_persists_across_restart() {
    let mut server1 = ServerProcess::start_single(1);
    let mut client1 = client_for(server1.addr()).await;

    let topic = unique_topic("e2e-restart");

    let create_resp = client1
        .create_topic(proto::CreateTopicRequest {
            name: topic.clone(),
            partition_count: 1,
            replication_factor: 1,
        })
        .await
        .expect("create_topic rpc")
        .into_inner();
    assert!(
        create_resp.error.is_none(),
        "create_topic error: {:?}",
        create_resp.error
    );

    let values: Vec<&str> = vec!["a", "b", "c"];
    for (i, v) in values.iter().enumerate() {
        let resp = client1
            .produce(proto::ProduceRequest {
                key: b"key".to_vec(),
                value: v.as_bytes().to_vec(),
                topic: topic.clone(),
                partition: Some(0),
                acks: 0,
                producer_id: 0,
                producer_epoch: 0,
                first_sequence: i as u32,
                headers: vec![],
                is_transactional: false,
            })
            .await
            .expect("produce rpc")
            .into_inner();
        assert!(resp.error.is_none(), "produce error: {:?}", resp.error);
    }

    drop(client1);
    let data_dir = server1.take_data_dir();
    drop(server1);

    let addr = alloc_addr();
    let bin = server_bin();
    let mut child = Command::new(bin)
        .arg("--listen-addr")
        .arg(&addr)
        .arg("--broker-id")
        .arg("1")
        .arg("--data-dir")
        .arg(data_dir.to_str().unwrap())
        .spawn()
        .expect("failed to spawn chronicle-server (restart)");

    wait_for_server(&addr, Duration::from_secs(10))
        .unwrap_or_else(|| panic!("restarted server on {addr} did not become ready"));
    thread::sleep(Duration::from_millis(300));

    let mut client2 = client_for(&addr).await;
    let fetch_resp = client2
        .fetch(proto::FetchRequest {
            offset: 0,
            max_records: 10,
            topic: topic.clone(),
            partition: 0,
            isolation_level: proto::IsolationLevel::ReadUncommitted as i32,
        })
        .await
        .expect("fetch rpc after restart")
        .into_inner();
    assert!(
        fetch_resp.error.is_none(),
        "fetch error after restart: {:?}",
        fetch_resp.error
    );
    assert_eq!(fetch_resp.records.len(), values.len());

    for (i, rec) in fetch_resp.records.iter().enumerate() {
        assert_eq!(rec.offset, i as u64);
        assert_eq!(String::from_utf8_lossy(&rec.value), values[i]);
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&data_dir);
}

// ---------------------------------------------------------------------------
// Test 3: Multi-broker replication and failover
// ---------------------------------------------------------------------------

#[ignore] // slow: requires Raft leader election + heartbeat-driven failover
#[tokio::test(flavor = "multi_thread")]
async fn multi_broker_replication_and_failover() {
    let addr1 = alloc_addr();
    let addr2 = alloc_addr();
    let addr3 = alloc_addr();

    eprintln!("[e2e] broker addrs: b1={addr1} b2={addr2} b3={addr3}");

    let data1 = temp_data_dir(10);
    let data2 = temp_data_dir(20);
    let data3 = temp_data_dir(30);

    let bin = server_bin();

    let peers1 = format!("2=http://{},3=http://{}", addr2, addr3);
    let peers2 = format!("1=http://{},3=http://{}", addr1, addr3);
    let peers3 = format!("1=http://{},2=http://{}", addr1, addr2);

    eprintln!("[e2e] spawning 3 brokers…");

    let mut s1 = Command::new(&bin)
        .arg("--listen-addr")
        .arg(&addr1)
        .arg("--broker-id")
        .arg("1")
        .arg("--data-dir")
        .arg(data1.to_str().unwrap())
        .arg("--peers")
        .arg(&peers1)
        .spawn()
        .expect("spawn broker 1");

    let mut s2 = Command::new(&bin)
        .arg("--listen-addr")
        .arg(&addr2)
        .arg("--broker-id")
        .arg("2")
        .arg("--data-dir")
        .arg(data2.to_str().unwrap())
        .arg("--peers")
        .arg(&peers2)
        .spawn()
        .expect("spawn broker 2");

    let mut s3 = Command::new(&bin)
        .arg("--listen-addr")
        .arg(&addr3)
        .arg("--broker-id")
        .arg("3")
        .arg("--data-dir")
        .arg(data3.to_str().unwrap())
        .arg("--peers")
        .arg(&peers3)
        .spawn()
        .expect("spawn broker 3");

    for (a, label) in [
        (&addr1, "broker1"),
        (&addr2, "broker2"),
        (&addr3, "broker3"),
    ] {
        eprintln!("[e2e] waiting for {label} TCP on {a}…");
        wait_for_server(a, Duration::from_secs(20))
            .unwrap_or_else(|| panic!("{label} on {a} not ready"));
        eprintln!("[e2e] {label} TCP ready");
    }

    eprintln!("[e2e] sleeping 8s for raft election + broker registration…");
    sleep(Duration::from_secs(8)).await;

    eprintln!("[e2e] connecting gRPC client to broker1…");
    let mut client = client_for(&addr1).await;
    let topic = unique_topic("e2e-repl");

    eprintln!("[e2e] creating topic {topic}…");
    let create_resp = client
        .create_topic(proto::CreateTopicRequest {
            name: topic.clone(),
            partition_count: 1,
            replication_factor: 3,
        })
        .await
        .expect("create_topic rpc")
        .into_inner();
    assert!(
        create_resp.error.is_none(),
        "create_topic error: {:?}",
        create_resp.error
    );
    eprintln!("[e2e] topic created, sleeping 2s for replication setup…");

    sleep(Duration::from_secs(2)).await;

    let values: Vec<&str> = vec!["x", "y", "z"];
    for (i, v) in values.iter().enumerate() {
        eprintln!("[e2e] producing record {i} value={v} (acks=all)…");
        let resp = client
            .produce(proto::ProduceRequest {
                key: b"k".to_vec(),
                value: v.as_bytes().to_vec(),
                topic: topic.clone(),
                partition: Some(0),
                acks: 2, // Acks::All
                producer_id: 0,
                producer_epoch: 0,
                first_sequence: i as u32,
                headers: vec![],
                is_transactional: false,
            })
            .await
            .expect("produce rpc")
            .into_inner();
        assert!(resp.error.is_none(), "produce error: {:?}", resp.error);
        eprintln!("[e2e] produced record {i} offset={}", resp.offset);
    }

    eprintln!("[e2e] fetching metadata to find partition leader…");
    let meta = client
        .get_metadata(proto::GetMetadataRequest {
            topics: vec![topic.clone()],
        })
        .await
        .expect("get_metadata")
        .into_inner();
    assert_eq!(meta.topics.len(), 1);
    let part = &meta.topics[0].partitions[0];
    let leader = part.leader_broker_id;
    eprintln!(
        "[e2e] partition leader={leader} replicas={:?} isr={:?} hwm={} leo={}",
        part.replica_broker_ids, part.isr_broker_ids, part.high_watermark, part.log_end_offset
    );
    assert!((1..=3).contains(&leader), "unexpected leader {leader}");

    let addrs = [&addr1, &addr2, &addr3];
    eprintln!("[e2e] killing leader broker {leader}…");
    let surviving_addr = match leader {
        1 => {
            let _ = s1.kill();
            let _ = s1.wait();
            addrs[1]
        }
        2 => {
            let _ = s2.kill();
            let _ = s2.wait();
            addrs[2]
        }
        3 => {
            let _ = s3.kill();
            let _ = s3.wait();
            addrs[0]
        }
        _ => unreachable!(),
    };
    eprintln!("[e2e] leader killed, polling metadata via {surviving_addr} for failover…");

    let mut new_client = client_for(surviving_addr).await;
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut new_leader = None;
    while Instant::now() < deadline {
        sleep(Duration::from_secs(2)).await;
        match new_client
            .get_metadata(proto::GetMetadataRequest {
                topics: vec![topic.clone()],
            })
            .await
        {
            Ok(resp) => {
                let m = resp.into_inner();
                if let Some(t) = m.topics.first() {
                    if let Some(p) = t.partitions.first() {
                        eprintln!(
                            "[e2e] poll: leader={} isr={:?} hwm={} leo={}",
                            p.leader_broker_id,
                            p.isr_broker_ids,
                            p.high_watermark,
                            p.log_end_offset
                        );
                        if p.leader_broker_id != leader && p.leader_broker_id != 0 {
                            new_leader = Some(p.leader_broker_id);
                            break;
                        }
                    }
                } else {
                    eprintln!("[e2e] poll: no topics in metadata");
                }
            }
            Err(e) => {
                eprintln!("[e2e] poll: get_metadata error: {e}");
            }
        }
    }
    assert!(
        new_leader.is_some(),
        "leader did not change from {leader} within timeout"
    );
    eprintln!("[e2e] new leader is {}", new_leader.unwrap());

    let fetch_addr = match new_leader.unwrap() {
        1 => addrs[0],
        2 => addrs[1],
        3 => addrs[2],
        _ => unreachable!(),
    };
    eprintln!("[e2e] fetching records from new leader at {fetch_addr}…");
    let mut fetch_client = client_for(fetch_addr).await;
    let fetch_resp = fetch_client
        .fetch(proto::FetchRequest {
            offset: 0,
            max_records: 10,
            topic: topic.clone(),
            partition: 0,
            isolation_level: proto::IsolationLevel::ReadUncommitted as i32,
        })
        .await
        .expect("fetch after failover")
        .into_inner();

    assert!(
        fetch_resp.error.is_none(),
        "fetch error after failover: {:?}",
        fetch_resp.error
    );
    assert_eq!(fetch_resp.records.len(), values.len());

    for (i, rec) in fetch_resp.records.iter().enumerate() {
        assert_eq!(rec.offset, i as u64);
        assert_eq!(String::from_utf8_lossy(&rec.value), values[i]);
    }
    eprintln!("[e2e] all records verified after failover");

    let _ = s1.kill();
    let _ = s1.wait();
    let _ = s2.kill();
    let _ = s2.wait();
    let _ = s3.kill();
    let _ = s3.wait();
    let _ = std::fs::remove_dir_all(&data1);
    let _ = std::fs::remove_dir_all(&data2);
    let _ = std::fs::remove_dir_all(&data3);
}

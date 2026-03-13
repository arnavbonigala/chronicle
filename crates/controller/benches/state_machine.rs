use chronicle_controller::{MetadataRequest, StateMachineData, StateMachineStore};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

// ─── helpers ────────────────────────────────────────────────────────────────

fn fresh() -> (std::sync::Arc<StateMachineStore>, StateMachineData) {
    let (store, _rx) = StateMachineStore::new();
    let sm = StateMachineData::default();
    (store, sm)
}

fn with_brokers(n: u32) -> (std::sync::Arc<StateMachineStore>, StateMachineData) {
    let (store, mut sm) = fresh();
    for i in 1..=n {
        store.apply_command(
            &mut sm,
            &MetadataRequest::RegisterBroker {
                id: i,
                addr: format!("http://127.0.0.1:{}", 9000 + i),
            },
        );
    }
    (store, sm)
}

fn with_topic(
    store: &std::sync::Arc<StateMachineStore>,
    sm: &mut StateMachineData,
    topic: &str,
    partitions: u32,
) {
    store.apply_command(
        sm,
        &MetadataRequest::CreateTopic {
            name: topic.into(),
            partition_count: partitions,
            replication_factor: 1,
        },
    );
}

// ─── heartbeat (most frequent Raft command) ──────────────────────────────────

fn bench_heartbeat(c: &mut Criterion) {
    let (store, mut sm) = with_brokers(3);
    let req = MetadataRequest::Heartbeat {
        broker_id: 1,
        timestamp_ms: 1_000_000,
    };

    let mut group = c.benchmark_group("state_machine");
    group.throughput(Throughput::Elements(1));
    group.bench_function("heartbeat", |b| {
        b.iter(|| store.apply_command(&mut sm, &req));
    });
    group.finish();
}

// ─── create_topic (includes compute_assignments) ─────────────────────────────

fn bench_create_topic(c: &mut Criterion) {
    let cases: &[(u32, u32, u32, &str)] = &[
        (1, 8, 3, "8p-rf3-3brokers"),
        (3, 64, 3, "64p-rf3-3brokers"),
        (5, 100, 3, "100p-rf3-5brokers"),
    ];

    let mut group = c.benchmark_group("create_topic");
    for &(brokers, partitions, rf, label) in cases {
        group.throughput(Throughput::Elements(partitions as u64));
        group.bench_function(BenchmarkId::new("create", label), |b| {
            b.iter_batched(
                || {
                    let (store, mut sm) = fresh();
                    for i in 1..=brokers {
                        store.apply_command(
                            &mut sm,
                            &MetadataRequest::RegisterBroker {
                                id: i,
                                addr: format!("http://127.0.0.1:{}", 9000 + i),
                            },
                        );
                    }
                    (store, sm)
                },
                |(store, mut sm)| {
                    store.apply_command(
                        &mut sm,
                        &MetadataRequest::CreateTopic {
                            name: "bench".into(),
                            partition_count: partitions,
                            replication_factor: rf,
                        },
                    )
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ─── join_group (includes compute_group_assignments) ─────────────────────────

fn bench_join_group(c: &mut Criterion) {
    let cases: &[(u32, u32, &str)] = &[
        (4, 1, "4p-1member"),
        (32, 4, "32p-4members"),
        (100, 10, "100p-10members"),
    ];

    let mut group = c.benchmark_group("join_group");
    for &(partitions, members, label) in cases {
        group.throughput(Throughput::Elements(partitions as u64));
        group.bench_function(BenchmarkId::new("join", label), |b| {
            b.iter_batched(
                || {
                    let (store, mut sm) = with_brokers(1);
                    with_topic(&store, &mut sm, "orders", partitions);
                    // Pre-join all-but-last members
                    for i in 0..members.saturating_sub(1) {
                        store.apply_command(
                            &mut sm,
                            &MetadataRequest::JoinGroup {
                                group_id: "g".into(),
                                member_id: format!("c{i}"),
                                topics: vec!["orders".into()],
                                session_timeout_ms: 10_000,
                            },
                        );
                    }
                    (store, sm)
                },
                |(store, mut sm)| {
                    store.apply_command(
                        &mut sm,
                        &MetadataRequest::JoinGroup {
                            group_id: "g".into(),
                            member_id: format!("c{members}"),
                            topics: vec!["orders".into()],
                            session_timeout_ms: 10_000,
                        },
                    )
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

// ─── commit_offset ───────────────────────────────────────────────────────────

fn bench_commit_offset(c: &mut Criterion) {
    let offset_counts: &[(usize, &str)] = &[
        (1, "1_partition"),
        (10, "10_partitions"),
        (100, "100_partitions"),
    ];

    let mut group = c.benchmark_group("commit_offset");
    for &(n_offsets, label) in offset_counts {
        let (store, mut sm) = with_brokers(1);
        with_topic(&store, &mut sm, "orders", n_offsets as u32);
        store.apply_command(
            &mut sm,
            &MetadataRequest::JoinGroup {
                group_id: "g".into(),
                member_id: "c1".into(),
                topics: vec!["orders".into()],
                session_timeout_ms: 10_000,
            },
        );

        let offsets: Vec<(String, u32, u64)> = (0..n_offsets as u32)
            .map(|p| ("orders".to_string(), p, 100))
            .collect();
        let req = MetadataRequest::CommitOffset {
            group_id: "g".into(),
            offsets,
        };

        group.throughput(Throughput::Elements(n_offsets as u64));
        group.bench_function(BenchmarkId::new("commit", label), |b| {
            b.iter(|| store.apply_command(&mut sm, &req));
        });
    }
    group.finish();
}

// ─── snapshot serialization ──────────────────────────────────────────────────

fn bench_snapshot_serialize(c: &mut Criterion) {
    let cases: &[(u32, u32, u32, &str)] = &[
        (3, 4, 2, "3brokers-4topics-2groups"),
        (10, 50, 10, "10brokers-50topics-10groups"),
    ];

    let mut group = c.benchmark_group("snapshot");
    for &(brokers, topics, groups, label) in cases {
        let (store, mut sm) = with_brokers(brokers);
        for t in 0..topics {
            with_topic(&store, &mut sm, &format!("topic-{t}"), 4);
        }
        for g in 0..groups {
            store.apply_command(
                &mut sm,
                &MetadataRequest::JoinGroup {
                    group_id: format!("group-{g}"),
                    member_id: "m1".into(),
                    topics: vec!["topic-0".into()],
                    session_timeout_ms: 10_000,
                },
            );
        }

        group.throughput(Throughput::Elements(1));
        group.bench_function(BenchmarkId::new("serialize", label), |b| {
            b.iter(|| serde_json::to_vec(&sm).unwrap());
        });
        group.bench_function(BenchmarkId::new("deserialize", label), |b| {
            let bytes = serde_json::to_vec(&sm).unwrap();
            b.iter(|| serde_json::from_slice::<StateMachineData>(&bytes).unwrap());
        });
    }
    group.finish();
}

// ─── criterion wiring ───────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_heartbeat,
    bench_create_topic,
    bench_join_group,
    bench_commit_offset,
    bench_snapshot_serialize,
);
criterion_main!(benches);

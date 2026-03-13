use std::sync::Arc;

use chronicle_storage::{PartitionAssignment, StorageConfig, TopicStore};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::tempdir;

use chronicle_replication::{ReplicaManager, assignment::compute_assignments};

// ─── helpers ────────────────────────────────────────────────────────────────

fn make_assignments_storage(
    partition_count: u32,
    rf: u32,
    broker_ids: &[u32],
) -> Vec<PartitionAssignment> {
    compute_assignments(partition_count, rf, broker_ids)
}

/// Set up a ReplicaManager as leader of `partitions` partitions,
/// with `other_replicas` as follower broker IDs.
fn setup_leader(partitions: u32, other_replicas: &[u32]) -> (Arc<TopicStore>, Arc<ReplicaManager>) {
    let dir = tempdir().unwrap();
    std::mem::forget(dir.path().to_path_buf()); // keep on disk
    let dir_path = dir.keep();
    let store = Arc::new(
        TopicStore::open(StorageConfig {
            data_dir: dir_path.clone(),
            segment_max_bytes: 10 * 1024 * 1024,
        })
        .unwrap(),
    );

    let mut all_brokers = vec![1u32];
    all_brokers.extend_from_slice(other_replicas);
    let assignments = make_assignments_storage(partitions, all_brokers.len() as u32, &all_brokers);

    let local: Vec<u32> = (0..partitions).collect();
    store
        .create_topic(
            "bench",
            partitions,
            all_brokers.len() as u32,
            &assignments,
            &local,
        )
        .unwrap();

    let rm = Arc::new(ReplicaManager::new(1, store.clone()));
    rm.register_topic("bench", &assignments);
    (store, rm)
}

// ─── assignment computation ──────────────────────────────────────────────────

fn bench_compute_assignments(c: &mut Criterion) {
    let cases: &[(u32, u32, &[u32], &str)] = &[
        (8, 3, &[1, 2, 3], "8p-3r-3brokers"),
        (64, 3, &[1, 2, 3], "64p-3r-3brokers"),
        (
            1000,
            3,
            &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "1000p-3r-10brokers",
        ),
    ];

    let mut group = c.benchmark_group("compute_assignments");
    for &(partitions, rf, brokers, label) in cases {
        group.throughput(Throughput::Elements(partitions as u64));
        group.bench_function(label, |b| {
            b.iter(|| compute_assignments(partitions, rf, brokers));
        });
    }
    group.finish();
}

// ─── is_leader (per-produce hot path) ───────────────────────────────────────

fn bench_is_leader(c: &mut Criterion) {
    let (_store, rm) = setup_leader(1, &[2, 3]);

    let mut group = c.benchmark_group("replica_manager");
    group.throughput(Throughput::Elements(1));

    group.bench_function("is_leader_true", |b| {
        b.iter(|| rm.is_leader("bench", 0));
    });

    // unknown topic — worst case: allocates String, misses in HashMap
    group.bench_function("is_leader_unknown_topic", |b| {
        b.iter(|| rm.is_leader("nonexistent", 0));
    });

    group.finish();
}

// ─── sequence check (idempotent producer hot path) ───────────────────────────

fn bench_check_sequence(c: &mut Criterion) {
    let (_store, rm) = setup_leader(1, &[]);
    // Seed a known sequence so we can test all code paths.
    rm.record_sequence("bench", 0, 42, 0, 99, 999);

    let mut group = c.benchmark_group("check_sequence");
    group.throughput(Throughput::Elements(1));

    // non-idempotent producer — returns Accept immediately
    group.bench_function("non_idempotent", |b| {
        b.iter(|| rm.check_sequence("bench", 0, 0, 0, 0));
    });

    // Accept: next sequence in order
    group.bench_function("accept_next", |b| {
        b.iter(|| rm.check_sequence("bench", 0, 42, 0, 100));
    });

    // Duplicate: same sequence as last recorded
    group.bench_function("duplicate", |b| {
        b.iter(|| rm.check_sequence("bench", 0, 42, 0, 99));
    });

    // Out of order: gap in sequence
    group.bench_function("out_of_order", |b| {
        b.iter(|| rm.check_sequence("bench", 0, 42, 0, 200));
    });

    group.finish();
}

// ─── record_sequence (per-produce write path) ────────────────────────────────

fn bench_record_sequence(c: &mut Criterion) {
    let (_store, rm) = setup_leader(1, &[]);

    let mut group = c.benchmark_group("record_sequence");
    group.throughput(Throughput::Elements(1));

    group.bench_function("record_seq", |b| {
        let mut seq = 0u32;
        let mut offset = 0u64;
        b.iter(|| {
            rm.record_sequence("bench", 0, 42, 0, seq, offset);
            seq = seq.wrapping_add(1);
            offset += 1;
        });
    });

    group.finish();
}

// ─── update_follower_progress (per-ReplicateFetch path) ──────────────────────

fn bench_update_follower_progress(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_follower_progress");
    group.throughput(Throughput::Elements(1));

    let cases: &[(&[u32], &str)] = &[
        (&[], "leader_only"),
        (&[2u32], "1_follower"),
        (&[2u32, 3, 4], "3_followers"),
    ];
    for &(extra_replicas, label) in cases {
        let (store, rm) = setup_leader(1, extra_replicas);
        // Write one record so leader LEO is 1
        let topic = store.topic("bench").unwrap();
        topic
            .partition(0)
            .unwrap()
            .write()
            .unwrap()
            .append(b"k", b"v")
            .unwrap();

        let follower_id = extra_replicas.first().copied().unwrap_or(2);

        group.bench_function(label, |b| {
            let mut leo = 0u64;
            b.iter(|| {
                rm.update_follower_progress("bench", 0, follower_id, leo);
                leo = leo.wrapping_add(1) % 2;
            });
        });
    }

    group.finish();
}

// ─── check_isr_expiry (background task) ──────────────────────────────────────

fn bench_check_isr_expiry(c: &mut Criterion) {
    let partition_counts: &[(u32, &str)] = &[
        (10, "10_partitions"),
        (100, "100_partitions"),
        (1000, "1000_partitions"),
    ];

    let mut group = c.benchmark_group("check_isr_expiry");
    for &(partitions, label) in partition_counts {
        let (_store, rm) = setup_leader(partitions, &[2, 3]);

        group.bench_function(BenchmarkId::new("expiry", label), |b| {
            b.iter(|| rm.check_isr_expiry(std::time::Duration::from_secs(10)));
        });
    }
    group.finish();
}

// ─── criterion wiring ───────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_compute_assignments,
    bench_is_leader,
    bench_check_sequence,
    bench_record_sequence,
    bench_update_follower_progress,
    bench_check_isr_expiry,
);
criterion_main!(benches);

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::tempdir;

use chronicle_storage::{Log, Record, StorageConfig, segment::Segment};

// ─── helpers ────────────────────────────────────────────────────────────────

fn config(dir: &std::path::Path, seg_max: u64) -> StorageConfig {
    StorageConfig {
        data_dir: dir.to_path_buf(),
        segment_max_bytes: seg_max,
    }
}

fn make_record(key_len: usize, value_len: usize) -> Record {
    Record::new(
        0,
        1_710_000_000_000,
        Bytes::from(vec![b'k'; key_len]),
        Bytes::from(vec![b'v'; value_len]),
    )
}

// ─── record encode ──────────────────────────────────────────────────────────

fn bench_record_encode(c: &mut Criterion) {
    let sizes: &[(usize, usize, &str)] = &[
        (16, 64, "16B-key/64B-val"),
        (32, 1024, "32B-key/1KB-val"),
        (64, 65536, "64B-key/64KB-val"),
    ];

    let mut group = c.benchmark_group("record_encode");
    for &(klen, vlen, label) in sizes {
        let record = make_record(klen, vlen);
        group.throughput(Throughput::Bytes(record.encoded_size() as u64));
        group.bench_function(BenchmarkId::new("encode", label), |b| {
            let mut buf = Vec::with_capacity(record.encoded_size());
            b.iter(|| {
                buf.clear();
                record.encode(&mut buf);
            });
        });
    }
    group.finish();
}

// ─── record decode ──────────────────────────────────────────────────────────

fn bench_record_decode(c: &mut Criterion) {
    let sizes: &[(usize, usize, &str)] = &[
        (16, 64, "16B-key/64B-val"),
        (32, 1024, "32B-key/1KB-val"),
        (64, 65536, "64B-key/64KB-val"),
    ];

    let mut group = c.benchmark_group("record_decode");
    for &(klen, vlen, label) in sizes {
        let mut record = make_record(klen, vlen);
        record.offset = 0;
        let mut buf = Vec::new();
        record.encode(&mut buf);
        group.throughput(Throughput::Bytes(buf.len() as u64));
        group.bench_function(BenchmarkId::new("decode", label), |b| {
            b.iter(|| {
                let mut cursor = std::io::Cursor::new(&buf);
                Record::decode(&mut cursor).unwrap().unwrap()
            });
        });
    }
    group.finish();
}

// ─── segment append ─────────────────────────────────────────────────────────

fn bench_segment_append(c: &mut Criterion) {
    let sizes: &[(usize, usize, &str)] = &[
        (16, 64, "16B-key/64B-val"),
        (32, 1024, "32B-key/1KB-val"),
        (64, 65536, "64B-key/64KB-val"),
    ];

    let mut group = c.benchmark_group("segment_append");
    for &(klen, vlen, label) in sizes {
        let key = vec![b'k'; klen];
        let val = vec![b'v'; vlen];
        // Pre-calculate throughput using a sample record
        let sample = make_record(klen, vlen);
        group.throughput(Throughput::Bytes(sample.encoded_size() as u64));

        group.bench_function(BenchmarkId::new("append", label), |b| {
            // One segment per benchmark run (large max to avoid rolling)
            let dir = tempdir().unwrap();
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            b.iter(|| seg.append(&key, &val).unwrap());
        });
    }
    group.finish();
}

// ─── segment read ───────────────────────────────────────────────────────────

fn bench_segment_read_at(c: &mut Criterion) {
    let sizes: &[(usize, usize, &str)] =
        &[(16, 64, "16B-key/64B-val"), (32, 1024, "32B-key/1KB-val")];
    const N: u64 = 10_000;

    let mut group = c.benchmark_group("segment_read_at");
    for &(klen, vlen, label) in sizes {
        let key = vec![b'k'; klen];
        let val = vec![b'v'; vlen];
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        for _ in 0..N {
            seg.append(&key, &val).unwrap();
        }

        let sample = make_record(klen, vlen);
        group.throughput(Throughput::Bytes(sample.encoded_size() as u64));

        // Measure a read at the midpoint (index lookup + disk seek + decode)
        group.bench_function(BenchmarkId::new("read_at_mid", label), |b| {
            b.iter(|| seg.read_at(N / 2).unwrap());
        });
    }
    group.finish();
}

fn bench_segment_read_from(c: &mut Criterion) {
    const N: u64 = 10_000;
    const BATCH: u32 = 1_000;
    let klen = 16usize;
    let vlen = 64usize;
    let key = vec![b'k'; klen];
    let val = vec![b'v'; vlen];

    let dir = tempdir().unwrap();
    let mut seg = Segment::create(dir.path(), 0).unwrap();
    for _ in 0..N {
        seg.append(&key, &val).unwrap();
    }

    let sample = make_record(klen, vlen);
    let mut group = c.benchmark_group("segment_read_from");
    group.throughput(Throughput::Bytes(
        sample.encoded_size() as u64 * BATCH as u64,
    ));
    group.bench_function("read_from_start_1k", |b| {
        b.iter(|| seg.read_from(0, BATCH).unwrap());
    });
    group.finish();
}

// ─── log append ─────────────────────────────────────────────────────────────

fn bench_log_append(c: &mut Criterion) {
    let sizes: &[(usize, usize, &str)] = &[
        (16, 64, "16B-key/64B-val"),
        (32, 1024, "32B-key/1KB-val"),
        (64, 65536, "64B-key/64KB-val"),
    ];

    let mut group = c.benchmark_group("log_append");
    for &(klen, vlen, label) in sizes {
        let key = vec![b'k'; klen];
        let val = vec![b'v'; vlen];
        let sample = make_record(klen, vlen);
        group.throughput(Throughput::Bytes(sample.encoded_size() as u64));

        // Single large segment — no rolling during the benchmark
        group.bench_function(BenchmarkId::new("no_rolling", label), |b| {
            let dir = tempdir().unwrap();
            let mut log = Log::open(config(dir.path(), 512 * 1024 * 1024)).unwrap();
            b.iter(|| log.append(&key, &val).unwrap());
        });
    }
    group.finish();
}

fn bench_log_append_with_rolling(c: &mut Criterion) {
    // Tiny segments force a roll every ~2 records (100 bytes ≈ 1-2 small records)
    const SEG_MAX: u64 = 512;
    let key = b"key12345".as_slice();
    let val = b"value_data_for_rolling_bench____".as_slice();
    let sample = make_record(key.len(), val.len());

    let mut group = c.benchmark_group("log_append_with_rolling");
    group.throughput(Throughput::Bytes(sample.encoded_size() as u64));
    group.bench_function("512B_segments", |b| {
        let dir = tempdir().unwrap();
        let mut log = Log::open(config(dir.path(), SEG_MAX)).unwrap();
        b.iter(|| log.append(key, val).unwrap());
    });
    group.finish();
}

// ─── log read ───────────────────────────────────────────────────────────────

fn bench_log_read(c: &mut Criterion) {
    const PRELOAD: u64 = 100_000;
    let batches: &[(u32, &str)] = &[
        (100, "100_records"),
        (1_000, "1k_records"),
        (10_000, "10k_records"),
    ];

    let key = vec![b'k'; 16];
    let val = vec![b'v'; 64];
    let dir = tempdir().unwrap();
    let mut log = Log::open(config(dir.path(), 512 * 1024 * 1024)).unwrap();
    for _ in 0..PRELOAD {
        log.append(&key, &val).unwrap();
    }

    let sample = make_record(16, 64);
    let mut group = c.benchmark_group("log_read");
    for &(batch, label) in batches {
        group.throughput(Throughput::Bytes(
            sample.encoded_size() as u64 * batch as u64,
        ));
        group.bench_function(BenchmarkId::new("from_offset_0", label), |b| {
            b.iter(|| log.read(0, batch).unwrap());
        });
        // Read from the middle of the log
        group.bench_function(BenchmarkId::new("from_middle", label), |b| {
            b.iter(|| log.read(PRELOAD / 2, batch).unwrap());
        });
    }
    group.finish();
}

// ─── log open (crash recovery) ──────────────────────────────────────────────

fn bench_log_open(c: &mut Criterion) {
    let record_counts: &[(u64, &str)] = &[
        (1_000, "1k_records"),
        (10_000, "10k_records"),
        (100_000, "100k_records"),
    ];

    let key = vec![b'k'; 16];
    let val = vec![b'v'; 64];

    let mut group = c.benchmark_group("log_open");
    for &(n, label) in record_counts {
        // Pre-create a persistent directory with N records.
        // We keep it alive for the lifetime of the benchmark group via a leak.
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(config(dir.path(), 512 * 1024 * 1024)).unwrap();
            for _ in 0..n {
                log.append(&key, &val).unwrap();
            }
        }

        group.bench_function(BenchmarkId::new("open", label), |b| {
            // Each open() runs recover() on the active segment (re-scans all records in it).
            // Sealed segments are trusted as-is, but the last segment is always re-scanned.
            // With 512 MiB segments and ~100 byte records, all records land in one segment,
            // so the full log is re-scanned on every open.
            b.iter(|| Log::open(config(dir.path(), 512 * 1024 * 1024)).unwrap());
        });
    }
    group.finish();
}

// ─── timestamp lookup ───────────────────────────────────────────────────────

fn bench_find_offset_by_timestamp(c: &mut Criterion) {
    const N: u64 = 100_000;
    let key = vec![b'k'; 16];
    let dir = tempdir().unwrap();
    let mut log = Log::open(config(dir.path(), 512 * 1024 * 1024)).unwrap();

    // Write records with synthetic monotonically-increasing timestamps.
    // We must use append_record to control the timestamp field.
    for i in 0..N {
        let record = Record::new(
            i,
            1_000_000 + i * 10, // 10 ms apart
            Bytes::from(key.clone()),
            Bytes::from(vec![b'v'; 64]),
        );
        log.append_record(&record).unwrap();
    }

    let mut group = c.benchmark_group("find_offset_by_timestamp");
    group.throughput(Throughput::Elements(1));

    // Lookup at various positions
    for (ts, label) in [
        (1_000_000, "start"),
        (1_000_000 + (N / 2) * 10, "middle"),
        (1_000_000 + (N - 1) * 10, "end"),
    ] {
        group.bench_function(label, |b| {
            b.iter(|| log.find_offset_by_timestamp(ts));
        });
    }
    group.finish();
}

// ─── index lookup ───────────────────────────────────────────────────────────

fn bench_index_lookup(c: &mut Criterion) {
    use chronicle_storage::index::Index;

    let sizes: &[(usize, &str)] = &[(1_000, "1k"), (100_000, "100k")];

    let mut group = c.benchmark_group("index_lookup");
    group.throughput(Throughput::Elements(1));
    for &(n, label) in sizes {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench.index");
        let mut idx = Index::create(&path).unwrap();
        for i in 0..n as u32 {
            idx.append(i, i * 64).unwrap();
        }

        group.bench_function(BenchmarkId::new("lookup_mid", label), |b| {
            let target = (n / 2) as u32;
            b.iter(|| idx.lookup(target).unwrap());
        });
    }
    group.finish();
}

// ─── criterion wiring ───────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_record_encode,
    bench_record_decode,
    bench_segment_append,
    bench_segment_read_at,
    bench_segment_read_from,
    bench_log_append,
    bench_log_append_with_rolling,
    bench_log_read,
    bench_log_open,
    bench_find_offset_by_timestamp,
    bench_index_lookup,
);
criterion_main!(benches);

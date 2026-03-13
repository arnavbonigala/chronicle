use bytes::Bytes;
use chronicle_stream::{Filter, FlatMap, Map, Operator, Passthrough, RegexFilter, StreamRecord};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

// ─── helpers ────────────────────────────────────────────────────────────────

fn make_record(value: &'static [u8]) -> StreamRecord {
    StreamRecord {
        key: Bytes::from_static(b"order-key"),
        value: Bytes::from_static(value),
        timestamp_ms: 1_710_000_000_000,
        offset: 42,
        topic: "orders".to_string(),
        partition: 0,
    }
}

// ─── individual operators ────────────────────────────────────────────────────

fn bench_operators(c: &mut Criterion) {
    let record_small = make_record(b"hello world");
    let record_large = make_record(b"the quick brown fox jumps over the lazy dog and then some more text to make it longer than a cache line for sure absolutely");

    let mut group = c.benchmark_group("operators");
    group.throughput(Throughput::Elements(1));

    // Passthrough: clone only
    let passthrough = Passthrough;
    group.bench_function("passthrough_small", |b| {
        b.iter(|| passthrough.process(&record_small));
    });
    group.bench_function("passthrough_large", |b| {
        b.iter(|| passthrough.process(&record_large));
    });

    // Filter - pass (clone path)
    let filter_pass = Filter::new(|_: &StreamRecord| true);
    group.bench_function("filter_pass", |b| {
        b.iter(|| filter_pass.process(&record_small));
    });

    // Filter - drop (no allocation)
    let filter_drop = Filter::new(|_: &StreamRecord| false);
    group.bench_function("filter_drop", |b| {
        b.iter(|| filter_drop.process(&record_small));
    });

    // Map: transform value
    let map_op = Map::new(|r: &StreamRecord| {
        let mut out = r.clone();
        out.value = Bytes::from(format!("processed:{}", String::from_utf8_lossy(&r.value)));
        out
    });
    group.bench_function("map_small", |b| {
        b.iter(|| map_op.process(&record_small));
    });

    // FlatMap 1→1
    let flatmap_1to1 = FlatMap::new(|r: &StreamRecord| vec![r.clone()]);
    group.bench_function("flatmap_1to1", |b| {
        b.iter(|| flatmap_1to1.process(&record_small));
    });

    // FlatMap 1→5 (split)
    let flatmap_1to5 = FlatMap::new(|r: &StreamRecord| {
        (0..5)
            .map(|i| {
                let mut out = r.clone();
                out.offset = i;
                out
            })
            .collect()
    });
    group.bench_function("flatmap_1to5", |b| {
        b.iter(|| flatmap_1to5.process(&record_small));
    });

    // FlatMap 1→100
    let flatmap_1to100 = FlatMap::new(|r: &StreamRecord| {
        (0..100)
            .map(|i| {
                let mut out = r.clone();
                out.offset = i;
                out
            })
            .collect()
    });
    group.bench_function("flatmap_1to100", |b| {
        b.iter(|| flatmap_1to100.process(&record_small));
    });

    group.finish();
}

// ─── regex filter ────────────────────────────────────────────────────────────

fn bench_regex_filter(c: &mut Criterion) {
    let cases: &[(&str, &[u8], &str, bool, &str)] = &[
        (r"hello", b"hello world", "simple_match", true, "simple"),
        (
            r"hello",
            b"goodbye world",
            "simple_no_match",
            false,
            "simple",
        ),
        (
            r"^\d{4}-\d{2}-\d{2}$",
            b"2024-01-15",
            "date_match",
            true,
            "date",
        ),
        (
            r"^\d{4}-\d{2}-\d{2}$",
            b"not-a-date",
            "date_no_match",
            false,
            "date",
        ),
        (
            r"^(foo|bar|baz|qux|quux|corge|grault|garply)\d+$",
            b"foo123",
            "alternation_match",
            true,
            "alternation",
        ),
        (
            r"^(foo|bar|baz|qux|quux|corge|grault|garply)\d+$",
            b"zzz999",
            "alternation_no_match",
            false,
            "alternation",
        ),
    ];

    let mut group = c.benchmark_group("regex_filter");
    group.throughput(Throughput::Elements(1));
    for &(pattern, value, label, _, _) in cases {
        let op = RegexFilter::new(pattern).unwrap();
        let record = StreamRecord {
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(value),
            timestamp_ms: 0,
            offset: 0,
            topic: "t".to_string(),
            partition: 0,
        };
        group.bench_function(label, |b| {
            b.iter(|| op.process(&record));
        });
    }
    group.finish();
}

// ─── operator pipelines ──────────────────────────────────────────────────────

fn bench_pipeline(c: &mut Criterion) {
    let record = make_record(b"order:12345:accepted:amount=500");

    let mut group = c.benchmark_group("pipeline");
    group.throughput(Throughput::Elements(1));

    // 3-stage pipeline: filter → map → filter
    group.bench_function("3_ops_filter_map_filter", |b| {
        let f1 = Filter::new(|r: &StreamRecord| !r.value.is_empty());
        let m1 = Map::new(|r: &StreamRecord| {
            let mut out = r.clone();
            out.value = Bytes::from(String::from_utf8_lossy(&r.value).to_uppercase());
            out
        });
        let f2 = Filter::new(|r: &StreamRecord| r.value.len() < 1000);

        b.iter(|| {
            let stage1 = f1.process(&record);
            let stage2: Vec<_> = stage1.iter().flat_map(|r| m1.process(r)).collect();
            let stage3: Vec<_> = stage2.iter().flat_map(|r| f2.process(r)).collect();
            stage3
        });
    });

    // 5-stage: filter → map → filter → map → filter
    group.bench_function("5_ops", |b| {
        let ops: Vec<Box<dyn Operator>> = vec![
            Box::new(Filter::new(|r: &StreamRecord| !r.value.is_empty())),
            Box::new(Map::new(|r: &StreamRecord| {
                let mut out = r.clone();
                out.offset += 1;
                out
            })),
            Box::new(Filter::new(|r: &StreamRecord| r.value.len() < 2000)),
            Box::new(Map::new(|r: &StreamRecord| {
                let mut out = r.clone();
                out.timestamp_ms += 1;
                out
            })),
            Box::new(Filter::new(|_: &StreamRecord| true)),
        ];

        b.iter(|| {
            let mut current = vec![record.clone()];
            for op in &ops {
                current = current.iter().flat_map(|r| op.process(r)).collect();
            }
            current
        });
    });

    group.finish();
}

// ─── pipeline throughput (batch of records) ───────────────────────────────────

fn bench_pipeline_batch(c: &mut Criterion) {
    let batches: &[(usize, &str)] = &[(100, "100"), (1_000, "1k"), (10_000, "10k")];
    let ops: Vec<Box<dyn Operator>> = vec![
        Box::new(Filter::new(|r: &StreamRecord| r.offset.is_multiple_of(2))),
        Box::new(Map::new(|r: &StreamRecord| {
            let mut out = r.clone();
            out.timestamp_ms += 1;
            out
        })),
        Box::new(Passthrough),
    ];

    let mut group = c.benchmark_group("pipeline_batch");
    for &(n, label) in batches {
        let records: Vec<StreamRecord> = (0..n as u64)
            .map(|i| StreamRecord {
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"some value"),
                timestamp_ms: 1_000_000,
                offset: i,
                topic: "bench".to_string(),
                partition: 0,
            })
            .collect();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("3ops", label), |b| {
            b.iter(|| {
                let mut current: Vec<StreamRecord> = records.clone();
                for op in &ops {
                    current = current.iter().flat_map(|r| op.process(r)).collect();
                }
                current
            });
        });
    }
    group.finish();
}

// ─── criterion wiring ───────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_operators,
    bench_regex_filter,
    bench_pipeline,
    bench_pipeline_batch,
);
criterion_main!(benches);

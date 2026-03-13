# Chronicle

Chronicle is a distributed append-only log system implemented in Rust. It covers the core mechanisms of systems like Apache Kafka: partitioned logs, ISR-based replication, Raft-driven cluster coordination, consumer groups, idempotent producers, and transactional writes.

## Architecture

Chronicle is structured as a multi-crate workspace:

| Crate | Role |
|---|---|
| `chronicle-server` | Broker binary; hosts the gRPC service |
| `chronicle-storage` | On-disk segment storage (`.log`, `.index`, `.timeindex`) |
| `chronicle-replication` | ISR management, high-watermark, producer idempotence |
| `chronicle-controller` | Raft-based metadata controller (topic assignments, consumer groups, transactions) |
| `chronicle-stream` | Stream processing job operator framework |
| `chronicle-cli` | gRPC CLI client |

All client-broker and broker-broker communication is gRPC over HTTP/2 (Tonic/Prost).

## Features

**Storage**
- Append-only segmented log per partition (`.log` + sparse `.index` + `.timeindex`)
- CRC-32 integrity check on every record read
- Segment crash recovery: corrupt tail truncated and index rebuilt on open
- Timestamp-based offset lookup via `TimeIndex`

**Replication**
- Leader/follower model per partition
- In-sync replica (ISR) set tracking with configurable lag threshold
- High-watermark advancement on follower fetch acknowledgement
- `Acks::All` waits for full ISR before committing

**Cluster Coordination**
- Raft consensus via `openraft` for cluster metadata
- Broker heartbeat monitoring with dead-broker detection (15s timeout)
- ISR-based leader failover on broker failure

**Producers**
- Idempotent delivery via per-producer sequence number tracking
- Transactional writes: `BeginTransaction` → `AddPartitionsToTxn` → `EndTransaction` → `WriteTxnMarkers`

**Consumers**
- Offset-based sequential consumption
- Consumer group join/leave/heartbeat/rebalance lifecycle
- Committed offset storage and fetch
- `READ_UNCOMMITTED` and `READ_COMMITTED` isolation levels

**Stream Processing**
- Stream jobs with operator chains (filter, map) via `chronicle-stream`
- Job lifecycle managed through the controller state machine

## Limitations

- Controller state machine (topic assignments, consumer group state, Raft log) is in-memory. Data partition files survive restarts; cluster metadata does not unless a Raft snapshot was installed.

## Running

```bash
cargo build --release

./target/release/chronicle-server --broker-id 1 --listen-addr 127.0.0.1:9092 --peers 2=127.0.0.1:9093,3=127.0.0.1:9094
./target/release/chronicle-server --broker-id 2 --listen-addr 127.0.0.1:9093 --peers 1=127.0.0.1:9092,3=127.0.0.1:9094
./target/release/chronicle-server --broker-id 3 --listen-addr 127.0.0.1:9094 --peers 1=127.0.0.1:9092,2=127.0.0.1:9093
```

| Flag | Default | Description |
|---|---|---|
| `--broker-id` | `0` | Numeric broker ID |
| `--listen-addr` | `127.0.0.1:9092` | gRPC listen address |
| `--data-dir` | `./data` | On-disk data directory |
| `--segment-max-bytes` | `10485760` | Segment rollover size (bytes) |
| `--peers` | *(empty)* | Comma-separated `id=addr` peer list |

Single-broker mode (no `--peers`) runs without Raft.

## Benchmarks

All benchmarks run on Apple M3, 16 GiB RAM, macOS, Rust 1.94.0, `release` profile.

| Layer | Benchmark | Latency | Throughput |
|---|---|---|---|
| Storage | Segment append (80 B) | 5.85 µs | ~170k writes/s |
| Storage | Log sequential read (1k × 80 B) | 129 µs | 966 MiB/s |
| Storage | Crash recovery (100k records) | 39.3 ms | 2.54 M records/s |
| Replication | `is_leader` check | 37 ns | 26.9 M/s |
| Replication | `check_sequence` (accept) | 42.6 ns | 23.5 M/s |
| Replication | `update_follower_progress` | 98 ns | 10.2 M/s |
| Replication | ISR expiry (1000 partitions) | 14.7 µs | — |
| Controller | Heartbeat command | 25 ns | 40 M/s |
| Controller | `JoinGroup` (100p / 10m) | 27.8 µs | — |
| Controller | Snapshot serialize (50 topics) | 21.5 µs | — |
| Controller | Snapshot deserialize (50 topics) | 77.8 µs | — |
| Stream | Passthrough clone | 60 ns | 16.5 M/s |
| Stream | Filter (pass) | 57 ns | 17.6 M/s |
| Stream | Pipeline 3-op batch (1k) | 147 µs | 6.82 M records/s |

## Documentation

See `docs/ARCHITECTURE.md` for a detailed design walkthrough.
See `docs/BENCHMARKS.md` for full results and analysis.
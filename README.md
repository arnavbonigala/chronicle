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

## Documentation

See `docs/ARCHITECTURE.md` for a detailed design walkthrough.

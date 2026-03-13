# Chronicle Architecture

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Crate Structure](#2-crate-structure)
3. [Storage Engine](#3-storage-engine)
4. [Topics and Partitioning](#4-topics-and-partitioning)
5. [Replication](#5-replication)
6. [Cluster Coordination](#6-cluster-coordination)
7. [Consumer Groups](#7-consumer-groups)
8. [Advanced Features](#8-advanced-features)
9. [Write Path](#9-write-path)
10. [Read Path](#10-read-path)
11. [Failure Model](#11-failure-model)
12. [Known Limitations](#12-known-limitations)

---

## 1. System Overview

Chronicle is a distributed append-only log. Producers write records to named topics; consumers read records sequentially by offset. Topics are divided into partitions for horizontal scale. Each partition is replicated across multiple brokers, with one broker acting as leader for writes and reads.

All inter-node communication is gRPC over HTTP/2. There are two gRPC services on each broker:

- **`Chronicle`** — client-facing service (produce, fetch, topic management, consumer groups, transactions, stream jobs, and inter-broker replication)
- **`ControllerService`** — internal Raft transport (AppendEntries, Vote, InstallSnapshot, broker registration, heartbeat, metadata proposals)

Both services share a single listener on the same address.

---

## 2. Crate Structure

```
chronicle/
  Cargo.toml                  workspace root
  proto/
    chronicle.proto            single proto file for both services
  crates/
    storage/                  chronicle-storage
    replication/              chronicle-replication
    controller/               chronicle-controller
    server/                   chronicle-server  (binary)
    cli/                      chronicle-cli     (binary)
    stream/                   chronicle-stream
```

### Dependency graph

```
chronicle-server
  ├── chronicle-storage
  ├── chronicle-replication
  │     └── chronicle-storage
  └── chronicle-controller
        └── chronicle-storage

chronicle-cli         (gRPC client only, no storage dep)
chronicle-stream      (gRPC client, operator framework)
```

### Crate responsibilities

| Crate | Responsibility |
|---|---|
| `chronicle-storage` | On-disk segment files, offset index, time index, crash recovery, `TopicStore` |
| `chronicle-replication` | `ReplicaManager` (leader/follower state, ISR, HWM, LSO, producer dedup), `FollowerFetcher` |
| `chronicle-controller` | Raft state machine (`openraft`), cluster metadata (topics, brokers, consumer groups, transactions, stream jobs) |
| `chronicle-server` | gRPC service handlers, broker startup, `MetadataReactor`, heartbeat loops |
| `chronicle-cli` | Command-line client with 18+ subcommands |
| `chronicle-stream` | `Operator` trait, built-in operators, `StreamProcessor` consumer-producer loop |

---

## 3. Storage Engine

### 3.1 On-disk layout

```
{data_dir}/topics/{topic}/
  meta.bin                            8 bytes: partition_count u32 LE + replication_factor u32 LE
  assignments.bin                     binary replica lists (written by Phase 3+)
  partition-{N}/
    00000000000000000000.log          record data
    00000000000000000000.index        sparse offset → file position index
    00000000000000000000.timeindex    timestamp → relative offset index
    00000000000000001024.log
    ...
```

Segment filenames use zero-padded 20-digit base offsets. A new segment is created when the active segment exceeds `segment_max_bytes` (default 10 MiB).

### 3.2 Record format (v2)

Every record is written in the following binary layout:

```
[length: u32]           bytes from `attributes` through end of value
[crc32: u32]            CRC-32C of bytes from `attributes` through end of value
[attributes: u8]        bit 0 = is_control, bit 1 = is_transactional
[offset: u64]
[timestamp_ms: u64]
[producer_id: u64]      0 = non-idempotent
[producer_epoch: u16]
[sequence_number: u32]
[header_count: u32]
for each header:
  [key_len: u32][key utf8]
  [value_len: u32][value]
[key_len: u32][key]
[value_len: u32][value]
```

CRC-32C is verified on every read. A failed CRC raises `StorageError::CorruptRecord`.

### 3.3 Offset index

Each segment has a companion `.index` file with 8-byte dense entries:

```
[relative_offset: u32][position: u32]
```

`relative_offset` = `record.offset - segment.base_offset`. `position` is the byte offset into the `.log` file. Index entries are loaded into memory at open time and used for O(log n) binary-search seeks.

### 3.4 Time index

Each segment has a `.timeindex` file with 12-byte entries:

```
[timestamp_ms: u64][relative_offset: u32]
```

`Log::find_offset_by_timestamp(ts)` binary-searches the time index to resolve a timestamp to its nearest offset. Used by the `OffsetForTimestamp` RPC.

### 3.5 Crash recovery

On `Log::open`, the broker:

1. Lists all `.log` files, sorts by base offset
2. For the **active** (last) segment:
   - Scans forward, validating each record's CRC
   - Truncates the file at the first corrupt or incomplete record
   - Rebuilds `.index` and `.timeindex` from the validated records
3. Sets `next_offset` to one past the last valid record

Older (sealed) segments are trusted as-is. If an index file is missing or its entry count mismatches the log, it is rebuilt from the log file.

### 3.6 Key types

```rust
pub struct Log { dir: PathBuf, segments: Vec<Segment>, config: StorageConfig }
pub struct Segment { base_offset: u64, log_file: File, index: Index, time_index: TimeIndex, size: u64, next_offset: u64 }
pub struct TopicStore { data_dir: PathBuf, topics: RwLock<HashMap<String, Arc<TopicState>>> }
pub struct TopicState { meta: TopicMeta, partitions: HashMap<u32, RwLock<Log>>, assignments: Vec<PartitionAssignment> }
```

`TopicStore` provides per-partition `RwLock<Log>` granularity: concurrent reads across partitions, exclusive writes per partition, no cross-partition blocking.

---

## 4. Topics and Partitioning

### 4.1 Topic creation

`CreateTopic` is proposed to the Raft controller. The state machine computes partition assignments and emits a `TopicCreated` change. Each broker's `MetadataReactor` receives the change and creates its local partition directories.

### 4.2 Partition routing

When a producer does not specify a partition:

- **Key present**: `partition = crc32(key) % partition_count`
- **No key**: round-robin via `AtomicU32` counter

### 4.3 Replica assignment

Given sorted broker IDs and replication factor `rf`, partition `p` is assigned to:

```
replicas[i] = sorted_broker_ids[(p + i) % num_brokers]   for i in 0..min(rf, num_brokers)
replicas[0] = leader
```

This is computed deterministically by every node from the same inputs. Only the broker IDs that appear in a partition's replica list create a local `partition-{N}/` directory for that partition.

---

## 5. Replication

### 5.1 Model

Each partition has one **leader** and zero or more **followers**. Only the leader accepts client writes and reads. Followers pull records from the leader using the `ReplicateFetch` RPC.

### 5.2 Follower fetch loop (`FollowerFetcher`)

Each followed partition runs a background tokio task:

1. Connect to the leader broker
2. Read local LEO: `log.latest_offset()`
3. Send `ReplicateFetchRequest { broker_id, topic, partition, fetch_offset: local_leo, leader_epoch }`
4. On success: append received records at their original offsets via `log.append_at(offset, ...)`, update local HWM
5. On `NOT_LEADER_FOR_PARTITION`: back off and retry
6. Sleep 100 ms between iterations; skip sleep when records were returned (tight catch-up)

`CancellationToken` is used to cleanly stop fetchers on leadership change or topic deletion.

### 5.3 High watermark (HWM)

The leader tracks `follower_leos: HashMap<broker_id, u64>` inside `LeaderState`. Each `ReplicateFetch` call reports the follower's LEO. After updating the follower's entry, the leader recomputes:

```
HWM = min(LEO) across all ISR members (including the leader itself)
```

HWM is monotonically non-decreasing. It is broadcast via a `tokio::sync::watch` channel, which `acks=all` produce handlers wait on.

### 5.4 In-sync replicas (ISR)

A follower is in ISR if its last fetch was within a configurable lag window (default 10 s). The broker runs a periodic ISR check task that calls `check_isr_expiry`. A follower removed from ISR causes HWM to be recomputed on the reduced set. A follower re-enters ISR once its LEO reaches the current HWM.

ISR changes are proposed to the controller as `MetadataRequest::UpdateISR`, keeping the cluster-wide view consistent.

### 5.5 Leader epoch

Every leadership change increments the **leader epoch**. Epoch is included in `ReplicateFetch` requests and responses. Stale-epoch replication requests are rejected with `NOT_LEADER_FOR_PARTITION`, forcing the follower to reconnect to the new leader. Clients receive the current leader hint in error responses.

### 5.6 Acknowledgment modes

| Mode | Behavior |
|---|---|
| `ACKS_NONE` | Respond immediately, no durability guarantee |
| `ACKS_LEADER` | Respond after leader appends locally (default) |
| `ACKS_ALL` | Wait until `HWM >= produced_offset` across the full ISR |

---

## 6. Cluster Coordination

### 6.1 Embedded Raft controller

All brokers participate in a Raft consensus group via `openraft`. The Raft leader doubles as the cluster controller. There is no separate controller process.

Each broker runs:
- A `Raft<TypeConfig>` instance (in-memory log store + state machine)
- A `ControllerService` gRPC handler that receives Raft RPCs from peers

### 6.2 State machine

The Raft state machine holds `ClusterState`:

```rust
pub struct ClusterState {
    pub brokers: HashMap<u32, BrokerRegistration>,
    pub topics: HashMap<String, TopicMetadata>,
    pub consumer_groups: HashMap<String, ConsumerGroupState>,
    pub transactions: HashMap<u64, TransactionState>,
    pub stream_jobs: HashMap<String, StreamJobMeta>,
    pub next_producer_id: u64,
    pub transactional_ids: HashMap<String, TransactionalIdMapping>,
}
```

`MetadataRequest` variants (the Raft log entry type):

```
RegisterBroker, Heartbeat, CreateTopic, DeleteTopic,
UpdateLeader, UpdateISR, MarkBrokerDead,
JoinGroup, LeaveGroup, ConsumerHeartbeat, CommitOffset, RemoveExpiredMember,
AllocateProducerId,
BeginTransaction, AddPartitionsToTxn, AddOffsetsToTxn, EndTransaction,
WriteTxnMarkerComplete, TxnOffsetCommit,
RegisterStreamJob, UpdateStreamJobStatus, DeleteStreamJob
```

### 6.3 MetadataReactor

Each broker subscribes to a `broadcast::Receiver<MetadataChange>` from the state machine's `apply()` method. The `MetadataReactor` runs as a background task and translates changes into local actions:

| Change | Local action |
|---|---|
| `TopicCreated` | Create local partition dirs, register with `ReplicaManager`, spawn `FollowerFetcher`s |
| `TopicDeleted` | Cancel fetchers, delete local partitions |
| `LeaderChanged` | Cancel old fetcher; if new leader is self, call `promote_to_leader`; otherwise call `demote_to_follower` and spawn new fetcher |
| `BrokerDead` | Logged; `LeaderChanged` events follow for affected partitions |
| `ISRChanged` | Update ISR in `ReplicaManager` |

### 6.4 Broker heartbeats and failover

Each broker proposes `MetadataRequest::Heartbeat` to the Raft leader every 3 seconds.

The Raft leader runs a checker loop every 5 seconds. For each broker whose `last_heartbeat_ms` exceeds 15 seconds:

1. Propose `MarkBrokerDead`
2. For each partition where the dead broker is the current leader:
   - Select the first ISR member that is not the dead broker
   - Propose `UpdateLeader { new_leader, epoch: current_epoch + 1 }`

This triggers `LeaderChanged` notifications on all brokers via the state machine broadcast.

### 6.5 Bootstrap

The lowest-ID broker calls `raft.initialize()` with all peer node IDs and addresses as initial Raft membership. All brokers must be up before the first `initialize` call. After Raft elects a leader, each broker proposes `RegisterBroker` for itself.

Single-broker mode (no `--peers`): Raft is not initialized; the server runs identically to a standalone broker with HWM = LEO.

---

## 7. Consumer Groups

Consumer group state lives entirely in the Raft state machine. All mutating operations (join, leave, heartbeat, offset commit) are Raft proposals; read-only operations (fetch offsets, list groups, describe group) read directly from the local state machine copy.

### 7.1 Lifecycle

```
JoinGroup → assigned partitions + generation_id
  └─ loop:
       Fetch → records
       CommitOffset (auto or manual)
       ConsumerHeartbeat → rebalance_required?
         └─ if true: rejoin
LeaveGroup
```

### 7.2 Partition assignment

On each join or leave, the state machine recomputes group assignments:

1. Collect the union of all subscribed topics across all members
2. Enumerate all `(topic, partition_id)` pairs, sorted
3. Sort member IDs alphabetically
4. Round-robin distribute partitions across members: `partition[i] → members[i % len(members)]`

Generation ID increments on every join, leave, or session expiry.

### 7.3 Session expiry

The Raft leader's checker loop (same loop as broker heartbeats) calls `check_consumer_heartbeats(10_000)` every 5 seconds. Timed-out members receive `RemoveExpiredMember` proposals, which trigger a rebalance for the group.

A consumer detects that a rebalance has occurred when its `ConsumerHeartbeat` response returns `rebalance_required = true` (generation mismatch).

---

## 8. Advanced Features

### 8.1 Idempotent producers

Producers call `InitProducerId` to obtain a `(producer_id, producer_epoch)` pair from the controller (allocated via a monotonically increasing counter in Raft state). Each `ProduceRequest` then includes `producer_id`, `producer_epoch`, and `first_sequence`.

The leader maintains `ProducerSequenceState` per producer in `LeaderState`:

```rust
struct ProducerSequenceState {
    producer_epoch: u16,
    last_sequence: u32,
    last_offset: u64,
}
```

On produce, `check_sequence` returns one of:

| Result | Meaning |
|---|---|
| `Accept` | New sequence; proceed |
| `Duplicate` | Same sequence seen before; return existing offset, skip append |
| `OutOfOrder` | Gap in sequence; return error |
| `FencedEpoch` | Producer epoch is stale; producer has been fenced |

On promotion to leader, `rebuild_producer_state` scans the log to restore dedup state from records with `producer_id != 0`.

### 8.2 Exactly-once semantics (transactions)

Transactions implement a two-phase protocol coordinated through the Raft state machine.

**Producer flow:**

```
InitProducerId(transactional_id)     → (producer_id, epoch)
BeginTransaction(producer_id)
AddPartitionsToTxn(producer_id, partitions)
Produce(is_transactional=true, producer_id, ...)   [for each record]
AddOffsetsToTxn(producer_id, group_id)             [optional: atomic offset commit]
TxnOffsetCommit(producer_id, group_id, offsets)
EndTransaction(producer_id, commit=true|false)
```

**Commit flow (inside `EndTransaction`):**

1. Controller transitions transaction to `PrepareCommit`, returns affected partitions
2. Server sends `WriteTxnMarkers` to each partition leader
3. Each leader appends a control record (`is_control=true`, `is_transactional=true`) with a COMMIT or ABORT key
4. Server proposes `WriteTxnMarkerComplete` to finalize; controller transitions to `CompleteCommit` and applies any pending offset commits

**Last stable offset (LSO):**

`LeaderState` tracks `ongoing_txns: HashMap<producer_id, first_offset>`. LSO is:

```
LSO = min(earliest ongoing_txn first_offset, HWM)
```

`READ_COMMITTED` consumers read only up to LSO and filter out:
- Control records (`is_control = true`)
- Transactional records from producers whose transactions appear in the `aborted_txns` list

`READ_UNCOMMITTED` consumers read up to HWM with no filtering (default behavior).

### 8.3 Time index and time-travel reads

The `TimeIndex` per segment stores `(timestamp_ms, relative_offset)` for every record. `Log::find_offset_by_timestamp(ts)` binary-searches across segments to return the earliest offset at or after `ts`.

The `OffsetForTimestamp` RPC exposes this. The CLI `consume --from-timestamp <ms>` uses it to resolve a start offset before initiating a standard fetch.

### 8.4 Stream processing

`chronicle-stream` provides a typed operator pipeline:

```rust
pub trait Operator: Send + Sync {
    fn process(&self, record: ProcessingRecord) -> Vec<OutputRecord>;
}
```

Built-in operators: `FilterOperator`, `MapOperator`, `FlatMapOperator`, `RegexFilterOperator`, `PassthroughOperator`.

`StreamProcessor` runs a consumer-producer loop. In **exactly-once mode**, each batch is wrapped in a transaction:

```
BeginTransaction
  Fetch batch from source partitions
  Run through operator chain
  Produce to sink (AddPartitionsToTxn first)
  AddOffsetsToTxn
EndTransaction(commit)
```

In **at-least-once mode**, offsets are committed after successful produce without a transaction envelope.

Stream job configurations are stored in the Raft state machine (`StreamJobMeta`) and managed via `CreateStreamJob` / `ListStreamJobs` / `DescribeStreamJob` / `DeleteStreamJob` RPCs. `RunStreamJob` in the CLI fetches the config from the cluster and runs the processor locally.

---

## 9. Write Path

```
Producer
  │
  ├─ route_partition(key) → partition_id (crc32 or round-robin)
  │
  └─ ProduceRequest { topic, partition, key, value, acks, producer_id, epoch, sequence }
       │
       ▼
  [Broker: partition leader]
       │
       ├─ Leadership check (ReplicaManager::is_leader)
       │     └─ NOT_LEADER → return error with leader_broker_id hint
       │
       ├─ Idempotency check (if producer_id != 0)
       │     ├─ Duplicate → return existing offset (no append)
       │     ├─ OutOfOrder / FencedEpoch → return error
       │     └─ Accept → continue
       │
       ├─ log.append_with_producer(...) → offset
       │
       ├─ replica_manager.record_sequence(...)
       │
       ├─ [if transactional] replica_manager.track_transaction_record(...)
       │
       └─ acks handling:
             ACKS_NONE   → respond immediately
             ACKS_LEADER → respond now
             ACKS_ALL    → wait on hwm_rx until HWM >= offset
```

Concurrently, followers pull via `ReplicateFetch`:

```
[Follower broker]
  FollowerFetcher loop:
    ReplicateFetch { fetch_offset = local_LEO, leader_epoch }
    → leader reads log, updates follower LEO, recomputes HWM
    → follower appends records at exact offsets (log.append_at)
    → follower updates local HWM
```

---

## 10. Read Path

```
Consumer
  │
  └─ FetchRequest { topic, partition, offset, max_records, isolation_level }
       │
       ▼
  [Broker: partition leader]
       │
       ├─ Leadership check
       │
       ├─ Determine upper bound:
       │     READ_UNCOMMITTED → HWM
       │     READ_COMMITTED   → LSO
       │
       ├─ log.read(offset, max_records) capped at upper bound
       │
       ├─ [READ_COMMITTED only]
       │     filter control records
       │     filter records from aborted transactions
       │
       └─ FetchResponse { records, high_watermark }
```

Consumers below the high watermark are served entirely from disk using the offset index (binary search to segment, then sequential scan). No in-memory record buffering.

---

## 11. Failure Model

### Leader failure

1. Followers stop receiving fetch responses; `FollowerFetcher` backs off
2. Raft leader's `check_heartbeats` detects missed heartbeats after 15 s
3. `MarkBrokerDead` + `UpdateLeader` proposed and committed in Raft
4. All brokers receive `LeaderChanged` via state machine broadcast
5. New leader: `promote_to_leader(epoch + 1)`, resumes serving writes
6. Old followers: `demote_to_follower`, spawn new `FollowerFetcher` to new leader
7. Clients receive `NOT_LEADER_FOR_PARTITION` (with new leader hint) and must retry

No data loss for any record that was committed (HWM-advanced) before the failure, because HWM only advances when a quorum of ISR members have replicated the record.

### Follower failure

- Partition writes continue as long as the remaining ISR has quorum
- Failed follower is evicted from ISR after `isr_lag_max` (default 10 s)
- `UpdateISR` proposed to Raft
- HWM recomputed on reduced ISR

### Controller (Raft) failure

Raft handles this natively. If the Raft leader fails, the remaining nodes elect a new leader. Metadata operations are blocked during election (typically < 1 s). The new leader resumes `check_heartbeats` and `check_consumer_heartbeats`.

### Partial writes / disk corruption

Handled by segment crash recovery at startup: the active segment's tail is scanned record-by-record; the first corrupt record causes the file to be truncated there. Sealed segments are not re-scanned on every startup.

---

## 12. Known Limitations

**In-memory Raft state machine.** The controller state machine (`ClusterState`) is held in `RwLock<StateMachineData>` with no durable backing store. The `openraft` log store is also in-memory (`BTreeMap`). Topic partition files survive broker restarts, but cluster metadata (topic assignments, consumer group state, transaction state, leader epochs) does not persist across restarts unless a Raft snapshot was installed before shutdown.

**No log retention.** Segments are never deleted. A long-running cluster will grow disk usage without bound.

**No log compaction.** The log is append-only with no key-based compaction.

**Single listener port.** Both `Chronicle` and `ControllerService` gRPC services share a single address. There is no port separation between client-facing and internal traffic.

**Static cluster membership.** Adding or removing brokers requires restarting all nodes with updated `--peers`. Dynamic membership changes via Raft `change_membership` are not exposed.

**No TLS.** All gRPC connections are plaintext.

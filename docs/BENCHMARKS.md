# Chronicle Benchmarks

## Environment

| Field | Value |
|---|---|
| CPU | Apple M3 |
| RAM | 16 GiB |
| OS | macOS (darwin 24.6.0) |
| Rust | 1.94.0 |
| Criterion | 0.8.2 |
| Profile | `release` |
| Measurement | 100 samples per benchmark, 3 s warm-up |

Benchmarks:

| Crate | Bench file | Run command |
|---|---|---|
| `chronicle-storage` | `crates/storage/benches/storage.rs` | `cargo bench --bench storage -p chronicle-storage` |
| `chronicle-replication` | `crates/replication/benches/replication.rs` | `cargo bench --bench replication -p chronicle-replication` |
| `chronicle-controller` | `crates/controller/benches/state_machine.rs` | `cargo bench --bench state_machine -p chronicle-controller` |
| `chronicle-stream` | `crates/stream/benches/stream.rs` | `cargo bench --bench stream -p chronicle-stream` |

---

## Storage Benchmarks (`chronicle-storage`)

### 1. Record Encode

Measures the CPU cost of serializing one `Record` into a `Vec<u8>`, including CRC-32C computation.

| Payload | Mean time | Throughput |
|---|---|---|
| 16 B key / 64 B val | 38.9 ns | 3.14 GiB/s |
| 32 B key / 1 KB val | 150 ns | 6.86 GiB/s |
| 64 B key / 64 KB val | 7.37 µs | 8.29 GiB/s |

**Observation:** Throughput scales well with larger payloads because the per-call overhead (CRC setup, Vec allocation, field writes) is amortized. Small records are encode-overhead dominated (~2/3 of the 80-byte encoded record is fixed-width metadata).

---

### 2. Record Decode

Measures `Record::decode` on a pre-encoded byte buffer (no disk I/O).

| Payload | Mean time | Throughput |
|---|---|---|
| 16 B key / 64 B val | 106.6 ns | 1.14 GiB/s |
| 32 B key / 1 KB val | 228.6 ns | 4.51 GiB/s |
| 64 B key / 64 KB val | 10.56 µs | 5.79 GiB/s |

**Observation:** Decode is ~2.5× slower than encode for small records. This is expected: decode allocates a fresh `Vec<u8>` for the payload, then allocates `Bytes` for key and value. Encode allocates one `Vec` and writes into it sequentially. For large records (64 KB value), the `memcpy` cost dominates both directions and they converge.

---

### 3. Segment Append

Measures a single `Segment::append` call: CRC + encode, `write_all` to the OS page cache, and in-memory index/time-index updates. No `fsync`.

| Payload | Mean time | Throughput |
|---|---|---|
| 16 B key / 64 B val | 5.85 µs | 21.3 MiB/s |
| 32 B key / 1 KB val | 6.88 µs | 153 MiB/s |
| 64 B key / 64 KB val | 41.6 µs | 1.47 GiB/s |

**Observation:** The dominant cost for small records is not I/O but syscall overhead — each `append` issues at minimum three `write` syscalls (log file, index file, time-index file). The throughput jumps from 21 MiB/s for 80-byte records to 153 MiB/s for 1 KB records because the per-syscall cost is fixed. For 64 KB records, memcpy becomes the bottleneck and throughput approaches raw memory bandwidth.

**Root cause of low small-record throughput:** `Index::append` and `TimeIndex::append` each issue two separate `write_all` calls (one per field). Combined with the log file write, every append makes at least five syscalls. Merging these into a single write per file would roughly halve the syscall count. See [Bug/Observation #1](#1-multiple-write-syscalls-per-append) in `bug_tracking.md`.

---

### 4. Segment Read

#### Random read (`read_at` — single record, index lookup + seek + decode)

| Payload | Mean time | Throughput |
|---|---|---|
| 16 B key / 64 B val | 961 ns | 130 MiB/s |
| 32 B key / 1 KB val | 1.07 µs | 991 MiB/s |

**Observation:** Both cases access the midpoint of a 10,000-record segment. The in-memory index binary search is O(log n) and takes ~6–14 ns (see §7). The remaining time is a `BufReader` seek + a decode. Small records have lower throughput because the per-operation overhead dominates.

#### Sequential batch read (`read_from` — 1,000 records from offset 0)

| Batch | Mean time | Throughput |
|---|---|---|
| 1,000 × (16 B key / 64 B val) | 126.9 µs | 984 MiB/s |

**Observation:** Sequential reads are limited by `BufReader` + `Record::decode` throughput. At ~980 MiB/s, we are close to the in-memory decode ceiling seen in §2. This confirms that reads are CPU-bound on record deserialization, not I/O-bound (data is in OS page cache after initial population).

---

### 5. Log Append

Measures `Log::append`, which adds a segment-roll check on top of `Segment::append`.

#### Without segment rolling (512 MiB max segment)

| Payload | Mean time | Throughput |
|---|---|---|
| 16 B key / 64 B val | 5.90 µs | 21.2 MiB/s |
| 32 B key / 1 KB val | 6.87 µs | 153 MiB/s |
| 64 B key / 64 KB val | 62.5 µs | 1.00 GiB/s |

Results match the `Segment::append` numbers almost exactly — the extra size check adds no measurable overhead.

#### With aggressive segment rolling (512 B max segment)

| Payload | Mean time | Throughput |
|---|---|---|
| 32 B key / 32 B val | 44.7 µs | 1.94 MiB/s |

**Observation:** Each append triggers a segment roll: `Segment::create` opens three new files (`.log`, `.index`, `.timeindex`) and writes to them. This is ~8× slower than a no-roll append of the same payload. Segment rolling is expected to be infrequent in production (default 10 MiB segments), but this quantifies the cost when it does occur.

---

### 6. Log Read

Measures `Log::read` across a pre-populated 100,000-record log (all records in one segment due to 512 MiB limit). Benchmarks both reads from offset 0 and reads from the midpoint (~50,000).

| Batch | Start offset | Mean time | Throughput |
|---|---|---|---|
| 100 records | 0 | 13.5 µs | 927 MiB/s |
| 100 records | 50,000 | 13.2 µs | 944 MiB/s |
| 1,000 records | 0 | 129.4 µs | 966 MiB/s |
| 1,000 records | 50,000 | 128.7 µs | 971 MiB/s |
| 10,000 records | 0 | 1.37 ms | 914 MiB/s |
| 10,000 records | 50,000 | 1.30 ms | 958 MiB/s |

**Observation:** Throughput is consistent at ~930–970 MiB/s regardless of start offset. The in-memory binary-search index makes middle-of-log seeks as fast as seeks from offset 0. Throughput is stable across batch sizes, indicating consistent decode throughput with no cache effects at this working set size. The 10k-record batch shows slightly higher variance (more time in GC-like flush pressure from allocating many `Record` structs).

---

### 7. Log Open (Crash Recovery)

Measures `Log::open` on a pre-existing single-segment log. Every open re-runs `Segment::recover`, which scans every record in the active segment, rebuilds the `.index` and `.timeindex` files.

| Record count | Mean time | Records/s | MiB/s (approx) |
|---|---|---|---|
| 1,000 | 639 µs | 1.56 M/s | 121 MiB/s |
| 10,000 | 3.99 ms | 2.51 M/s | 194 MiB/s |
| 100,000 | 39.3 ms | 2.54 M/s | 197 MiB/s |

**Observation:** Recovery throughput scales linearly — 10× more records takes ~10× longer. The 1k case is slightly slower per record because `Log::open` overhead (directory scan, metadata parsing) is a larger fraction of the total. At 100k records (~7.6 MiB on disk), recovery takes 39 ms.

**Critical finding:** With the default 10 MiB segment, an active segment can hold ~131,000 records of the smallest payload. Crash recovery would scan all of them before the broker starts accepting connections. For a production deployment expecting rapid restarts, this is a meaningful startup cost. See [Bug/Observation #2](#2-crash-recovery-always-rebuilds-index-even-when-uncorrupted) in `bug_tracking.md`.

**Scaling projection:**

| Segment size | Approx records (80 B each) | Recovery time |
|---|---|---|
| 10 MiB (default) | ~130,000 | ~51 ms |
| 100 MiB | ~1,300,000 | ~510 ms |
| 1 GiB | ~13,000,000 | ~5 s |

---

### 8. Timestamp Lookup (`find_offset_by_timestamp`)

Measures the full in-memory `Log::find_offset_by_timestamp` over a 100,000-record log.

| Position | Mean time |
|---|---|
| Start (first record) | 15.6 ns |
| Middle (50,000th record) | 16.7 ns |
| End (last record) | 15.6 ns |

**Observation:** All positions resolve in ~16 ns. This is a pure in-memory binary search over the `TimeIndex` entries. The single-segment case avoids any inter-segment bisection overhead. Result is essentially free from the caller's perspective.

---

### 9. Index Lookup

Measures `Index::lookup` (binary search over in-memory `Vec<IndexEntry>`).

| Index size | Mean time | Throughput |
|---|---|---|
| 1,000 entries | 6.5 ns | 153 M lookups/s |
| 100,000 entries | 14.4 ns | 69 M lookups/s |

**Observation:** The 2.2× latency increase from 1k to 100k entries matches the O(log n) prediction: log₂(100,000)/log₂(1,000) ≈ 2.2. The entire index fits in L1/L2 cache for 1k entries (~8 KB), whereas 100k entries (~800 KB) spill into L2/L3.

---

## Replication Benchmarks (`chronicle-replication`)

Benchmarks cover the hot paths exercised on every produce and fetch request: partition assignment computation, leader checks, idempotent sequence tracking, high-watermark recomputation, and ISR expiry scanning.

---

### 10. Partition Assignment Computation

`assignment::compute_assignments` builds the complete replica placement for all partitions round-robining across brokers. Called on topic creation (via Raft command) and on broker registration.

| Partitions / RF / Brokers | Mean time | Throughput |
|---|---|---|
| 8p / RF=3 / 3 brokers | 357 ns | 22.4 M partitions/s |
| 64p / RF=3 / 3 brokers | 2.56 µs | 25.0 M partitions/s |
| 1000p / RF=3 / 10 brokers | 39.3 µs | 25.4 M partitions/s |

**Observation:** Throughput is constant at ~25 M partitions/s regardless of scale — the algorithm is a tight iterator chain with no branching or lookups, so it is compute-bound by the `Vec` allocations per `PartitionAssignment`. The 1,000-partition case allocates 1,000 `Vec<u32>` for replica lists; switching to a flat representation would eliminate the per-partition allocation.

---

### 11. ReplicaManager Hot Paths

#### `is_leader` (checked on every produce request)

| Variant | Mean time | Throughput |
|---|---|---|
| `is_leader` (registered topic, true) | 37.2 ns | 26.9 M ops/s |
| `is_leader` (unknown topic) | 31.6 ns | 31.6 M ops/s |

**Key finding:** Both variants have essentially the same cost. The bottleneck is **not** the HashMap miss but the allocation of a temporary `PartitionKey { topic: topic.to_string(), partition }` on every call. Every produce and fetch request allocates a `String` just to perform the HashMap lookup, then immediately drops it. See [Bug #6](#6-partitionkey-allocates-a-string-on-every-hot-path-lookup) in `bug_tracking.md`.

#### `check_sequence` (idempotent producer dedup)

| Path | Mean time | Throughput |
|---|---|---|
| Non-idempotent (producer_id=0, early return) | 3.27 ns | 306 M ops/s |
| Accept — next sequence | 42.6 ns | 23.5 M ops/s |
| Duplicate detection | 45.2 ns | 22.1 M ops/s |
| Out-of-order detection | 45.0 ns | 22.2 M ops/s |

**Observation:** The non-idempotent fast-path (used by non-transactional producers) is 13× faster than the full idempotent check. Duplicate and out-of-order checks are identical in cost — both reach the same HashMap depth in `producer_states`. The ~42 ns for the idempotent path covers: `to_string()` for `PartitionKey`, `RwLock::read()` acquire + release, and two nested HashMap lookups.

#### `record_sequence` (write path — after each append)

| Benchmark | Mean time | Throughput |
|---|---|---|
| `record_sequence` | 40.4 ns | 24.8 M ops/s |

**Observation:** Write-lock cost dominates. The actual `HashMap::insert` is fast; the 40 ns is `to_string()` + `RwLock::write()` + insert. Nearly identical to the `check_sequence` accept path.

#### `update_follower_progress` (per-ReplicateFetch call)

| ISR size | Mean time | Throughput |
|---|---|---|
| Leader only (no followers) | 91.2 ns | 11.0 M ops/s |
| 1 follower | 98.0 ns | 10.2 M ops/s |
| 3 followers | 102.2 ns | 9.8 M ops/s |

**Observation:** The 11 ns increase from 1 to 3 followers is very small — `recompute_hwm` iterates over the ISR vec which has only 3 elements. The dominant cost (91 ns baseline) is acquiring the write lock + `to_string()` + `leader_leo_inner()` which acquires a second read lock on the partition `Log` while the `ReplicaManager` write lock is already held. See [Bug #7](#7-check_isr_expiry-holds-global-write-lock-across-all-partitions) in `bug_tracking.md`.

---

### 12. ISR Expiry Check (background task)

`check_isr_expiry` runs every ~10 seconds to evict lagging followers from the ISR. It acquires the global `ReplicaManager` write lock and scans all partitions.

| Partitions | Mean time |
|---|---|
| 10 | 142 ns |
| 100 | 1.13 µs |
| 1,000 | 14.7 µs |

**Key finding:** Scaling is linear (~14 ns/partition). During those 14.7 µs for a 1,000-partition broker, **no produce or fetch can proceed** because the global write lock is held. With typical 10-second ISR check intervals the absolute time is negligible, but the lock stall duration grows linearly with partition count. See [Bug #7](#7-check_isr_expiry-holds-global-write-lock-across-all-partitions) in `bug_tracking.md`.

---

## Controller / Raft State Machine Benchmarks (`chronicle-controller`)

These benchmarks measure the synchronous core of the Raft state machine — the `apply_command` function — without Raft consensus overhead. Every command is applied to an in-memory `StateMachineData` struct.

---

### 13. Heartbeat Command

The most frequent Raft command: brokers heartbeat every few seconds.

| Benchmark | Mean time | Throughput |
|---|---|---|
| `Heartbeat` | 25.0 ns | 40.0 M ops/s |

**Observation:** Purely a `HashMap::get_mut` + timestamp write. 25 ns is dominated by the `HashMap` lookup on the broker_id. This is effectively free in steady state.

---

### 14. `CreateTopic` (partition assignment via Raft)

Includes `compute_assignments` + `HashMap::insert` for the topic metadata.

| Partitions / RF / Brokers | Mean time | Throughput |
|---|---|---|
| 8p / RF=3 / 3 brokers | 3.06 µs | 2.61 M partitions/s |
| 64p / RF=3 / 3 brokers | 16.9 µs | 3.80 M partitions/s |
| 100p / RF=3 / 5 brokers | 25.6 µs | 3.90 M partitions/s |

**Observation:** Most of the cost is `compute_assignments` (see §10) plus allocating and inserting all partition metadata into the ClusterState HashMaps. Per-partition throughput (~3–4 M/s) is 6–8× lower than raw `compute_assignments` because of the additional metadata allocation.

---

### 15. `JoinGroup` (consumer group partition assignment)

`JoinGroup` calls `compute_group_assignments` which sorts all member IDs, iterates all subscribed partitions, and round-robins assignment.

| Partitions / Members | Mean time |
|---|---|
| 4p / 1 member | 2.63 µs |
| 32p / 4 members | 10.6 µs |
| 100p / 10 members | 27.8 µs |

**Key finding:** Scaling is roughly O(partitions × members). Each `JoinGroup` event triggers a full partition reassignment across all members. With 100 partitions and 10 consumers (~1,000 topic-partition-member combinations), one consumer join takes 27.8 µs. Under rapid consumer churn (rebalances), this cost multiplies. See [Bug #8](#8-compute_group_assignments-called-on-every-joinleave-is-on-every-event) in `bug_tracking.md`.

---

### 16. `CommitOffset`

The most frequent consumer-side Raft command.

| Partitions committed | Mean time | Throughput |
|---|---|---|
| 1 partition | 71.8 ns | 13.9 M ops/s |
| 10 partitions | 401 ns | 24.9 M partitions/s |
| 100 partitions | 4.36 µs | 22.9 M partitions/s |

**Observation:** Cost is O(n) in the number of offsets committed — each offset is a `HashMap::insert`. The overhead per partition is ~40 ns (matching a single `HashMap::insert` for the `TopicPartitionKey`).

---

### 17. Raft Snapshot Serialization / Deserialization

On Raft log compaction, the entire `StateMachineData` is serialized to JSON. On snapshot install, it is deserialized.

| Cluster size | Serialize | Deserialize | Ratio |
|---|---|---|---|
| 3 brokers / 4 topics / 2 groups | 2.76 µs | 8.22 µs | 3.0× |
| 10 brokers / 50 topics / 10 groups | 21.5 µs | 77.8 µs | 3.6× |

**Key finding:** Deserialization is ~3–3.6× slower than serialization. For a cluster with 50 topics and 10 consumer groups, a snapshot install costs 77.8 µs just for JSON parsing. A snapshot install blocks the Raft state machine. Switching to a binary format (e.g., `bincode` or `rmp-serde`) would reduce both directions by ~5–10×.

---

## Stream Benchmarks (`chronicle-stream`)

Benchmarks cover individual operators, operator pipelines, and batch throughput.

---

### 18. Individual Operators

| Operator | Variant | Mean time | Notes |
|---|---|---|---|
| `Passthrough` | small value (11 B) | 60.4 ns | Baseline: cost is `StreamRecord::clone()` |
| `Passthrough` | large value (130 B) | 60.7 ns | Same cost — `Bytes::clone()` is O(1) |
| `Filter` | pass (clone path) | 56.9 ns | ~same as `Passthrough` |
| `Filter` | drop (no clone) | 1.11 ns | Returns empty `Vec` — no allocation |
| `Map` | value transform | 199.9 ns | `record.clone()` + `format!()` + new `Bytes` |
| `FlatMap` | 1→1 | 59.5 ns | Equivalent to `Passthrough` |
| `FlatMap` | 1→5 | 269 ns | 53.8 ns/output record |
| `FlatMap` | 1→100 | 5.11 µs | 51.1 ns/output record |

**Key finding:** `Passthrough` and `filter_pass` cost the same (~60 ns) regardless of payload size. The bottleneck is **not** the `Bytes` clone (which is O(1) reference count) but the `String::clone()` for the `topic` field in `StreamRecord`. A 60 ns String clone is the minimum cost for any operator that produces output. See [Bug #9](#9-streamrecordtopic-is-string-heap-allocation-on-every-operator-output) in `bug_tracking.md`.

`filter_drop` costs 1.11 ns — sub-nanosecond predicate evaluation — confirming the filter fast path adds essentially no overhead when records are rejected.

---

### 19. RegexFilter

| Pattern | Match? | Mean time |
|---|---|---|
| `hello` (simple) | yes | 75.3 ns |
| `hello` (simple) | no | 19.1 ns |
| `^\d{4}-\d{2}-\d{2}$` (date) | yes | 82.9 ns |
| `^\d{4}-\d{2}-\d{2}$` (date) | no | 17.9 ns |
| 8-way alternation | yes | 71.7 ns |
| 8-way alternation | no | 13.9 ns |

**Key finding:** Non-match paths are 4–5× faster than match paths because they skip the `record.clone()`. When the regex fails, only the predicate evaluation runs. When it succeeds, the full clone (60 ns String allocation) dominates. The regex evaluation itself adds only ~12–15 ns on top of the clone cost.

---

### 20. Operator Pipelines

| Pipeline | Mean time | Throughput |
|---|---|---|
| `Filter → Map → Filter` (3 ops) | 410 ns | 2.44 M records/s |
| `Filter → Map → Filter → Map → Filter` (5 ops) | 629 ns | 1.59 M records/s |

**Observation:** Each additional operator stage adds approximately one `record.clone()` cost (~60 ns). The 5-op pipeline is 220 ns slower than the 3-op pipeline, consistent with two additional clones.

---

### 21. Pipeline Batch Throughput

A 3-stage pipeline (`Filter → Map → Passthrough`) processing batches of records.

| Batch size | Mean time | Throughput |
|---|---|---|
| 100 records | 15.8 µs | 6.32 M records/s |
| 1,000 records | 146.5 µs | 6.82 M records/s |
| 10,000 records | 1.49 ms | 6.72 M records/s |

**Observation:** Throughput stabilizes at ~6.7 M records/s above 1k-record batches. This is consistent with ~150 ns/record (60 ns clone × 2 stage transitions + Vec allocation overhead). Throughput does not degrade at larger batch sizes, indicating no cache pressure at this scale — all working memory fits in L2/L3.

---

## Cross-Crate Summary

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

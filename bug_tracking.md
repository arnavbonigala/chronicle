# Bug Tracking

Issues and performance observations surfaced during benchmarking (March 2026).

---

## #1 Multiple write syscalls per index append

**Kind:** Performance / design  
**File:** `crates/storage/src/index.rs`, `crates/storage/src/time_index.rs`  
**Severity:** Medium — limits small-record write throughput

### Description

`Index::append` issues two separate `write_all` calls:

```rust
// index.rs
self.file.write_all(&relative_offset.to_be_bytes())?;  // 4 bytes
self.file.write_all(&position.to_be_bytes())?;          // 4 bytes
```

`TimeIndex::append` does the same (8-byte timestamp + 4-byte offset = two writes). Combined with the log file write, every `Segment::append` call issues at least **five syscalls**:

1. `.log` file: `write_all(record_bytes)`
2. `.index` file: `write_all(&relative_offset)` (4 bytes)
3. `.index` file: `write_all(&position)` (4 bytes)
4. `.timeindex` file: `write_all(&timestamp_ms)` (8 bytes)
5. `.timeindex` file: `write_all(&relative_offset)` (4 bytes)

### Steps to reproduce

Run the segment append benchmark:
```bash
cargo bench --bench storage -p chronicle-storage -- segment_append
```

Observe that small-record (80 B) throughput is ~21 MiB/s — roughly 170,000 records/second — while decode throughput is >1 GiB/s and raw page-cache write bandwidth is much higher. The gap is syscall overhead, not I/O or CPU.

### Fix

Combine each pair of writes into a single stack-allocated buffer:

```rust
// index.rs: replace two write_all calls with one
let mut buf = [0u8; 8];
buf[0..4].copy_from_slice(&relative_offset.to_be_bytes());
buf[4..8].copy_from_slice(&position.to_be_bytes());
self.file.write_all(&buf)?;
```

Apply the same pattern to `TimeIndex::append` (12-byte entry). This halves the syscall count for index + time-index writes.

For further improvement, wrap the log file and both index files in `BufWriter` and flush on segment roll or explicit `Segment::flush()`. This would batch many small writes into a single syscall and push small-record throughput toward the memory-bandwidth ceiling (~1 GiB/s).

---

## #2 Crash recovery always rebuilds the index

**Kind:** Performance  
**File:** `crates/storage/src/segment.rs`, `Segment::recover`  
**Severity:** Medium — increases broker startup time proportionally with active segment size

### Description

`Segment::recover` always rewrites both the `.index` and `.timeindex` files, even when no corruption is found:

```rust
pub fn recover(&mut self) -> Result<()> {
    // ...
    // Even if no truncation occurred and valid_size == original size:
    self.index = Index::rebuild(&index_path, &log_path, self.base_offset)?;
    self.time_index = TimeIndex::rebuild(&timeindex_path, &log_path, self.base_offset)?;
    Ok(())
}
```

`Index::rebuild` scans the entire `.log` file, then truncates and rewrites the `.index` file. On a clean restart (no corruption), this is wasted I/O.

### Steps to reproduce

```bash
# Observe that log_open time scales linearly with record count
cargo bench --bench storage -p chronicle-storage -- log_open
```

Benchmark results on Apple M3:

| Record count | Open time |
|---|---|
| 1,000 | 639 µs |
| 10,000 | 3.99 ms |
| 100,000 | 39.3 ms |

With the default 10 MiB segment and 80-byte records, an active segment holds ~130,000 records, so a clean restart takes ~50 ms just for recovery. At 100 MiB segments, this exceeds 500 ms.

### Fix

Track whether truncation was performed during the scan:

```rust
let truncated = valid_size < self.size;
self.size = valid_size;
self.next_offset = self.base_offset + count;

if truncated {
    self.index = Index::rebuild(&index_path, &log_path, self.base_offset)?;
    self.time_index = TimeIndex::rebuild(&timeindex_path, &log_path, self.base_offset)?;
} else {
    // Re-use the existing index files; they are consistent with the log.
    self.index = Index::load(&index_path)?;
    self.time_index = TimeIndex::load(&timeindex_path)?;
}
```

This makes clean restarts O(index_file_size) instead of O(log_file_size), typically 8–12× faster since the index is 8 bytes per record vs. 60–70 bytes per record on disk.

---

## #3 Vec allocation per record write

**Kind:** Performance  
**File:** `crates/storage/src/record.rs`, `Record::write_to`  
**Severity:** Low — contributes to allocation pressure at high write rates

### Description

Every `write_to` call allocates a fresh `Vec` and immediately drops it after the write:

```rust
pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
    let mut buf = Vec::with_capacity(self.encoded_size());
    self.encode(&mut buf);
    writer.write_all(&buf)?;
    Ok(())
}
```

At 170,000 writes/sec (small-record benchmark rate), this is 170,000 heap allocations and frees per second, all immediately short-lived.

### Steps to reproduce

This is observable in profiler output — look for high allocation count in the hot path. The benchmark shows small-record append latency of ~5.85 µs; a meaningful fraction of this is the allocation.

### Fix

Pass a reusable `&mut Vec<u8>` scratch buffer from the call site:

```rust
pub fn encode_into(&self, buf: &mut Vec<u8>) {
    buf.clear();
    // existing encode() logic...
}
```

Callers that own the segment (e.g., `write_record`) would hold one long-lived `encode_buf` field or pass it in, eliminating the per-record allocation.

---

## #4 `File::flush` in `write_record` is a no-op

**Kind:** Code clarity  
**File:** `crates/storage/src/segment.rs`, `Segment::write_record`  
**Severity:** Low — not a correctness issue, but potentially misleading

### Description

```rust
fn write_record(&mut self, record: &Record) -> Result<u64> {
    let position = self.size as u32;
    record.write_to(&mut self.log_file)?;
    self.log_file.flush()?;   // ← no-op on std::fs::File
    ...
}
```

`std::fs::File` has no internal write buffer. Its `flush()` implementation returns `Ok(())` immediately. Data reaches the OS page cache via `write_all` before `flush()` is called. This is not a data-loss bug — crash recovery handles uncommitted tail data. However, contributors might read `flush()` as "this write is durable", which it is not. Durability requires `sync_all()` (fsync), which is only done in `Segment::flush()`.

### Fix

Remove the `self.log_file.flush()?;` call from `write_record`. It has no effect and its presence implies a durability guarantee that does not exist. Add a comment on `write_record` clarifying that writes target the OS page cache only, with durability deferred to explicit `Segment::flush()`.

---

## #5 Index position field silently overflows for segments > 4 GiB

**Kind:** Correctness (edge case)  
**File:** `crates/storage/src/segment.rs`, `Segment::write_record`  
**Severity:** Low — unreachable with default configuration, latent with very large segment limits

### Description

The byte position of each record within its segment is tracked as `u32`:

```rust
let position = self.size as u32;  // truncates if size >= 4 GiB
```

`Index` also stores positions as `u32`. If a segment grows beyond `u32::MAX` (4,294,967,295 bytes ≈ 4 GiB), the position wraps silently. Subsequent index lookups would seek to the wrong byte offset, corrupting reads.

With the default `segment_max_bytes = 10 MiB`, this is unreachable. It becomes reachable if an operator sets `segment_max_bytes` to a value above 4 GiB, or if future code removes the segment size check.

### Steps to reproduce

1. Set `segment_max_bytes = u64::MAX` in `StorageConfig`.
2. Write enough large-value records to push segment size past 4 GiB.
3. Read a record whose physical position exceeds 4 GiB.
4. Observe a seek to the wrong file position, returning corrupt or wrong data.

### Fix

Change the position representation in `IndexEntry` and related I/O to `u64` (8 bytes per position field), or add an explicit assertion/error in `write_record`:

```rust
let position = u32::try_from(self.size).map_err(|_| StorageError::SegmentTooLarge {
    size: self.size,
})?;
```

The latter is a smaller change and provides a clear error rather than silent corruption.

---

## #6 `PartitionKey` allocates a `String` on every hot-path lookup

**Kind:** Performance  
**File:** `crates/replication/src/replica_manager.rs`  
**Severity:** Medium — adds ~30 ns per produce/fetch due to heap allocation in the hot path

### Description

Every `is_leader`, `check_sequence`, `record_sequence`, `update_follower_progress`, and `leader_for` call constructs a temporary `PartitionKey` using `topic.to_string()`:

```rust
let key = PartitionKey {
    topic: topic.to_string(),  // heap allocation every call
    partition,
};
state.get(&key)
```

This `String` is allocated on the heap, used for the HashMap lookup, then immediately dropped. On the benchmark:

| Variant | Mean time |
|---|---|
| `is_leader` (known topic, true) | 37.2 ns |
| `is_leader` (unknown topic) | 31.6 ns |

Both costs are nearly identical — confirming the HashMap miss cost is negligible and the dominant expense is the String allocation. At 26.9 M `is_leader` ops/s on a single partition, each produce request allocates and frees at minimum two temporary Strings (one for `is_leader`, one for `record_sequence`).

### Steps to reproduce

```bash
cargo bench --bench replication -p chronicle-replication -- is_leader
```

### Fix

Use a borrowed `HashMap` key via a custom `Borrow` implementation:

```rust
use std::borrow::Borrow;

// Lookup type that borrows &str instead of owning String
struct PartitionKeyRef<'a> {
    topic: &'a str,
    partition: u32,
}

// Implement Hash + Eq consistent with PartitionKey
// then use state.get(&lookup_ref)
```

Alternatively, intern topic strings at registration time (e.g., store `Arc<str>`) so that clone is O(1) reference count increment rather than a heap copy.

---

## #7 `check_isr_expiry` holds global write lock across all partitions

**Kind:** Performance  
**File:** `crates/replication/src/replica_manager.rs`, `ReplicaManager::check_isr_expiry`  
**Severity:** Medium — blocks all produce/fetch for the duration of the scan

### Description

`check_isr_expiry` acquires the single global `RwLock<HashMap<PartitionKey, PartitionReplicaState>>` write lock and scans every partition before releasing it:

```rust
pub fn check_isr_expiry(&self, lag_time_max: Duration) {
    let mut state = self.state.write().unwrap();  // global write lock
    let now = Instant::now();
    for (key, pstate) in state.iter_mut() {       // scan ALL partitions
        if let PartitionReplicaState::Leader(l) = pstate {
            l.isr.retain(...);
        }
    }
    // lock released here
}
```

Benchmark results (2 followers per partition):

| Partitions | Time with global write lock held |
|---|---|
| 10 | 142 ns |
| 100 | 1.13 µs |
| 1,000 | 14.7 µs |

During those 14.7 µs for a 1,000-partition broker, **every** concurrent `is_leader`, `check_sequence`, `record_sequence`, and `update_follower_progress` call is blocked. This is a stop-the-world pause on the produce/fetch path.

The same single-lock design also affects `update_follower_progress`, which holds the global write lock while calling `leader_leo_inner` — a function that acquires a second read lock on the partition's `Log`. This is a nested lock acquisition pattern that increases contention pressure.

### Steps to reproduce

```bash
cargo bench --bench replication -p chronicle-replication -- check_isr_expiry
```

### Fix

Replace the single global `RwLock<HashMap<...>>` with per-partition locks:

```rust
state: HashMap<PartitionKey, RwLock<PartitionReplicaState>>,
// wrapped in Arc to allow concurrent access without locking the whole map
```

ISR expiry would then hold only a per-partition write lock for the duration of a single partition scan (~14 ns), rather than the global lock for the full scan. Produce/fetch on unrelated partitions would be unaffected.

---

## #8 `compute_group_assignments` called on every join/leave is O(P × M)

**Kind:** Performance  
**File:** `crates/controller/src/state_machine.rs`, `JoinGroup` and `LeaveGroup` handlers  
**Severity:** Medium — rebalance latency grows with cluster size

### Description

Every `JoinGroup` and `LeaveGroup` event triggers a full partition reassignment across all consumer group members:

```rust
group.assignments = compute_group_assignments(&group.members, &sm.cluster_state.topics);
```

`compute_group_assignments` iterates all subscribed topics, collects all partitions, sorts all member IDs, then assigns partitions round-robin:

```rust
fn compute_group_assignments(...) {
    // collect all partitions from all subscribed topics
    let mut all_partitions = Vec::new();
    for topic_name in &subscribed {
        for p in 0..meta.partition_count { all_partitions.push(...); }
    }
    // sort members (for determinism)
    member_ids.sort();
    // round-robin assignment
    for (i, tp) in all_partitions.iter().enumerate() {
        assignments.get_mut(member_ids[i % member_ids.len()]).unwrap().push(tp.clone());
    }
}
```

Benchmark results:

| Partitions / Members | JoinGroup time |
|---|---|
| 4p / 1 member | 2.63 µs |
| 32p / 4 members | 10.6 µs |
| 100p / 10 members | 27.8 µs |

Each additional member joining a group with 100 partitions costs 27.8 µs as a synchronous Raft command. Under a rolling restart scenario with 50 consumers, this generates 50 × 27.8 µs = 1.39 ms of serial Raft commands, plus the Raft consensus latency per command.

Additionally, `TopicPartitionKey` derives `Clone` and contains two owned heap values (`topic: String`, `partition: u32`). Every partition in `all_partitions` is a `String` clone.

### Steps to reproduce

```bash
cargo bench --bench state_machine -p chronicle-controller -- join_group
```

### Fix

For the assignment recomputation, avoid full recloning of `TopicPartitionKey` by storing `Arc<str>` for topic names. For the rebalance algorithm itself, incremental assignment (only move partitions from members who left/joined) would reduce the Raft command cost from O(P × M) to O(P / M) in the common case where one member joins or leaves.

---

## #9 `StreamRecord::topic` is `String` — heap allocation on every operator output

**Kind:** Performance  
**File:** `crates/stream/src/operator.rs`, `StreamRecord`  
**Severity:** Low — caps single-operator throughput at ~16 M records/s

### Description

`StreamRecord` contains `topic: String`:

```rust
pub struct StreamRecord {
    pub key: Bytes,         // O(1) clone — reference counted
    pub value: Bytes,       // O(1) clone — reference counted
    pub timestamp_ms: u64,  // Copy
    pub offset: u64,        // Copy
    pub topic: String,      // O(n) clone — heap allocation every clone
    pub partition: u32,     // Copy
}
```

Every operator that produces output calls `record.clone()`, which allocates a new `String` for `topic`. Benchmark evidence:

| Operator | Variant | Time |
|---|---|---|
| `Passthrough` | any payload size | ~60 ns |
| `Filter` | drop (no clone) | 1.11 ns |

The 59 ns gap between pass and drop is entirely the `String::clone()` cost. `Bytes::clone()` is O(1) (atomic reference count), so payload size does not matter — confirmed by `passthrough_small` (60.4 ns) vs `passthrough_large` (60.7 ns) being identical.

At 16.5 M records/s, a single pipeline stage processing 1 M records/s would spend ~60 ms/s on String heap allocations alone.

### Steps to reproduce

```bash
cargo bench --bench stream -p chronicle-stream -- passthrough
cargo bench --bench stream -p chronicle-stream -- filter
```

### Fix

Change `topic: String` to `topic: Arc<str>` or `topic: Bytes`:

```rust
pub struct StreamRecord {
    pub topic: Arc<str>,  // O(1) clone — same as Bytes
    ...
}
```

`Arc<str>` clones in ~10 ns (atomic increment) vs ~50–60 ns for a short `String` clone. This would roughly halve the per-record cost for any pass-through operator, pushing throughput toward ~32 M records/s.

---

## #10 `rebuild_producer_state` reads entire log into memory

**Kind:** Performance  
**File:** `crates/replication/src/replica_manager.rs`, `ReplicaManager::rebuild_producer_state`  
**Severity:** Low — only called at startup/leadership change, but memory-intensive for large logs

### Description

```rust
pub fn rebuild_producer_state(&self, topic: &str, partition: u32, log: &chronicle_storage::Log) {
    let records = log
        .read(log.earliest_offset(), u32::MAX)  // reads ALL records into a Vec
        .unwrap_or_default();
    ...
}
```

`u32::MAX` (4,294,967,295) as `max_records` means this attempts to read the entire partition log into a `Vec<Record>` in memory. For a log with 1M records at 80 bytes/record, this allocates ~80 MiB. For a log with 100k records at 1 KB/record, this allocates ~100 MiB.

### Steps to reproduce

Deploy a broker, write 1M records to a partition, then trigger a leader change. The new leader calls `rebuild_producer_state` which reads all 1M records into memory before accepting any produce requests.

### Fix

Scan records sequentially without materializing the full `Vec`, keeping only the last sequence number seen per producer ID. Alternatively, scan from the *end* of the log backward (since we only need the most recent sequence per producer):

```rust
// Only need the last N offsets per producer — scan recent tail only
let tail_size = 1_000_000;  // configurable
let start = log.latest_offset().saturating_sub(tail_size);
let records = log.read(start, tail_size as u32).unwrap_or_default();
```

For a fully correct implementation without memory spikes, implement a streaming iterator over the log segments rather than materializing into a Vec.

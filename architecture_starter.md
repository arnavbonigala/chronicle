# Architecture: Distributed Log System

## 1. Overview

This project is a Kafka-like distributed log system that provides durable, partitioned, replicated, append-only storage for event streams. Producers write records to topics, consumers read records by offset, and brokers coordinate replication and failover to preserve availability and consistency.

The system is designed to demonstrate core distributed systems concepts in a realistic way, including partition leadership, quorum-based replication, leader election, offset tracking, crash recovery, and fault-tolerant client interaction.

The primary goals are:

- provide ordered, durable append-only logs
- support horizontal scalability through partitioning
- tolerate broker failures through replication
- expose consumer offset management and consumer groups
- serve as a serious systems project rather than a toy queue

---

## 2. Design Goals

### Functional goals

- create and manage topics with configurable partition counts and replication factors
- append records to partitions
- read records sequentially by offset
- replicate data across brokers
- elect new leaders when brokers fail
- track committed consumer offsets
- support multiple consumers in a group

### Non-functional goals

- preserve record ordering within each partition
- ensure committed records are not lost under single-broker failure
- make writes fast through sequential disk append
- make recovery deterministic after crash or restart
- support benchmarkable throughput and latency
- keep the architecture extensible for exactly-once semantics and stream processing

### Explicit non-goals for v1

The first complete version does not need:

- geo-replication
- cross-datacenter placement
- tiered storage
- full log compaction
- full SQL engine
- arbitrary dynamic cluster resizing without restart

These can be added later.

---

## 3. System Model

The system consists of a cluster of brokers. Each broker stores some set of topic partitions on local disk. Every partition has one leader and zero or more followers. Producers send writes to the leader of a partition. Consumers fetch records from partition leaders and track progress using offsets.

A metadata layer maintains:

- topic definitions
- partition assignments
- leader and follower placement
- broker liveness
- consumer group metadata

The cluster assumes crash-stop failures and unreliable networks. Nodes may fail, restart, lag, or become partitioned. The design uses quorum-based replication and leader election to avoid split brain and to preserve committed data.

---

## 4. High-Level Architecture

```text
                  +----------------------+
                  |      Producer        |
                  +----------+-----------+
                             |
                             v
                   +--------------------+
                   |    Client Router   |
                   | metadata + retry   |
                   +---------+----------+
                             |
               +-------------+-------------+
               |                           |
               v                           v
      +------------------+        +------------------+
      | Partition Leader | <----> | Metadata Layer   |
      |     Broker A     |        | election/config  |
      +--------+---------+        +------------------+
               |
     +---------+---------+
     |                   |
     v                   v
+------------+     +------------+
| Follower B |     | Follower C |
+------------+     +------------+

Consumers fetch records by topic, partition, and offset.
Offsets are stored in replicated metadata or an internal offsets topic.

## 5. Core Components

### 5.1 Broker

A broker is a storage and serving node. It is responsible for:

- storing partition replicas on disk
- serving produce and fetch requests
- replicating leader partitions to followers
- participating in leader election or receiving assignments from the metadata layer
- exposing health and replication state

Each broker may simultaneously host:

- leader replicas for some partitions
- follower replicas for other partitions

### 5.2 Metadata Layer

The metadata layer is responsible for cluster coordination. It tracks:

- registered brokers
- topic metadata
- partition-to-broker assignments
- current partition leaders
- broker liveness
- consumer group membership and assignments

This can be implemented in one of two ways:

embedded consensus among brokers using Raft

a separate controller quorum

For this project, the recommended approach is a Raft-backed controller layer because it cleanly demonstrates distributed coordination and keeps leader election logic explicit.

### 5.3 Producer Client

The producer client:

- fetches metadata
- hashes keys to partitions
- routes writes to current partition leaders
- retries on leader change or transient failure
- optionally batches records for throughput
- optionally includes producer ID and sequence numbers for idempotency

### 5.4 Consumer Client

The consumer client:

- joins a consumer group or reads independently
- fetches records by topic-partition-offset
- tracks current position
- commits offsets
- handles rebalance events when group membership changes

### 5.5 Storage Engine

The storage engine manages append-only on-disk logs. It provides:

- sequential append
- indexed reads by offset
- segment rolling
- recovery from crash
- retention enforcement

### 5.6 Replication Engine

The replication engine is responsible for:

- follower fetch from leader
- leader tracking follower lag
- advancing the high watermark
- marking records committed once quorum replication is achieved

### 5.7 Group Coordinator

The group coordinator manages consumer groups. It handles:

- group membership
- partition assignment
- rebalance on join/leave/failure
- offset commits

For simplicity, the first version may assign one coordinator broker per consumer group using a deterministic mapping.

## 6. Data Model

### 6.1 Topic

A topic is a named logical stream of records.

```text
Topic {
  name: String
  partition_count: u32
  replication_factor: u32
  config: TopicConfig
}
```

### 6.2 Partition

A partition is an ordered append-only log.

```text
PartitionMetadata {
  topic: String
  partition_id: u32
  leader_broker_id: BrokerId
  replica_broker_ids: Vec<BrokerId>
  isr_broker_ids: Vec<BrokerId>
}
```

ISR stands for in-sync replicas.

### 6.3 Record

A record is the atomic unit of data.

```text
Record {
  offset: u64
  timestamp_ms: u64
  key: Bytes
  value: Bytes
  headers: Map<String, Bytes>
  producer_id: Option<u64>
  sequence_number: Option<u32>
}
```

### 6.4 Consumer Offset

```text
ConsumerOffset {
  group_id: String
  topic: String
  partition_id: u32
  committed_offset: u64
  commit_timestamp_ms: u64
}
```

### 6.5 Broker Metadata

```text
BrokerMetadata {
  broker_id: BrokerId
  host: String
  port: u16
  status: BrokerStatus
  last_heartbeat_ms: u64
}
```

## 7. Partitioning Model

A topic is divided into multiple partitions to enable horizontal scale.

Partition routing rule:

```text
partition = hash(record.key) % partition_count
```

If no key is present, the producer may use round-robin assignment.

Partitioning guarantees

- ordering is guaranteed only within a single partition
- records with the same key always map to the same partition, assuming stable partition count
- throughput scales with the number of partitions and brokers

Tradeoffs

Increasing partitions improves parallelism, but increases:

- metadata size
- consumer rebalance complexity
- replication traffic
- operational overhead

## 8. Replication Model

Each partition has one leader and one or more followers.

Write path

- producer sends record to partition leader
- leader appends record to local log
- leader sends replication data to followers or followers fetch from leader
- followers append the record
- leader advances commit point once quorum replication is reached
- leader acknowledges success to the producer

Replication terminology

- LEO: log end offset, the latest appended offset on a replica
- high watermark: highest offset known to be replicated to quorum; consumers should only read up to this point
- ISR: in-sync replicas, followers that are sufficiently caught up

Commit rule

- A record is considered committed only when it has been replicated to a quorum of replicas, typically a majority.
- This ensures that committed records survive single-node failure, assuming a replication factor of at least 3.

Follower behavior

Followers continuously fetch from the leader:

```text
FetchReplicationRequest {
  topic
  partition_id
  fetch_offset
}
```

The leader responds with records starting from that offset.

Replica lag handling

If a follower lags too far behind or stops responding, it is removed from ISR. Once it catches up again, it can re-enter ISR.

## 9. Leader Election

Leader election is required whenever:

- a broker fails
- a partition leader becomes unavailable
- the cluster starts with no active leader
- metadata ownership changes

Recommended approach

Use Raft for metadata consensus. The Raft leader acts as controller and assigns partition leaders. This separates:

- metadata consensus
- partition data replication

This is simpler than running independent Raft groups for every partition and is closer to a Kafka-like architecture.

Election responsibilities

The controller must:

- detect broker failure using heartbeats
- choose a new leader from ISR
- publish updated metadata
- notify clients or make metadata discoverable through refresh

Leader selection rule

When a leader fails, select a new leader from ISR if possible. Avoid selecting an out-of-sync replica because that could lose committed data or force truncation.

Split-brain prevention

The metadata layer uses quorum-based consensus. Only the elected controller can publish authoritative leader assignments.

## 10. Consumer Model

Consumers read records sequentially by offset. They may consume independently or as part of a consumer group.

Independent consumer

An independent consumer manually tracks offsets and reads directly.

Consumer group

A consumer group coordinates multiple consumers so that each partition is assigned to at most one member of the group at a time.

Example:

```text
Topic orders has partitions 0, 1, 2, 3

Consumer group analytics:
  consumer A -> partitions 0, 2
  consumer B -> partitions 1, 3
```

Offset commits

Offsets represent the next record to read. A consumer commits offsets periodically or after successful processing.

Delivery semantics

- at-most-once: commit before processing
- at-least-once: commit after processing
- exactly-once: requires idempotent writes and transactional coordination, planned as an advanced extension

Rebalancing

When a consumer joins or leaves a group, partitions must be reassigned. During rebalance:

- assignments are recalculated
- consumers revoke old partitions
- consumers receive new partitions
- processing resumes from committed offsets

## 11. Storage Engine Design

Each partition replica is stored as a sequence of immutable log segments on disk.

Directory layout

```text
data/
  topics/
    orders/
      partition-0/
        segment-00000000000000000000.log
        segment-00000000000000000000.index
        segment-00000000000000000000.timeindex
        segment-00000000000000001024.log
        ...
```

Segment files

- A segment stores records in append-only order. Each segment begins at a base offset.

Record layout

A record may be stored in binary form like:

```text
[length][crc][attributes][timestamp][offset_delta][key_length][key][value_length][value]
```

The exact layout can be simplified for the first version, but it should support:

efficient append

sequential scan

basic corruption detection

future extensibility

Index files

Each segment should have an offset index mapping logical offsets to byte positions. This allows efficient seeking without scanning from the beginning of the file.

Optional time index:

maps timestamps to approximate offsets

supports time-travel reads such as "find first offset after timestamp T"

Segment rolling

A segment rolls when it reaches:

configured maximum size, or

configured maximum age

Crash recovery

- On startup, the broker scans segment files, verifies integrity, rebuilds missing in-memory indexes if needed, truncates partial tail writes, and restores the last known log end offset.

Retention

Retention policies may be:

- time-based
- size-based

Old closed segments may be deleted once they exceed policy thresholds and are no longer required.

## 12. Metadata Storage

Metadata must survive restarts and remain consistent across failures.

Metadata includes

- brokers
- topics
- partition assignments
- current leaders
- ISR state
- consumer group data
- offset commit records

Recommended design

Store metadata in a replicated Raft log. The state machine applies metadata updates deterministically.

Examples of metadata commands:

- register broker
- create topic
- update partition leader
- update ISR
- join consumer group
- commit offset

This ensures metadata decisions are durable and consistent.

## 13. APIs and Protocol

The system should use a binary TCP or gRPC protocol rather than a REST-only design. This better matches infrastructure systems and avoids unnecessary overhead.

Core requests

```text
Produce
ProduceRequest {
  topic
  partition_id
  records[]
  required_acks
}

Produce response
ProduceResponse {
  base_offset
  high_watermark
  error_code
}

Fetch
FetchRequest {
  topic
  partition_id
  offset
  max_bytes
}

Fetch response
FetchResponse {
  records[]
  high_watermark
  error_code
}

Metadata
MetadataRequest {
  topics[]
}

MetadataResponse {
  topic_metadata[]
  broker_metadata[]
}

Offset commit
CommitOffsetRequest {
  group_id
  topic
  partition_id
  offset
}

Join group
JoinGroupRequest {
  group_id
  member_id
  subscriptions[]
}
```

Error handling

Common error types:

- unknown topic
- not leader for partition
- stale metadata
- offset out of range
- broker unavailable
- timeout
- invalid request

Clients must refresh metadata and retry appropriately when leadership changes.

## 14. Write Path in Detail

The write path is the heart of the system.

Step-by-step flow

- producer fetches metadata if needed
- producer selects partition
- producer sends record batch to leader
- leader validates request
- leader appends batch to local segment
- leader exposes new LEO to followers
- followers replicate and acknowledge progress
- leader advances high watermark when quorum is satisfied
- leader replies to producer based on required ack mode

Required acknowledgments

Supported modes may include:

- acks=0: do not wait for acknowledgment
- acks=1: acknowledge after leader append
- acks=all: acknowledge after quorum replication

For correctness, acks=all is the strongest and should be used in serious tests.

## 15. Read Path in Detail

Consumers fetch records from leaders starting at a requested offset.

Step-by-step flow

- consumer obtains metadata and assigned partitions
- consumer sends fetch request with current offset
- broker locates segment and record position using index
- broker returns records up to high watermark
- consumer processes records
- consumer commits updated offset when appropriate

Read isolation

To avoid exposing uncommitted data, consumers should not read beyond the high watermark.

## 16. Failure Model

The system must explicitly address common failure cases.

Broker crash

If a follower crashes:

- replication factor temporarily degrades
- writes continue if quorum still exists

If a leader crashes:

- controller detects missed heartbeats
- elects a new leader from ISR
- updates metadata
- producers and consumers refresh and retry

Network partition

If the cluster splits, only the side with metadata quorum may continue making authoritative leadership decisions. This avoids split brain.

Slow follower

A slow follower may fall out of ISR. It can remain a replica, but cannot count toward the commit quorum until sufficiently caught up.

Disk corruption or partial write

During recovery, a broker verifies segment integrity and truncates incomplete tail data if necessary.

Controller failure

If the metadata leader fails, the metadata quorum elects a new one and resumes operation.

## 17. Recovery and Restart Semantics

A broker restart should be deterministic.

On startup, a broker must:

- load local segment files
- rebuild indexes if needed
- determine local log end offsets
- restore replica states
- re-register with metadata quorum
- resume follower catch-up or leader service depending on assignment

Leader epoch handling

To avoid stale leaders serving old state, leader changes should increment a leader epoch. Clients and followers can use epochs to detect stale metadata and prevent invalid replication.

## 18. Consistency Guarantees

Ordering

- The system guarantees ordering within a partition.

Durability

- If a record is acknowledged with quorum semantics, it should survive any single-broker failure under normal assumptions.

Availability

The system remains available for writes and reads as long as:

- metadata quorum is available, and
- a quorum of partition replicas is available for the target partition

Consumer visibility

- Consumers should only observe committed records up to the partition high watermark.

## 19. Exactly-Once Semantics Extension

Exactly-once semantics are an advanced feature and should be built after the core system is stable.

Problem

Retries can produce duplicate records if a producer does not know whether a prior attempt succeeded.

Proposed solution

Support idempotent producers using:

- producer ID
- per-partition sequence numbers

The leader tracks the latest accepted sequence number for each producer and rejects duplicates.

Transactional extension

For true end-to-end exactly-once semantics across input consumption and output production, add:

- transactional writes
- atomic offset commit with produced output
- transaction coordinator

This is a major extension and not required for the core implementation.

## 20. Time-Travel Query Extension

Because logs are immutable, consumers can replay from any retained offset.

Basic time-travel support

Support queries such as:

- fetch from offset N
- fetch starting at the first record after timestamp T

This requires a timestamp index per segment.

Use cases

- debugging
- replay after downstream bug
- historical analytics
- deterministic reproduction of state

## 21. Stream Processing Extension

A stream processing layer can treat topics as input streams and produce derived topics.

Example

```text
orders -> filter(amount > 1000) -> large_orders
```

Minimal architecture

A stream processor is just a specialized consumer-producer:

- consume from input topic
- process records
- produce to output topic
- commit offsets

To preserve correctness, offset commit should happen only after output production succeeds.

## 22. Security and Access Control

Security is not the first priority for the initial version, but the architecture should allow it later.

Potential extensions:

TLS between clients and brokers

broker authentication

topic-level authorization

ACLs for producers and consumers

For now, the system may assume a trusted local cluster environment.

## 23. Observability

A serious distributed system needs visibility.

Metrics

Expose at minimum:

- produce requests per second
- fetch requests per second
- bytes written per second
- bytes read per second
- replication lag per follower
- ISR size per partition
- consumer lag
- leader election count
- request latency percentiles

Logging

Log important events such as:

- broker startup and shutdown
- leader changes
- ISR membership changes
- segment roll and retention deletion
- offset commits
- rebalance events

Tracing

Optional later extension:

- trace produce and fetch paths across brokers

## 24. Testing Strategy

Testing is a first-class part of the architecture.

Unit tests

Test:

- segment append and read
- index lookup
- checksum validation
- partitioning logic
- offset commit semantics

Integration tests

Test:

- produce and fetch across multiple brokers
- follower catch-up
- leader failover
- broker restart recovery
- consumer group rebalance

Fault injection tests

Test:

- kill leader during active writes
- delay follower replication
- drop network traffic between brokers
- restart controller
- force stale metadata on clients

Performance tests

Measure:

- throughput under batch writes
- p50, p95, p99 latency
- replication lag under load
- consumer lag growth and recovery

## 25. Deployment Model

For development, the system should run locally as multiple broker processes on one machine.

Local cluster example

- 3 brokers
- 3 metadata/controller nodes or a 3-node embedded controller quorum
- 1 producer benchmark client
- 1 or more consumer clients

Persistent state

Each broker should have its own local data directory and configuration file.

Configuration examples

```text
broker_id = 1
listen_addr = 127.0.0.1:9092
data_dir = ./data/broker-1
controller_quorum = 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
```

## 26. Implementation Plan

This project should be implemented in phases.

Phase 1: single-node append-only log

- local segment files
- append and fetch by offset
- basic producer and consumer CLI

Phase 2: topics and partitions

- topic metadata
- partition routing
- multiple partition logs per broker

Phase 3: replication

- leader and follower roles
- follower fetch
- quorum-based commit
- high watermark tracking

Phase 4: metadata consensus and leader election

- broker heartbeats
- controller quorum
- partition leader assignment
- failover handling

Phase 5: consumer groups

- group coordinator
- partition assignment
- offset commit and rebalance

Phase 6: advanced features

- idempotent producers
- exactly-once semantics
- time index and time-travel reads
- stream processing jobs

## 27. Tradeoffs and Design Rationale

Why leader-based replication

Leader-based replication is simpler and more understandable than leaderless designs for an append-only ordered log. It preserves ordering naturally and mirrors industry systems such as Kafka.

Why append-only segments

Sequential append is fast, simple, durable, and easy to recover. It also enables replay and historical queries.

Why partition-level ordering only

Global ordering would destroy scalability. Partition-level ordering is the standard compromise used in real log systems.

Why a separate metadata consensus layer

Using consensus for metadata rather than every partition keeps the design manageable and matches the intended learning goals.

Why offsets instead of acknowledgments per message

Offsets are compact, replayable, and easy for consumers to reason about. They support batch processing and recovery naturally.

## 28. Open Questions

These are implementation decisions that should be finalized during development:

- should replication be push-based or follower-pull
- should offsets live in metadata state or in an internal replicated offsets topic
- what binary wire format should be used
- what batching strategy should producers use
- how aggressive should ISR eviction be
- how should partition reassignment be handled in later versions
- how should log truncation be handled after unclean failover

## 29. Success Criteria

The project is considered successful if it can demonstrate all of the following:

- create a topic with multiple partitions and replicas
- produce records and preserve partition order
- replicate records across brokers
- commit only after quorum replication
- survive leader failure without losing committed records
- allow consumers to resume from committed offsets
- rebalance consumer groups when membership changes
- recover cleanly after broker restart
- provide benchmark and fault-injection evidence in the repository

## 30. Future Work

Once the core system is complete, the most valuable extensions are:

- idempotent producers
- transactional exactly-once pipeline support
- stream processing runtime
- log compaction
- dynamic broker addition and partition reassignment
- snapshotting and faster recovery
- tiered storage
- web-based admin dashboard
- SQL-like query interface over retained logs

## 31. Conclusion

This system is intentionally ambitious. It is not just a queue, but a serious distributed systems project that combines storage engine design, replication, coordination, failure handling, and streaming semantics.

The core architectural idea is simple: an append-only partitioned log replicated across brokers with leader-based writes and offset-based reads. The complexity comes from making that model durable, scalable, and fault tolerant.

If implemented faithfully, this project demonstrates practical understanding of the same foundational ideas that power real-world systems such as Kafka, Pulsar, and Redpanda.
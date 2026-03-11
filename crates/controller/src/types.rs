use std::collections::HashMap;
use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = MetadataRequest,
        R = MetadataResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetadataRequest {
    RegisterBroker {
        id: u32,
        addr: String,
    },
    Heartbeat {
        broker_id: u32,
        timestamp_ms: u64,
    },
    CreateTopic {
        name: String,
        partition_count: u32,
        replication_factor: u32,
    },
    DeleteTopic {
        name: String,
    },
    UpdateLeader {
        topic: String,
        partition: u32,
        new_leader: u32,
        epoch: u64,
    },
    UpdateISR {
        topic: String,
        partition: u32,
        isr: Vec<u32>,
    },
    MarkBrokerDead {
        broker_id: u32,
    },
    JoinGroup {
        group_id: String,
        member_id: String,
        topics: Vec<String>,
        session_timeout_ms: u64,
    },
    LeaveGroup {
        group_id: String,
        member_id: String,
    },
    ConsumerHeartbeat {
        group_id: String,
        member_id: String,
    },
    CommitOffset {
        group_id: String,
        offsets: Vec<(String, u32, u64)>,
    },
    RemoveExpiredMember {
        group_id: String,
        member_id: String,
    },
    AllocateProducerId {
        transactional_id: Option<String>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetadataResponse {
    Ok,
    Error(String),
    TopicCreated {
        assignments: Vec<PartitionAssignmentMeta>,
    },
    GroupJoined {
        generation_id: u64,
        member_id: String,
        assignments: Vec<(String, u32)>,
    },
    GroupState {
        generation_id: u64,
    },
    ProducerIdAllocated {
        producer_id: u64,
        producer_epoch: u16,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TransactionalIdMapping {
    pub producer_id: u64,
    pub producer_epoch: u16,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ClusterState {
    pub brokers: HashMap<u32, BrokerRegistration>,
    pub topics: HashMap<String, TopicMetadata>,
    pub consumer_groups: HashMap<String, ConsumerGroupState>,
    pub next_producer_id: u64,
    pub transactional_ids: HashMap<String, TransactionalIdMapping>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ConsumerGroupState {
    pub group_id: String,
    pub generation_id: u64,
    pub members: HashMap<String, GroupMember>,
    pub assignments: HashMap<String, Vec<TopicPartitionKey>>,
    pub offsets: HashMap<TopicPartitionKey, CommittedOffset>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub member_id: String,
    pub subscriptions: Vec<String>,
    pub session_timeout_ms: u64,
    pub last_heartbeat_ms: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct TopicPartitionKey {
    pub topic: String,
    pub partition: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommittedOffset {
    pub offset: u64,
    pub timestamp_ms: u64,
}

impl ClusterState {
    pub fn live_broker_ids(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = self
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Live)
            .map(|b| b.id)
            .collect();
        ids.sort();
        ids
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum BrokerStatus {
    Live,
    Dead,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BrokerRegistration {
    pub id: u32,
    pub addr: String,
    pub last_heartbeat_ms: u64,
    pub status: BrokerStatus,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
    pub assignments: Vec<PartitionAssignmentMeta>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PartitionAssignmentMeta {
    pub partition_id: u32,
    pub leader: u32,
    pub leader_epoch: u64,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
}

#[derive(Debug, Clone)]
pub enum MetadataChange {
    BrokerRegistered {
        id: u32,
        addr: String,
    },
    BrokerDead {
        id: u32,
    },
    TopicCreated {
        name: String,
        assignments: Vec<PartitionAssignmentMeta>,
    },
    TopicDeleted {
        name: String,
    },
    LeaderChanged {
        topic: String,
        partition: u32,
        new_leader: u32,
        epoch: u64,
    },
    ISRChanged {
        topic: String,
        partition: u32,
        isr: Vec<u32>,
    },
}

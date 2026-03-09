use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MetadataResponse {
    Ok,
    Error(String),
    TopicCreated {
        assignments: Vec<PartitionAssignmentMeta>,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ClusterState {
    pub brokers: HashMap<u32, BrokerRegistration>,
    pub topics: HashMap<String, TopicMetadata>,
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

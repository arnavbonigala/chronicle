use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use chronicle_storage::{PartitionAssignment, TopicStore};
use tokio::sync::watch;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct PartitionKey {
    topic: String,
    partition: u32,
}

struct ProducerSequenceState {
    producer_epoch: u16,
    last_sequence: u32,
    last_offset: u64,
}

#[derive(Debug, PartialEq)]
pub enum SequenceCheckResult {
    Accept,
    Duplicate { existing_offset: u64 },
    OutOfOrder,
    FencedEpoch,
}

#[derive(Debug, Clone)]
pub struct AbortedTxn {
    pub producer_id: u64,
    pub first_offset: u64,
    pub last_offset: u64,
}

struct LeaderState {
    replicas: Vec<u32>,
    isr: Vec<u32>,
    follower_leos: HashMap<u32, u64>,
    follower_last_fetch: HashMap<u32, Instant>,
    high_watermark: u64,
    hwm_tx: watch::Sender<u64>,
    #[allow(dead_code)]
    hwm_rx: watch::Receiver<u64>,
    leader_epoch: u64,
    producer_states: HashMap<u64, ProducerSequenceState>,
    ongoing_txns: HashMap<u64, u64>,
    aborted_txns: Vec<AbortedTxn>,
}

struct FollowerState {
    leader_id: u32,
    high_watermark: u64,
    leader_epoch: u64,
}

enum PartitionReplicaState {
    Leader(Box<LeaderState>),
    Follower(FollowerState),
}

pub struct PartitionReplicaInfo {
    pub leader_id: u32,
    pub replicas: Vec<u32>,
    pub isr: Vec<u32>,
    pub high_watermark: u64,
}

pub struct ReplicaManager {
    broker_id: u32,
    store: Arc<TopicStore>,
    state: RwLock<HashMap<PartitionKey, PartitionReplicaState>>,
}

impl ReplicaManager {
    pub fn new(broker_id: u32, store: Arc<TopicStore>) -> Self {
        Self {
            broker_id,
            store,
            state: RwLock::new(HashMap::new()),
        }
    }

    pub fn store(&self) -> &Arc<TopicStore> {
        &self.store
    }

    pub fn broker_id(&self) -> u32 {
        self.broker_id
    }

    pub fn register_topic(&self, topic: &str, assignments: &[PartitionAssignment]) {
        let mut state = self.state.write().unwrap();
        for a in assignments {
            if !a.replicas.contains(&self.broker_id) {
                continue;
            }
            let key = PartitionKey {
                topic: topic.to_string(),
                partition: a.partition_id,
            };
            let leader_id = a.replicas[0];
            if leader_id == self.broker_id {
                let (hwm_tx, hwm_rx) = watch::channel(0u64);
                let followers: Vec<u32> = a
                    .replicas
                    .iter()
                    .copied()
                    .filter(|&r| r != self.broker_id)
                    .collect();
                let now = Instant::now();
                let follower_leos: HashMap<u32, u64> =
                    followers.iter().map(|&id| (id, 0u64)).collect();
                let follower_last_fetch: HashMap<u32, Instant> =
                    followers.iter().map(|&id| (id, now)).collect();
                state.insert(
                    key,
                    PartitionReplicaState::Leader(Box::new(LeaderState {
                        replicas: a.replicas.clone(),
                        isr: a.replicas.clone(),
                        follower_leos,
                        follower_last_fetch,
                        high_watermark: 0,
                        hwm_tx,
                        hwm_rx,
                        leader_epoch: 0,
                        producer_states: HashMap::new(),
                        ongoing_txns: HashMap::new(),
                        aborted_txns: Vec::new(),
                    })),
                );
            } else {
                state.insert(
                    key,
                    PartitionReplicaState::Follower(FollowerState {
                        leader_id,
                        high_watermark: 0,
                        leader_epoch: 0,
                    }),
                );
            }
        }
    }

    pub fn is_leader(&self, topic: &str, partition: u32) -> bool {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        matches!(state.get(&key), Some(PartitionReplicaState::Leader(_)))
    }

    pub fn leader_for(&self, topic: &str, partition: u32) -> Option<u32> {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(_)) => Some(self.broker_id),
            Some(PartitionReplicaState::Follower(f)) => Some(f.leader_id),
            None => None,
        }
    }

    pub fn high_watermark(&self, topic: &str, partition: u32) -> u64 {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => l.high_watermark,
            Some(PartitionReplicaState::Follower(f)) => f.high_watermark,
            None => 0,
        }
    }

    pub fn hwm_receiver(&self, topic: &str, partition: u32) -> Option<watch::Receiver<u64>> {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => Some(l.hwm_tx.subscribe()),
            _ => None,
        }
    }

    pub fn update_follower_progress(
        &self,
        topic: &str,
        partition: u32,
        follower_id: u32,
        follower_leo: u64,
    ) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Leader(l)) = state.get_mut(&key) {
            l.follower_leos.insert(follower_id, follower_leo);
            l.follower_last_fetch.insert(follower_id, Instant::now());

            if !l.isr.contains(&follower_id) && follower_leo >= l.high_watermark {
                l.isr.push(follower_id);
                tracing::info!(topic, partition, follower_id, "follower re-entered ISR");
            }

            let leader_leo = self.leader_leo_inner(topic, partition);
            Self::recompute_hwm(l, self.broker_id, leader_leo);
        }
    }

    pub fn update_follower_hwm(&self, topic: &str, partition: u32, hwm: u64) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Follower(f)) = state.get_mut(&key)
            && hwm > f.high_watermark
        {
            f.high_watermark = hwm;
        }
    }

    pub fn isr(&self, topic: &str, partition: u32) -> Vec<u32> {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => l.isr.clone(),
            _ => vec![],
        }
    }

    pub fn check_isr_expiry(&self, lag_time_max: Duration) {
        let mut state = self.state.write().unwrap();
        let now = Instant::now();
        for (key, pstate) in state.iter_mut() {
            if let PartitionReplicaState::Leader(l) = pstate {
                let before = l.isr.len();
                l.isr.retain(|&id| {
                    if id == self.broker_id {
                        return true;
                    }
                    match l.follower_last_fetch.get(&id) {
                        Some(&last) => now.duration_since(last) < lag_time_max,
                        None => false,
                    }
                });
                if l.isr.len() < before {
                    tracing::warn!(
                        topic = %key.topic,
                        partition = key.partition,
                        isr_size = l.isr.len(),
                        "ISR shrunk due to follower timeout"
                    );
                }
            }
        }
    }

    pub fn partition_info(&self, topic: &str, partition: u32) -> Option<PartitionReplicaInfo> {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => Some(PartitionReplicaInfo {
                leader_id: self.broker_id,
                replicas: l.replicas.clone(),
                isr: l.isr.clone(),
                high_watermark: l.high_watermark,
            }),
            Some(PartitionReplicaState::Follower(f)) => Some(PartitionReplicaInfo {
                leader_id: f.leader_id,
                replicas: vec![],
                isr: vec![],
                high_watermark: f.high_watermark,
            }),
            None => None,
        }
    }

    pub fn followed_partitions(&self) -> Vec<(String, u32, u32)> {
        let state = self.state.read().unwrap();
        state
            .iter()
            .filter_map(|(key, pstate)| {
                if let PartitionReplicaState::Follower(f) = pstate {
                    Some((key.topic.clone(), key.partition, f.leader_id))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn leader_epoch(&self, topic: &str, partition: u32) -> u64 {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => l.leader_epoch,
            Some(PartitionReplicaState::Follower(f)) => f.leader_epoch,
            None => 0,
        }
    }

    pub fn promote_to_leader(&self, topic: &str, partition: u32, epoch: u64, replicas: &[u32]) {
        self.promote_to_leader_with_leo(topic, partition, epoch, replicas, 0)
    }

    pub fn promote_to_leader_with_leo(
        &self,
        topic: &str,
        partition: u32,
        epoch: u64,
        replicas: &[u32],
        log_end_offset: u64,
    ) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        let (hwm_tx, hwm_rx) = watch::channel(log_end_offset);
        let followers: Vec<u32> = replicas
            .iter()
            .copied()
            .filter(|&r| r != self.broker_id)
            .collect();
        let now = Instant::now();
        let follower_leos: HashMap<u32, u64> = followers.iter().map(|&id| (id, 0u64)).collect();
        let follower_last_fetch: HashMap<u32, Instant> =
            followers.iter().map(|&id| (id, now)).collect();
        state.insert(
            key,
            PartitionReplicaState::Leader(Box::new(LeaderState {
                replicas: replicas.to_vec(),
                isr: replicas.to_vec(),
                follower_leos,
                follower_last_fetch,
                high_watermark: log_end_offset,
                hwm_tx,
                hwm_rx,
                leader_epoch: epoch,
                producer_states: HashMap::new(),
                ongoing_txns: HashMap::new(),
                aborted_txns: Vec::new(),
            })),
        );
    }

    pub fn demote_to_follower(&self, topic: &str, partition: u32, new_leader: u32, epoch: u64) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        state.insert(
            key,
            PartitionReplicaState::Follower(FollowerState {
                leader_id: new_leader,
                high_watermark: 0,
                leader_epoch: epoch,
            }),
        );
    }

    pub fn update_leader(&self, topic: &str, partition: u32, new_leader: u32, epoch: u64) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Follower(f)) = state.get_mut(&key) {
            f.leader_id = new_leader;
            f.leader_epoch = epoch;
        }
    }

    pub fn registered_partitions(&self, topic: &str) -> Vec<(u32, bool)> {
        let state = self.state.read().unwrap();
        state
            .iter()
            .filter(|(k, _)| k.topic == topic)
            .map(|(k, ps)| {
                let is_leader = matches!(ps, PartitionReplicaState::Leader(_));
                (k.partition, is_leader)
            })
            .collect()
    }

    pub fn check_sequence(
        &self,
        topic: &str,
        partition: u32,
        producer_id: u64,
        producer_epoch: u16,
        sequence: u32,
    ) -> SequenceCheckResult {
        if producer_id == 0 {
            return SequenceCheckResult::Accept;
        }
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        let leader = match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => l,
            _ => return SequenceCheckResult::Accept,
        };
        match leader.producer_states.get(&producer_id) {
            None => {
                if sequence == 0 {
                    SequenceCheckResult::Accept
                } else {
                    SequenceCheckResult::OutOfOrder
                }
            }
            Some(ps) => {
                if ps.producer_epoch > producer_epoch {
                    return SequenceCheckResult::FencedEpoch;
                }
                if ps.producer_epoch < producer_epoch {
                    return SequenceCheckResult::Accept;
                }
                if sequence == ps.last_sequence.wrapping_add(1) {
                    SequenceCheckResult::Accept
                } else if sequence <= ps.last_sequence {
                    SequenceCheckResult::Duplicate {
                        existing_offset: ps.last_offset,
                    }
                } else {
                    SequenceCheckResult::OutOfOrder
                }
            }
        }
    }

    pub fn record_sequence(
        &self,
        topic: &str,
        partition: u32,
        producer_id: u64,
        producer_epoch: u16,
        sequence: u32,
        offset: u64,
    ) {
        if producer_id == 0 {
            return;
        }
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Leader(l)) = state.get_mut(&key) {
            l.producer_states.insert(
                producer_id,
                ProducerSequenceState {
                    producer_epoch,
                    last_sequence: sequence,
                    last_offset: offset,
                },
            );
        }
    }

    pub fn rebuild_producer_state(
        &self,
        topic: &str,
        partition: u32,
        log: &chronicle_storage::Log,
    ) {
        let records = log
            .read(log.earliest_offset(), u32::MAX)
            .unwrap_or_default();
        let mut producer_states: HashMap<u64, ProducerSequenceState> = HashMap::new();
        for r in &records {
            if r.producer_id != 0 {
                producer_states.insert(
                    r.producer_id,
                    ProducerSequenceState {
                        producer_epoch: r.producer_epoch,
                        last_sequence: r.sequence_number,
                        last_offset: r.offset,
                    },
                );
            }
        }
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Leader(l)) = state.get_mut(&key) {
            l.producer_states = producer_states;
        }
    }

    pub fn track_txn_write(&self, topic: &str, partition: u32, producer_id: u64, offset: u64) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Leader(l)) = state.get_mut(&key) {
            l.ongoing_txns.entry(producer_id).or_insert(offset);
        }
    }

    pub fn complete_txn(
        &self,
        topic: &str,
        partition: u32,
        producer_id: u64,
        committed: bool,
        marker_offset: u64,
    ) {
        let mut state = self.state.write().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        if let Some(PartitionReplicaState::Leader(l)) = state.get_mut(&key)
            && let Some(first_offset) = l.ongoing_txns.remove(&producer_id)
            && !committed
        {
            l.aborted_txns.push(AbortedTxn {
                producer_id,
                first_offset,
                last_offset: marker_offset,
            });
        }
    }

    pub fn last_stable_offset(&self, topic: &str, partition: u32) -> u64 {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => {
                if l.ongoing_txns.is_empty() {
                    l.high_watermark
                } else {
                    let min_txn_offset = l.ongoing_txns.values().copied().min().unwrap();
                    min_txn_offset.min(l.high_watermark)
                }
            }
            _ => 0,
        }
    }

    pub fn aborted_txns_in_range(
        &self,
        topic: &str,
        partition: u32,
        from: u64,
        to: u64,
    ) -> Vec<AbortedTxn> {
        let state = self.state.read().unwrap();
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };
        match state.get(&key) {
            Some(PartitionReplicaState::Leader(l)) => l
                .aborted_txns
                .iter()
                .filter(|a| a.last_offset >= from && a.first_offset < to)
                .cloned()
                .collect(),
            _ => vec![],
        }
    }

    fn leader_leo_inner(&self, topic: &str, partition: u32) -> u64 {
        self.store
            .topic(topic)
            .and_then(|t| {
                t.partition(partition)
                    .map(|l| l.read().unwrap().latest_offset())
            })
            .unwrap_or(0)
    }

    fn recompute_hwm(leader_state: &mut LeaderState, self_broker_id: u32, leader_leo: u64) {
        let min_isr_leo = leader_state
            .isr
            .iter()
            .map(|&id| {
                if id == self_broker_id {
                    leader_leo
                } else {
                    leader_state.follower_leos.get(&id).copied().unwrap_or(0)
                }
            })
            .min()
            .unwrap_or(0);
        if min_isr_leo > leader_state.high_watermark {
            leader_state.high_watermark = min_isr_leo;
            leader_state.hwm_tx.send(min_isr_leo).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronicle_storage::StorageConfig;
    use tempfile::tempdir;

    fn setup(broker_id: u32) -> (Arc<TopicStore>, Arc<ReplicaManager>) {
        let dir = tempdir().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            segment_max_bytes: 10 * 1024 * 1024,
        };
        let store = Arc::new(TopicStore::open(config).unwrap());
        let rm = Arc::new(ReplicaManager::new(broker_id, store.clone()));
        // Leak the tempdir so it doesn't get cleaned up during the test
        std::mem::forget(dir);
        (store, rm)
    }

    fn make_assignments() -> Vec<PartitionAssignment> {
        vec![
            PartitionAssignment {
                partition_id: 0,
                replicas: vec![1, 2, 3],
            },
            PartitionAssignment {
                partition_id: 1,
                replicas: vec![2, 3, 1],
            },
        ]
    }

    #[test]
    fn hwm_starts_at_zero() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(rm.high_watermark("t", 0), 0);
    }

    #[test]
    fn is_leader_correct() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert!(rm.is_leader("t", 0));
        assert!(!rm.is_leader("t", 1));
    }

    #[test]
    fn leader_for_returns_correct_id() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(rm.leader_for("t", 0), Some(1));
        assert_eq!(rm.leader_for("t", 1), Some(2));
    }

    #[test]
    fn hwm_advances_with_follower_progress() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();

        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
            log.append(b"k2", b"v2").unwrap();
        }

        rm.update_follower_progress("t", 0, 2, 1);
        assert_eq!(rm.high_watermark("t", 0), 0);

        rm.update_follower_progress("t", 0, 3, 1);
        assert_eq!(rm.high_watermark("t", 0), 1);

        rm.update_follower_progress("t", 0, 2, 2);
        rm.update_follower_progress("t", 0, 3, 2);
        assert_eq!(rm.high_watermark("t", 0), 2);
    }

    #[test]
    fn hwm_does_not_decrease() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
            log.append(b"k2", b"v2").unwrap();
        }

        rm.update_follower_progress("t", 0, 2, 2);
        rm.update_follower_progress("t", 0, 3, 2);
        assert_eq!(rm.high_watermark("t", 0), 2);

        rm.update_follower_progress("t", 0, 2, 1);
        assert_eq!(rm.high_watermark("t", 0), 2);
    }

    #[test]
    fn hwm_does_not_exceed_leader_leo() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
        }

        rm.update_follower_progress("t", 0, 2, 100);
        rm.update_follower_progress("t", 0, 3, 100);
        assert_eq!(rm.high_watermark("t", 0), 1);
    }

    #[test]
    fn isr_shrinks_on_timeout() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(rm.isr("t", 0).len(), 3);

        std::thread::sleep(Duration::from_millis(50));
        rm.check_isr_expiry(Duration::from_millis(10));

        let isr = rm.isr("t", 0);
        assert_eq!(isr, vec![1]);
    }

    #[test]
    fn follower_re_enters_isr() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        std::thread::sleep(Duration::from_millis(50));
        rm.check_isr_expiry(Duration::from_millis(10));
        assert_eq!(rm.isr("t", 0), vec![1]);

        rm.update_follower_progress("t", 0, 2, 0);
        let isr = rm.isr("t", 0);
        assert!(isr.contains(&2));
    }

    #[test]
    fn promote_to_leader_creates_leader_state() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert!(!rm.is_leader("t", 1));

        rm.promote_to_leader("t", 1, 5, &[2, 3, 1]);
        assert!(rm.is_leader("t", 1));
        assert_eq!(rm.leader_epoch("t", 1), 5);
        assert_eq!(rm.leader_for("t", 1), Some(1));
    }

    #[test]
    fn demote_to_follower_creates_follower_state() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert!(rm.is_leader("t", 0));

        rm.demote_to_follower("t", 0, 2, 3);
        assert!(!rm.is_leader("t", 0));
        assert_eq!(rm.leader_epoch("t", 0), 3);
        assert_eq!(rm.leader_for("t", 0), Some(2));
    }

    #[test]
    fn epoch_tracked_through_promote_demote_cycle() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(rm.leader_epoch("t", 0), 0);

        rm.demote_to_follower("t", 0, 2, 1);
        assert_eq!(rm.leader_epoch("t", 0), 1);

        rm.promote_to_leader("t", 0, 2, &[1, 2, 3]);
        assert_eq!(rm.leader_epoch("t", 0), 2);
        assert!(rm.is_leader("t", 0));
    }

    #[test]
    fn update_leader_changes_follower_leader() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(rm.leader_for("t", 1), Some(2));

        rm.update_leader("t", 1, 3, 5);
        assert_eq!(rm.leader_for("t", 1), Some(3));
        assert_eq!(rm.leader_epoch("t", 1), 5);
    }

    #[test]
    fn registered_partitions_returns_all() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        let mut parts = rm.registered_partitions("t");
        parts.sort_by_key(|p| p.0);
        assert_eq!(parts, vec![(0, true), (1, false)]);
    }

    #[tokio::test]
    async fn hwm_receiver_notifies() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
        }

        let mut rx = rm.hwm_receiver("t", 0).unwrap();

        let rm2 = rm.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            rm2.update_follower_progress("t", 0, 2, 1);
            rm2.update_follower_progress("t", 0, 3, 1);
        });

        tokio::time::timeout(Duration::from_secs(1), rx.changed())
            .await
            .expect("timeout waiting for HWM")
            .expect("watch error");
        assert_eq!(*rx.borrow(), 1);
    }

    #[test]
    fn check_sequence_accept_first() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(
            rm.check_sequence("t", 0, 42, 0, 0),
            SequenceCheckResult::Accept
        );
    }

    #[test]
    fn check_sequence_accept_next() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        rm.record_sequence("t", 0, 42, 0, 0, 100);
        assert_eq!(
            rm.check_sequence("t", 0, 42, 0, 1),
            SequenceCheckResult::Accept
        );
    }

    #[test]
    fn check_sequence_duplicate() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        rm.record_sequence("t", 0, 42, 0, 5, 100);
        assert_eq!(
            rm.check_sequence("t", 0, 42, 0, 5),
            SequenceCheckResult::Duplicate {
                existing_offset: 100
            }
        );
    }

    #[test]
    fn check_sequence_out_of_order() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        rm.record_sequence("t", 0, 42, 0, 0, 100);
        assert_eq!(
            rm.check_sequence("t", 0, 42, 0, 5),
            SequenceCheckResult::OutOfOrder
        );
    }

    #[test]
    fn check_sequence_fenced_epoch() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        rm.record_sequence("t", 0, 42, 3, 0, 100);
        assert_eq!(
            rm.check_sequence("t", 0, 42, 1, 1),
            SequenceCheckResult::FencedEpoch
        );
    }

    #[test]
    fn check_sequence_new_epoch_resets() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        rm.record_sequence("t", 0, 42, 0, 5, 100);
        assert_eq!(
            rm.check_sequence("t", 0, 42, 1, 0),
            SequenceCheckResult::Accept
        );
    }

    #[test]
    fn check_sequence_non_idempotent_always_accepts() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());
        assert_eq!(
            rm.check_sequence("t", 0, 0, 0, 999),
            SequenceCheckResult::Accept
        );
    }

    #[test]
    fn lso_equals_hwm_when_no_txns() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
        }
        rm.update_follower_progress("t", 0, 2, 1);
        rm.update_follower_progress("t", 0, 3, 1);

        assert_eq!(rm.last_stable_offset("t", 0), rm.high_watermark("t", 0));
    }

    #[test]
    fn lso_capped_by_ongoing_txn() {
        let (store, rm) = setup(1);
        store
            .create_topic("t", 2, 3, &make_assignments(), &[0, 1])
            .unwrap();
        rm.register_topic("t", &make_assignments());

        {
            let topic = store.topic("t").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k1", b"v1").unwrap();
            log.append(b"k2", b"v2").unwrap();
            log.append(b"k3", b"v3").unwrap();
        }
        rm.update_follower_progress("t", 0, 2, 3);
        rm.update_follower_progress("t", 0, 3, 3);
        assert_eq!(rm.high_watermark("t", 0), 3);

        rm.track_txn_write("t", 0, 100, 1);
        assert_eq!(rm.last_stable_offset("t", 0), 1);
    }

    #[test]
    fn complete_txn_commit_advances_lso() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());

        rm.track_txn_write("t", 0, 100, 5);
        rm.complete_txn("t", 0, 100, true, 10);
        assert_eq!(rm.last_stable_offset("t", 0), 0);
        assert!(rm.aborted_txns_in_range("t", 0, 0, 100).is_empty());
    }

    #[test]
    fn complete_txn_abort_records_aborted() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());

        rm.track_txn_write("t", 0, 100, 5);
        rm.complete_txn("t", 0, 100, false, 10);

        let aborted = rm.aborted_txns_in_range("t", 0, 0, 100);
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].producer_id, 100);
        assert_eq!(aborted[0].first_offset, 5);
        assert_eq!(aborted[0].last_offset, 10);
    }

    #[test]
    fn aborted_txns_in_range_filters_correctly() {
        let (_store, rm) = setup(1);
        rm.register_topic("t", &make_assignments());

        rm.track_txn_write("t", 0, 100, 5);
        rm.complete_txn("t", 0, 100, false, 10);
        rm.track_txn_write("t", 0, 200, 20);
        rm.complete_txn("t", 0, 200, false, 25);

        assert_eq!(rm.aborted_txns_in_range("t", 0, 0, 30).len(), 2);
        assert_eq!(rm.aborted_txns_in_range("t", 0, 11, 30).len(), 1);
        assert_eq!(rm.aborted_txns_in_range("t", 0, 0, 5).len(), 0);
    }
}

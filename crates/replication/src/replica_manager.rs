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

struct LeaderState {
    replicas: Vec<u32>,
    isr: Vec<u32>,
    follower_leos: HashMap<u32, u64>,
    follower_last_fetch: HashMap<u32, Instant>,
    high_watermark: u64,
    hwm_tx: watch::Sender<u64>,
    #[allow(dead_code)]
    hwm_rx: watch::Receiver<u64>,
}

struct FollowerState {
    leader_id: u32,
    high_watermark: u64,
}

enum PartitionReplicaState {
    Leader(LeaderState),
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
                    PartitionReplicaState::Leader(LeaderState {
                        replicas: a.replicas.clone(),
                        isr: a.replicas.clone(),
                        follower_leos,
                        follower_last_fetch,
                        high_watermark: 0,
                        hwm_tx,
                        hwm_rx,
                    }),
                );
            } else {
                state.insert(
                    key,
                    PartitionReplicaState::Follower(FollowerState {
                        leader_id,
                        high_watermark: 0,
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
        if let Some(PartitionReplicaState::Follower(f)) = state.get_mut(&key) {
            if hwm > f.high_watermark {
                f.high_watermark = hwm;
            }
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
}

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::Entry;
use openraft::LogId;
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use openraft::storage::RaftLogReader;
use openraft::storage::RaftLogStorage;
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig};

pub struct LogStore {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,
    committed: RwLock<Option<LogId<NodeId>>>,
    log: RwLock<BTreeMap<u64, String>>,
    vote: RwLock<Option<Vote<NodeId>>>,
}

impl LogStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            last_purged_log_id: RwLock::new(None),
            committed: RwLock::new(None),
            log: RwLock::new(BTreeMap::new()),
            vote: RwLock::new(None),
        })
    }
}

impl RaftLogReader<TypeConfig> for Arc<LogStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let entries = log
            .range(range)
            .map(|(_, val)| serde_json::from_str(val).map_err(|e| StorageIOError::read_logs(&e)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries)
    }
}

impl RaftLogStorage<TypeConfig> for Arc<LogStore> {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last_serialized = log.iter().next_back().map(|(_, ent)| ent);
        let last = match last_serialized {
            None => None,
            Some(s) => {
                let ent: Entry<TypeConfig> =
                    serde_json::from_str(s).map_err(|e| StorageIOError::read_logs(&e))?;
                Some(*ent.get_log_id())
            }
        };
        let last_purged = *self.last_purged_log_id.read().await;
        let last = last.or(last_purged);
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut v = self.vote.write().await;
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut c = self.committed.write().await;
        *c = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(*self.committed.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            let s = serde_json::to_string(&entry)
                .map_err(|e| StorageIOError::write_log_entry(*entry.get_log_id(), &e))?;
            log.insert(entry.log_id.index, s);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut log = self.log.write().await;
        let keys: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut ld = self.last_purged_log_id.write().await;
            *ld = Some(log_id);
        }
        {
            let mut log = self.log.write().await;
            let keys: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
            for key in keys {
                log.remove(&key);
            }
        }
        Ok(())
    }
}

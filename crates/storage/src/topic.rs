use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::log::Log;

#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub name: String,
    pub partition_count: u32,
    pub replication_factor: u32,
}

pub struct TopicState {
    pub meta: TopicMeta,
    partitions: Vec<RwLock<Log>>,
}

impl TopicState {
    pub fn partition(&self, id: u32) -> Option<&RwLock<Log>> {
        self.partitions.get(id as usize)
    }

    pub fn partition_count(&self) -> u32 {
        self.meta.partition_count
    }
}

pub struct TopicStore {
    data_dir: PathBuf,
    segment_max_bytes: u64,
    topics: RwLock<HashMap<String, Arc<TopicState>>>,
}

impl TopicStore {
    pub fn open(config: StorageConfig) -> Result<Self> {
        let topics_dir = config.data_dir.join("topics");
        fs::create_dir_all(&topics_dir)?;

        let mut topics = HashMap::new();
        for entry in fs::read_dir(&topics_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let topic_name = entry.file_name().to_string_lossy().into_owned();
            let topic_dir = entry.path();
            let meta = read_meta(&topic_dir.join("meta.bin"), topic_name.clone())?;
            let partitions = open_partitions(&topic_dir, &meta, config.segment_max_bytes)?;
            topics.insert(topic_name, Arc::new(TopicState { meta, partitions }));
        }

        Ok(Self {
            data_dir: config.data_dir,
            segment_max_bytes: config.segment_max_bytes,
            topics: RwLock::new(topics),
        })
    }

    pub fn create_topic(
        &self,
        name: &str,
        partition_count: u32,
        replication_factor: u32,
    ) -> Result<()> {
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(name) {
            return Err(StorageError::TopicAlreadyExists {
                name: name.to_string(),
            });
        }

        let topic_dir = self.data_dir.join("topics").join(name);
        fs::create_dir_all(&topic_dir)?;

        let meta = TopicMeta {
            name: name.to_string(),
            partition_count,
            replication_factor,
        };
        write_meta(&topic_dir.join("meta.bin"), &meta)?;

        let partitions = open_partitions(&topic_dir, &meta, self.segment_max_bytes)?;
        topics.insert(name.to_string(), Arc::new(TopicState { meta, partitions }));
        Ok(())
    }

    pub fn delete_topic(&self, name: &str) -> Result<()> {
        let mut topics = self.topics.write().unwrap();
        if topics.remove(name).is_none() {
            return Err(StorageError::UnknownTopic {
                name: name.to_string(),
            });
        }
        let topic_dir = self.data_dir.join("topics").join(name);
        fs::remove_dir_all(&topic_dir)?;
        Ok(())
    }

    pub fn list_topics(&self) -> Vec<TopicMeta> {
        let topics = self.topics.read().unwrap();
        topics.values().map(|t| t.meta.clone()).collect()
    }

    pub fn topic(&self, name: &str) -> Option<Arc<TopicState>> {
        let topics = self.topics.read().unwrap();
        topics.get(name).cloned()
    }
}

fn open_partitions(
    topic_dir: &Path,
    meta: &TopicMeta,
    segment_max_bytes: u64,
) -> Result<Vec<RwLock<Log>>> {
    let mut partitions = Vec::with_capacity(meta.partition_count as usize);
    for pid in 0..meta.partition_count {
        let partition_dir = topic_dir.join(format!("partition-{pid}"));
        let log = Log::open(StorageConfig {
            data_dir: partition_dir,
            segment_max_bytes,
        })?;
        partitions.push(RwLock::new(log));
    }
    Ok(partitions)
}

fn write_meta(path: &Path, meta: &TopicMeta) -> Result<()> {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&meta.partition_count.to_le_bytes());
    buf.extend_from_slice(&meta.replication_factor.to_le_bytes());
    fs::write(path, &buf)?;
    Ok(())
}

fn read_meta(path: &Path, name: String) -> Result<TopicMeta> {
    let data = fs::read(path)?;
    if data.len() < 8 {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("meta.bin too short: {} bytes", data.len()),
        )));
    }
    let partition_count = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let replication_factor = u32::from_le_bytes(data[4..8].try_into().unwrap());
    Ok(TopicMeta {
        name,
        partition_count,
        replication_factor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &Path) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_path_buf(),
            segment_max_bytes: 10 * 1024 * 1024,
        }
    }

    #[test]
    fn open_empty_dir() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        assert!(store.list_topics().is_empty());
    }

    #[test]
    fn create_and_get_topic() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("orders", 4, 1).unwrap();

        let topic = store.topic("orders").unwrap();
        assert_eq!(topic.partition_count(), 4);
        assert_eq!(topic.meta.replication_factor, 1);
    }

    #[test]
    fn create_duplicate_topic() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("orders", 4, 1).unwrap();

        let err = store.create_topic("orders", 2, 1).unwrap_err();
        assert!(matches!(err, StorageError::TopicAlreadyExists { .. }));
    }

    #[test]
    fn delete_topic() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("orders", 2, 1).unwrap();
        store.delete_topic("orders").unwrap();

        assert!(store.topic("orders").is_none());
        assert!(store.list_topics().is_empty());
        assert!(!dir.path().join("topics/orders").exists());
    }

    #[test]
    fn delete_unknown_topic() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();

        let err = store.delete_topic("nope").unwrap_err();
        assert!(matches!(err, StorageError::UnknownTopic { .. }));
    }

    #[test]
    fn list_topics() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("a", 1, 1).unwrap();
        store.create_topic("b", 3, 1).unwrap();

        let mut names: Vec<String> = store.list_topics().iter().map(|m| m.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn partition_write_and_read() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("t", 2, 1).unwrap();

        let topic = store.topic("t").unwrap();

        {
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k0", b"v0").unwrap();
        }
        {
            let mut log = topic.partition(1).unwrap().write().unwrap();
            log.append(b"k1", b"v1").unwrap();
        }

        let log0 = topic.partition(0).unwrap().read().unwrap();
        let recs = log0.read(0, 10).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key.as_ref(), b"k0");

        let log1 = topic.partition(1).unwrap().read().unwrap();
        let recs = log1.read(0, 10).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key.as_ref(), b"k1");
    }

    #[test]
    fn partition_out_of_range() {
        let dir = tempdir().unwrap();
        let store = TopicStore::open(test_config(dir.path())).unwrap();
        store.create_topic("t", 2, 1).unwrap();

        let topic = store.topic("t").unwrap();
        assert!(topic.partition(2).is_none());
    }

    #[test]
    fn reopen_persists_topics_and_data() {
        let dir = tempdir().unwrap();
        {
            let store = TopicStore::open(test_config(dir.path())).unwrap();
            store.create_topic("orders", 2, 3).unwrap();
            let topic = store.topic("orders").unwrap();
            let mut log = topic.partition(0).unwrap().write().unwrap();
            log.append(b"k", b"v").unwrap();
        }

        let store = TopicStore::open(test_config(dir.path())).unwrap();
        let topics = store.list_topics();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "orders");
        assert_eq!(topics[0].partition_count, 2);
        assert_eq!(topics[0].replication_factor, 3);

        let topic = store.topic("orders").unwrap();
        let log = topic.partition(0).unwrap().read().unwrap();
        let recs = log.read(0, 10).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].value.as_ref(), b"v");
    }

    #[test]
    fn meta_bin_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("meta.bin");
        let meta = TopicMeta {
            name: "test".into(),
            partition_count: 8,
            replication_factor: 3,
        };
        write_meta(&path, &meta).unwrap();
        let loaded = read_meta(&path, "test".into()).unwrap();
        assert_eq!(loaded.partition_count, 8);
        assert_eq!(loaded.replication_factor, 3);
    }
}

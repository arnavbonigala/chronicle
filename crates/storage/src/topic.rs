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

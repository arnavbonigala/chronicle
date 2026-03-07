use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupt record at offset {offset}")]
    CorruptRecord { offset: u64 },

    #[error("offset {requested} out of range [{earliest}, {latest})")]
    OffsetOutOfRange {
        requested: u64,
        earliest: u64,
        latest: u64,
    },

    #[error("invalid segment filename: {0}")]
    InvalidSegmentFile(PathBuf),

    #[error("unknown topic: {name}")]
    UnknownTopic { name: String },

    #[error("unknown partition {partition} for topic {topic} (count: {count})")]
    UnknownPartition {
        topic: String,
        partition: u32,
        count: u32,
    },

    #[error("topic already exists: {name}")]
    TopicAlreadyExists { name: String },
}

pub type Result<T> = std::result::Result<T, StorageError>;

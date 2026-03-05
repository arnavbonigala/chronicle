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
}

pub type Result<T> = std::result::Result<T, StorageError>;

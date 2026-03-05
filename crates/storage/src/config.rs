use std::path::PathBuf;

pub struct StorageConfig {
    pub data_dir: PathBuf,
    /// Maximum segment file size in bytes before rolling to a new segment.
    pub segment_max_bytes: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            segment_max_bytes: 10 * 1024 * 1024, // 10 MiB
        }
    }
}

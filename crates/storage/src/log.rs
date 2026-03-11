use std::path::PathBuf;

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::record::Record;
use crate::segment::{parse_base_offset, Segment};

pub struct Log {
    dir: PathBuf,
    segments: Vec<Segment>,
    config: StorageConfig,
}

impl Log {
    /// Open an existing log directory or initialize a new one.
    /// Runs crash recovery on the active (last) segment.
    pub fn open(config: StorageConfig) -> Result<Self> {
        let dir = config.data_dir.clone();
        std::fs::create_dir_all(&dir)?;

        let mut base_offsets = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(offset) = parse_base_offset(&name_str) {
                base_offsets.push(offset);
            }
        }
        base_offsets.sort();
        base_offsets.dedup();

        let mut segments = Vec::new();
        for (i, &base) in base_offsets.iter().enumerate() {
            let mut seg = Segment::open(&dir, base)?;
            // Recover only the last (active) segment
            if i == base_offsets.len() - 1 {
                seg.recover()?;
            }
            segments.push(seg);
        }

        if segments.is_empty() {
            segments.push(Segment::create(&dir, 0)?);
        }

        Ok(Self {
            dir,
            segments,
            config,
        })
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<u64> {
        // Roll if the active segment exceeds the size limit
        let active = self.segments.last().unwrap();
        if active.size() >= self.config.segment_max_bytes {
            let new_base = active.next_offset();
            let new_seg = Segment::create(&self.dir, new_base)?;
            self.segments.push(new_seg);
            tracing::info!(new_base_offset = new_base, "rolled to new segment");
        }

        let active = self.segments.last_mut().unwrap();
        active.append(key, value)
    }

    /// Append a fully constructed record to the active segment.
    pub fn append_record(&mut self, record: &Record) -> Result<u64> {
        let active = self.segments.last().unwrap();
        if active.size() >= self.config.segment_max_bytes {
            let new_base = active.next_offset();
            let new_seg = Segment::create(&self.dir, new_base)?;
            self.segments.push(new_seg);
            tracing::info!(new_base_offset = new_base, "rolled to new segment");
        }

        let active = self.segments.last_mut().unwrap();
        active.append_record(record)
    }

    pub fn append_at(&mut self, offset: u64, key: &[u8], value: &[u8]) -> Result<u64> {
        let active = self.segments.last().unwrap();
        if active.size() >= self.config.segment_max_bytes {
            let new_base = active.next_offset();
            let new_seg = Segment::create(&self.dir, new_base)?;
            self.segments.push(new_seg);
            tracing::info!(new_base_offset = new_base, "rolled to new segment");
        }

        let active = self.segments.last_mut().unwrap();
        active.append_at(offset, key, value)
    }

    /// Read up to `max_records` starting at `offset`, continuing across segment boundaries.
    pub fn read(&self, offset: u64, max_records: u32) -> Result<Vec<Record>> {
        let earliest = self.earliest_offset();
        let latest = self.latest_offset();

        if offset >= latest {
            return Ok(Vec::new());
        }
        if offset < earliest {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest,
                latest,
            });
        }

        let seg_idx = self.find_segment(offset);
        let mut records = Vec::new();
        let mut remaining = max_records;

        for seg in &self.segments[seg_idx..] {
            if remaining == 0 {
                break;
            }
            let batch = seg.read_from(offset + records.len() as u64, remaining)?;
            remaining -= batch.len() as u32;
            records.extend(batch);
        }

        Ok(records)
    }

    pub fn earliest_offset(&self) -> u64 {
        self.segments.first().map_or(0, |s| s.base_offset())
    }

    /// LEO: one past the last written offset.
    pub fn latest_offset(&self) -> u64 {
        self.segments.last().map_or(0, |s| s.next_offset())
    }

    /// Find the first offset with timestamp >= the given timestamp.
    /// Returns `None` if no records exist at or after the timestamp.
    pub fn find_offset_by_timestamp(&self, timestamp_ms: u64) -> Option<u64> {
        if self.segments.is_empty() {
            return None;
        }
        // Find the right segment: the last segment whose first timestamp <= target.
        // We iterate forward and find the first segment whose first timestamp > target,
        // then use the one before it.
        let mut seg_idx = 0;
        for (i, seg) in self.segments.iter().enumerate() {
            match seg.first_timestamp() {
                Some(ts) if ts <= timestamp_ms => seg_idx = i,
                Some(_) => break,
                None => continue,
            }
        }
        // Try to find within the selected segment
        if let Some(offset) = self.segments[seg_idx].find_offset_by_timestamp(timestamp_ms) {
            return Some(offset);
        }
        // If not found in the selected segment, try subsequent segments
        for seg in &self.segments[seg_idx + 1..] {
            if let Some(offset) = seg.find_offset_by_timestamp(timestamp_ms) {
                return Some(offset);
            }
        }
        None
    }

    fn find_segment(&self, offset: u64) -> usize {
        match self
            .segments
            .binary_search_by_key(&offset, |s| s.base_offset())
        {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn test_config(dir: &Path) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_path_buf(),
            segment_max_bytes: 10 * 1024 * 1024,
        }
    }

    #[test]
    fn append_and_read_back() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(test_config(dir.path())).unwrap();

        let o0 = log.append(b"k0", b"v0").unwrap();
        let o1 = log.append(b"k1", b"v1").unwrap();
        assert_eq!(o0, 0);
        assert_eq!(o1, 1);
        assert_eq!(log.latest_offset(), 2);

        let records = log.read(0, 10).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref(), b"k0");
        assert_eq!(records[1].key.as_ref(), b"k1");
    }

    #[test]
    fn segment_rolling() {
        let dir = tempdir().unwrap();
        let config = StorageConfig {
            data_dir: dir.path().to_path_buf(),
            segment_max_bytes: 100, // very small to force rolling
        };
        let mut log = Log::open(config).unwrap();

        for i in 0..20u32 {
            log.append(format!("k{}", i).as_bytes(), b"value_data_here")
                .unwrap();
        }

        assert!(log.segments.len() > 1, "should have rolled segments");
        assert_eq!(log.latest_offset(), 20);

        // Read across segment boundaries
        let records = log.read(0, 20).unwrap();
        assert_eq!(records.len(), 20);
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.offset, i as u64);
        }
    }

    #[test]
    fn reopen_preserves_data() {
        let dir = tempdir().unwrap();
        {
            let mut log = Log::open(test_config(dir.path())).unwrap();
            log.append(b"a", b"1").unwrap();
            log.append(b"b", b"2").unwrap();
        }
        let log = Log::open(test_config(dir.path())).unwrap();
        assert_eq!(log.latest_offset(), 2);
        let records = log.read(0, 10).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref(), b"a");
        assert_eq!(records[1].key.as_ref(), b"b");
    }

    #[test]
    fn read_empty_returns_empty() {
        let dir = tempdir().unwrap();
        let log = Log::open(test_config(dir.path())).unwrap();
        let records = log.read(0, 10).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn offset_out_of_range() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(test_config(dir.path())).unwrap();
        log.append(b"k", b"v").unwrap();

        // Reading beyond latest returns empty
        let records = log.read(1, 10).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn append_at_correct_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(test_config(dir.path())).unwrap();
        assert_eq!(log.append_at(0, b"k0", b"v0").unwrap(), 0);
        assert_eq!(log.append_at(1, b"k1", b"v1").unwrap(), 1);
        assert_eq!(log.latest_offset(), 2);

        let records = log.read(0, 10).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key.as_ref(), b"k0");
        assert_eq!(records[1].key.as_ref(), b"k1");
    }

    #[test]
    fn append_at_wrong_offset() {
        let dir = tempdir().unwrap();
        let mut log = Log::open(test_config(dir.path())).unwrap();
        let err = log.append_at(5, b"k", b"v").unwrap_err();
        assert!(matches!(err, StorageError::OffsetOutOfRange { .. }));
    }
}

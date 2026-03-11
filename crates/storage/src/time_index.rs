use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::Result;
use crate::record::Record;

const ENTRY_SIZE: usize = 12; // u64 timestamp_ms + u32 relative_offset

#[derive(Debug, Clone, Copy)]
struct TimeIndexEntry {
    timestamp_ms: u64,
    relative_offset: u32,
}

pub struct TimeIndex {
    file: File,
    entries: Vec<TimeIndexEntry>,
}

impl TimeIndex {
    pub fn create(path: &Path) -> Result<Self> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let file = OpenOptions::new().append(true).read(true).open(path)?;
        Ok(Self {
            file,
            entries: Vec::new(),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).append(true).open(path)?;
        let len = file.metadata()?.len() as usize;
        let entry_count = len / ENTRY_SIZE;

        let mut entries = Vec::with_capacity(entry_count);
        let mut reader = BufReader::new(&file);
        let mut buf = [0u8; ENTRY_SIZE];
        for _ in 0..entry_count {
            reader.read_exact(&mut buf)?;
            entries.push(TimeIndexEntry {
                timestamp_ms: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
                relative_offset: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            });
        }

        Ok(Self { file, entries })
    }

    pub fn rebuild(timeindex_path: &Path, log_path: &Path, base_offset: u64) -> Result<Self> {
        let log_file = File::open(log_path)?;
        let mut reader = BufReader::new(log_file);
        let mut entries = Vec::new();
        while let Some(record) = Record::decode(&mut reader)? {
            let rel = (record.offset - base_offset) as u32;
            entries.push(TimeIndexEntry {
                timestamp_ms: record.timestamp_ms,
                relative_offset: rel,
            });
        }

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(timeindex_path)?;
            let mut writer = BufWriter::new(file);
            for entry in &entries {
                writer.write_all(&entry.timestamp_ms.to_be_bytes())?;
                writer.write_all(&entry.relative_offset.to_be_bytes())?;
            }
            writer.flush()?;
        }

        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(timeindex_path)?;

        Ok(Self { file, entries })
    }

    pub fn append(&mut self, timestamp_ms: u64, relative_offset: u32) -> Result<()> {
        let entry = TimeIndexEntry {
            timestamp_ms,
            relative_offset,
        };
        self.file.write_all(&timestamp_ms.to_be_bytes())?;
        self.file.write_all(&relative_offset.to_be_bytes())?;
        self.entries.push(entry);
        Ok(())
    }

    /// Binary search for the first entry with timestamp_ms >= target.
    /// Returns the relative_offset of that entry.
    pub fn lookup(&self, timestamp_ms: u64) -> Option<u32> {
        if self.entries.is_empty() {
            return None;
        }
        let idx = self
            .entries
            .partition_point(|e| e.timestamp_ms < timestamp_ms);
        self.entries.get(idx).map(|e| e.relative_offset)
    }

    /// Return the timestamp of the first entry, if any.
    pub fn first_timestamp(&self) -> Option<u64> {
        self.entries.first().map(|e| e.timestamp_ms)
    }

    pub fn flush(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_append_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut ti = TimeIndex::create(&path).unwrap();

        ti.append(1000, 0).unwrap();
        ti.append(2000, 1).unwrap();
        ti.append(3000, 2).unwrap();

        assert_eq!(ti.lookup(1000), Some(0));
        assert_eq!(ti.lookup(2000), Some(1));
        assert_eq!(ti.lookup(3000), Some(2));
        assert_eq!(ti.len(), 3);
    }

    #[test]
    fn lookup_between_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut ti = TimeIndex::create(&path).unwrap();

        ti.append(1000, 0).unwrap();
        ti.append(3000, 1).unwrap();
        ti.append(5000, 2).unwrap();

        // 2000 is between 1000 and 3000, should return first entry >= 2000 which is 3000 -> offset 1
        assert_eq!(ti.lookup(2000), Some(1));
        assert_eq!(ti.lookup(4000), Some(2));
    }

    #[test]
    fn lookup_before_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut ti = TimeIndex::create(&path).unwrap();

        ti.append(5000, 0).unwrap();
        ti.append(6000, 1).unwrap();

        // Before all entries, returns the first entry
        assert_eq!(ti.lookup(1000), Some(0));
    }

    #[test]
    fn lookup_after_all() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut ti = TimeIndex::create(&path).unwrap();

        ti.append(1000, 0).unwrap();
        ti.append(2000, 1).unwrap();

        // After all entries, returns None
        assert_eq!(ti.lookup(3000), None);
    }

    #[test]
    fn lookup_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let ti = TimeIndex::create(&path).unwrap();
        assert_eq!(ti.lookup(1000), None);
    }

    #[test]
    fn load_persisted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        {
            let mut ti = TimeIndex::create(&path).unwrap();
            ti.append(1000, 0).unwrap();
            ti.append(2000, 1).unwrap();
            ti.flush().unwrap();
        }

        let ti = TimeIndex::load(&path).unwrap();
        assert_eq!(ti.len(), 2);
        assert_eq!(ti.lookup(1000), Some(0));
        assert_eq!(ti.lookup(2000), Some(1));
    }

    #[test]
    fn first_timestamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.timeindex");
        let mut ti = TimeIndex::create(&path).unwrap();
        assert_eq!(ti.first_timestamp(), None);
        ti.append(5000, 0).unwrap();
        ti.append(6000, 1).unwrap();
        assert_eq!(ti.first_timestamp(), Some(5000));
    }
}

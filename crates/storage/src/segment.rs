use std::fs::{File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::error::{Result, StorageError};
use crate::index::Index;
use crate::record::Record;

const FILENAME_WIDTH: usize = 20;

pub struct Segment {
    dir: PathBuf,
    base_offset: u64,
    log_file: File,
    index: Index,
    size: u64,
    next_offset: u64,
}

impl Segment {
    pub fn create(dir: &Path, base_offset: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let (log_path, index_path) = segment_paths(dir, base_offset);
        let log_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .read(true)
            .open(&log_path)?;
        let index = Index::create(&index_path)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            index,
            size: 0,
            next_offset: base_offset,
        })
    }

    pub fn open(dir: &Path, base_offset: u64) -> Result<Self> {
        let (log_path, index_path) = segment_paths(dir, base_offset);
        let log_file = OpenOptions::new().read(true).write(true).open(&log_path)?;
        let size = log_file.metadata()?.len();

        let index = if index_path.exists() {
            Index::load(&index_path)?
        } else {
            Index::rebuild(&index_path, &log_path, base_offset)?
        };

        let next_offset = base_offset + index.entry_count() as u64;

        Ok(Self {
            dir: dir.to_path_buf(),
            base_offset,
            log_file,
            index,
            size,
            next_offset,
        })
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<u64> {
        let offset = self.next_offset;
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let record = Record {
            offset,
            timestamp_ms,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };

        let position = self.size as u32;
        record.write_to(&mut self.log_file)?;
        self.log_file.flush()?;

        let relative_offset = (offset - self.base_offset) as u32;
        self.index.append(relative_offset, position)?;

        self.size += record.encoded_size() as u64;
        self.next_offset = offset + 1;
        Ok(offset)
    }

    pub fn read_at(&self, offset: u64) -> Result<Record> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Err(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest: self.base_offset,
                latest: self.next_offset,
            });
        }
        let relative = (offset - self.base_offset) as u32;
        let position = self
            .index
            .lookup(relative)
            .ok_or(StorageError::OffsetOutOfRange {
                requested: offset,
                earliest: self.base_offset,
                latest: self.next_offset,
            })?;

        let mut reader = BufReader::new(&self.log_file);
        reader.seek(SeekFrom::Start(position as u64))?;
        Record::decode(&mut reader)?.ok_or(StorageError::CorruptRecord { offset })
    }

    pub fn read_from(&self, start_offset: u64, max_records: u32) -> Result<Vec<Record>> {
        if start_offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let start = start_offset.max(self.base_offset);
        let relative = (start - self.base_offset) as u32;

        let idx = match self.index.find_index(relative) {
            Some(i) => i,
            None => return Ok(Vec::new()),
        };

        let position = self.index.position_at(idx).unwrap();
        let mut reader = BufReader::new(&self.log_file);
        reader.seek(SeekFrom::Start(position as u64))?;

        let mut records = Vec::new();
        for _ in 0..max_records {
            match Record::decode(&mut reader)? {
                Some(r) => records.push(r),
                None => break,
            }
        }
        Ok(records)
    }

    /// Validate all records in the segment, truncate at first corruption, rebuild index.
    pub fn recover(&mut self) -> Result<()> {
        let (log_path, index_path) = segment_paths(&self.dir, self.base_offset);
        let mut reader = BufReader::new(File::open(&log_path)?);
        let mut valid_size: u64 = 0;
        let mut count: u64 = 0;

        loop {
            let pos_before = valid_size;
            match Record::decode(&mut reader) {
                Ok(Some(record)) => {
                    valid_size += record.encoded_size() as u64;
                    count += 1;
                }
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!(
                        base_offset = self.base_offset,
                        truncate_at = pos_before,
                        "truncating corrupt tail in segment"
                    );
                    self.log_file.set_len(pos_before)?;
                    break;
                }
            }
        }

        self.size = valid_size;
        self.next_offset = self.base_offset + count;
        self.index = Index::rebuild(&index_path, &log_path, self.base_offset)?;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.log_file.sync_all()?;
        self.index.flush()?;
        Ok(())
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

pub fn segment_paths(dir: &Path, base_offset: u64) -> (PathBuf, PathBuf) {
    let name = format!("{:0>width$}", base_offset, width = FILENAME_WIDTH);
    (
        dir.join(format!("{}.log", name)),
        dir.join(format!("{}.index", name)),
    )
}

/// Parse a base offset from a segment filename like "00000000000000001024.log".
pub fn parse_base_offset(filename: &str) -> Option<u64> {
    let stem = filename.strip_suffix(".log")?;
    stem.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_read() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();

        let o0 = seg.append(b"k0", b"v0").unwrap();
        let o1 = seg.append(b"k1", b"v1").unwrap();
        assert_eq!(o0, 0);
        assert_eq!(o1, 1);

        let r0 = seg.read_at(0).unwrap();
        assert_eq!(r0.key.as_ref(), b"k0");
        assert_eq!(r0.value.as_ref(), b"v0");

        let r1 = seg.read_at(1).unwrap();
        assert_eq!(r1.key.as_ref(), b"k1");
        assert_eq!(r1.value.as_ref(), b"v1");
    }

    #[test]
    fn read_from_sequential() {
        let dir = tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0).unwrap();
        for i in 0..5u8 {
            seg.append(&[i], &[i + 10]).unwrap();
        }

        let records = seg.read_from(2, 10).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, 2);
        assert_eq!(records[2].offset, 4);
    }

    #[test]
    fn open_existing() {
        let dir = tempdir().unwrap();
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"a", b"1").unwrap();
            seg.append(b"b", b"2").unwrap();
            seg.flush().unwrap();
        }
        let seg = Segment::open(dir.path(), 0).unwrap();
        assert_eq!(seg.next_offset(), 2);
        let r = seg.read_at(1).unwrap();
        assert_eq!(r.key.as_ref(), b"b");
    }

    #[test]
    fn recover_truncates_corrupt_tail() {
        let dir = tempdir().unwrap();
        let log_path = segment_paths(dir.path(), 0).0;
        {
            let mut seg = Segment::create(dir.path(), 0).unwrap();
            seg.append(b"good", b"data").unwrap();
            seg.flush().unwrap();
        }
        // Append garbage bytes to simulate partial write
        {
            let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
            f.write_all(&[0xFF; 20]).unwrap();
        }

        let mut seg = Segment::open(dir.path(), 0).unwrap();
        seg.recover().unwrap();
        assert_eq!(seg.next_offset(), 1);
        let r = seg.read_at(0).unwrap();
        assert_eq!(r.key.as_ref(), b"good");
    }
}

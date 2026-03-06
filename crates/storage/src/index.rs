use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::record::Record;

const ENTRY_SIZE: usize = 8; // u32 relative_offset + u32 position

#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    relative_offset: u32,
    position: u32,
}

pub struct Index {
    path: PathBuf,
    file: File,
    entries: Vec<IndexEntry>,
}

impl Index {
    pub fn create(path: &Path) -> Result<Self> {
        // Truncate first, then reopen in append mode
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let file = OpenOptions::new().append(true).read(true).open(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            entries: Vec::new(),
        })
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(path)?;
        let meta = file.metadata()?;
        let len = meta.len() as usize;
        let entry_count = len / ENTRY_SIZE;

        let mut entries = Vec::with_capacity(entry_count);
        let mut reader = BufReader::new(&file);
        let mut buf = [0u8; ENTRY_SIZE];
        for _ in 0..entry_count {
            reader.read_exact(&mut buf)?;
            entries.push(IndexEntry {
                relative_offset: u32::from_be_bytes(buf[0..4].try_into().unwrap()),
                position: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            });
        }

        Ok(Self {
            path: path.to_path_buf(),
            file,
            entries,
        })
    }

    /// Rebuild the index by scanning the .log file from the beginning.
    pub fn rebuild(index_path: &Path, log_path: &Path, base_offset: u64) -> Result<Self> {
        let log_file = File::open(log_path)?;
        let mut reader = BufReader::new(log_file);
        let mut entries = Vec::new();
        let mut file_pos: u64 = 0;

        loop {
            match Record::decode(&mut reader)? {
                Some(record) => {
                    let rel = (record.offset - base_offset) as u32;
                    entries.push(IndexEntry {
                        relative_offset: rel,
                        position: file_pos as u32,
                    });
                    file_pos += record.encoded_size() as u64;
                }
                None => break,
            }
        }

        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(index_path)?;
            let mut writer = BufWriter::new(file);
            for entry in &entries {
                writer.write_all(&entry.relative_offset.to_be_bytes())?;
                writer.write_all(&entry.position.to_be_bytes())?;
            }
            writer.flush()?;
        }

        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(index_path)?;

        Ok(Self {
            path: index_path.to_path_buf(),
            file,
            entries,
        })
    }

    pub fn append(&mut self, relative_offset: u32, position: u32) -> Result<()> {
        let entry = IndexEntry {
            relative_offset,
            position,
        };
        self.file.write_all(&relative_offset.to_be_bytes())?;
        self.file.write_all(&position.to_be_bytes())?;
        self.entries.push(entry);
        Ok(())
    }

    /// Binary search for the byte position of the given relative offset.
    pub fn lookup(&self, relative_offset: u32) -> Option<u32> {
        self.entries
            .binary_search_by_key(&relative_offset, |e| e.relative_offset)
            .ok()
            .map(|i| self.entries[i].position)
    }

    /// Find the index of the entry with the given relative offset, for sequential scanning.
    pub fn find_index(&self, relative_offset: u32) -> Option<usize> {
        self.entries
            .binary_search_by_key(&relative_offset, |e| e.relative_offset)
            .ok()
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Get the byte position for the entry at a given vec index.
    pub fn position_at(&self, idx: usize) -> Option<u32> {
        self.entries.get(idx).map(|e| e.position)
    }

    pub fn flush(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// Truncate the index to contain only the first `count` entries, rewriting the file.
    pub fn truncate(&mut self, count: usize) -> Result<()> {
        self.entries.truncate(count);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(&file);
        for entry in &self.entries {
            writer.write_all(&entry.relative_offset.to_be_bytes())?;
            writer.write_all(&entry.position.to_be_bytes())?;
        }
        writer.flush()?;
        // Reopen in append mode for future writes
        self.file = OpenOptions::new()
            .append(true)
            .read(true)
            .open(&self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_append_lookup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");
        let mut idx = Index::create(&path).unwrap();

        idx.append(0, 0).unwrap();
        idx.append(1, 100).unwrap();
        idx.append(2, 250).unwrap();

        assert_eq!(idx.lookup(0), Some(0));
        assert_eq!(idx.lookup(1), Some(100));
        assert_eq!(idx.lookup(2), Some(250));
        assert_eq!(idx.lookup(3), None);
    }

    #[test]
    fn load_persisted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.index");
        {
            let mut idx = Index::create(&path).unwrap();
            idx.append(0, 0).unwrap();
            idx.append(1, 64).unwrap();
            idx.flush().unwrap();
        }

        let idx = Index::load(&path).unwrap();
        assert_eq!(idx.entry_count(), 2);
        assert_eq!(idx.lookup(0), Some(0));
        assert_eq!(idx.lookup(1), Some(64));
    }
}

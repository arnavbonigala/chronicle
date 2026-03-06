use bytes::Bytes;
use std::io::{Read, Write};

use crate::error::{Result, StorageError};

/// On-disk layout:
/// [length: u32][crc32: u32][offset: u64][timestamp_ms: u64]
/// [key_len: u32][key][value_len: u32][value]
///
/// `length` covers bytes from crc32 through end of value.
/// `crc32` covers bytes from offset through end of value.
const HEADER_SIZE: usize = 4 + 4 + 8 + 8 + 4 + 4; // length + crc + offset + ts + key_len + val_len = 32

#[derive(Debug, Clone)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Bytes,
    pub value: Bytes,
}

impl Record {
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.key.len() + self.value.len()
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let payload_len = (4 + 8 + 8 + 4 + self.key.len() + 4 + self.value.len()) as u32; // crc through value

        let crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&self.offset.to_be_bytes());
            hasher.update(&self.timestamp_ms.to_be_bytes());
            hasher.update(&(self.key.len() as u32).to_be_bytes());
            hasher.update(&self.key);
            hasher.update(&(self.value.len() as u32).to_be_bytes());
            hasher.update(&self.value);
            hasher.finalize()
        };

        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&(self.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.value);
    }

    /// Decode one record from a reader. Returns `Ok(None)` on clean EOF at record boundary.
    pub fn decode(reader: &mut impl Read) -> Result<Option<Self>> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let payload_len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; payload_len];
        reader.read_exact(&mut payload)?;

        let crc_stored = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let crc_data = &payload[4..];
        let crc_computed = crc32fast::hash(crc_data);

        if crc_stored != crc_computed {
            // Try to extract offset for the error message
            let offset = if crc_data.len() >= 8 {
                u64::from_be_bytes(crc_data[0..8].try_into().unwrap())
            } else {
                0
            };
            return Err(StorageError::CorruptRecord { offset });
        }

        let mut pos = 4; // skip crc
        let offset = u64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let timestamp_ms = u64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let key_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let key = Bytes::copy_from_slice(&payload[pos..pos + key_len]);
        pos += key_len;
        let value_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let value = Bytes::copy_from_slice(&payload[pos..pos + value_len]);

        Ok(Some(Record {
            offset,
            timestamp_ms,
            key,
            value,
        }))
    }

    /// Write the encoded record directly to a writer.
    pub fn write_to(&self, writer: &mut impl Write) -> Result<()> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        self.encode(&mut buf);
        writer.write_all(&buf)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip() {
        let record = Record {
            offset: 42,
            timestamp_ms: 1710000000000,
            key: Bytes::from_static(b"key1"),
            value: Bytes::from_static(b"hello world"),
        };
        let mut buf = Vec::new();
        record.encode(&mut buf);
        assert_eq!(buf.len(), record.encoded_size());

        let mut cursor = Cursor::new(&buf);
        let decoded = Record::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp_ms, 1710000000000);
        assert_eq!(decoded.key, Bytes::from_static(b"key1"));
        assert_eq!(decoded.value, Bytes::from_static(b"hello world"));
    }

    #[test]
    fn eof_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        assert!(Record::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn corrupt_crc_detected() {
        let record = Record {
            offset: 0,
            timestamp_ms: 0,
            key: Bytes::new(),
            value: Bytes::from_static(b"data"),
        };
        let mut buf = Vec::new();
        record.encode(&mut buf);
        // Flip a byte in the value region
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(matches!(
            Record::decode(&mut cursor),
            Err(StorageError::CorruptRecord { .. })
        ));
    }
}

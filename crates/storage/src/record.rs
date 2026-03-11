use bytes::Bytes;
use std::io::{Read, Write};

use crate::error::{Result, StorageError};

/// On-disk layout v2:
/// [length: u32]            -- byte count from crc32 through end of value
/// [crc32: u32]             -- CRC-32C of bytes from attributes through end of value
/// [attributes: u8]         -- bit 0: is_control, bit 1: is_transactional
/// [offset: u64]
/// [timestamp_ms: u64]
/// [producer_id: u64]
/// [producer_epoch: u16]
/// [sequence_number: u32]
/// [header_count: u32]
/// for each header:
///   [header_key_len: u32][header_key_utf8]
///   [header_value_len: u32][header_value]
/// [key_len: u32][key]
/// [value_len: u32][value]

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub offset: u64,
    pub timestamp_ms: u64,
    pub key: Bytes,
    pub value: Bytes,
    pub headers: Vec<RecordHeader>,
    pub producer_id: u64,
    pub producer_epoch: u16,
    pub sequence_number: u32,
    pub is_transactional: bool,
    pub is_control: bool,
}

impl Record {
    pub fn new(offset: u64, timestamp_ms: u64, key: Bytes, value: Bytes) -> Self {
        Self {
            offset,
            timestamp_ms,
            key,
            value,
            headers: Vec::new(),
            producer_id: 0,
            producer_epoch: 0,
            sequence_number: 0,
            is_transactional: false,
            is_control: false,
        }
    }

    fn headers_size(&self) -> usize {
        let mut size = 0;
        for h in &self.headers {
            size += 4 + h.key.len() + 4 + h.value.len();
        }
        size
    }

    /// Total on-disk size including the length prefix.
    pub fn encoded_size(&self) -> usize {
        // length(4) + crc(4) + attributes(1) + offset(8) + timestamp(8)
        // + producer_id(8) + producer_epoch(2) + sequence_number(4)
        // + header_count(4) + headers_data
        // + key_len(4) + key + value_len(4) + value
        4 + 4
            + 1
            + 8
            + 8
            + 8
            + 2
            + 4
            + 4
            + self.headers_size()
            + 4
            + self.key.len()
            + 4
            + self.value.len()
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        let payload_len = (self.encoded_size() - 4) as u32; // everything after the length field

        let attributes = (self.is_control as u8) | ((self.is_transactional as u8) << 1);

        let crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[attributes]);
            hasher.update(&self.offset.to_be_bytes());
            hasher.update(&self.timestamp_ms.to_be_bytes());
            hasher.update(&self.producer_id.to_be_bytes());
            hasher.update(&self.producer_epoch.to_be_bytes());
            hasher.update(&self.sequence_number.to_be_bytes());
            hasher.update(&(self.headers.len() as u32).to_be_bytes());
            for h in &self.headers {
                hasher.update(&(h.key.len() as u32).to_be_bytes());
                hasher.update(h.key.as_bytes());
                hasher.update(&(h.value.len() as u32).to_be_bytes());
                hasher.update(&h.value);
            }
            hasher.update(&(self.key.len() as u32).to_be_bytes());
            hasher.update(&self.key);
            hasher.update(&(self.value.len() as u32).to_be_bytes());
            hasher.update(&self.value);
            hasher.finalize()
        };

        buf.extend_from_slice(&payload_len.to_be_bytes());
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.push(attributes);
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&self.timestamp_ms.to_be_bytes());
        buf.extend_from_slice(&self.producer_id.to_be_bytes());
        buf.extend_from_slice(&self.producer_epoch.to_be_bytes());
        buf.extend_from_slice(&self.sequence_number.to_be_bytes());
        buf.extend_from_slice(&(self.headers.len() as u32).to_be_bytes());
        for h in &self.headers {
            buf.extend_from_slice(&(h.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(h.key.as_bytes());
            buf.extend_from_slice(&(h.value.len() as u32).to_be_bytes());
            buf.extend_from_slice(&h.value);
        }
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
        let crc_data = &payload[4..]; // attributes through end
        let crc_computed = crc32fast::hash(crc_data);

        if crc_stored != crc_computed {
            let offset = if crc_data.len() >= 9 {
                u64::from_be_bytes(crc_data[1..9].try_into().unwrap())
            } else {
                0
            };
            return Err(StorageError::CorruptRecord { offset });
        }

        let mut pos = 4; // skip crc
        let attributes = payload[pos];
        pos += 1;
        let is_control = (attributes & 1) != 0;
        let is_transactional = (attributes & 2) != 0;

        let offset = u64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let timestamp_ms = u64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let producer_id = u64::from_be_bytes(payload[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let producer_epoch = u16::from_be_bytes(payload[pos..pos + 2].try_into().unwrap());
        pos += 2;
        let sequence_number = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap());
        pos += 4;

        let header_count = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let hk_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let hk = String::from_utf8_lossy(&payload[pos..pos + hk_len]).into_owned();
            pos += hk_len;
            let hv_len = u32::from_be_bytes(payload[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let hv = Bytes::copy_from_slice(&payload[pos..pos + hv_len]);
            pos += hv_len;
            headers.push(RecordHeader { key: hk, value: hv });
        }

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
            headers,
            producer_id,
            producer_epoch,
            sequence_number,
            is_transactional,
            is_control,
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
        let record = Record::new(
            42,
            1710000000000,
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"hello world"),
        );
        let mut buf = Vec::new();
        record.encode(&mut buf);
        assert_eq!(buf.len(), record.encoded_size());

        let mut cursor = Cursor::new(&buf);
        let decoded = Record::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.timestamp_ms, 1710000000000);
        assert_eq!(decoded.key, Bytes::from_static(b"key1"));
        assert_eq!(decoded.value, Bytes::from_static(b"hello world"));
        assert_eq!(decoded.producer_id, 0);
        assert_eq!(decoded.producer_epoch, 0);
        assert_eq!(decoded.sequence_number, 0);
        assert!(!decoded.is_transactional);
        assert!(!decoded.is_control);
        assert!(decoded.headers.is_empty());
    }

    #[test]
    fn roundtrip_with_all_fields() {
        let record = Record {
            offset: 100,
            timestamp_ms: 1710000000000,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            headers: vec![
                RecordHeader {
                    key: "content-type".to_string(),
                    value: Bytes::from_static(b"application/json"),
                },
                RecordHeader {
                    key: "trace-id".to_string(),
                    value: Bytes::from_static(b"abc123"),
                },
            ],
            producer_id: 42,
            producer_epoch: 3,
            sequence_number: 7,
            is_transactional: true,
            is_control: false,
        };
        let mut buf = Vec::new();
        record.encode(&mut buf);
        assert_eq!(buf.len(), record.encoded_size());

        let mut cursor = Cursor::new(&buf);
        let d = Record::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(d.offset, 100);
        assert_eq!(d.producer_id, 42);
        assert_eq!(d.producer_epoch, 3);
        assert_eq!(d.sequence_number, 7);
        assert!(d.is_transactional);
        assert!(!d.is_control);
        assert_eq!(d.headers.len(), 2);
        assert_eq!(d.headers[0].key, "content-type");
        assert_eq!(d.headers[0].value, Bytes::from_static(b"application/json"));
        assert_eq!(d.headers[1].key, "trace-id");
        assert_eq!(d.headers[1].value, Bytes::from_static(b"abc123"));
    }

    #[test]
    fn roundtrip_control_record() {
        let record = Record {
            offset: 50,
            timestamp_ms: 1710000000000,
            key: Bytes::from_static(&[0, 0, 0, 1]),
            value: Bytes::new(),
            headers: Vec::new(),
            producer_id: 10,
            producer_epoch: 1,
            sequence_number: 0,
            is_transactional: true,
            is_control: true,
        };
        let mut buf = Vec::new();
        record.encode(&mut buf);

        let mut cursor = Cursor::new(&buf);
        let d = Record::decode(&mut cursor).unwrap().unwrap();
        assert!(d.is_control);
        assert!(d.is_transactional);
        assert_eq!(d.producer_id, 10);
    }

    #[test]
    fn eof_returns_none() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        assert!(Record::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn corrupt_crc_detected() {
        let record = Record::new(0, 0, Bytes::new(), Bytes::from_static(b"data"));
        let mut buf = Vec::new();
        record.encode(&mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let mut cursor = Cursor::new(&buf);
        assert!(matches!(
            Record::decode(&mut cursor),
            Err(StorageError::CorruptRecord { .. })
        ));
    }
}

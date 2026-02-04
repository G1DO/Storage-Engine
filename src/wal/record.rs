// TODO [M06]: Implement WAL record format
//   - Serialization: WALRecord → bytes on disk
//   - Deserialization: bytes → WALRecord
//   - CRC computation and verification

use crate::error::{Error, Result};

/// Record type stored in the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    Put = 0x01,
    Delete = 0x02,
}

impl RecordType {
    fn from_u8(byte: u8) -> Result<Self> {
        match byte {
            0x01 => Ok(RecordType::Put),
            0x02 => Ok(RecordType::Delete),
            _ => Err(Error::Corruption(format!("invalid record type: {}", byte))),
        }
    }
}

/// A single record in the WAL.
///
/// On-disk format:
/// ```text
/// ┌──────────┬────────┬──────────┬───────────┬───────────┬──────────┐
/// │ CRC (4B) │ Len(4B)│ Type(1B) │ Key Len(4B│ Key (var) │Val (var) │
/// └──────────┴────────┴──────────┴───────────┴───────────┴──────────┘
/// ```
///
/// CRC covers everything after the CRC field itself.
/// If CRC doesn't match on read, the record was a partial write (crash mid-write)
/// and recovery stops here — all preceding records are valid.
#[derive(Debug, Clone)]
pub struct WALRecord {
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

// Header sizes
const CRC_SIZE: usize = 4;
const LEN_SIZE: usize = 4;
const TYPE_SIZE: usize = 1;
const KEY_LEN_SIZE: usize = 4;
const HEADER_SIZE: usize = CRC_SIZE + LEN_SIZE + TYPE_SIZE + KEY_LEN_SIZE;

impl WALRecord {
    /// Create a Put record.
    pub fn put(key: Vec<u8>, value: Vec<u8>) -> Self {
        WALRecord {
            record_type: RecordType::Put,
            key,
            value,
        }
    }

    /// Create a Delete record.
    pub fn delete(key: Vec<u8>) -> Self {
        WALRecord {
            record_type: RecordType::Delete,
            key,
            value: Vec::new(),
        }
    }

    /// Serialize this record to bytes (including CRC header).
    pub fn encode(&self) -> Vec<u8> {
        let payload_len = TYPE_SIZE + KEY_LEN_SIZE + self.key.len() + self.value.len();
        let total_len = CRC_SIZE + LEN_SIZE + payload_len;

        let mut buf = Vec::with_capacity(total_len);

        // Reserve space for CRC (we'll fill it at the end)
        buf.extend_from_slice(&[0u8; CRC_SIZE]);

        // Length (of everything after CRC and Length fields)
        buf.extend_from_slice(&(payload_len as u32).to_le_bytes());

        // Type
        buf.push(self.record_type as u8);

        // Key length
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());

        // Key
        buf.extend_from_slice(&self.key);

        // Value
        buf.extend_from_slice(&self.value);

        // Compute CRC over everything after CRC field
        let crc = crc32fast::hash(&buf[CRC_SIZE..]);
        buf[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());

        buf
    }

    /// Deserialize a record from bytes. Returns error if CRC doesn't match.
    pub fn decode(data: &[u8]) -> Result<Self> {
        // Need at least header
        if data.len() < HEADER_SIZE {
            return Err(Error::Corruption("record too short".into()));
        }

        // Read CRC
        let stored_crc = u32::from_le_bytes(data[0..4].try_into().unwrap());

        // Read length
        let payload_len = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;

        // Check we have enough data
        let total_len = CRC_SIZE + LEN_SIZE + payload_len;
        if data.len() < total_len {
            return Err(Error::Corruption("record truncated".into()));
        }

        // Verify CRC (covers everything after CRC field)
        let computed_crc = crc32fast::hash(&data[CRC_SIZE..total_len]);
        if stored_crc != computed_crc {
            return Err(Error::Corruption("CRC mismatch".into()));
        }

        // Parse the payload
        let mut offset = CRC_SIZE + LEN_SIZE;

        // Type
        let record_type = RecordType::from_u8(data[offset])?;
        offset += TYPE_SIZE;

        // Key length
        let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += KEY_LEN_SIZE;

        // Key
        if offset + key_len > total_len {
            return Err(Error::Corruption("key length exceeds record".into()));
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;

        // Value (rest of the record)
        let value = data[offset..total_len].to_vec();

        Ok(WALRecord {
            record_type,
            key,
            value,
        })
    }

    /// Size of this record when serialized on disk.
    pub fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.key.len() + self.value.len()
    }
}

/// Magic number to identify SSTable files.
pub const SSTABLE_MAGIC: u64 = 0x4C534D5F53535400; // "LSM_SST\0"

/// Metadata about an SSTable file, stored in the manifest.
#[derive(Debug, Clone)]
pub struct SSTableMeta {
    /// Unique SSTable identifier.
    pub id: u64,
    /// Level this SSTable belongs to (0 = freshly flushed).
    pub level: u32,
    /// Smallest key in the SSTable.
    pub min_key: Vec<u8>,
    /// Largest key in the SSTable.
    pub max_key: Vec<u8>,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of entries (including tombstones).
    pub entry_count: u64,
}

/// An entry in the SSTable's index block.
/// Maps a block's last key to its location in the file.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    /// Last (largest) key in the block.
    pub last_key: Vec<u8>,
    /// Byte offset of the block in the file.
    pub offset: u64,
    /// Size of the block in bytes.
    pub size: u64,
}

impl IndexEntry {
    /// Encode this index entry to bytes.
    /// Format: [key_len(2B)][key][offset(8B)][size(8B)]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.last_key.len() + 16);
        buf.extend_from_slice(&(self.last_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.last_key);
        buf.extend_from_slice(&self.offset.to_le_bytes());
        buf.extend_from_slice(&self.size.to_le_bytes());
        buf
    }

    /// Decode an index entry from bytes, returning (entry, bytes_consumed).
    pub fn decode(data: &[u8]) -> crate::error::Result<(Self, usize)> {
        if data.len() < 2 {
            return Err(crate::error::Error::Corruption(
                "index entry too short".into(),
            ));
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        let total = 2 + key_len + 16;
        if data.len() < total {
            return Err(crate::error::Error::Corruption(
                "index entry truncated".into(),
            ));
        }
        let last_key = data[2..2 + key_len].to_vec();
        let offset = u64::from_le_bytes(data[2 + key_len..10 + key_len].try_into().unwrap());
        let size = u64::from_le_bytes(data[10 + key_len..18 + key_len].try_into().unwrap());
        Ok((IndexEntry { last_key, offset, size }, total))
    }
}

/// The footer sits at the end of the SSTable file.
/// It tells the reader where to find the index block and meta blocks.
///
/// ```text
/// ┌──────────────────────────────────────┐
/// │ Index block offset (8B)              │
/// │ Index block size (8B)                │
/// │ Meta block offset (8B)               │
/// │ Meta block size (8B)                 │
/// │ Magic number (8B)                    │
/// └──────────────────────────────────────┘
/// ```
#[derive(Debug, Clone)]
pub struct Footer {
    pub index_block_offset: u64,
    pub index_block_size: u64,
    pub meta_block_offset: u64,
    pub meta_block_size: u64,
    pub magic: u64,
}

impl Footer {
    /// Size of the footer in bytes (fixed).
    pub const SIZE: usize = 8 * 5; // 40 bytes

    /// Encode footer to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::SIZE);
        buf.extend_from_slice(&self.index_block_offset.to_le_bytes());
        buf.extend_from_slice(&self.index_block_size.to_le_bytes());
        buf.extend_from_slice(&self.meta_block_offset.to_le_bytes());
        buf.extend_from_slice(&self.meta_block_size.to_le_bytes());
        buf.extend_from_slice(&self.magic.to_le_bytes());
        buf
    }

    /// Decode footer from bytes.
    pub fn decode(data: &[u8]) -> crate::error::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(crate::error::Error::Corruption(
                "footer too short".into(),
            ));
        }
        let index_block_offset = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let index_block_size = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let meta_block_offset = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let meta_block_size = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let magic = u64::from_le_bytes(data[32..40].try_into().unwrap());

        if magic != SSTABLE_MAGIC {
            return Err(crate::error::Error::Corruption(format!(
                "bad magic: expected {:#x}, got {:#x}",
                SSTABLE_MAGIC, magic
            )));
        }

        Ok(Footer {
            index_block_offset,
            index_block_size,
            meta_block_offset,
            meta_block_size,
            magic,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_roundtrip() {
        let footer = Footer {
            index_block_offset: 4096,
            index_block_size: 512,
            meta_block_offset: 0,
            meta_block_size: 0,
            magic: SSTABLE_MAGIC,
        };
        let encoded = footer.encode();
        assert_eq!(encoded.len(), Footer::SIZE);
        let decoded = Footer::decode(&encoded).unwrap();
        assert_eq!(decoded.index_block_offset, 4096);
        assert_eq!(decoded.index_block_size, 512);
        assert_eq!(decoded.meta_block_offset, 0);
        assert_eq!(decoded.meta_block_size, 0);
        assert_eq!(decoded.magic, SSTABLE_MAGIC);
    }

    #[test]
    fn footer_bad_magic() {
        let mut encoded = Footer {
            index_block_offset: 0,
            index_block_size: 0,
            meta_block_offset: 0,
            meta_block_size: 0,
            magic: SSTABLE_MAGIC,
        }
        .encode();
        // Corrupt the magic
        encoded[32] = 0xFF;
        assert!(Footer::decode(&encoded).is_err());
    }

    #[test]
    fn footer_too_short() {
        assert!(Footer::decode(&[0u8; 10]).is_err());
    }

    #[test]
    fn index_entry_roundtrip() {
        let entry = IndexEntry {
            last_key: b"cherry".to_vec(),
            offset: 0,
            size: 4096,
        };
        let encoded = entry.encode();
        let (decoded, consumed) = IndexEntry::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.last_key, b"cherry");
        assert_eq!(decoded.offset, 0);
        assert_eq!(decoded.size, 4096);
    }
}

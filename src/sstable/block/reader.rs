use crate::error::Result;
use crate::iterator::StorageIterator;

/// A deserialized block. Holds the raw bytes + parsed offset array.
///
/// Supports two access patterns:
/// - Point lookup via binary search over the offset array
/// - Sequential scan via BlockIterator
pub struct Block {
    /// Just the entry bytes (offset array and count are stripped off after decode)
    data: Vec<u8>,
    /// Byte offset of each entry within `data`, parsed from the block tail
    offsets: Vec<u16>,
}

impl Block {
    /// Decode a block from raw bytes produced by BlockBuilder::build().
    pub fn decode(raw: Vec<u8>) -> Result<Self> {
        // Step 1: read num_entries from last 2 bytes
        let num_entries = u16::from_le_bytes([raw[raw.len() - 2], raw[raw.len() - 1]]) as usize;

        // Step 2: parse offset array (sits right before the 2-byte count)
        let offsets_start = raw.len() - 2 - (num_entries * 2);
        let mut offsets = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let pos = offsets_start + i * 2;
            offsets.push(u16::from_le_bytes([raw[pos], raw[pos + 1]]));
        }

        // Step 3: entry data is everything before the offset array
        let mut data = raw;
        data.truncate(offsets_start);

        Ok(Self { data, offsets })
    }

    /// Read the key at a given entry index.
    /// Entry layout: [key_len(2B)][val_len(2B)][key][value]
    pub fn key_at(&self, index: usize) -> &[u8] {
        let offset = self.offsets[index] as usize;
        let key_len = u16::from_le_bytes([self.data[offset], self.data[offset + 1]]) as usize;
        // key starts after key_len(2B) + val_len(2B) = 4 bytes
        &self.data[offset + 4..offset + 4 + key_len]
    }

    /// Read the value at a given entry index.
    pub fn value_at(&self, index: usize) -> &[u8] {
        let offset = self.offsets[index] as usize;
        let key_len = u16::from_le_bytes([self.data[offset], self.data[offset + 1]]) as usize;
        let val_len = u16::from_le_bytes([self.data[offset + 2], self.data[offset + 3]]) as usize;
        let val_start = offset + 4 + key_len;
        &self.data[val_start..val_start + val_len]
    }

    /// Get the offset array.
    pub fn offsets(&self) -> &[u16] {
        &self.offsets
    }

    /// Point lookup: binary search for a key within the block.
    /// Returns the value if found, None otherwise.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let mut lo = 0usize;
        let mut hi = self.offsets.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.key_at(mid);

            match mid_key.cmp(key) {
                std::cmp::Ordering::Equal => return Some(self.value_at(mid)),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Create an iterator positioned at the first entry.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            index: 0,
        }
    }
}

/// Sequential iterator over entries in a block.
pub struct BlockIterator<'a> {
    block: &'a Block,
    /// Current entry index; invalid when index >= block.offsets.len()
    index: usize,
}

impl<'a> StorageIterator for BlockIterator<'a> {
    fn key(&self) -> &[u8] {
        self.block.key_at(self.index)
    }

    fn value(&self) -> &[u8] {
        self.block.value_at(self.index)
    }

    fn is_valid(&self) -> bool {
        self.index < self.block.offsets.len()
    }

    fn next(&mut self) -> Result<()> {
        self.index += 1;
        Ok(())
    }

    /// Seek to the first entry with key >= target.
    /// Uses binary search — same logic as get() but finds the
    /// leftmost entry >= target instead of an exact match.
    fn seek(&mut self, key: &[u8]) -> Result<()> {
        let mut lo = 0usize;
        let mut hi = self.block.offsets.len();

        // Standard "lower_bound" binary search: find smallest index
        // where key_at(index) >= target
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.block.key_at(mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        self.index = lo; // equals offsets.len() if all keys < target
        Ok(())
    }
}

use crate::error::Result;
use crate::iterator::StorageIterator;

/// A decoded data block. Supports point lookups (binary search) and iteration.
pub struct Block {
    /// Raw block data (entries + offset array + num_entries).
    data: Vec<u8>,
    /// Parsed offsets for each entry (from the offset array at the end).
    offsets: Vec<u16>,
}

impl Block {
    /// Decode a block from its on-disk bytes.
    ///
    /// Layout: [entries...][offset_array (2B each)][num_entries (2B)]
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        if data.len() < 2 {
            return Err(crate::error::Error::Corruption(
                "block too short".into(),
            ));
        }

        // Read num_entries from the last 2 bytes
        let num_entries = u16::from_le_bytes([
            data[data.len() - 2],
            data[data.len() - 1],
        ]) as usize;

        // Read offset array (before num_entries)
        let offsets_start = data.len() - 2 - num_entries * 2;
        let mut offsets = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            let pos = offsets_start + i * 2;
            let offset = u16::from_le_bytes([data[pos], data[pos + 1]]);
            offsets.push(offset);
        }

        Ok(Block { data, offsets })
    }

    /// Point lookup: binary search for key, return value if found.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let mut lo = 0usize;
        let mut hi = self.offsets.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, _) = self.entry_at(mid);
            match k.cmp(key) {
                std::cmp::Ordering::Equal => {
                    let (_, v) = self.entry_at(mid);
                    return Some(v);
                }
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }

        None
    }

    /// Create an iterator over all entries in the block.
    pub fn iter(&self) -> BlockIterator<'_> {
        BlockIterator {
            block: self,
            index: 0,
        }
    }

    /// Read key and value at the given entry index.
    fn entry_at(&self, index: usize) -> (&[u8], &[u8]) {
        let offset = self.offsets[index] as usize;
        let key_len = u16::from_le_bytes([
            self.data[offset],
            self.data[offset + 1],
        ]) as usize;
        let val_len = u16::from_le_bytes([
            self.data[offset + 2],
            self.data[offset + 3],
        ]) as usize;

        let key_start = offset + 4;
        let val_start = key_start + key_len;

        (
            &self.data[key_start..key_start + key_len],
            &self.data[val_start..val_start + val_len],
        )
    }

    /// Number of entries in this block.
    fn num_entries(&self) -> usize {
        self.offsets.len()
    }
}

/// Iterator over entries in a block.
pub struct BlockIterator<'a> {
    block: &'a Block,
    index: usize,
}

impl<'a> StorageIterator for BlockIterator<'a> {
    fn key(&self) -> &[u8] {
        let (k, _) = self.block.entry_at(self.index);
        k
    }

    fn value(&self) -> &[u8] {
        let (_, v) = self.block.entry_at(self.index);
        v
    }

    fn is_valid(&self) -> bool {
        self.index < self.block.num_entries()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.index += 1;
        }
        Ok(())
    }

    fn seek(&mut self, key: &[u8]) -> Result<()> {
        // Binary search: find first entry with key >= target
        let mut lo = 0usize;
        let mut hi = self.block.num_entries();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, _) = self.block.entry_at(mid);
            if k < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        self.index = lo;
        Ok(())
    }
}

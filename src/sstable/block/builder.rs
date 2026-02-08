/// Accumulates sorted key-value pairs and serializes them into a block.
///
/// A block is typically 4KB (matching OS page size / SSD block size).
/// Contains sorted entries + an offset array for binary search.
///
/// On-disk layout of a block:
/// ```text
/// ┌────────────────────────────────────────────┐
/// │ Entry 0: [key_len(2B)][val_len(2B)][key][value] │
/// │ Entry 1: ...                                │
/// │ Entry N: ...                                │
/// ├────────────────────────────────────────────┤
/// │ Offset array: [off_0(2B)][off_1(2B)]...[off_N(2B)] │
/// │ Num entries (2B)                            │
/// └────────────────────────────────────────────┘
/// ```
///
/// The offset array at the end enables binary search without parsing
/// every entry — jump to offsets[mid], read the key, compare.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
}

impl BlockBuilder {
    /// Create a new block builder with target block size.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size,
        }
    }

    /// Add a key-value pair to the block.
    /// Returns false if the block is full (entry doesn't fit).
    /// First entry is always accepted even if it exceeds block_size.
    /// Entries MUST be added in sorted key order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let entry_size = 2 + 2 + key.len() + value.len(); // key_len + val_len + key + value

        // Check if adding this entry would exceed the target block size.
        // Always accept the first entry so we never produce an empty block.
        if !self.offsets.is_empty() && self.estimated_size() + entry_size > self.block_size {
            return false;
        }

        // Record offset of this entry
        self.offsets.push(self.data.len() as u16);

        // Serialize: key_len (2B) | val_len (2B) | key | value
        self.data.extend_from_slice(&(key.len() as u16).to_le_bytes());
        self.data.extend_from_slice(&(value.len() as u16).to_le_bytes());
        self.data.extend_from_slice(key);
        self.data.extend_from_slice(value);

        true
    }

    /// Finalize the block: append offset array and entry count.
    pub fn build(self) -> Vec<u8> {
        let mut block = self.data;

        // Append offset array
        for offset in &self.offsets {
            block.extend_from_slice(&offset.to_le_bytes());
        }

        // Append num entries
        block.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());

        block
    }

    /// Current estimated size of the block (data + offsets + count).
    pub fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * 2 + 2
    }

    /// Whether the block is empty (no entries added).
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

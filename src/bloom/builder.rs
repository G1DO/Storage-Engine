use crate::bloom::BloomFilter;

// TODO [M18]: Integrate bloom filter builder into SSTable build path

/// Convenience builder for constructing a bloom filter during SSTable creation.
///
/// Usage during SSTable build:
/// 1. Create BloomFilterBuilder with estimated key count
/// 2. Call add_key() for every key written to the SSTable
/// 3. Call build() to get the final BloomFilter for serialization
pub struct BloomFilterBuilder {
    // TODO [M18]: Fields
    //   - keys: Vec<Vec<u8>> or directly building the BloomFilter
    //   - estimated_count: usize
    //   - false_positive_rate: f64
}

impl BloomFilterBuilder {
    /// Create a builder expecting approximately `estimated_keys` keys.
    pub fn new(_estimated_keys: usize, _false_positive_rate: f64) -> Self {
        todo!("[M18]: Initialize builder")
    }

    /// Add a key to the bloom filter being built.
    pub fn add_key(&mut self, _key: &[u8]) {
        todo!("[M18]: Insert into underlying bloom filter")
    }

    /// Finalize and return the bloom filter.
    pub fn build(self) -> BloomFilter {
        todo!("[M18]: Return the constructed bloom filter")
    }
}

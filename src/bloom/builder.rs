use crate::bloom::BloomFilter;

/// Convenience builder for constructing a bloom filter during SSTable creation.
///
/// Wraps a BloomFilter directly — keys are inserted on the fly rather than
/// buffered, so memory usage stays proportional to the filter size (bits),
/// not the number of keys.
pub struct BloomFilterBuilder {
    filter: BloomFilter,
}

impl BloomFilterBuilder {
    /// Create a builder expecting approximately `estimated_keys` keys.
    pub fn new(estimated_keys: usize, false_positive_rate: f64) -> Self {
        Self {
            filter: BloomFilter::new(estimated_keys, false_positive_rate),
        }
    }

    /// Add a key to the bloom filter being built.
    pub fn add_key(&mut self, key: &[u8]) {
        self.filter.insert(key);
    }

    /// Finalize and return the bloom filter.
    pub fn build(self) -> BloomFilter {
        self.filter
    }
}

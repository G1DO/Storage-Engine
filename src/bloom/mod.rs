pub mod builder;

use xxhash_rust::xxh3::xxh3_128;

/// Probabilistic data structure: "is this key in the set?"
///
/// - If any bit is 0 → key is DEFINITELY NOT in the set
/// - If all bits are 1 → key is PROBABLY in the set (false positive possible)
///
/// Used in SSTable reads to skip SSTables that definitely don't contain
/// the target key. On a miss-heavy workload, this reduces disk reads by 90%+.
///
/// Sizing:
///   bits_per_key = -1.44 * log2(false_positive_rate)
///   num_hashes = bits_per_key * ln(2)
///
///   1% FPR  → ~10 bits/key, 7 hashes
///   0.1% FPR → ~14 bits/key, 10 hashes
///
/// Hash trick: don't need k independent hash functions.
/// Use double hashing: h_i(key) = h1(key) + i * h2(key) (mod m)
/// where h1, h2 come from splitting a 128-bit hash into two 64-bit halves.
pub struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: u32,
    num_bits: u32,
}

impl BloomFilter {
    /// Create a new bloom filter sized for expected_items at the given FPR.
    ///
    /// # Arguments
    /// * `expected_items` - Expected number of items to be inserted
    /// * `false_positive_rate` - Target false positive rate (e.g., 0.01 for 1%)
    ///
    /// # Panics
    /// Panics if expected_items is 0 or FPR is not in (0, 1).
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        assert!(expected_items > 0, "expected_items must be > 0");
        assert!(
            false_positive_rate > 0.0 && false_positive_rate < 1.0,
            "FPR must be in (0, 1)"
        );

        // Calculate bits per key: -1.44 * log2(FPR)
        let bits_per_key = -1.44 * false_positive_rate.log2();

        // Calculate total bits needed
        let num_bits = ((expected_items as f64) * bits_per_key).ceil() as u32;

        // Ensure at least some bits
        let num_bits = num_bits.max(64); // At least 64 bits

        // Calculate number of hash functions: bits_per_key * ln(2)
        let num_hashes = (bits_per_key * 2.0f64.ln()).ceil() as u32;
        let num_hashes = num_hashes.max(1); // At least 1 hash

        // Allocate bit array (packed into u64s)
        let num_u64s = ((num_bits as usize) + 63) / 64;
        let bits = vec![0u64; num_u64s];

        Self {
            bits,
            num_hashes,
            num_bits,
        }
    }

    /// Add a key to the bloom filter.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = self.hash_key(key);

        // Set k bits using double hashing
        for i in 0..self.num_hashes {
            let pos = self.get_position(h1, h2, i);
            self.set_bit(pos);
        }
    }

    /// Check if a key MIGHT be in the set.
    /// false → definitely not here. true → probably here.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = self.hash_key(key);

        // Check k bits using double hashing
        for i in 0..self.num_hashes {
            let pos = self.get_position(h1, h2, i);
            if !self.check_bit(pos) {
                return false; // Definitely not in set
            }
        }

        true // Probably in set
    }

    /// Serialize the bloom filter to bytes (for writing into SSTable).
    pub fn serialize(&self) -> Vec<u8> {
        todo!("[M17]: Encode num_hashes, num_bits, bit array")
    }

    /// Deserialize a bloom filter from bytes (when opening an SSTable).
    pub fn deserialize(_data: &[u8]) -> crate::error::Result<Self> {
        todo!("[M17]: Decode num_hashes, num_bits, bit array")
    }

    /// Get the number of hash functions used.
    pub fn num_hashes(&self) -> u32 {
        self.num_hashes
    }

    /// Get the total number of bits in the filter.
    pub fn num_bits(&self) -> u32 {
        self.num_bits
    }

    /// Hash a key and return two 64-bit hashes (h1, h2) for double hashing.
    fn hash_key(&self, key: &[u8]) -> (u64, u64) {
        let hash128 = xxh3_128(key);

        // Split 128-bit hash into two 64-bit halves
        let h1 = (hash128 & 0xFFFFFFFFFFFFFFFF) as u64;
        let h2 = (hash128 >> 64) as u64;

        (h1, h2)
    }

    /// Calculate bit position using double hashing.
    fn get_position(&self, h1: u64, h2: u64, i: u32) -> u32 {
        // h_i = (h1 + i * h2) mod num_bits
        let i = i as u64;
        let pos = (h1.wrapping_add(i.wrapping_mul(h2))) % (self.num_bits as u64);
        pos as u32
    }

    /// Set a bit at the given position.
    fn set_bit(&mut self, pos: u32) {
        let word_index = (pos / 64) as usize;
        let bit_index = pos % 64;
        self.bits[word_index] |= 1 << bit_index;
    }

    /// Check if a bit is set at the given position.
    fn check_bit(&self, pos: u32) -> bool {
        let word_index = (pos / 64) as usize;
        let bit_index = pos % 64;
        (self.bits[word_index] >> bit_index) & 1 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let mut bf = BloomFilter::new(100, 0.01);
        bf.insert(b"hello");
        assert!(bf.may_contain(b"hello"));
        assert!(!bf.may_contain(b"world"));
    }
}

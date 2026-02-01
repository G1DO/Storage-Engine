// TODO [M01]: These types are used everywhere — understand them before coding anything

/// Raw key bytes.
pub type Key = Vec<u8>;

/// Raw value bytes.
pub type Value = Vec<u8>;

/// Distinguishes puts from deletes in the storage engine.
/// A Delete writes a tombstone — the key isn't removed, it's marked as deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    /// A normal put operation.
    Put = 0x01,
    /// A delete (tombstone marker).
    Delete = 0x02,
}

/// Internal key format: user key + sequence number + value type.
///
/// Ordering: (user_key ASC, sequence DESC).
/// This ensures the newest version of a key always comes first during merging.
///
/// The sequence number is a monotonically increasing counter assigned to each
/// write operation. It provides a total ordering of all writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Key,
    pub sequence: u64,
    pub value_type: ValueType,
}

// TODO [M01]: Implement Ord for InternalKey
//   - Primary sort: user_key ascending (lexicographic)
//   - Secondary sort: sequence descending (newest first)
//   - This ordering is CRITICAL for correctness of reads and compaction

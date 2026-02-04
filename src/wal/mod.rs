pub mod reader;
pub mod record;
pub mod writer;

pub use record::{WALRecord, RecordType};

// TODO [M10]: Implement configurable sync policies

/// Controls when the WAL is fsync'd to disk.
///
/// Trade-off: durability vs throughput.
///   - EveryWrite: zero data loss, ~10x slower (each fsync waits for disk)
///   - EveryNWrites: batched durability, lose up to N writes on crash
///   - EveryNMillis: bounded loss window, much higher throughput
///
/// RocksDB defaults to NOT fsync'ing WAL (!), letting the OS decide.
pub enum SyncPolicy {
    /// fsync after every record. Safest, slowest.
    EveryWrite,
    /// fsync every N records. Batched durability.
    EveryNWrites(usize),
    /// fsync on timer. Bounded data loss window.
    EveryNMillis(u64),
}

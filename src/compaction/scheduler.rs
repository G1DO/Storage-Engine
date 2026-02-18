use crate::error::Result;

// TODO [M22]: Implement background compaction scheduler

/// Runs compaction in a background thread while reads and writes continue.
///
/// Concurrency model:
/// - Compaction reads OLD SSTables and writes NEW ones
/// - Reads must still see the old SSTables until compaction completes
/// - When done, atomically swap old for new in the manifest
/// - Use a version/snapshot mechanism: readers hold a reference to the
///   current version while compaction builds a new version
///
/// Flow:
///   loop {
///       wait for trigger (new SSTable flushed)
///       pick_compaction() → CompactionTask
///       execute compaction (merge-sort → write new SSTables)
///       atomically install new version in manifest
///       delete old SSTable files
///   }
pub struct CompactionScheduler {
    // TODO [M22]: Fields
    //   - strategy: Box<dyn CompactionStrategy>
    //   - trigger: Condvar or channel receiver
    //   - thread handle
}

impl CompactionScheduler {
    /// Start the background compaction scheduler.
    pub fn start() -> Result<Self> {
        todo!("[M22]: Spawn background thread, set up trigger channel")
    }

    /// Signal that a new SSTable was flushed (may trigger compaction).
    pub fn notify_flush(&self) {
        todo!("[M22]: Wake up the background thread")
    }

    /// Shut down the compaction scheduler gracefully.
    pub fn shutdown(self) -> Result<()> {
        todo!("[M22]: Signal thread to stop, join")
    }
}

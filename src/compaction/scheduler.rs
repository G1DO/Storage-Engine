use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::compaction::CompactionStrategy;
use crate::error::Result;
use crate::iterator::merge::MergeIterator;
use crate::iterator::vec_iter::VecIterator;
use crate::iterator::StorageIterator;
use crate::manifest::version::{Version, VersionSet};
use crate::sstable::builder::SSTableBuilder;
use crate::sstable::reader::SSTable;

enum CompactionMessage {
    Flush,
    Shutdown,
}

/// Runs compaction in a background thread while reads and writes continue.
pub struct CompactionScheduler {
    sender: Sender<CompactionMessage>,
    handle: JoinHandle<()>,
}

impl CompactionScheduler {
    /// Start the background compaction scheduler.
    pub fn start(
        version_set: Arc<VersionSet>,
        strategy: Arc<dyn CompactionStrategy>,
        db_path: PathBuf,
        block_size: usize,
    ) -> Result<Self> {
        let (sender, receiver) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            loop {
                match receiver.recv() {
                    Ok(CompactionMessage::Flush) => {
                        let _ = run_compaction(
                            &version_set,
                            &*strategy,
                            &db_path,
                            block_size,
                        );
                    }
                    Ok(CompactionMessage::Shutdown) => break,
                    Err(_) => break,
                }
            }
        });

        Ok(CompactionScheduler { sender, handle })
    }

    /// Signal that a new SSTable was flushed (may trigger compaction).
    pub fn notify_flush(&self) {
        let _ = self.sender.send(CompactionMessage::Flush);
    }

    /// Shut down the compaction scheduler gracefully.
    pub fn shutdown(self) -> Result<()> {
        let _ = self.sender.send(CompactionMessage::Shutdown);
        let _ = self.handle.join();
        Ok(())
    }
}

/// Path to an SSTable file given its ID.
fn sst_path(db_path: &Path, id: u64) -> PathBuf {
    db_path.join(format!("{:06}.sst", id))
}

/// Execute one round of compaction if the strategy says it's needed.
fn run_compaction(
    version_set: &VersionSet,
    strategy: &dyn CompactionStrategy,
    db_path: &Path,
    block_size: usize,
) -> Result<()> {
    // 1. Read current levels (clone to release lock quickly)
    let levels = {
        let current = version_set.current();
        let v = current.read().unwrap();
        v.levels.clone()
    };

    // 2. Ask strategy if compaction is needed
    let task = match strategy.pick_compaction(&levels) {
        Some(task) => task,
        None => return Ok(()),
    };

    // 3. Read input SSTables into VecIterators
    let mut iters: Vec<Box<dyn StorageIterator>> = Vec::new();
    for meta in &task.inputs {
        let path = sst_path(db_path, meta.id);
        let sst = SSTable::open(&path)?;
        let mut entries = Vec::new();
        let mut iter = sst.iter()?;
        while iter.is_valid() {
            entries.push((iter.key().to_vec(), iter.value().to_vec()));
            iter.next()?;
        }
        iters.push(Box::new(VecIterator::new(entries)));
    }

    // 4. Merge
    let mut merge = MergeIterator::new(iters)?;

    // 5. Write output SSTable
    let new_id = version_set.next_sst_id();
    let output_path = sst_path(db_path, new_id);
    let mut builder = SSTableBuilder::new(&output_path, new_id, block_size)?;
    while merge.is_valid() {
        builder.add(merge.key(), merge.value())?;
        merge.next()?;
    }
    let mut new_meta = builder.finish()?;
    new_meta.level = task.output_level;

    // 6. Install new version
    {
        let current = version_set.current();
        let old_v = current.read().unwrap();
        let mut new_levels = old_v.levels.clone();
        drop(old_v); // release read lock before write lock

        let input_ids: HashSet<u64> = task.inputs.iter().map(|s| s.id).collect();
        for level in &mut new_levels {
            level.retain(|sst| !input_ids.contains(&sst.id));
        }
        new_levels[task.output_level as usize].push(new_meta);

        version_set.install(Version { levels: new_levels });
    }

    // 7. Delete old SSTable files
    for meta in &task.inputs {
        let _ = std::fs::remove_file(sst_path(db_path, meta.id));
    }

    Ok(())
}

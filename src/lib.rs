//! # LSM-Tree Storage Engine
//!
//! A key-value storage engine using the Log-Structured Merge-Tree design.
//! Built from scratch to understand database internals.
//!
//! ## Core idea
//! Instead of updating data in place (B-Tree), buffer writes in memory,
//! flush them as sorted files, and merge those files in the background.
//! This turns random writes into sequential writes — 100-1000x faster
//! on real hardware.

pub mod bloom;
pub mod cache;
pub mod compaction;
pub mod db;
pub mod error;
pub mod iterator;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod types;
pub mod wal;

// Public re-exports for the top-level API
pub use db::{DB, Options, Stats};
pub use error::{Error, Result};

# LSM-Tree Storage Engine

A key-value storage engine using the Log-Structured Merge-Tree (LSM-tree) design, built from scratch in Rust.

## Why LSM-Trees?

The core problem: how do you store key-value pairs on disk with fast writes AND fast reads?

| Approach | Write | Read | Problem |
|----------|-------|------|---------|
| Append-only file | O(1) | O(n) | Can't find anything |
| Sorted file | O(n) | O(log n) | Rewrite entire file on insert |
| B-Tree (Postgres) | O(log n) | O(log n) | Random IO on every write |
| **LSM-Tree** | **O(1) amortized** | **O(log n)** | Background compaction cost |

**The insight:** buffer writes in memory, flush as sorted files, merge in the background. Random writes become sequential writes — 100-1000x faster on real hardware.

## Architecture

```
                     PUT / GET / DELETE
                           │
                           ▼
                 ┌───────────────────┐
                 │    Public API      │
                 │  put() get() del() │
                 └────────┬──────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
     ┌──────────┐  ┌──────────┐  ┌──────────────┐
     │   WAL    │  │ Memtable │  │  Block Cache  │
     │ (crash   │  │ (active  │  │  (read cache) │
     │ recovery)│  │  writes) │  │               │
     └──────────┘  └─────┬────┘  └──────────────┘
                         │
                   (flush when full)
                         │
                         ▼
           ┌─────────────────────────┐
           │      SSTable Layer       │
           │                         │
           │  Level 0:  [SST][SST]   │  ← freshly flushed
           │  Level 1:  [SST][SST]   │  ← compacted, no overlap
           │  Level 2:  [SST][SST]   │  ← larger, older
           │  Level 3:  [SST]....    │
           │                         │
           │  Each SST contains:     │
           │  ┌────────────────────┐ │
           │  │ Data Blocks        │ │
           │  │ Index Block        │ │
           │  │ Bloom Filter       │ │
           │  │ Footer/Metadata    │ │
           │  └────────────────────┘ │
           └─────────────────────────┘
```

## Components

| Component | What It Does |
|-----------|-------------|
| **Memtable** | In-memory sorted buffer (skip list). Entry point for all writes. |
| **WAL** | Write-ahead log. Durably records writes before memtable. Crash recovery. |
| **SSTable** | On-disk sorted, immutable file. Efficient reads via index + binary search. |
| **Bloom Filter** | Probabilistic "is key here?" check. Avoids unnecessary disk reads. |
| **Compaction** | Background merge of SSTables. Controls space and read amplification. |
| **Block Cache** | LRU cache for hot data blocks. Reduces disk IO. |
| **Manifest** | Tracks which SSTables exist. Enables recovery after crash. |

## Build & Run

```bash
# Build
cargo build

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Lint
cargo clippy

# Format
cargo fmt

# Benchmarks (once implemented)
cargo bench

# Check without building
cargo check
```

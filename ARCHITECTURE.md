# Architecture

## Component Dependency Graph

```
                         ┌─────────┐
                         │   db    │
                         └────┬────┘
               ┌──────┬──────┼──────┬────────┐
               ▼      ▼      ▼      ▼        ▼
          ┌────────┐ ┌───┐ ┌─────┐ ┌────┐ ┌────────┐
          │memtable│ │wal│ │cache│ │comp│ │manifest│
          └───┬────┘ └─┬─┘ └──┬──┘ └─┬──┘ └───┬────┘
              │        │      │      │         │
              │        │      ▼      ▼         │
              │        │   ┌───────────┐       │
              │        │   │  sstable  │       │
              │        │   └─────┬─────┘       │
              │        │         │             │
              │        │         ▼             │
              │        │      ┌──────┐         │
              │        │      │bloom │         │
              │        │      └──────┘         │
              │        │                       │
              ▼        ▼           ▼           ▼
         ┌──────────────────────────────────────┐
         │       iterator  /  types  /  error   │
         └──────────────────────────────────────┘
```

- **db** depends on all modules — it's the public API coordinator
- **memtable** depends only on types/error (pure in-memory)
- **wal** depends on types/error (disk IO, no other modules)
- **sstable** depends on bloom, types/error
- **compaction** depends on sstable, iterator
- **manifest** depends on wal (reuses WAL record format), sstable::footer (SSTableMeta)
- **cache** depends on sstable::block
- **iterator** depends on types/error (trait definition + MergeIterator)

## Directory and File Structure

```
src/
├── lib.rs                    — Root module declarations and public re-exports
├── error.rs                  — Unified Error enum and Result<T> type alias
├── types.rs                  — Key, Value, InternalKey, ValueType
│
├── memtable/
│   ├── mod.rs                — MemTable struct (wraps SkipList, tracks size)
│   ├── skiplist.rs           — SkipList, SkipNode (probabilistic sorted structure)
│   └── iterator.rs           — MemTableIterator (implements StorageIterator)
│
├── wal/
│   ├── mod.rs                — SyncPolicy enum, module re-exports
│   ├── record.rs             — WALRecord, RecordType, serialization format
│   ├── writer.rs             — WALWriter (append + fsync)
│   └── reader.rs             — WALReader, WALIterator (replay for recovery)
│
├── sstable/
│   ├── mod.rs                — Module re-exports
│   ├── block/
│   │   ├── mod.rs            — Block module re-exports
│   │   ├── builder.rs        — BlockBuilder (accumulates sorted KVs, serializes)
│   │   └── reader.rs         — Block (deserialized), BlockIterator
│   ├── builder.rs            — SSTableBuilder (blocks + index + bloom + footer)
│   ├── reader.rs             — SSTable (open file, point lookup, bloom check)
│   ├── iterator.rs           — SSTableIterator, SSTableRangeIterator
│   └── footer.rs             — Footer, IndexEntry, SSTableMeta
│
├── bloom/
│   ├── mod.rs                — BloomFilter (bit array + k hash functions)
│   └── builder.rs            — BloomFilterBuilder (sizing + construction)
│
├── compaction/
│   ├── mod.rs                — CompactionStyle enum, CompactionTask, Strategy trait
│   ├── size_tiered.rs        — SizeTieredStrategy
│   ├── leveled.rs            — LeveledStrategy
│   └── scheduler.rs          — CompactionScheduler (background thread)
│
├── iterator/
│   ├── mod.rs                — StorageIterator trait (key, value, is_valid, next, seek)
│   └── merge.rs              — MergeIterator (k-way merge with dedup + tombstone skip)
│
├── manifest/
│   ├── mod.rs                — Manifest, ManifestRecord (flush/compaction events)
│   └── version.rs            — Version (which SSTables at which levels), VersionSet
│
├── cache/
│   ├── mod.rs                — BlockCache (wraps LRU, keyed by sst_id + offset)
│   └── lru.rs                — LRUCache<K, V> (generic, HashMap + doubly-linked list)
│
└── db/
    ├── mod.rs                — DB struct, Options, Stats, public API
    └── snapshot.rs           — Snapshot (frozen view of DB state)
```

## Interface Boundaries

Each module exposes a narrow public API and hides internals.

### types (src/types.rs)
**Exposes:** `Key`, `Value`, `ValueType`, `InternalKey`
**Hides:** Nothing — these are shared primitives

### error (src/error.rs)
**Exposes:** `Error`, `Result<T>`
**Hides:** Nothing

### iterator (src/iterator/)
**Exposes:** `StorageIterator` trait, `MergeIterator`
**Hides:** Heap internals of MergeIterator

### memtable (src/memtable/)
**Exposes:** `MemTable` (put, get, delete, iter, size, is_full), `MemTableIterator`
**Hides:** `SkipList`, `SkipNode`, level randomization, internal node layout

### wal (src/wal/)
**Exposes:** `WALWriter`, `WALReader`, `WALRecord`, `SyncPolicy`
**Hides:** CRC computation, record binary format, file positioning

### sstable (src/sstable/)
**Exposes:** `SSTable` (open, get, iter, range_iter), `SSTableBuilder`, `SSTableMeta`, `IndexEntry`
**Hides:** `Block`, `BlockBuilder`, `BlockIterator`, `Footer`, on-disk binary format

### bloom (src/bloom/)
**Exposes:** `BloomFilter` (new, insert, may_contain, serialize, deserialize), `BloomFilterBuilder`
**Hides:** Hash function internals, bit manipulation

### compaction (src/compaction/)
**Exposes:** `CompactionStyle`, `CompactionStrategy` trait, `CompactionScheduler`
**Hides:** Compaction picking heuristics, merge implementation

### manifest (src/manifest/)
**Exposes:** `Manifest`, `ManifestRecord`, `Version`
**Hides:** `VersionSet` internals, manifest file format

### cache (src/cache/)
**Exposes:** `BlockCache` (get, insert), cache stats
**Hides:** `LRUCache` eviction internals, linked list

### db (src/db/)
**Exposes:** `DB` (open, put, get, delete, scan, snapshot, flush, compact_range, close), `Options`, `Stats`, `Snapshot`
**Hides:** All coordination logic — memtable rotation, flush triggering, read path orchestration

## Data Flow Diagrams

### Put(key, value)

```
Client
  │
  ▼
DB.put(key, value)
  │
  ├──1──► WALWriter.append(PutRecord{key, value})
  │         │
  │         └──► WALWriter.sync()  (per SyncPolicy)
  │
  ├──2──► MemTable.put(key, value)
  │         │
  │         └──► SkipList.insert(InternalKey, value)
  │
  └──3──► if MemTable.is_full(threshold):
            │
            ├──► Freeze current memtable (becomes immutable)
            ├──► Create new active memtable + new WAL
            └──► Trigger flush (background):
                   │
                   ├──► SSTableBuilder.add() for each entry in sorted order
                   ├──► SSTableBuilder.finish() → writes SST file + fsync
                   ├──► Manifest.record_flush(new_sst_meta)
                   ├──► Delete old WAL file
                   └──► Maybe trigger compaction
```

### Get(key)

```
Client
  │
  ▼
DB.get(key)
  │
  ├──1──► Active MemTable.get(key)
  │         found? → return value (or None if tombstone)
  │
  ├──2──► Immutable MemTable.get(key)  (if exists)
  │         found? → return value (or None if tombstone)
  │
  ├──3──► Level 0 SSTables (newest first, ALL of them — may overlap)
  │         for each SST:
  │           ├── BloomFilter.may_contain(key)? → no → skip
  │           ├── Index binary search → find block
  │           ├── BlockCache.get(sst_id, offset)? → cache hit
  │           └── Read block from disk → BlockCache.insert()
  │           Block.get(key) → found? return
  │
  ├──4──► Level 1 SSTable (ONE — no overlap within level)
  │         Same bloom → index → block path
  │
  ├──5──► Level 2, 3, ... (one SSTable per level)
  │
  └──6──► Not found → return None
```

### Memtable Flush

```
Trigger: MemTable.size() >= threshold
  │
  ├──1──► Freeze memtable: move active → immutable
  │         (new active memtable + WAL created)
  │
  ├──2──► Iterate immutable memtable in sorted order
  │         │
  │         ▼
  │       SSTableBuilder
  │         ├── For each (key, value):
  │         │     ├── BloomFilterBuilder.add(key)
  │         │     ├── BlockBuilder.add(key, value)
  │         │     └── If block full:
  │         │           ├── Write block to file
  │         │           └── Record IndexEntry(last_key, offset, size)
  │         │
  │         └── finish():
  │               ├── Flush last block
  │               ├── Write bloom filter (meta block)
  │               ├── Write index block
  │               ├── Write footer
  │               └── fsync
  │
  ├──3──► Manifest.record_flush(SSTableMeta{id, level:0, min_key, max_key})
  │
  ├──4──► Delete old WAL file
  │
  └──5──► Drop immutable memtable reference
```

### Compaction

```
Trigger: Level L exceeds size/count threshold
  │
  ├──1──► Pick input SSTables
  │         Size-tiered: all SSTables at Level 0
  │         Leveled: one SSTable from Level L
  │                  + overlapping SSTables from Level L+1
  │
  ├──2──► Create iterators over all input SSTables
  │
  ├──3──► MergeIterator (k-way merge)
  │         ├── For duplicate keys: keep newest (lowest level / highest seq)
  │         └── For tombstones:
  │               ├── Bottommost level? → drop tombstone
  │               └── Not bottommost? → keep tombstone
  │
  ├──4──► Write merged output as new SSTable(s) at Level L+1
  │         (via SSTableBuilder, same as flush)
  │
  ├──5──► Manifest.record_compaction(added: [...], removed: [...])
  │
  └──6──► Delete old input SSTable files
          (only after manifest is fsync'd)
```

### Crash Recovery

```
DB.open(path)
  │
  ├──1──► Read MANIFEST file
  │         Replay ManifestRecords → reconstruct Version
  │         (which SSTables exist, at which levels)
  │
  ├──2──► Open all active SSTables listed in Version
  │         Load index blocks + bloom filters into memory
  │
  ├──3──► Find WAL files newer than last flushed SSTable
  │
  ├──4──► For each WAL file:
  │         WALReader.iter()
  │           ├── Valid record (CRC ok) → replay into MemTable
  │           └── Corrupt record (CRC fail) → stop
  │               (partial write from crash, preceding records valid)
  │
  ├──5──► MemTable now contains all unflushed data
  │
  └──6──► Ready to serve reads and writes
```

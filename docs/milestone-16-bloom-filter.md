# Milestone 16: Bloom Filter Core with Double Hashing

## Overview

M16 implements a **bloom filter** — a probabilistic data structure that answers
"is this key in this SSTable?" without reading from disk. It's the first line
of defense against unnecessary I/O in the read path.

- **No** → key is **definitely not** in the SSTable. Skip it.
- **Yes** → key is **probably** in the SSTable. Do the disk read.

On miss-heavy workloads this eliminates 90%+ of unnecessary disk reads.

## Where It Fits

```
GET("user_123")
  → Check MemTable (in-memory)
  → For each SSTable:
      → Check Bloom Filter       ← M16
         → NO  → skip (zero I/O)
         → YES → point lookup    ← M14
```

## Implementation

**File:** `src/bloom/mod.rs` (148 lines)
**Dependency:** `xxhash-rust` v0.8 (xxh3 feature) — fast 128-bit hashing
**Tests:** `tests/bloom_tests.rs` (181 lines, 9 test cases)

### Data Structure

```rust
pub struct BloomFilter {
    bits: Vec<u64>,   // bit array packed into 64-bit words
    num_hashes: u32,  // number of hash functions (k)
    num_bits: u32,    // total bits in the filter (m)
}
```

### Sizing Formula

Given `n` expected items and a target false positive rate (FPR):

```
bits_per_key = -1.44 * log2(FPR)
total_bits   = n * bits_per_key
num_hashes   = ceil(bits_per_key * ln(2))
```

| FPR Target | Bits/Key | Hash Functions |
|------------|----------|----------------|
| 10%        | ~5       | 3              |
| 1%         | ~10      | 7              |
| 0.1%       | ~14      | 10             |

### Double Hashing Trick

Instead of k independent hash functions, we use **one** 128-bit xxh3 hash
split into two 64-bit halves:

```
h1 = lower 64 bits of xxh3_128(key)
h2 = upper 64 bits of xxh3_128(key)

h_i(key) = (h1 + i * h2) mod num_bits    for i in 0..k
```

This is proven equivalent to k independent hashes (Kirsch & Mitzenmacher, 2006)
but requires only a single hash computation per key.

### Operations

**Insert:** Compute k bit positions via double hashing. Set all to 1.

**Query (`may_contain`):** Compute the same k positions. If ANY bit is 0,
return `false` (definitely not present). If all bits are 1, return `true`
(probably present — false positive possible).

Key property: **zero false negatives**. If a key was inserted, `may_contain`
always returns `true`.

### Bit Manipulation

Bits are packed into `Vec<u64>` words:

```rust
// Set bit at position pos
fn set_bit(&mut self, pos: u32) {
    let word_index = (pos / 64) as usize;
    let bit_index = pos % 64;
    self.bits[word_index] |= 1 << bit_index;
}

// Check bit at position pos
fn check_bit(&self, pos: u32) -> bool {
    let word_index = (pos / 64) as usize;
    let bit_index = pos % 64;
    (self.bits[word_index] >> bit_index) & 1 == 1
}
```

## Test Coverage

| Test | Description |
|------|-------------|
| `empty_filter_returns_false` | Empty filter has no false positives |
| `inserted_key_found` | Guarantees zero false negatives |
| `different_key_not_found` | Similar keys (hello/world/hell) don't collide |
| `duplicate_insert_no_error` | Re-inserting same key is idempotent |
| `multiple_keys` | Multiple insertions, all retrievable |
| `false_positive_rate` | 10K keys, measured **0.9% actual** vs 1% target |
| `various_fpr_values` | Validated at 10%, 5%, 1%, 0.1% FPR targets |
| `empty_key` | Empty byte slice as key |
| `large_key` | 1MB key works correctly |
| `binary_keys` | Keys with null bytes and high bytes |

## What Comes Next

- **M17:** `serialize()` / `deserialize()` — persist bloom filters to disk
- **M18:** Integrate bloom filter builder into SSTable build path so every
  SSTable gets a bloom filter automatically

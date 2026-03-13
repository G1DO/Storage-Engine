// M33: Benchmark Suite
//
// Measures performance of the LSM-tree engine across different workload patterns.
// Run with: cargo bench

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use lsm_engine::{DB, Options};
use rand::Rng;
use tempfile::tempdir;

const NUM_KEYS: u32 = 10_000;
const VALUE_SIZE: usize = 100;

fn make_key(i: u32) -> Vec<u8> {
    format!("key_{:08}", i).into_bytes()
}

fn make_value() -> Vec<u8> {
    vec![0x42; VALUE_SIZE]
}

fn bench_opts() -> Options {
    Options {
        memtable_size: 64 * 1024, // 64KB — small to trigger flushes
        ..Options::default()
    }
}

// =============================================================================
// 1. Sequential writes: 10K sorted keys
// =============================================================================
fn bench_sequential_writes(c: &mut Criterion) {
    let value = make_value();

    c.bench_function("sequential_writes_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = DB::open(dir.path(), bench_opts()).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..NUM_KEYS {
                    db.put(&make_key(i), &value).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

// =============================================================================
// 2. Random writes: 10K random keys
// =============================================================================
fn bench_random_writes(c: &mut Criterion) {
    let value = make_value();

    c.bench_function("random_writes_10k", |b| {
        b.iter_batched(
            || {
                let mut rng = rand::thread_rng();
                let keys: Vec<u32> = (0..NUM_KEYS)
                    .map(|_| rng.gen_range(0..NUM_KEYS * 10))
                    .collect();
                let dir = tempdir().unwrap();
                let db = DB::open(dir.path(), bench_opts()).unwrap();
                (dir, db, keys)
            },
            |(_dir, db, keys)| {
                for k in &keys {
                    db.put(&make_key(*k), &value).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

// =============================================================================
// 3. Sequential reads: pre-populate 10K, read all in order
// =============================================================================
fn bench_sequential_reads(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), bench_opts()).unwrap();
    let value = make_value();

    for i in 0..NUM_KEYS {
        db.put(&make_key(i), &value).unwrap();
    }
    db.flush().unwrap();

    c.bench_function("sequential_reads_10k", |b| {
        b.iter(|| {
            for i in 0..NUM_KEYS {
                let _ = db.get(&make_key(i)).unwrap();
            }
        });
    });
}

// =============================================================================
// 4. Random reads: pre-populate 10K, read 10K random (80% hit, 20% miss)
// =============================================================================
fn bench_random_reads(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), bench_opts()).unwrap();
    let value = make_value();

    for i in 0..NUM_KEYS {
        db.put(&make_key(i), &value).unwrap();
    }
    db.flush().unwrap();

    // Pre-generate random keys: 80% within range (hits), 20% outside (misses)
    let mut rng = rand::thread_rng();
    let lookup_keys: Vec<Vec<u8>> = (0..NUM_KEYS)
        .map(|_| {
            if rng.gen_range(0..100) < 80 {
                make_key(rng.gen_range(0..NUM_KEYS)) // hit
            } else {
                make_key(NUM_KEYS + rng.gen_range(0..NUM_KEYS)) // miss
            }
        })
        .collect();

    c.bench_function("random_reads_10k_80hit_20miss", |b| {
        b.iter(|| {
            for key in &lookup_keys {
                let _ = db.get(key).unwrap();
            }
        });
    });
}

// =============================================================================
// 5. Mixed workload: interleaved 50/50 read/write
// =============================================================================
fn bench_mixed_workload(c: &mut Criterion) {
    let value = make_value();

    c.bench_function("mixed_50_50_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let db = DB::open(dir.path(), bench_opts()).unwrap();
                // Pre-populate half the keys so reads can hit
                for i in 0..NUM_KEYS / 2 {
                    db.put(&make_key(i), &value).unwrap();
                }
                let mut rng = rand::thread_rng();
                let ops: Vec<bool> = (0..NUM_KEYS).map(|_| rng.gen_range(0..2) == 0).collect();
                (dir, db, ops)
            },
            |(_dir, db, ops)| {
                for (i, is_write) in ops.iter().enumerate() {
                    let key = make_key(i as u32);
                    if *is_write {
                        db.put(&key, &value).unwrap();
                    } else {
                        let _ = db.get(&key).unwrap();
                    }
                }
            },
            BatchSize::PerIteration,
        );
    });
}

// =============================================================================
// 6. Compaction impact: write throughput with small memtable to trigger flushes
// =============================================================================
fn bench_compaction_impact(c: &mut Criterion) {
    let value = make_value();

    c.bench_function("writes_with_compaction_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let opts = Options {
                    memtable_size: 16 * 1024, // 16KB — very small to trigger many flushes
                    ..Options::default()
                };
                let db = DB::open(dir.path(), opts).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                for i in 0..NUM_KEYS {
                    db.put(&make_key(i), &value).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
}

// =============================================================================
// 7. Recovery time: populate, drop without close, measure DB::open() time
// =============================================================================
fn bench_recovery_time(c: &mut Criterion) {
    let value = make_value();

    c.bench_function("recovery_time_10k", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                {
                    let db = DB::open(dir.path(), bench_opts()).unwrap();
                    for i in 0..NUM_KEYS {
                        db.put(&make_key(i), &value).unwrap();
                    }
                    // Drop without calling close() — simulates crash
                    // DB::drop() should flush, but WAL replay will still happen on reopen
                }
                dir
            },
            |dir| {
                // Measure only the reopen (WAL replay + manifest load)
                let _db = DB::open(dir.path(), bench_opts()).unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

criterion_group!(
    benches,
    bench_sequential_writes,
    bench_random_writes,
    bench_sequential_reads,
    bench_random_reads,
    bench_mixed_workload,
    bench_compaction_impact,
    bench_recovery_time,
);
criterion_main!(benches);

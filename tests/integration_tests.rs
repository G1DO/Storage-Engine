// TODO [M32]: Integration tests for the full DB API
//
// Test scenarios:
//   - Open DB, put, get, close, reopen, get (persistence)
//   - Put, delete, get returns None (tombstone)
//   - Write enough to trigger flush, verify data survives
//   - Write enough to trigger compaction, verify data survives
//   - Crash simulation: kill mid-write, recover, verify consistency
//   - Range scan correctness
//   - Snapshot isolation: writes after snapshot aren't visible

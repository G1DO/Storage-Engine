pub mod version;

use crate::error::{Error, Result};
use crate::sstable::footer::SSTableMeta;
use crc32fast::Hasher;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

// TODO [M27]: Implement manifest writer
// TODO [M29]: Implement manifest compaction (snapshot current version)

/// Types of records stored in the manifest.
///
/// The manifest is a log of every structural change to the database.
/// On recovery, replay it to know which SSTables are current.
#[derive(Debug)]
pub enum ManifestRecord {
    /// A new SSTable was created (from flush or compaction).
    NewSSTable(SSTableMeta),
    /// A compaction completed: some SSTables added, some removed.
    CompactionComplete {
        added: Vec<SSTableMeta>,
        removed: Vec<u64>, // SSTable IDs
    },
    /// Record the current WAL log number. On recovery, replay WALs with id >= this.
    SetLogNumber(u64),
}

// Helper: append a record as [len(4)][payload][crc(4)]
fn append_record(file: &mut std::fs::File, payload: &[u8]) -> Result<()> {
    let len = payload.len() as u32;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(payload)?;
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let crc = hasher.finalize();
    file.write_all(&crc.to_le_bytes())?;
    file.sync_all()?;
    Ok(())
}

// Encode/decode SSTableMeta to a compact byte representation.
fn encode_meta(m: &SSTableMeta) -> Vec<u8> {
    // layout: [id(8)][level(4)][min_len(4)][min_key][max_len(4)][max_key][file_size(8)][entry_count(8)]
    let mut v = Vec::with_capacity(64 + m.min_key.len() + m.max_key.len());
    v.extend_from_slice(&m.id.to_le_bytes());
    v.extend_from_slice(&m.level.to_le_bytes());
    v.extend_from_slice(&(m.min_key.len() as u32).to_le_bytes());
    v.extend_from_slice(&m.min_key);
    v.extend_from_slice(&(m.max_key.len() as u32).to_le_bytes());
    v.extend_from_slice(&m.max_key);
    v.extend_from_slice(&m.file_size.to_le_bytes());
    v.extend_from_slice(&m.entry_count.to_le_bytes());
    v
}

fn decode_meta(data: &[u8]) -> Result<SSTableMeta> {
    let (m, _read) = decode_meta_with_consumed(data)?;
    Ok(m)
}

fn decode_meta_with_consumed(data: &[u8]) -> Result<(SSTableMeta, usize)> {
    let mut p = 0usize;
    if p + 8 + 4 > data.len() {
        return Err(Error::Corruption("meta too short".into()));
    }
    let id = u64::from_le_bytes(data[p..p + 8].try_into().unwrap());
    p += 8;
    let level = u32::from_le_bytes(data[p..p + 4].try_into().unwrap());
    p += 4;
    if p + 4 > data.len() {
        return Err(Error::Corruption("meta truncated".into()));
    }
    let min_len = u32::from_le_bytes(data[p..p + 4].try_into().unwrap()) as usize;
    p += 4;
    if p + min_len > data.len() {
        return Err(Error::Corruption("meta min_key truncated".into()));
    }
    let min_key = data[p..p + min_len].to_vec();
    p += min_len;
    if p + 4 > data.len() {
        return Err(Error::Corruption("meta truncated after min".into()));
    }
    let max_len = u32::from_le_bytes(data[p..p + 4].try_into().unwrap()) as usize;
    p += 4;
    if p + max_len > data.len() {
        return Err(Error::Corruption("meta max_key truncated".into()));
    }
    let max_key = data[p..p + max_len].to_vec();
    p += max_len;
    if p + 8 + 8 > data.len() {
        return Err(Error::Corruption("meta truncated final".into()));
    }
    let file_size = u64::from_le_bytes(data[p..p + 8].try_into().unwrap());
    p += 8;
    let entry_count = u64::from_le_bytes(data[p..p + 8].try_into().unwrap());
    p += 8;

    Ok((
        SSTableMeta {
            id,
            level,
            min_key,
            max_key,
            file_size,
            entry_count,
        },
        p,
    ))
}

/// The manifest: a durable log of database structure changes.
///
/// Reuses the WAL format (CRC + records) — same append-only,
/// crash-safe log pattern.
///
/// Example manifest contents:
/// ```text
/// Record 1: NewSSTable { id: 1, level: 0, min: "a", max: "f" }
/// Record 2: NewSSTable { id: 2, level: 0, min: "d", max: "k" }
/// Record 3: CompactionComplete { added: [id:3@L1], removed: [1, 2] }
/// ```
pub struct Manifest {
    path: PathBuf,
    // File handle opened for append/read
    file: std::fs::File,
    current_version: version::Version,
    /// WAL ID of the current active WAL at last flush. Recovery replays WALs >= this.
    log_number: u64,
    /// Next SSTable ID to use (max seen across all SSTableMeta + 1).
    next_sst_id: u64,
}

impl Manifest {
    /// Open or create a manifest at the given path.
    pub fn open(_path: &std::path::Path) -> Result<Self> {
        let path_buf = _path.to_path_buf();

        // Ensure file exists (create if missing)
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(_path)?;

        // Read entire file to replay records
        let mut data = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut data)?;

        // Replay records
        let mut offset = 0usize;
        let mut version = version::Version::new(7); // default levels
        let mut parsed = 0usize;
        let mut log_number: u64 = 0;
        let mut max_sst_id: u64 = 0;

        while offset + 4 <= data.len() {
            let len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + len + 4 > data.len() {
                // truncated record at end
                break;
            }
            let payload = &data[offset..offset + len];
            offset += len;
            let crc_read = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            // verify crc
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let crc_calc = hasher.finalize();
            if crc_calc != crc_read {
                // corruption detected; stop replay
                break;
            }

            // decode payload
            match payload[0] {
                1 => {
                    // NewSSTable
                    let meta = decode_meta(&payload[1..])?;
                    if meta.id >= max_sst_id {
                        max_sst_id = meta.id;
                    }
                    // ensure levels vector big enough
                    let lvl = meta.level as usize;
                    if version.levels.len() <= lvl {
                        version.levels.resize(lvl + 1, Vec::new());
                    }
                    version.levels[lvl].push(meta);
                }
                2 => {
                    // CompactionComplete
                    let mut p = 1usize;
                    if p + 4 > payload.len() {
                        break;
                    }
                    let added_count =
                        u32::from_le_bytes(payload[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    let mut added = Vec::with_capacity(added_count);
                    for _ in 0..added_count {
                        let (m, read) = decode_meta_with_consumed(&payload[p..])?;
                        p += read;
                        added.push(m);
                    }
                    if p + 4 > payload.len() {
                        break;
                    }
                    let removed_count =
                        u32::from_le_bytes(payload[p..p + 4].try_into().unwrap()) as usize;
                    p += 4;
                    let mut removed = Vec::with_capacity(removed_count);
                    for _ in 0..removed_count {
                        if p + 8 > payload.len() {
                            break;
                        }
                        let id = u64::from_le_bytes(payload[p..p + 8].try_into().unwrap());
                        p += 8;
                        removed.push(id);
                    }

                    // apply removal
                    for id in removed.iter() {
                        for lvl in version.levels.iter_mut() {
                            lvl.retain(|m| m.id != *id);
                        }
                    }
                    // apply additions
                    for m in added.into_iter() {
                        if m.id >= max_sst_id {
                            max_sst_id = m.id;
                        }
                        let lvl = m.level as usize;
                        if version.levels.len() <= lvl {
                            version.levels.resize(lvl + 1, Vec::new());
                        }
                        version.levels[lvl].push(m);
                    }
                }
                3 => {
                    if payload.len() < 9 {
                        break;
                    }
                    log_number = u64::from_le_bytes(payload[1..9].try_into().unwrap());
                }
                _ => {
                    // unknown record type — stop
                    break;
                }
            }

            parsed += 1;
        }

        // If file was non-empty but we parsed zero valid records, treat as corruption
        if !data.is_empty() && parsed == 0 {
            return Err(Error::Corruption("no valid manifest records".into()));
        }

        Ok(Self {
            path: path_buf,
            file,
            current_version: version,
            log_number,
            next_sst_id: max_sst_id + 1,
        })
    }

    /// Record that a new SSTable was created from a memtable flush.
    pub fn record_flush(&mut self, _new_sst: SSTableMeta) -> Result<()> {
        // encode payload: [type=1][meta bytes]
        let mut payload = Vec::with_capacity(256);
        payload.push(1u8);
        payload.extend_from_slice(&encode_meta(&_new_sst));
        append_record(&mut self.file, &payload)?;

        // update in-memory version
        let lvl = _new_sst.level as usize;
        if self.current_version.levels.len() <= lvl {
            self.current_version.levels.resize(lvl + 1, Vec::new());
        }
        self.current_version.levels[lvl].push(_new_sst);
        Ok(())
    }

    /// Record that a compaction completed.
    pub fn record_compaction(
        &mut self,
        _added: Vec<SSTableMeta>,
        _removed: Vec<u64>,
    ) -> Result<()> {
        // payload: [type=2][added_count(4)][added...][removed_count(4)][removed ids...]
        let mut payload = Vec::with_capacity(256);
        payload.push(2u8);
        payload.extend_from_slice(&(_added.len() as u32).to_le_bytes());
        for m in _added.iter() {
            payload.extend_from_slice(&encode_meta(m));
        }
        payload.extend_from_slice(&(_removed.len() as u32).to_le_bytes());
        for id in _removed.iter() {
            payload.extend_from_slice(&id.to_le_bytes());
        }

        append_record(&mut self.file, &payload)?;

        // apply removals
        for id in _removed.iter() {
            for lvl in self.current_version.levels.iter_mut() {
                lvl.retain(|m| m.id != *id);
            }
        }
        // apply additions
        for m in _added.into_iter() {
            let lvl = m.level as usize;
            if self.current_version.levels.len() <= lvl {
                self.current_version.levels.resize(lvl + 1, Vec::new());
            }
            self.current_version.levels[lvl].push(m);
        }

        Ok(())
    }

    /// Record the current active WAL number in the manifest.
    /// Called after each flush so recovery knows which WALs to replay.
    pub fn record_log_number(&mut self, log_number: u64) -> Result<()> {
        let mut payload = Vec::with_capacity(9);
        payload.push(3u8);
        payload.extend_from_slice(&log_number.to_le_bytes());
        append_record(&mut self.file, &payload)?;
        self.log_number = log_number;
        Ok(())
    }

    /// The WAL number from the last flush. Recovery replays WALs >= this value.
    pub fn log_number(&self) -> u64 {
        self.log_number
    }

    /// Next SSTable ID to allocate (1 past the highest seen in manifest).
    pub fn next_sst_id(&self) -> u64 {
        self.next_sst_id
    }

    /// Get the current version (which SSTables exist at which levels).
    pub fn current_version(&self) -> &version::Version {
        &self.current_version
    }

    /// Compact the manifest: snapshot current version to a new file.
    /// The old manifest can then be deleted.
    pub fn compact(&mut self) -> Result<()> {
        todo!("[M29]: Write full Version snapshot, rotate manifest file")
    }
}

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use ahash::AHashSet;
use anyhow::{Context, Result};
use tracing::{info, warn};

/// Bounded FIFO cache for under-min depth-2 pairs used by subset pruning.
///
/// Keys are encoded as `i | (j << 32)` with `i < j`.
pub(crate) struct SubsetPruningCache {
    keys: VecDeque<u64>,
    set: AHashSet<u64>,
    capacity: usize,
}

pub(crate) const SUBSET_CACHE_CAPACITY: usize = 5_000_000;

impl SubsetPruningCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            keys: VecDeque::new(),
            set: AHashSet::new(),
            capacity,
        }
    }

    pub(crate) fn encode_pair(i: usize, j: usize) -> u64 {
        debug_assert!(i < j);
        (i as u64) | ((j as u64) << 32)
    }

    pub(crate) fn insert_pair(&mut self, i: usize, j: usize) {
        if i >= j {
            return;
        }
        let key = Self::encode_pair(i, j);
        if self.set.contains(&key) {
            return;
        }
        self.set.insert(key);
        self.keys.push_back(key);
        if self.keys.len() > self.capacity {
            if let Some(old) = self.keys.pop_front() {
                self.set.remove(&old);
            }
        }
    }

    pub(crate) fn view(&self) -> &AHashSet<u64> {
        &self.set
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    pub(crate) fn keys_snapshot(&self) -> Vec<u64> {
        self.keys.iter().copied().collect()
    }

    pub(crate) fn save_to_file(&self, path: &Path) -> Result<()> {
        let buf = encode_cache_snapshot(self.keys.iter().copied());
        fs::write(path, &buf).with_context(|| {
            format!("Failed to write subset pruning cache to {}", path.display())
        })?;
        Ok(())
    }

    pub(crate) fn load_from_file(path: &Path, capacity: usize) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(capacity));
        }
        let data = fs::read(path).with_context(|| {
            format!(
                "Failed to read subset pruning cache from {}",
                path.display()
            )
        })?;
        decode_cache_snapshot(&data, capacity)
    }
}

/// Background saver that persists subset pruning cache snapshots without
/// blocking the main evaluation loop.
pub(crate) struct SubsetCacheSaver {
    tx: mpsc::SyncSender<Vec<u64>>,
}

impl SubsetCacheSaver {
    pub(crate) fn new(path: PathBuf) -> Result<(Self, thread::JoinHandle<()>)> {
        let (tx, rx) = mpsc::sync_channel::<Vec<u64>>(1);
        let builder = thread::Builder::new()
            .name("subset-cache-saver".to_string())
            // Give the saver thread a larger stack in case downstream
            // logging or filesystem layers use recursion.
            .stack_size(8 * 1024 * 1024);
        let handle = builder
            .spawn(move || {
                while let Ok(snapshot) = rx.recv() {
                    let start = Instant::now();
                    let count = snapshot.len();
                    let buf = encode_cache_snapshot(snapshot);
                    let tmp_path = path.with_extension("bin.tmp");
                    let write_result =
                        fs::write(&tmp_path, &buf).and_then(|_| fs::rename(&tmp_path, &path));
                    let elapsed_ms = (start.elapsed().as_secs_f32() * 1000.0).round() as u64;
                    match write_result {
                        Ok(_) => {
                            info!(
                                entries = %format_int(count as u128),
                                save_ms = %format_int(elapsed_ms),
                                path = %path.display(),
                                "Subset pruning cache async save completed"
                            );
                        }
                        Err(error) => {
                            warn!(
                                ?error,
                                save_ms = %format_int(elapsed_ms),
                                path = %path.display(),
                                "Subset pruning cache async save failed"
                            );
                        }
                    }
                }
            })
            .context("failed to spawn subset-cache-saver thread")?;
        Ok((SubsetCacheSaver { tx }, handle))
    }

    pub(crate) fn enqueue_blocking(&self, snapshot: Vec<u64>) {
        if snapshot.is_empty() {
            return;
        }
        let _ = self.tx.send(snapshot);
    }
}

fn encode_cache_snapshot(keys: impl IntoIterator<Item = u64>) -> Vec<u8> {
    let keys: Vec<u64> = keys.into_iter().collect();
    let version: u32 = 1;
    let reserved: u32 = 0;
    let count: u64 = keys.len() as u64;
    let mut buf = Vec::with_capacity(16 + keys.len() * 8);
    buf.extend_from_slice(&version.to_le_bytes());
    buf.extend_from_slice(&reserved.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    for key in keys {
        buf.extend_from_slice(&key.to_le_bytes());
    }
    buf
}

fn decode_cache_snapshot(data: &[u8], capacity: usize) -> Result<SubsetPruningCache> {
    if data.len() < 16 {
        return Ok(SubsetPruningCache::new(capacity));
    }
    let mut version_bytes = [0u8; 4];
    version_bytes.copy_from_slice(&data[0..4]);
    let version = u32::from_le_bytes(version_bytes);
    let mut count_bytes = [0u8; 8];
    count_bytes.copy_from_slice(&data[8..16]);
    let count = u64::from_le_bytes(count_bytes);
    if version != 1 {
        return Ok(SubsetPruningCache::new(capacity));
    }
    let available = (data.len() - 16) / 8;
    let take = available.min((count as usize).min(capacity));
    let mut cache = SubsetPruningCache::new(capacity);
    for idx in 0..take {
        let start = 16 + idx * 8;
        let end = start + 8;
        let mut key_bytes = [0u8; 8];
        key_bytes.copy_from_slice(&data[start..end]);
        let key = u64::from_le_bytes(key_bytes);
        if cache.set.insert(key) {
            cache.keys.push_back(key);
        }
    }
    Ok(cache)
}

fn format_int<T: Into<u128>>(value: T) -> String {
    let s = value.into().to_string();
    let len = s.len();
    if len <= 3 {
        return s;
    }
    let mut out = String::with_capacity(len + len / 3);
    let mut count = 0usize;
    for ch in s.chars().rev() {
        if count == 3 {
            out.push(',');
            count = 0;
        }
        out.push(ch);
        count += 1;
    }
    out.chars().rev().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subset_cache_roundtrips_binary_snapshot() {
        let mut cache = SubsetPruningCache::new(10);
        cache.insert_pair(1, 2);
        cache.insert_pair(2, 4);

        let bytes = encode_cache_snapshot(cache.keys_snapshot());
        let loaded = decode_cache_snapshot(&bytes, 10).expect("snapshot should decode");

        assert_eq!(loaded.len(), 2);
        assert!(
            loaded
                .view()
                .contains(&SubsetPruningCache::encode_pair(1, 2))
        );
        assert!(
            loaded
                .view()
                .contains(&SubsetPruningCache::encode_pair(2, 4))
        );
    }

    #[test]
    fn subset_cache_respects_capacity_when_loading() {
        let bytes = encode_cache_snapshot([SubsetPruningCache::encode_pair(1, 2), 3, 4]);
        let loaded = decode_cache_snapshot(&bytes, 2).expect("snapshot should decode");

        assert_eq!(loaded.len(), 2);
    }
}

use super::*;
use crate::resources::Budget;
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque, hash_map::RandomState},
    hash::BuildHasher,
    sync::atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
struct Shard {
    entries: HashMap<String, Arc<Prepared>>,
    fifo: VecDeque<String>,
    bytes: usize,
}
pub(crate) struct Cache {
    shards: [RwLock<Shard>; 8],
    hash: RandomState,
    hits: AtomicU64,
    misses: AtomicU64,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(Shard::default())),
            hash: RandomState::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
}
impl Cache {
    pub fn prepare(&self, text: &str, budget: &Budget) -> Result<Arc<Prepared>> {
        if text.len() > MAX_STATEMENT_BYTES {
            return Err(Error::limit("DSL statement exceeds 64 KiB"));
        }
        let shard = &self.shards[self.hash.hash_one(text) as usize % self.shards.len()];
        if let Some(plan) = shard.read().entries.get(text).cloned() {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(plan);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let size = text.len() * 64 + 4096;
        let charge = budget.reserve(size)?;
        let mut prepared = Prepared::parse(text)?;
        prepared._charge = Some(charge);
        let prepared = Arc::new(prepared);
        // Oversized preparations execute but do not evict the entire hot cache.
        if size <= 512 << 10 {
            let mut shard = shard.write();
            if let Some(existing) = shard.entries.get(text) {
                return Ok(existing.clone());
            }
            while shard.entries.len() >= 32 || shard.bytes + size > 512 << 10 {
                let old = shard.fifo.pop_front().unwrap();
                if let Some(entry) = shard.entries.remove(&old) {
                    shard.bytes -= entry._charge.as_ref().unwrap().size();
                }
            }
            shard.entries.insert(text.into(), prepared.clone());
            shard.fifo.push_back(text.into());
            shard.bytes += size;
        }
        Ok(prepared)
    }
    pub fn stats(&self) -> Value {
        let mut bytes = 0;
        let mut entries = 0;
        for shard in &self.shards {
            let shard = shard.read();
            bytes += shard.bytes;
            entries += shard.entries.len();
        }
        serde_json::json!({"hits":self.hits.load(Ordering::Relaxed),"misses":self.misses.load(Ordering::Relaxed),"entries":entries,"reserved_bytes":bytes})
    }
}

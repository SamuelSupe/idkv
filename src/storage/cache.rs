use super::sst::{BlockMeta, Record};
use crate::resources::Reservation;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct CacheKey(pub Uuid, pub u64);
pub enum Contents {
    Data(Vec<Record>),
    Index(Vec<BlockMeta>),
}

pub struct Frame {
    pub contents: Contents,
    pub referenced: AtomicBool,
    pub reservation: Arc<Reservation>,
}
struct Shard {
    entries: HashMap<CacheKey, Arc<Frame>>,
    clock: VecDeque<CacheKey>,
    bytes: usize,
}
pub struct Cache {
    shards: Vec<Mutex<Shard>>,
    capacity: usize,
}
impl Cache {
    pub(crate) fn reserve(
        &self,
        budget: &crate::resources::Budget,
        bytes: usize,
    ) -> crate::Result<Reservation> {
        let mut result = budget.reserve(bytes);
        if result.is_ok() {
            return result;
        }
        for shard in &self.shards {
            let mut shard = shard.lock();
            for _ in 0..shard.clock.len().saturating_mul(2) {
                let Some(key) = shard.clock.pop_front() else {
                    break;
                };
                let frame = shard.entries.get(&key).unwrap();
                if frame.referenced.swap(false, Ordering::Relaxed) {
                    shard.clock.push_back(key);
                    continue;
                }
                let frame = shard.entries.remove(&key).unwrap();
                shard.bytes -= frame.reservation.size();
                drop(frame);
                result = budget.reserve(bytes);
                if result.is_ok() {
                    return result;
                }
            }
        }
        result
    }
    pub fn new(capacity: usize) -> Self {
        Self {
            shards: (0..8)
                .map(|_| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                        clock: VecDeque::new(),
                        bytes: 0,
                    })
                })
                .collect(),
            capacity: capacity / 8,
        }
    }
    fn shard(&self, key: &CacheKey) -> usize {
        (key.0.as_u128() as u64 ^ key.1.rotate_right(7)) as usize % self.shards.len()
    }
    pub fn get(&self, key: &CacheKey) -> Option<Arc<Frame>> {
        let shard = self.shards[self.shard(key)].lock();
        let frame = shard.entries.get(key)?.clone();
        frame.referenced.store(true, Ordering::Relaxed);
        Some(frame)
    }
    pub fn insert(&self, key: CacheKey, frame: Arc<Frame>) {
        let size = frame.reservation.size();
        if size > self.capacity {
            return;
        }
        let mut shard = self.shards[self.shard(&key)].lock();
        if shard.entries.contains_key(&key) {
            return;
        }
        let mut attempts = shard.clock.len() * 2 + 1;
        while shard.bytes + size > self.capacity && attempts > 0 {
            attempts -= 1;
            let Some(old_key) = shard.clock.pop_front() else {
                break;
            };
            let old = shard.entries.get(&old_key).unwrap();
            if old.referenced.swap(false, Ordering::Relaxed) {
                shard.clock.push_back(old_key);
            } else {
                let old = shard.entries.remove(&old_key).unwrap();
                shard.bytes -= old.reservation.size();
            }
        }
        if shard.bytes + size <= self.capacity {
            shard.bytes += size;
            shard.clock.push_back(key);
            shard.entries.insert(key, frame);
        }
    }
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock();
            shard.entries.clear();
            shard.clock.clear();
            shard.bytes = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Budget, Counter};

    #[test]
    fn a_failed_reservation_reclaims_only_the_cache_space_it_needs() {
        let budget = Budget {
            global: Counter::new(256),
            tenant: Counter::new(256),
        };
        let cache = Cache::new(1024);
        let keys: Vec<_> = (0..4).map(|n| CacheKey(Uuid::from_u128(n), 0)).collect();
        for &key in &keys {
            cache.insert(
                key,
                Arc::new(Frame {
                    contents: Contents::Data(vec![]),
                    referenced: AtomicBool::new(false),
                    reservation: Arc::new(budget.reserve(64).unwrap()),
                }),
            );
        }
        let _memory = cache.reserve(&budget, 64).unwrap();
        assert_eq!(
            keys.iter().filter(|key| cache.get(key).is_some()).count(),
            3
        );
    }
}

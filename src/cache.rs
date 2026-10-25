//! Sharded CLOCK page cache.
//!
//! Committed pages are immutable, so the cache hands out `Arc<Page>`
//! clones: a reader keeps using a page it holds even after the cache has
//! evicted it, and the reference count is the only "pin" there is. Each
//! slot remembers which version (generation) of the page it holds; a
//! lookup for a different generation is a miss.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One immutable page image.
pub(crate) struct Page {
    pub buf: Box<[u8]>,
}

/// Multiplicative hash for page ids (SipHash is needlessly slow here).
#[derive(Default)]
pub(crate) struct IdHasher(u64);

impl Hasher for IdHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x100_0000_01B3);
        }
    }
    fn write_u32(&mut self, n: u32) {
        self.0 = (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

pub(crate) type IdBuild = BuildHasherDefault<IdHasher>;
pub(crate) type IdMap<V> = HashMap<u32, V, IdBuild>;

struct Slot {
    id: u32,
    page: Arc<Page>,
    hot: bool,
}

struct Shard {
    map: IdMap<usize>,
    slots: Vec<Slot>,
    hand: usize,
    cap: usize,
}

pub(crate) struct Cache {
    shards: Box<[Mutex<Shard>]>,
    bits: u32,
    capacity: usize,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl Cache {
    pub fn new(capacity: usize) -> Cache {
        let capacity = capacity.max(1);
        let shards = (capacity / 64).next_power_of_two().clamp(1, 64);
        let per = capacity.div_ceil(shards);
        let shards: Vec<_> = (0..shards)
            .map(|_| {
                Mutex::new(Shard {
                    map: IdMap::default(),
                    slots: Vec::new(),
                    hand: 0,
                    cap: per,
                })
            })
            .collect();
        Cache {
            bits: shards.len().trailing_zeros(),
            shards: shards.into_boxed_slice(),
            capacity,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    #[inline]
    fn shard(&self, id: u32) -> &Mutex<Shard> {
        let i = if self.bits == 0 {
            0
        } else {
            (id.wrapping_mul(0x9E37_79B9) >> (32 - self.bits)) as usize
        };
        &self.shards[i]
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The cached image of `id` if it is generation `ver`.
    #[inline]
    pub fn get(&self, id: u32, ver: u64) -> Option<Arc<Page>> {
        let mut s = self.shard(id).lock().unwrap();
        let &i = s.map.get(&id)?;
        let slot = &mut s.slots[i];
        if crate::layout::page_ver(&slot.page.buf) != ver {
            return None;
        }
        slot.hot = true;
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(slot.page.clone())
    }

    /// Insert (or replace) the image of `id`. Cold pages are the first to
    /// go, which keeps a long scan from flushing the index pages.
    pub fn insert(&self, id: u32, page: Arc<Page>, hot: bool) {
        let mut s = self.shard(id).lock().unwrap();
        let s = &mut *s;
        if let Some(&i) = s.map.get(&id) {
            s.slots[i].page = page;
            s.slots[i].hot |= hot;
            return;
        }
        if s.slots.len() < s.cap {
            s.map.insert(id, s.slots.len());
            s.slots.push(Slot { id, page, hot });
            return;
        }
        loop {
            let h = s.hand;
            s.hand = (h + 1) % s.slots.len();
            let slot = &mut s.slots[h];
            if slot.hot {
                slot.hot = false;
                continue;
            }
            s.map.remove(&slot.id);
            *slot = Slot { id, page, hot };
            s.map.insert(id, h);
            return;
        }
    }

    pub fn used(&self) -> usize {
        self.shards.iter().map(|s| s.lock().unwrap().slots.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::set_page_ver;

    fn page(ver: u64) -> Arc<Page> {
        let mut buf = vec![0u8; 64].into_boxed_slice();
        set_page_ver(&mut buf, ver);
        Arc::new(Page { buf })
    }

    #[test]
    fn hits_misses_and_versions() {
        let c = Cache::new(4);
        c.insert(1, page(7), true);
        assert!(c.get(1, 7).is_some());
        assert!(c.get(1, 8).is_none());
        c.insert(1, page(8), true);
        assert!(c.get(1, 8).is_some());
        for id in 2..20 {
            c.insert(id, page(1), false);
        }
        assert!(c.used() <= 4);
    }
}

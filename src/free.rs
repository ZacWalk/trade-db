//! Free-page bookkeeping.
//!
//! Pages freed by transaction `F` stay *pending* until no snapshot older
//! than `F` can still be reading them (and, for relaxed durability, until
//! `F` itself is durable). Only then do they join the reusable pool. The
//! state lives in memory in the writer and is persisted inline in the meta
//! slot when it fits; otherwise the next writer rebuilds it by walking the
//! trees (see `Db::rebuild_free`).

use std::collections::{BTreeSet, VecDeque};

use crate::layout::{rd_u32, rd_u64};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FreeState {
    /// Pages that may be allocated right now.
    pub pool: BTreeSet<u32>,
    /// `(freeing txn, pages)`, oldest first.
    pub pending: VecDeque<(u64, Vec<u32>)>,
}

fn extents(ids: impl Iterator<Item = u32>) -> Vec<(u32, u32)> {
    let mut out: Vec<(u32, u32)> = Vec::new();
    for id in ids {
        match out.last_mut() {
            Some((s, l)) if *s as u64 + *l as u64 == id as u64 => *l += 1,
            _ => out.push((id, 1)),
        }
    }
    out
}

fn put_extents(out: &mut Vec<u8>, ext: &[(u32, u32)]) {
    out.extend_from_slice(&(ext.len() as u32).to_le_bytes());
    for (s, l) in ext {
        out.extend_from_slice(&s.to_le_bytes());
        out.extend_from_slice(&l.to_le_bytes());
    }
}

struct Reader<'a> {
    b: &'a [u8],
    o: usize,
}

impl Reader<'_> {
    fn u32(&mut self) -> Option<u32> {
        if self.o + 4 > self.b.len() {
            return None;
        }
        let v = rd_u32(self.b, self.o);
        self.o += 4;
        Some(v)
    }
    fn u64(&mut self) -> Option<u64> {
        if self.o + 8 > self.b.len() {
            return None;
        }
        let v = rd_u64(self.b, self.o);
        self.o += 8;
        Some(v)
    }
    fn extents(&mut self, limit: u64, out: &mut dyn FnMut(u32)) -> Option<()> {
        let n = self.u32()?;
        for _ in 0..n {
            let s = self.u32()?;
            let l = self.u32()?;
            if s as u64 + l as u64 > limit {
                return None;
            }
            for p in s..s + l {
                out(p);
            }
        }
        Some(())
    }
}

impl FreeState {
    pub fn free_count(&self) -> u64 {
        self.pool.len() as u64 + self.pending.iter().map(|(_, v)| v.len() as u64).sum::<u64>()
    }

    /// Move every pending group freed at or before `upto` into the pool.
    pub fn release_upto(&mut self, upto: u64) {
        while let Some((t, _)) = self.pending.front() {
            if *t > upto {
                break;
            }
            let (_, pages) = self.pending.pop_front().unwrap();
            self.pool.extend(pages);
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_extents(&mut out, &extents(self.pool.iter().copied()));
        out.extend_from_slice(&(self.pending.len() as u32).to_le_bytes());
        for (t, pages) in &self.pending {
            let mut sorted = pages.clone();
            sorted.sort_unstable();
            out.extend_from_slice(&t.to_le_bytes());
            put_extents(&mut out, &extents(sorted.into_iter()));
        }
        out
    }

    /// Decode, rejecting anything that names a page at or past `page_count`.
    pub fn decode(b: &[u8], page_count: u64) -> Option<FreeState> {
        let mut r = Reader { b, o: 0 };
        let mut st = FreeState::default();
        r.extents(page_count, &mut |p| {
            st.pool.insert(p);
        })?;
        let groups = r.u32()?;
        for _ in 0..groups {
            let t = r.u64()?;
            let mut pages = Vec::new();
            r.extents(page_count, &mut |p| pages.push(p))?;
            st.pending.push_back((t, pages));
        }
        (r.o == b.len()).then_some(st)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_release() {
        let mut f = FreeState::default();
        f.pool.extend([5, 6, 7, 9, 100]);
        f.pending.push_back((10, vec![40, 41, 3]));
        f.pending.push_back((12, vec![50]));
        let enc = f.encode();
        let mut d = FreeState::decode(&enc, 200).unwrap();
        assert_eq!(d.pool, f.pool);
        assert_eq!(d.pending[0].1, vec![3, 40, 41]);
        assert!(FreeState::decode(&enc, 60).is_none());
        assert!(FreeState::decode(&enc[..enc.len() - 1], 200).is_none());
        d.release_upto(11);
        assert!(d.pool.contains(&41));
        assert_eq!(d.pending.len(), 1);
        assert_eq!(d.free_count(), 9);
    }
}

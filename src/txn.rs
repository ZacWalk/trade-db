//! Write transactions: copy-on-write page management, B+tree mutation and
//! commit.
//!
//! A transaction never modifies a committed page. The first time it needs
//! to change one it copies it to a freshly allocated page (and records the
//! old one as freed), then rewrites every ancestor's pointer the same way —
//! path copying. Pages it allocated itself are modified in place and may
//! be spilled to disk early, because nothing committed can reach them.
//! Commit writes those pages, then flips the meta: that single write is
//! the atomic commit point.

use std::collections::HashSet;
use std::sync::Arc;

use crate::batch::{Batch, OpKind, StructOp};
use crate::cache::{IdBuild, IdMap};
use crate::db::{Durability, Inner};
use crate::error::{Error, Result};
use crate::free::FreeState;
use crate::layout::*;
use crate::meta::{DirRoot, Meta, umeta_apply};

/// A tree being modified.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeMut {
    pub root: PageRef,
    pub height: u8,
    pub count: u64,
    pub leaves: u64,
    /// Layout of this tree's leaves.
    pub lay: Leaf,
    pub owner: u64,
    pub flags: u8,
}

/// A new right sibling to link into a parent: first key, page, record count.
type Sibling = (Key, PageRef, u64);

pub(crate) enum Put {
    Inserted,
    Replaced,
    Duplicate,
}

/// What a successful commit produced, for the caller to publish.
pub(crate) struct Committed {
    pub meta: Arc<Meta>,
    pub free: FreeState,
    pub pages: Vec<(u32, Box<[u8]>)>,
}

pub(crate) struct Txn<'a> {
    pub db: &'a Inner,
    pub id: u64,
    pub base: Arc<Meta>,
    dirty: IdMap<Box<[u8]>>,
    spilled: HashSet<u32, IdBuild>,
    freed: Vec<u32>,
    free: FreeState,
    /// `(page, taken)` in order: undone in reverse on rollback.
    pool_log: Vec<(u32, bool)>,
    page_count: u64,
    pub dir: DirRoot,
    umeta: Option<Vec<u8>>,
    scratch: Vec<u8>,
    spill_limit: usize,
}

fn check_node(b: &[u8], t: &TreeMut, lv: u16, id: u32) -> Result<()> {
    let shape_ok = if lv == 0 {
        kind(b) == t.lay.kind() && (kind(b) != KIND_FIXED_LEAF || Leaf::of(b) == t.lay)
    } else {
        kind(b) == KIND_BRANCH
    };
    if !shape_ok || level(b) != lv || owner(b) != t.owner || flags(b) & FLAG_DIR != t.flags {
        return Err(Error::corrupt(id, "page does not belong where its parent points"));
    }
    Ok(())
}

// ---- leaf editing ----------------------------------------------------------

fn leaf_reset(b: &mut [u8], lay: Leaf) {
    set_count(b, 0);
    if lay == Leaf::Var {
        let end = body_end(b.len()) as u32;
        set_aux(b, end);
    }
}

/// Append a record that sorts after every record in the leaf and fits.
fn leaf_push(b: &mut [u8], lay: Leaf, key: Key, val: &[u8]) {
    let n = count(b);
    match lay {
        Leaf::Var => {
            let heap = aux(b) as usize - val.len();
            b[heap..heap + val.len()].copy_from_slice(val);
            let o = HDR + SLOT * n;
            wr_u64(b, o, key.time);
            wr_u32(b, o + 8, key.seq);
            wr_u16(b, o + 12, heap as u16);
            wr_u16(b, o + 14, val.len() as u16);
            set_aux(b, heap as u32);
        }
        Leaf::Fixed(f) => {
            wr_u64(b, f.times + 8 * n, key.time);
            wr_u32(b, f.seqs + 4 * n, key.seq);
            b[f.pay + f.rs * n..f.pay + f.rs * (n + 1)].copy_from_slice(val);
        }
    }
    set_count(b, n + 1);
}

/// Repack a variable leaf's value heap, dropping garbage.
fn compact(b: &mut [u8], scratch: &mut Vec<u8>) {
    scratch.clear();
    scratch.extend_from_slice(b);
    let n = count(b);
    let mut heap = body_end(b.len());
    for i in 0..n {
        let (off, len) = Leaf::Var.val(scratch, i);
        heap -= len;
        b[heap..heap + len].copy_from_slice(&scratch[off..off + len]);
        wr_u16(b, HDR + SLOT * i + 12, heap as u16);
    }
    set_aux(b, heap as u32);
}

fn live_bytes(b: &[u8], skip: Option<usize>) -> usize {
    (0..count(b))
        .filter(|&i| Some(i) != skip)
        .map(|i| Leaf::Var.val(b, i).1)
        .sum()
}

/// Insert at `pos` if it fits (compacting if that is what it takes).
fn leaf_try_insert(b: &mut [u8], lay: Leaf, pos: usize, key: Key, val: &[u8], scratch: &mut Vec<u8>) -> bool {
    let n = count(b);
    match lay {
        Leaf::Var => {
            let dir_end = HDR + SLOT * (n + 1);
            let mut heap = aux(b) as usize;
            if heap < dir_end + val.len() {
                if body_end(b.len()) < dir_end + live_bytes(b, None) + val.len() {
                    return false;
                }
                compact(b, scratch);
                heap = aux(b) as usize;
            }
            heap -= val.len();
            b[heap..heap + val.len()].copy_from_slice(val);
            b.copy_within(HDR + SLOT * pos..HDR + SLOT * n, HDR + SLOT * (pos + 1));
            let o = HDR + SLOT * pos;
            wr_u64(b, o, key.time);
            wr_u32(b, o + 8, key.seq);
            wr_u16(b, o + 12, heap as u16);
            wr_u16(b, o + 14, val.len() as u16);
            set_aux(b, heap as u32);
        }
        Leaf::Fixed(f) => {
            if n >= f.cap {
                return false;
            }
            b.copy_within(f.times + 8 * pos..f.times + 8 * n, f.times + 8 * (pos + 1));
            b.copy_within(f.seqs + 4 * pos..f.seqs + 4 * n, f.seqs + 4 * (pos + 1));
            b.copy_within(f.pay + f.rs * pos..f.pay + f.rs * n, f.pay + f.rs * (pos + 1));
            wr_u64(b, f.times + 8 * pos, key.time);
            wr_u32(b, f.seqs + 4 * pos, key.seq);
            b[f.pay + f.rs * pos..f.pay + f.rs * (pos + 1)].copy_from_slice(val);
        }
    }
    set_count(b, n + 1);
    true
}

/// Replace the value of record `pos` if the new value fits in the leaf.
fn leaf_overwrite(b: &mut [u8], lay: Leaf, pos: usize, val: &[u8], scratch: &mut Vec<u8>) -> bool {
    match lay {
        Leaf::Fixed(f) => {
            b[f.pay + f.rs * pos..f.pay + f.rs * (pos + 1)].copy_from_slice(val);
            true
        }
        Leaf::Var => {
            let o = HDR + SLOT * pos;
            let (off, len) = lay.val(b, pos);
            if val.len() <= len {
                b[off..off + val.len()].copy_from_slice(val);
                wr_u16(b, o + 14, val.len() as u16);
                return true;
            }
            let n = count(b);
            let mut heap = aux(b) as usize;
            if heap < HDR + SLOT * n + val.len() {
                if body_end(b.len()) < HDR + SLOT * n + live_bytes(b, Some(pos)) + val.len() {
                    return false;
                }
                wr_u16(b, o + 14, 0);
                compact(b, scratch);
                heap = aux(b) as usize;
            }
            heap -= val.len();
            b[heap..heap + val.len()].copy_from_slice(val);
            wr_u16(b, o + 12, heap as u16);
            wr_u16(b, o + 14, val.len() as u16);
            set_aux(b, heap as u32);
            true
        }
    }
}

/// Remove records `[a, z)`.
fn leaf_remove(b: &mut [u8], lay: Leaf, a: usize, z: usize) {
    let n = count(b);
    match lay {
        Leaf::Var => {
            b.copy_within(HDR + SLOT * z..HDR + SLOT * n, HDR + SLOT * a);
            if n - (z - a) == 0 {
                let end = body_end(b.len()) as u32;
                set_aux(b, end);
            }
        }
        Leaf::Fixed(f) => {
            b.copy_within(f.times + 8 * z..f.times + 8 * n, f.times + 8 * a);
            b.copy_within(f.seqs + 4 * z..f.seqs + 4 * n, f.seqs + 4 * a);
            b.copy_within(f.pay + f.rs * z..f.pay + f.rs * n, f.pay + f.rs * a);
        }
    }
    set_count(b, n - (z - a));
}

fn br_entries(b: &[u8]) -> Vec<Sibling> {
    (0..count(b))
        .map(|i| (br_key(b, i), br_child(b, i), br_count(b, i)))
        .collect()
}

/// Record `j` of a leaf being split: the old records with the new one
/// spliced in at `pos`.
fn split_item<'x>(old: &'x [u8], lay: Leaf, pos: usize, key: Key, val: &'x [u8], j: usize) -> (Key, &'x [u8]) {
    if j == pos {
        return (key, val);
    }
    let k = if j < pos { j } else { j - 1 };
    let (off, len) = lay.val(old, k);
    (lay.key(old, k), &old[off..off + len])
}

fn br_remove(b: &mut [u8], i: usize) {
    let n = count(b);
    b.copy_within(HDR + BENTRY * (i + 1)..HDR + BENTRY * n, HDR + BENTRY * i);
    set_count(b, n - 1);
}

/// Choose how to split `sizes` (bytes per record, slot included) into
/// leaves of `cap` bytes: the most even two-way split that fits, or else
/// three ways with the new record (`pos`) on its own.
fn var_groups(sizes: &[usize], cap: usize, pos: usize) -> Vec<(usize, usize)> {
    let total: usize = sizes.iter().sum();
    let mut best: Option<(usize, usize)> = None;
    let mut left = 0;
    for s in 1..sizes.len() {
        left += sizes[s - 1];
        let right = total - left;
        if left <= cap && right <= cap {
            let diff = left.abs_diff(right);
            if best.is_none_or(|(_, d)| diff < d) {
                best = Some((s, diff));
            }
        }
    }
    match best {
        Some((s, _)) => vec![(0, s), (s, sizes.len())],
        None => vec![(0, pos), (pos, pos + 1), (pos + 1, sizes.len())],
    }
}

impl<'a> Txn<'a> {
    pub fn new(db: &'a Inner, base: Arc<Meta>, free: FreeState, spill_limit: usize) -> Txn<'a> {
        Txn {
            db,
            id: base.txn + 1,
            page_count: base.page_count,
            dir: base.dir,
            base,
            dirty: IdMap::default(),
            spilled: HashSet::default(),
            freed: Vec::new(),
            free,
            pool_log: Vec::new(),
            umeta: None,
            scratch: Vec::new(),
            spill_limit,
        }
    }

    #[inline]
    fn ps(&self) -> usize {
        self.db.ps
    }

    /// Whether the transaction changed nothing at all.
    pub fn is_noop(&self) -> bool {
        self.dirty.is_empty()
            && self.spilled.is_empty()
            && self.freed.is_empty()
            && self.umeta.is_none()
            && self.dir == self.base.dir
    }

    #[inline]
    fn fresh(&self, page: u32) -> PageRef {
        PageRef { page, ver: self.id }
    }

    fn alloc(&mut self) -> Result<u32> {
        if let Some(p) = self.free.pool.pop_first() {
            self.pool_log.push((p, true));
            return Ok(p);
        }
        if self.page_count >= u32::MAX as u64 {
            return Err(Error::invalid("database is full: 2^32 pages"));
        }
        let p = self.page_count as u32;
        self.page_count += 1;
        Ok(p)
    }

    fn new_page(&mut self, kind: u8, flags: u8, level: u16, owner: u64, aux: u32) -> Result<u32> {
        let id = self.alloc()?;
        let mut b = vec![0u8; self.ps()].into_boxed_slice();
        init_header(&mut b, kind, flags, level, owner, id, self.id, aux);
        self.dirty.insert(id, b);
        Ok(id)
    }

    fn new_leaf(&mut self, t: &TreeMut) -> Result<u32> {
        let aux = match t.lay {
            Leaf::Var => body_end(self.ps()) as u32,
            Leaf::Fixed(f) => f.rs as u32,
        };
        self.new_page(t.lay.kind(), t.flags, 0, t.owner, aux)
    }

    #[inline]
    fn pg(&self, id: u32) -> &[u8] {
        &self.dirty[&id]
    }

    #[inline]
    fn pg_mut(&mut self, id: u32) -> &mut [u8] {
        self.dirty.get_mut(&id).expect("page is dirty in this transaction")
    }

    fn page_and_scratch(&mut self, id: u32) -> (&mut [u8], &mut Vec<u8>) {
        (
            self.dirty.get_mut(&id).expect("page is dirty in this transaction"),
            &mut self.scratch,
        )
    }

    fn unspill(&mut self, id: u32) -> Result<()> {
        if self.spilled.remove(&id) {
            let mut b = vec![0u8; self.ps()].into_boxed_slice();
            self.db.storage.read_at(&mut b, id as u64 * self.ps() as u64)?;
            if !crc_ok(&b) || page_ver(&b) != self.id || page_id(&b) != id {
                return Err(Error::corrupt(id, "spilled page did not read back intact"));
            }
            self.dirty.insert(id, b);
        }
        Ok(())
    }

    /// Make the node `r` writable, returning the page to modify.
    fn cow(&mut self, r: PageRef, t: &TreeMut, lv: u16) -> Result<u32> {
        if r.ver == self.id {
            self.unspill(r.page)?;
            check_node(self.pg(r.page), t, lv, r.page)?;
            return Ok(r.page);
        }
        let src = self.db.load(r, true, true)?;
        check_node(&src.buf, t, lv, r.page)?;
        let id = self.alloc()?;
        let mut b = src.buf.clone();
        set_page_id(&mut b, id);
        set_page_ver(&mut b, self.id);
        self.dirty.insert(id, b);
        self.freed.push(r.page);
        Ok(id)
    }

    /// Read the node `r` without copying it.
    fn with_node<T>(&mut self, r: PageRef, t: &TreeMut, lv: u16, f: impl FnOnce(&[u8]) -> T) -> Result<T> {
        if r.ver == self.id {
            self.unspill(r.page)?;
            let b = self.pg(r.page);
            check_node(b, t, lv, r.page)?;
            return Ok(f(b));
        }
        let p = self.db.load(r, true, true)?;
        check_node(&p.buf, t, lv, r.page)?;
        Ok(f(&p.buf))
    }

    fn free_page(&mut self, r: PageRef) {
        if r.ver == self.id {
            // Never committed: reusable within this very transaction.
            self.dirty.remove(&r.page);
            self.spilled.remove(&r.page);
            self.free.pool.insert(r.page);
            self.pool_log.push((r.page, false));
        } else {
            self.freed.push(r.page);
        }
    }

    /// Write pages in ascending id order, coalescing consecutive ids.
    fn write_pages(&self, ids: &[u32], staging: &mut Vec<u8>) -> Result<()> {
        let ps = self.ps();
        let max_run = ((1 << 20) / ps).max(1);
        let mut i = 0;
        while i < ids.len() {
            let mut j = i + 1;
            while j < ids.len() && ids[j] == ids[j - 1] + 1 && j - i < max_run {
                j += 1;
            }
            let off = ids[i] as u64 * ps as u64;
            if j - i == 1 {
                self.db.storage.write_at(&self.dirty[&ids[i]], off)?;
            } else {
                staging.clear();
                for id in &ids[i..j] {
                    staging.extend_from_slice(&self.dirty[id]);
                }
                self.db.storage.write_at(staging, off)?;
            }
            i = j;
        }
        Ok(())
    }

    /// Write freshly allocated leaves out early when the dirty set grows
    /// past the budget. Safe because nothing committed can reach them.
    pub fn maybe_spill(&mut self) -> Result<()> {
        if self.dirty.len() <= self.spill_limit {
            return Ok(());
        }
        let mut ids: Vec<u32> = self
            .dirty
            .iter()
            .filter(|(_, b)| kind(b) != KIND_BRANCH)
            .map(|(&id, _)| id)
            .collect();
        ids.sort_unstable();
        for id in &ids {
            stamp_crc(self.dirty.get_mut(id).unwrap());
        }
        let mut staging = Vec::new();
        self.write_pages(&ids, &mut staging)?;
        for id in ids {
            self.dirty.remove(&id);
            self.spilled.insert(id);
        }
        self.db.spills.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    // ---- B+tree mutation ------------------------------------------------

    /// Insert or replace one record.
    pub fn tree_put(&mut self, t: &mut TreeMut, key: Key, val: &[u8], overwrite: bool) -> Result<Put> {
        if t.root.is_null() {
            let id = self.new_leaf(t)?;
            let lay = t.lay;
            leaf_push(self.pg_mut(id), lay, key, val);
            *t = TreeMut {
                root: self.fresh(id),
                height: 1,
                count: 1,
                leaves: 1,
                ..*t
            };
            return Ok(Put::Inserted);
        }
        let top = t.height as u16 - 1;
        let root = self.cow(t.root, t, top)?;
        t.root = self.fresh(root);
        // (branch page, child index, branch is rightmost at its level)
        let mut path: Vec<(u32, usize, bool)> = Vec::with_capacity(t.height as usize);
        let (mut leftmost, mut rightmost) = (true, true);
        let mut id = root;
        for lv in (1..=top).rev() {
            let (i, child, n) = {
                let b = self.pg(id);
                let i = br_search(b, key);
                (i, br_child(b, i), count(b))
            };
            path.push((id, i, rightmost));
            leftmost &= i == 0;
            rightmost &= i + 1 == n;
            let cid = self.cow(child, t, lv - 1)?;
            if child != self.fresh(cid) {
                let r = self.fresh(cid);
                br_set_child(self.pg_mut(id), i, r);
            }
            id = cid;
        }
        let lay = t.lay;
        let (pos, exact) = lay.search(self.pg(id), key);
        let mut delta = 1i64;
        if exact {
            if !overwrite {
                return Ok(Put::Duplicate);
            }
            let (b, s) = self.page_and_scratch(id);
            if leaf_overwrite(b, lay, pos, val, s) {
                return Ok(Put::Replaced);
            }
            leaf_remove(self.pg_mut(id), lay, pos, pos + 1);
            delta = 0;
        }
        let (b, s) = self.page_and_scratch(id);
        let mut sibs = Vec::new();
        let mut exact_count = None;
        if !leaf_try_insert(b, lay, pos, key, val, s) {
            let (left, new) = self.split_leaf(t, id, pos, key, val, leftmost, rightmost)?;
            exact_count = Some(left);
            sibs = new;
        }
        let min_key = (pos == 0).then_some(key);
        self.grow(t, &path, sibs, exact_count, min_key, delta)?;
        Ok(if delta == 1 { Put::Inserted } else { Put::Replaced })
    }

    /// Split full leaf `id` while inserting `(key, val)` at `pos`. Returns
    /// the record count left in `id` and the new right siblings.
    #[allow(clippy::too_many_arguments)]
    fn split_leaf(
        &mut self,
        t: &mut TreeMut,
        id: u32,
        pos: usize,
        key: Key,
        val: &[u8],
        leftmost: bool,
        rightmost: bool,
    ) -> Result<(u64, Vec<Sibling>)> {
        let lay = t.lay;
        let old = self.pg(id).to_vec();
        let n = count(&old);
        let total = n + 1;
        let item = |j: usize| split_item(&old, lay, pos, key, val, j);
        let groups = if rightmost && pos == n {
            // Appending past the end of the newest leaf: start a new one and
            // leave this one full, so time-ordered ingest packs every page.
            vec![(0, n), (n, total)]
        } else if leftmost && pos == 0 {
            vec![(0, 1), (1, total)]
        } else {
            match lay {
                Leaf::Fixed(_) => vec![(0, total / 2), (total / 2, total)],
                Leaf::Var => {
                    let sizes: Vec<usize> = (0..total).map(|j| SLOT + item(j).1.len()).collect();
                    var_groups(&sizes, body_end(self.ps()) - HDR, pos)
                }
            }
        };
        let mut sibs = Vec::with_capacity(groups.len() - 1);
        for (g, &(a, z)) in groups.iter().enumerate() {
            if g == 0 && pos >= z {
                // The left part is a prefix of the records already in place:
                // truncate instead of rebuilding (the hot append path).
                leaf_remove(self.pg_mut(id), lay, z, n);
                continue;
            }
            let pid = if g == 0 { id } else { self.new_leaf(t)? };
            let b = self.pg_mut(pid);
            leaf_reset(b, lay);
            for j in a..z {
                let (k, v) = item(j);
                leaf_push(b, lay, k, v);
            }
            if g > 0 {
                sibs.push((item(a).0, self.fresh(pid), (z - a) as u64));
            }
        }
        t.leaves += sibs.len() as u64;
        Ok(((groups[0].1 - groups[0].0) as u64, sibs))
    }

    /// Propagate an insertion up the copied path: counts, the first key,
    /// new siblings, branch splits and finally a new root.
    fn grow(
        &mut self,
        t: &mut TreeMut,
        path: &[(u32, usize, bool)],
        mut sibs: Vec<Sibling>,
        mut exact: Option<u64>,
        mut min_key: Option<Key>,
        delta: i64,
    ) -> Result<()> {
        let bcap = branch_cap(self.ps());
        for &(bid, i, rightmost) in path.iter().rev() {
            let b = self.pg_mut(bid);
            let c = exact.unwrap_or_else(|| (br_count(b, i) as i64 + delta) as u64);
            br_set_count(b, i, c);
            if let Some(k) = min_key {
                br_set_key(b, i, k);
                if i != 0 {
                    min_key = None;
                }
            }
            exact = None;
            if sibs.is_empty() {
                continue;
            }
            let n = count(b);
            if n + sibs.len() <= bcap {
                b.copy_within(
                    HDR + BENTRY * (i + 1)..HDR + BENTRY * n,
                    HDR + BENTRY * (i + 1 + sibs.len()),
                );
                for (j, &(k, r, c)) in sibs.iter().enumerate() {
                    br_set(b, i + 1 + j, k, r, c);
                }
                set_count(b, n + sibs.len());
                sibs.clear();
            } else {
                let append = rightmost && i + 1 == n;
                let (left, right) = self.split_branch(t, bid, i, &sibs, append)?;
                exact = Some(left);
                sibs = vec![right];
            }
        }
        if !sibs.is_empty() {
            let old = t.root;
            let old_min = {
                let b = self.pg(old.page);
                if kind(b) == KIND_BRANCH {
                    br_key(b, 0)
                } else {
                    t.lay.key(b, 0)
                }
            };
            let lv = t.height as u16;
            if t.height >= MAX_HEIGHT {
                return Err(Error::invalid("tree height limit reached"));
            }
            let nr = self.new_page(KIND_BRANCH, t.flags, lv, t.owner, 0)?;
            let b = self.pg_mut(nr);
            br_set(b, 0, old_min, old, exact.expect("split reports its left count"));
            for (j, &(k, r, c)) in sibs.iter().enumerate() {
                br_set(b, 1 + j, k, r, c);
            }
            set_count(b, 1 + sibs.len());
            t.root = self.fresh(nr);
            t.height += 1;
        }
        t.count = (t.count as i64 + delta) as u64;
        Ok(())
    }

    fn split_branch(
        &mut self,
        t: &TreeMut,
        bid: u32,
        i: usize,
        sibs: &[Sibling],
        append: bool,
    ) -> Result<(u64, Sibling)> {
        let mut all = br_entries(self.pg(bid));
        let lv = level(self.pg(bid));
        all.splice(i + 1..i + 1, sibs.iter().copied());
        let s = if append { all.len() - sibs.len() } else { all.len() / 2 };
        let rid = self.new_page(KIND_BRANCH, t.flags, lv, t.owner, 0)?;
        let b = self.pg_mut(bid);
        for (j, &(k, r, c)) in all[..s].iter().enumerate() {
            br_set(b, j, k, r, c);
        }
        set_count(b, s);
        let rb = self.pg_mut(rid);
        for (j, &(k, r, c)) in all[s..].iter().enumerate() {
            br_set(rb, j, k, r, c);
        }
        set_count(rb, all.len() - s);
        let left: u64 = all[..s].iter().map(|e| e.2).sum();
        let right: u64 = all[s..].iter().map(|e| e.2).sum();
        Ok((left, (all[s].0, self.fresh(rid), right)))
    }

    fn contains(&mut self, t: &TreeMut, key: Key) -> Result<bool> {
        let mut r = t.root;
        for lv in (1..t.height as u16).rev() {
            r = self.with_node(r, t, lv, |b| br_child(b, br_search(b, key)))?;
        }
        self.with_node(r, t, 0, |b| Leaf::of(b).search(b, key).1)
    }

    /// Delete one record; absent keys are a no-op.
    pub fn tree_del(&mut self, t: &mut TreeMut, key: Key) -> Result<bool> {
        if t.root.is_null() || !self.contains(t, key)? {
            return Ok(false);
        }
        let top = t.height as u16 - 1;
        let root = self.cow(t.root, t, top)?;
        t.root = self.fresh(root);
        let mut path: Vec<(u32, usize)> = Vec::with_capacity(t.height as usize);
        let mut id = root;
        for lv in (1..=top).rev() {
            let (i, child) = {
                let b = self.pg(id);
                let i = br_search(b, key);
                (i, br_child(b, i))
            };
            let cid = self.cow(child, t, lv - 1)?;
            let r = self.fresh(cid);
            br_set_child(self.pg_mut(id), i, r);
            path.push((id, i));
            id = cid;
        }
        let lay = t.lay;
        let (pos, exact) = lay.search(self.pg(id), key);
        if !exact {
            return Err(Error::corrupt(id, "key vanished between probe and delete"));
        }
        leaf_remove(self.pg_mut(id), lay, pos, pos + 1);
        let empty = count(self.pg(id)) == 0;
        let mut min_key = (!empty && pos == 0).then(|| lay.key(self.pg(id), 0));
        let mut removed = empty;
        if empty {
            self.free_page(self.fresh(id));
            t.leaves = t.leaves.saturating_sub(1);
        }
        for &(bid, i) in path.iter().rev() {
            if removed {
                br_remove(self.pg_mut(bid), i);
                if count(self.pg(bid)) == 0 {
                    self.free_page(self.fresh(bid));
                    continue;
                }
                removed = false;
                min_key = (i == 0).then(|| br_key(self.pg(bid), 0));
            } else {
                let b = self.pg_mut(bid);
                br_set_count(b, i, br_count(b, i).saturating_sub(1));
                if let Some(k) = min_key {
                    br_set_key(b, i, k);
                    if i != 0 {
                        min_key = None;
                    }
                }
            }
        }
        if removed {
            t.root = PageRef::NULL;
            t.height = 0;
        } else {
            self.collapse_root(t)?;
        }
        t.count = t.count.saturating_sub(1);
        Ok(true)
    }

    /// Replace a root branch that has a single child by that child.
    fn collapse_root(&mut self, t: &mut TreeMut) -> Result<()> {
        while t.height > 1 {
            let lv = t.height as u16 - 1;
            let (n, child) = self.with_node(t.root, t, lv, |b| (count(b), br_child(b, 0)))?;
            if n != 1 {
                break;
            }
            self.free_page(t.root);
            t.root = child;
            t.height -= 1;
        }
        Ok(())
    }

    fn node_min(&mut self, r: PageRef, t: &TreeMut, lv: u16) -> Result<Key> {
        self.with_node(r, t, lv, |b| if lv == 0 { Leaf::of(b).key(b, 0) } else { br_key(b, 0) })
    }

    /// Free a whole subtree. Leaves are freed by id, never read.
    pub fn free_subtree(&mut self, t: &mut TreeMut, r: PageRef, lv: u16) -> Result<()> {
        if lv == 0 {
            self.free_page(r);
            t.leaves = t.leaves.saturating_sub(1);
            return Ok(());
        }
        let children: Vec<PageRef> = self.with_node(r, t, lv, |b| (0..count(b)).map(|i| br_child(b, i)).collect())?;
        for c in children {
            self.free_subtree(t, c, lv - 1)?;
        }
        self.free_page(r);
        Ok(())
    }

    /// Delete every record in `[lo, hi]`, dropping whole subtrees where
    /// they fall entirely inside the range. Returns the number removed.
    pub fn tree_del_range(&mut self, t: &mut TreeMut, lo: Key, hi: Key) -> Result<u64> {
        if t.root.is_null() || lo > hi {
            return Ok(0);
        }
        let (root, removed) = self.del_range_at(t, t.root, t.height as u16 - 1, lo, hi, None)?;
        match root {
            None => {
                t.root = PageRef::NULL;
                t.height = 0;
            }
            Some(r) => {
                t.root = r;
                self.collapse_root(t)?;
            }
        }
        t.count = t.count.saturating_sub(removed);
        Ok(removed)
    }

    fn del_range_at(
        &mut self,
        t: &mut TreeMut,
        r: PageRef,
        lv: u16,
        lo: Key,
        hi: Key,
        upper: Option<Key>,
    ) -> Result<(Option<PageRef>, u64)> {
        if lv == 0 {
            let (n, a, z) = self.with_node(r, t, 0, |b| {
                let lay = Leaf::of(b);
                let n = count(b);
                let z = hi.succ().map_or(n, |h| lay.search(b, h).0);
                (n, lay.search(b, lo).0, z)
            })?;
            if a >= z {
                return Ok((Some(r), 0));
            }
            if a == 0 && z == n {
                self.free_page(r);
                t.leaves = t.leaves.saturating_sub(1);
                return Ok((None, n as u64));
            }
            let id = self.cow(r, t, 0)?;
            leaf_remove(self.pg_mut(id), t.lay, a, z);
            return Ok((Some(self.fresh(id)), (z - a) as u64));
        }
        let entries = self.with_node(r, t, lv, br_entries)?;
        let n = entries.len();
        let find = |k: Key| entries.partition_point(|e| e.0 <= k).saturating_sub(1);
        let (i0, i1) = (find(lo), find(hi));
        if entries[i1].0 > hi {
            return Ok((Some(r), 0));
        }
        let hi_succ = hi.succ();
        let mut removed = 0;
        // Copied only once something below actually changes.
        let mut id: Option<u32> = None;
        for i in (i0..=i1).rev() {
            let (k, child, cnt) = entries[i];
            let next = if i + 1 < n { Some(entries[i + 1].0) } else { upper };
            let covered = k >= lo
                && match (hi_succ, next) {
                    (None, _) => true,
                    (Some(h), Some(nx)) => nx <= h,
                    (Some(_), None) => false,
                };
            let (nr, rem) = if covered {
                self.free_subtree(t, child, lv - 1)?;
                (None, cnt)
            } else {
                self.del_range_at(t, child, lv - 1, lo, hi, next)?
            };
            // A child already copied earlier in this transaction comes back
            // with the same reference, so judge change by what was removed.
            if !covered && rem == 0 {
                continue;
            }
            removed += rem;
            let pid = match id {
                Some(p) => p,
                None => {
                    let p = self.cow(r, t, lv)?;
                    id = Some(p);
                    p
                }
            };
            match nr {
                None => br_remove(self.pg_mut(pid), i),
                Some(nr) => {
                    let min = if rem > 0 {
                        Some(self.node_min(nr, t, lv - 1)?)
                    } else {
                        None
                    };
                    let b = self.pg_mut(pid);
                    br_set_child(b, i, nr);
                    br_set_count(b, i, cnt.saturating_sub(rem));
                    if let Some(m) = min {
                        br_set_key(b, i, m);
                    }
                }
            }
        }
        let Some(id) = id else {
            return Ok((Some(r), 0));
        };
        if count(self.pg(id)) == 0 {
            self.free_page(self.fresh(id));
            return Ok((None, removed));
        }
        Ok((Some(self.fresh(id)), removed))
    }

    fn tree_min_max(&mut self, t: &TreeMut) -> Result<(Key, Key)> {
        let (mut lo, mut hi) = (t.root, t.root);
        for lv in (1..t.height as u16).rev() {
            lo = self.with_node(lo, t, lv, |b| br_child(b, 0))?;
            hi = self.with_node(hi, t, lv, |b| br_child(b, count(b) - 1))?;
        }
        let min = self.with_node(lo, t, 0, |b| Leaf::of(b).key(b, 0))?;
        let max = self.with_node(hi, t, 0, |b| Leaf::of(b).key(b, count(b) - 1))?;
        Ok((min, max))
    }

    // ---- symbol directory -------------------------------------------------

    fn dir_tree(&self) -> TreeMut {
        TreeMut {
            root: self.dir.root,
            height: self.dir.height,
            count: self.dir.symbols,
            leaves: self.dir.leaves,
            lay: Leaf::Fixed(Fixed::new(self.ps(), DIR_ENTRY_SIZE)),
            owner: 0,
            flags: FLAG_DIR,
        }
    }

    fn set_dir(&mut self, t: TreeMut) {
        self.dir = DirRoot {
            root: t.root,
            height: t.height,
            symbols: t.count,
            leaves: t.leaves,
        };
    }

    fn symbol_tree(&self, symbol: u64, e: &DirEntry) -> TreeMut {
        TreeMut {
            root: e.root,
            height: e.height,
            count: e.count,
            leaves: e.leaves,
            lay: Leaf::for_record_size(self.ps(), e.rs),
            owner: symbol,
            flags: 0,
        }
    }

    pub fn dir_get(&mut self, symbol: u64) -> Result<Option<DirEntry>> {
        let t = self.dir_tree();
        if t.root.is_null() {
            return Ok(None);
        }
        let key = Key::new(symbol, 0);
        let mut r = t.root;
        for lv in (1..t.height as u16).rev() {
            r = self.with_node(r, &t, lv, |b| br_child(b, br_search(b, key)))?;
        }
        self.with_node(r, &t, 0, |b| {
            let lay = Leaf::of(b);
            let (i, exact) = lay.search(b, key);
            exact.then(|| {
                let (off, len) = lay.val(b, i);
                DirEntry::decode(&b[off..off + len])
            })
        })
    }

    fn dir_put(&mut self, symbol: u64, e: &DirEntry) -> Result<()> {
        let mut t = self.dir_tree();
        self.tree_put(&mut t, Key::new(symbol, 0), &e.encode(), true)?;
        self.set_dir(t);
        Ok(())
    }

    fn dir_del(&mut self, symbol: u64) -> Result<()> {
        let mut t = self.dir_tree();
        self.tree_del(&mut t, Key::new(symbol, 0))?;
        self.set_dir(t);
        Ok(())
    }

    /// Write a symbol's tree back into its directory entry.
    fn store_symbol(&mut self, symbol: u64, mut e: DirEntry, t: &TreeMut) -> Result<()> {
        e.root = t.root;
        e.height = t.height;
        e.count = t.count;
        e.leaves = t.leaves;
        (e.min, e.max) = if t.count > 0 {
            self.tree_min_max(t)?
        } else {
            (Key::MIN, Key::MIN)
        };
        self.dir_put(symbol, &e)
    }

    // ---- operations -------------------------------------------------------

    pub fn define(&mut self, symbol: u64, rs: u32) -> Result<()> {
        let max = max_record_size(self.ps());
        if rs == 0 || rs as usize > max {
            return Err(Error::invalid(format!(
                "record size must be 1..={max} bytes for this page size, got {rs}"
            )));
        }
        match self.dir_get(symbol)? {
            Some(e) if e.rs == rs => Ok(()),
            Some(e) if e.count == 0 => self.dir_put(symbol, &DirEntry { rs, ..e }),
            Some(e) => Err(Error::invalid(format!(
                "symbol {symbol} already holds {} records as {}; it must be empty to change its layout",
                e.count,
                if e.rs == 0 {
                    "variable-width".to_string()
                } else {
                    format!("{}-byte records", e.rs)
                }
            ))),
            None => self.dir_put(
                symbol,
                &DirEntry {
                    rs,
                    ..DirEntry::default()
                },
            ),
        }
    }

    pub fn delete_range(&mut self, symbol: u64, lo: Key, hi: Key) -> Result<u64> {
        let Some(e) = self.dir_get(symbol)? else {
            return Ok(0);
        };
        let mut t = self.symbol_tree(symbol, &e);
        let n = self.tree_del_range(&mut t, lo, hi)?;
        // Store whenever records went or the tree moved: a copied root must
        // be what the directory points at. (Comparing trees alone misses
        // changes to pages this transaction had already copied.)
        if n > 0 || t != self.symbol_tree(symbol, &e) {
            self.store_symbol(symbol, e, &t)?;
        }
        Ok(n)
    }

    pub fn drop_symbol(&mut self, symbol: u64) -> Result<bool> {
        let Some(e) = self.dir_get(symbol)? else {
            return Ok(false);
        };
        let mut t = self.symbol_tree(symbol, &e);
        if !t.root.is_null() {
            let (root, top) = (t.root, t.height as u16 - 1);
            self.free_subtree(&mut t, root, top)?;
        }
        self.dir_del(symbol)?;
        Ok(true)
    }

    /// Apply a batch: structural operations in submission order, then the
    /// record operations sorted by key, then the metadata updates.
    pub fn apply(&mut self, batch: &Batch) -> Result<()> {
        for op in &batch.structs {
            match *op {
                StructOp::Define(s, rs) => self.define(s, rs)?,
                StructOp::DeleteRange(s, lo, hi) => {
                    self.delete_range(s, Key::new(lo, 0), Key::new(hi, u32::MAX))?;
                }
                StructOp::Drop(s) => {
                    self.drop_symbol(s)?;
                }
            }
            self.maybe_spill()?;
        }
        let ops = batch.sorted_ops();
        let mut i = 0;
        while i < ops.len() {
            let symbol = ops[i].symbol;
            let j = i + ops[i..].partition_point(|o| o.symbol == symbol);
            self.apply_symbol(symbol, &ops[i..j], &batch.data)?;
            i = j;
        }
        if !batch.meta.is_empty() {
            let cur = self.umeta.as_deref().unwrap_or(&self.base.umeta);
            self.umeta = Some(umeta_apply(cur, &batch.meta)?);
        }
        Ok(())
    }

    fn apply_symbol(&mut self, symbol: u64, ops: &[crate::batch::Op], data: &[u8]) -> Result<()> {
        let existing = self.dir_get(symbol)?;
        let e = match existing {
            Some(e) => e,
            None if ops.iter().any(|o| o.kind != OpKind::Delete) => DirEntry::default(),
            None => return Ok(()),
        };
        let mut t = self.symbol_tree(symbol, &e);
        let max_val = max_value_len(self.ps());
        // Tracked explicitly: once a structural op earlier in the batch has
        // copied this tree, edits keep the same root and `t` alone cannot
        // show that keys (and so min/max) changed.
        let mut changed = false;
        for op in ops {
            let key = Key::new(op.time, op.seq);
            if op.kind == OpKind::Delete {
                changed |= self.tree_del(&mut t, key)?;
            } else {
                let val = &data[op.off..op.off + op.len];
                if e.rs != 0 && val.len() != e.rs as usize {
                    return Err(Error::RecordSize {
                        symbol,
                        expected: e.rs,
                        actual: val.len(),
                    });
                }
                if val.len() > max_val {
                    return Err(Error::ValueTooLarge {
                        len: val.len(),
                        max: max_val,
                    });
                }
                if let Put::Duplicate = self.tree_put(&mut t, key, val, op.kind == OpKind::Overwrite)? {
                    return Err(Error::DuplicateKey {
                        symbol,
                        time: op.time,
                        seq: op.seq,
                    });
                }
                changed = true;
            }
            self.maybe_spill()?;
        }
        if existing.is_none() || changed {
            self.store_symbol(symbol, e, &t)?;
        }
        Ok(())
    }

    // ---- commit and rollback ---------------------------------------------

    /// Undo this transaction's effect on the free state.
    pub fn rollback(mut self) -> FreeState {
        for &(p, taken) in self.pool_log.iter().rev() {
            if taken {
                self.free.pool.insert(p);
            } else {
                self.free.pool.remove(&p);
            }
        }
        self.free
    }

    pub fn commit(mut self, durability: Durability, file_len: &mut u64) -> Result<Committed> {
        let ps = self.ps();
        let mut ids: Vec<u32> = self.dirty.keys().copied().collect();
        ids.sort_unstable();
        for id in &ids {
            stamp_crc(self.dirty.get_mut(id).unwrap());
        }
        let mut staging = Vec::new();
        self.write_pages(&ids, &mut staging)?;
        // Keep every allocated page inside the file, including ones this
        // transaction allocated and then freed without writing.
        let need = self.page_count * ps as u64;
        if *file_len < need {
            let len = self.db.storage.len()?;
            if len < need {
                self.db.storage.set_len(need)?;
            }
            *file_len = need.max(len);
        }
        if durability != Durability::None {
            self.db.storage.sync()?;
        }
        let freed = std::mem::take(&mut self.freed);
        if !freed.is_empty() {
            self.free.pending.push_back((self.id, freed));
        }
        let umeta: Arc<[u8]> = match self.umeta.take() {
            Some(u) => u.into(),
            None => self.base.umeta.clone(),
        };
        let slot = (self.id % 2) as usize;
        let draft = Meta {
            txn: self.id,
            page_size: ps as u32,
            page_count: self.page_count,
            dir: self.dir,
            umeta,
            slot,
            free_len: 0,
            free_crc: 0,
            hcrc: 0,
        };
        // Encoding is O(free pages); past what the slot could ever hold,
        // skip it and let the next writer handle rebuild instead.
        let inline = self.free.free_count() <= (META_FREE_CAP / 8) as u64 * 8;
        let bytes = draft.encode(inline.then_some(&self.free));
        let meta = Arc::new(Meta::from_encoded(&bytes, slot).expect("freshly encoded meta decodes"));
        self.db.storage.write_at(&bytes, slot as u64 * META_SLOT_SIZE)?;
        if durability == Durability::Immediate {
            self.db.storage.sync()?;
        }
        let pages = std::mem::take(&mut self.dirty).into_iter().collect();
        Ok(Committed {
            meta,
            free: self.free,
            pages,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_groups_prefers_even_two_way_split() {
        assert_eq!(var_groups(&[10, 10, 10, 10], 25, 1), vec![(0, 2), (2, 4)]);
        // 50 | 99 | 50 cannot be split two ways into 100-byte leaves.
        assert_eq!(var_groups(&[50, 99, 50], 100, 1), vec![(0, 1), (1, 2), (2, 3)]);
    }
}

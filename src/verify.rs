//! Whole-file integrity check and reachability walk.

use crate::db::Inner;
use crate::error::{Error, Result};
use crate::layout::*;
use crate::meta::Meta;
use crate::read::Tree;

/// Result of [`Db::verify`](crate::Db::verify).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyReport {
    /// Transaction that was checked.
    pub txn_id: u64,
    /// Symbols in the directory.
    pub symbols: u64,
    /// Records across all symbols.
    pub records: u64,
    /// Leaf pages holding records.
    pub leaf_pages: u64,
    /// Index (branch) pages of the symbol trees.
    pub branch_pages: u64,
    /// Pages of the symbol directory.
    pub directory_pages: u64,
    /// Pages not reachable from the checked commit (free, or awaiting reuse).
    pub unreachable_pages: u64,
    /// Number of problems found.
    pub errors: u64,
    /// Descriptions of the first problems found.
    pub problems: Vec<String>,
}

impl VerifyReport {
    /// Whether the file passed.
    pub fn is_ok(&self) -> bool {
        self.errors == 0
    }
}

/// One bit per page.
pub(crate) struct Bits(Vec<u64>, usize);

impl Bits {
    pub fn new(n: usize) -> Bits {
        Bits(vec![0; n.div_ceil(64)], n)
    }
    pub fn len(&self) -> usize {
        self.1
    }
    #[inline]
    pub fn get(&self, i: usize) -> bool {
        i < self.1 && self.0[i / 64] & (1 << (i % 64)) != 0
    }
    #[inline]
    pub fn set(&mut self, i: usize) {
        self.0[i / 64] |= 1 << (i % 64);
    }
    pub fn count(&self) -> usize {
        self.0.iter().map(|w| w.count_ones() as usize).sum()
    }
}

/// Facts about a verified subtree.
struct Sub {
    count: u64,
    min: Key,
    max: Key,
}

struct Walker<'a> {
    db: &'a Inner,
    meta: &'a Meta,
    used: Bits,
    rep: VerifyReport,
}

impl Walker<'_> {
    fn problem(&mut self, page: Option<u32>, msg: impl Into<String>) {
        self.rep.errors += 1;
        if self.rep.problems.len() < 32 {
            let msg = msg.into();
            self.rep.problems.push(match page {
                Some(p) => format!("page {p}: {msg}"),
                None => msg,
            });
        }
    }

    fn claim(&mut self, page: u32) -> bool {
        if page < self.db.first_data || page as u64 >= self.meta.page_count {
            self.problem(Some(page), "reference outside the allocated file");
            return false;
        }
        if self.used.get(page as usize) {
            self.problem(Some(page), "page referenced twice");
            return false;
        }
        self.used.set(page as usize);
        true
    }

    /// Check the subtree at `r`; `lo..hi` bounds the keys it may hold.
    fn node(&mut self, t: &Tree, r: PageRef, lv: u16, lo: Key, hi: Option<Key>, rs: u32) -> Option<Sub> {
        if !self.claim(r.page) {
            return None;
        }
        let p = match self.db.load_node(t, r, lv, true, false) {
            Ok(p) => p,
            Err(e) => {
                self.problem(Some(r.page), e.to_string());
                return None;
            }
        };
        let b = &p.buf;
        let n = count(b);
        let in_bounds = |k: Key| k >= lo && hi.is_none_or(|h| k < h);
        if lv == 0 {
            if n == 0 {
                self.problem(Some(r.page), "empty leaf");
                return None;
            }
            let lay = Leaf::of(b);
            if t.flags & FLAG_DIR == 0 {
                self.rep.leaf_pages += 1;
                let want = Leaf::for_record_size(b.len(), rs);
                if lay != want {
                    self.problem(Some(r.page), "leaf layout differs from the symbol's record size");
                }
            } else {
                self.rep.directory_pages += 1;
            }
            for i in 0..n {
                let k = lay.key(b, i);
                if i > 0 && lay.key(b, i - 1) >= k {
                    self.problem(Some(r.page), format!("keys out of order at slot {i}"));
                }
                if !in_bounds(k) {
                    self.problem(
                        Some(r.page),
                        format!("key ({}, {}) outside its parent's range", k.time, k.seq),
                    );
                }
            }
            if t.flags & FLAG_DIR != 0 {
                for i in 0..n {
                    let (off, len) = lay.val(b, i);
                    let symbol = lay.key(b, i).time;
                    if len != DIR_ENTRY_SIZE || lay.key(b, i).seq != 0 {
                        self.problem(Some(r.page), "malformed directory entry");
                        continue;
                    }
                    let e = DirEntry::decode(&b[off..off + len]);
                    self.symbol(symbol, &e);
                }
            }
            return Some(Sub {
                count: n as u64,
                min: lay.key(b, 0),
                max: lay.key(b, n - 1),
            });
        }
        if t.flags & FLAG_DIR == 0 {
            self.rep.branch_pages += 1;
        } else {
            self.rep.directory_pages += 1;
        }
        let mut total = 0u64;
        let mut max = lo;
        for i in 0..n {
            let k = br_key(b, i);
            let next = if i + 1 < n { Some(br_key(b, i + 1)) } else { hi };
            if i > 0 && br_key(b, i - 1) >= k {
                self.problem(Some(r.page), format!("separator keys out of order at entry {i}"));
                return None;
            }
            if !in_bounds(k) {
                self.problem(Some(r.page), "separator outside its parent's range");
            }
            let sub = self.node(t, br_child(b, i), lv - 1, k, next, rs)?;
            if sub.min != k {
                self.problem(Some(r.page), format!("entry {i} key is not its subtree's first key"));
            }
            if sub.count != br_count(b, i) {
                self.problem(
                    Some(r.page),
                    format!("entry {i} counts {} records, subtree has {}", br_count(b, i), sub.count),
                );
            }
            total += sub.count;
            max = sub.max;
        }
        Some(Sub {
            count: total,
            min: br_key(b, 0),
            max,
        })
    }

    fn symbol(&mut self, symbol: u64, e: &DirEntry) {
        self.rep.symbols += 1;
        let at = format!("symbol {symbol}");
        if e.rs != 0 && e.rs as usize > max_record_size(self.db.ps) {
            self.problem(None, format!("{at}: bad record size {}", e.rs));
            return;
        }
        if e.root.is_null() {
            if e.height != 0 || e.count != 0 || e.leaves != 0 {
                self.problem(None, format!("{at}: empty tree with non-zero statistics"));
            }
            return;
        }
        if e.height == 0 || e.height > MAX_HEIGHT {
            self.problem(None, format!("{at}: bad tree height {}", e.height));
            return;
        }
        let t = Tree::symbol(symbol, e);
        let leaves_before = self.rep.leaf_pages;
        let Some(sub) = self.node(&t, e.root, e.height as u16 - 1, Key::MIN, None, e.rs) else {
            return;
        };
        self.rep.records += sub.count;
        if sub.count != e.count {
            self.problem(
                None,
                format!("{at}: directory says {} records, tree has {}", e.count, sub.count),
            );
        }
        if self.rep.leaf_pages - leaves_before != e.leaves {
            self.problem(
                None,
                format!(
                    "{at}: directory says {} leaves, tree has {}",
                    e.leaves,
                    self.rep.leaf_pages - leaves_before
                ),
            );
        }
        if sub.min != e.min || sub.max != e.max {
            self.problem(None, format!("{at}: cached min/max do not match the records"));
        }
    }
}

impl Inner {
    /// Full structural check of `meta`, which must be pinned by a snapshot.
    pub(crate) fn verify(&self, meta: &Meta) -> Result<VerifyReport> {
        let mut w = Walker {
            db: self,
            meta,
            used: Bits::new(meta.page_count as usize),
            rep: VerifyReport {
                txn_id: meta.txn,
                ..VerifyReport::default()
            },
        };
        let t = Tree::dir(meta);
        if !t.root.is_null() && (t.height == 0 || t.height > MAX_HEIGHT) {
            w.problem(None, "bad directory height");
        } else if !t.root.is_null()
            && let Some(sub) = w.node(&t, t.root, t.height as u16 - 1, Key::MIN, None, DIR_ENTRY_SIZE as u32)
            && sub.count != meta.dir.symbols
        {
            w.problem(
                None,
                format!("meta counts {} symbols, directory has {}", meta.dir.symbols, sub.count),
            );
        }
        let reachable = w.used.count() as u64;
        w.rep.unreachable_pages = meta.page_count.saturating_sub(self.first_data as u64 + reachable);
        // A persisted free state must not list a live page. (One that does
        // not decode is legitimate — a torn meta write — and is rebuilt.)
        if meta.free_len != crate::meta::NO_FREE {
            let mut area = vec![0u8; meta.free_len as usize];
            let off = meta.slot as u64 * META_SLOT_SIZE + META_FREE_OFF as u64;
            if self.storage.read_at(&mut area, off).is_ok()
                && let Some(f) = meta.decode_free(&area)
            {
                let listed = f
                    .pool
                    .iter()
                    .copied()
                    .chain(f.pending.iter().flat_map(|(_, v)| v.iter().copied()));
                let live: Vec<u32> = listed.filter(|&p| w.used.get(p as usize)).collect();
                for p in live.into_iter().take(8) {
                    w.problem(Some(p), "listed as free but reachable");
                }
            }
        }
        Ok(w.rep)
    }

    /// Every page reachable from `meta`. Reads the directory and every
    /// symbol's branch pages; leaves are known from their parents and are
    /// not read, which keeps a rebuild cheap.
    pub(crate) fn mark_reachable(&self, meta: &Meta) -> Result<Bits> {
        let mut used = Bits::new(meta.page_count as usize);
        for p in 0..(self.first_data as usize).min(used.len()) {
            used.set(p);
        }
        let dt = Tree::dir(meta);
        if dt.root.is_null() {
            return Ok(used);
        }
        if dt.height == 0 || dt.height > MAX_HEIGHT {
            return Err(Error::corrupt(None, "bad directory height"));
        }
        let mut entries = Vec::new();
        self.mark(&dt, dt.root, dt.height as u16 - 1, &mut used, Some(&mut entries))?;
        for (symbol, e) in entries {
            if e.root.is_null() {
                continue;
            }
            if e.height == 0 || e.height > MAX_HEIGHT {
                return Err(Error::corrupt(None, format!("symbol {symbol} has a bad tree height")));
            }
            let t = Tree::symbol(symbol, &e);
            self.mark(&t, e.root, e.height as u16 - 1, &mut used, None)?;
        }
        Ok(used)
    }

    fn mark(
        &self,
        t: &Tree,
        r: PageRef,
        lv: u16,
        used: &mut Bits,
        mut entries: Option<&mut Vec<(u64, DirEntry)>>,
    ) -> Result<()> {
        let p = r.page as usize;
        if r.page < self.first_data || p >= used.len() || used.get(p) {
            return Err(Error::corrupt(r.page, "page referenced twice or outside the file"));
        }
        used.set(p);
        if lv == 0 {
            if let Some(out) = entries {
                let pg = self.load_node(t, r, 0, true, false)?;
                let lay = Leaf::of(&pg.buf);
                for i in 0..count(&pg.buf) {
                    let (off, len) = lay.val(&pg.buf, i);
                    if len != DIR_ENTRY_SIZE {
                        return Err(Error::corrupt(r.page, "malformed directory entry"));
                    }
                    out.push((lay.key(&pg.buf, i).time, DirEntry::decode(&pg.buf[off..off + len])));
                }
            }
            return Ok(());
        }
        let pg = self.load_node(t, r, lv, true, false)?;
        for i in 0..count(&pg.buf) {
            self.mark(t, br_child(&pg.buf, i), lv - 1, used, entries.as_deref_mut())?;
        }
        Ok(())
    }
}

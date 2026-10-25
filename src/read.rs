//! Read path: verified page loads, tree descent, cursors and point lookups.
//!
//! Reads run against one committed meta. A *stable* view (a registered
//! snapshot) is guaranteed that none of its pages are recycled, so any
//! mismatch is corruption. An *optimistic* view is not registered: if the
//! writer recycles a page the view still needs, the load reports
//! [`Error::Stale`] and the caller retries against a newer meta.

use std::sync::Arc;

use crate::cache::Page;
use crate::db::Inner;
use crate::error::{Error, Result};
use crate::layout::*;
use crate::meta::{Meta, umeta_entries};
use crate::record::{Columns, Entry, Value};

/// Where a tree lives and what its pages must say about themselves.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Tree {
    pub root: PageRef,
    pub height: u8,
    pub owner: u64,
    pub flags: u8,
}

impl Tree {
    pub fn dir(meta: &Meta) -> Tree {
        Tree {
            root: meta.dir.root,
            height: meta.dir.height,
            owner: 0,
            flags: FLAG_DIR,
        }
    }

    pub fn symbol(symbol: u64, e: &DirEntry) -> Tree {
        Tree {
            root: e.root,
            height: e.height,
            owner: symbol,
            flags: 0,
        }
    }
}

/// A position on one record of a tree. Holds the root-to-leaf path, so
/// stepping across leaves needs no sibling pointers.
pub(crate) struct Cursor {
    pub tree: Tree,
    pub path: Vec<(Arc<Page>, usize)>,
    pub leaf: Arc<Page>,
    pub lay: Leaf,
    pub n: usize,
    pub idx: usize,
}

impl Cursor {
    #[inline]
    pub fn key(&self) -> Key {
        self.lay.key(&self.leaf.buf, self.idx)
    }

    #[inline]
    pub fn value(&self) -> Value {
        let (off, len) = self.lay.val(&self.leaf.buf, self.idx);
        Value::new(self.leaf.clone(), off, len)
    }
}

impl Inner {
    /// Load and verify the page `r` refers to.
    pub(crate) fn load(&self, r: PageRef, stable: bool, hot: bool) -> Result<Arc<Page>> {
        if let Some(p) = self.cache.get(r.page, r.ver) {
            return Ok(p);
        }
        self.cache.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stale = |detail: &str| {
            if stable {
                Error::corrupt(r.page, detail.to_string())
            } else {
                Error::Stale
            }
        };
        if r.page < self.first_data || r.page as u64 >= u32::MAX as u64 {
            return Err(Error::corrupt(r.page, "reference to a reserved page"));
        }
        let mut buf = vec![0u8; self.ps].into_boxed_slice();
        match self.storage.read_at(&mut buf, r.page as u64 * self.ps as u64) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(stale("page beyond end of file"));
            }
            Err(e) => return Err(e.into()),
        }
        if !crc_ok(&buf) {
            return Err(stale("page checksum mismatch"));
        }
        if page_ver(&buf) != r.ver {
            return Err(stale("page is not the version its parent refers to"));
        }
        validate(&buf, r.page)?;
        let p = Arc::new(Page { buf });
        self.cache.insert(r.page, p.clone(), hot);
        Ok(p)
    }

    /// Load a node of tree `t` at `level`, checking it belongs there.
    pub(crate) fn load_node(&self, t: &Tree, r: PageRef, lv: u16, stable: bool, hot: bool) -> Result<Arc<Page>> {
        let p = self.load(r, stable, hot)?;
        let b = &p.buf;
        let shape_ok = if lv == 0 {
            kind(b) != KIND_BRANCH
        } else {
            kind(b) == KIND_BRANCH
        };
        if !shape_ok || level(b) != lv || owner(b) != t.owner || flags(b) & FLAG_DIR != t.flags {
            return Err(Error::corrupt(r.page, "page does not belong where its parent points"));
        }
        Ok(p)
    }

    /// Descend to the leaf covering `key`, without keeping the path.
    pub(crate) fn find_leaf(&self, t: &Tree, key: Key, stable: bool) -> Result<Arc<Page>> {
        let mut r = t.root;
        for lv in (1..t.height as u16).rev() {
            let p = self.load_node(t, r, lv, stable, true)?;
            r = br_child(&p.buf, br_search(&p.buf, key));
        }
        self.load_node(t, r, 0, stable, true)
    }

    /// Descend to the leaf covering `key`, positioned at its lower bound.
    pub(crate) fn descend(&self, t: &Tree, key: Key, stable: bool, hot: bool) -> Result<Cursor> {
        let mut path = Vec::with_capacity(t.height as usize);
        let mut r = t.root;
        for lv in (1..t.height as u16).rev() {
            let p = self.load_node(t, r, lv, stable, true)?;
            let i = br_search(&p.buf, key);
            r = br_child(&p.buf, i);
            path.push((p, i));
        }
        let leaf = self.load_node(t, r, 0, stable, hot)?;
        let lay = Leaf::of(&leaf.buf);
        let n = count(&leaf.buf);
        let (idx, _) = lay.search(&leaf.buf, key);
        Ok(Cursor {
            tree: *t,
            path,
            leaf,
            lay,
            n,
            idx,
        })
    }

    fn enter_leaf(&self, c: &mut Cursor, mut r: PageRef, rightmost: bool, stable: bool, hot: bool) -> Result<()> {
        let branch_levels = c.tree.height as usize - 1;
        while c.path.len() < branch_levels {
            let lv = (branch_levels - c.path.len()) as u16;
            let p = self.load_node(&c.tree, r, lv, stable, true)?;
            let i = if rightmost { count(&p.buf) - 1 } else { 0 };
            r = br_child(&p.buf, i);
            c.path.push((p, i));
        }
        let leaf = self.load_node(&c.tree, r, 0, stable, hot)?;
        c.lay = Leaf::of(&leaf.buf);
        c.n = count(&leaf.buf);
        c.leaf = leaf;
        c.idx = if rightmost { c.n.saturating_sub(1) } else { 0 };
        Ok(())
    }

    /// Move to the first record of the next leaf; `false` past the end.
    pub(crate) fn next_leaf(&self, c: &mut Cursor, stable: bool, hot: bool) -> Result<bool> {
        loop {
            while let Some((p, i)) = c.path.last() {
                if i + 1 < count(&p.buf) {
                    break;
                }
                c.path.pop();
            }
            let Some((p, i)) = c.path.last_mut() else {
                return Ok(false);
            };
            *i += 1;
            let r = br_child(&p.buf, *i);
            self.enter_leaf(c, r, false, stable, hot)?;
            if c.n > 0 {
                return Ok(true);
            }
        }
    }

    /// Move to the last record of the previous leaf; `false` before the start.
    pub(crate) fn prev_leaf(&self, c: &mut Cursor, stable: bool, hot: bool) -> Result<bool> {
        loop {
            while let Some((_, i)) = c.path.last() {
                if *i > 0 {
                    break;
                }
                c.path.pop();
            }
            let Some((p, i)) = c.path.last_mut() else {
                return Ok(false);
            };
            *i -= 1;
            let r = br_child(&p.buf, *i);
            self.enter_leaf(c, r, true, stable, hot)?;
            if c.n > 0 {
                return Ok(true);
            }
        }
    }

    /// First record `>= key`.
    pub(crate) fn seek_ge(&self, t: &Tree, key: Key, stable: bool, hot: bool) -> Result<Option<Cursor>> {
        if t.root.is_null() {
            return Ok(None);
        }
        let mut c = self.descend(t, key, stable, hot)?;
        if c.idx >= c.n && !self.next_leaf(&mut c, stable, hot)? {
            return Ok(None);
        }
        Ok(Some(c))
    }

    /// Last record `<= key`.
    pub(crate) fn seek_le(&self, t: &Tree, key: Key, stable: bool, hot: bool) -> Result<Option<Cursor>> {
        if t.root.is_null() {
            return Ok(None);
        }
        let mut c = self.descend(t, key, stable, hot)?;
        if c.idx < c.n && c.key() == key {
            return Ok(Some(c));
        }
        if c.idx > 0 {
            c.idx -= 1;
            return Ok(Some(c));
        }
        Ok(if self.prev_leaf(&mut c, stable, hot)? {
            Some(c)
        } else {
            None
        })
    }

    /// Number of records strictly below `key`.
    pub(crate) fn rank(&self, t: &Tree, key: Key, stable: bool) -> Result<u64> {
        if t.root.is_null() {
            return Ok(0);
        }
        let mut acc = 0u64;
        let mut r = t.root;
        for lv in (1..t.height as u16).rev() {
            let p = self.load_node(t, r, lv, stable, true)?;
            let i = br_search(&p.buf, key);
            acc += (0..i).map(|j| br_count(&p.buf, j)).sum::<u64>();
            r = br_child(&p.buf, i);
        }
        let leaf = self.load_node(t, r, 0, stable, true)?;
        Ok(acc + Leaf::of(&leaf.buf).search(&leaf.buf, key).0 as u64)
    }

    /// The directory entry of `symbol` in `meta`.
    pub(crate) fn dir_entry(&self, meta: &Meta, symbol: u64, stable: bool) -> Result<Option<DirEntry>> {
        let t = Tree::dir(meta);
        if t.root.is_null() {
            return Ok(None);
        }
        let key = Key::new(symbol, 0);
        let leaf = self.find_leaf(&t, key, stable)?;
        let b = &leaf.buf;
        let lay = Leaf::of(b);
        let (idx, exact) = lay.search(b, key);
        if !exact {
            return Ok(None);
        }
        let (off, len) = lay.val(b, idx);
        if len != DIR_ENTRY_SIZE {
            return Err(Error::corrupt(page_id(b), "directory leaf has the wrong record size"));
        }
        let e = DirEntry::decode(&b[off..off + len]);
        let shape_ok = e.height <= MAX_HEIGHT
            && (e.root.is_null() == (e.height == 0))
            && (e.root.is_null() == (e.count == 0))
            && (e.rs == 0 || e.rs as usize <= max_record_size(self.ps));
        if !shape_ok {
            return Err(Error::corrupt(
                page_id(b),
                format!("directory entry of symbol {symbol} is malformed"),
            ));
        }
        Ok(Some(e))
    }
}

/// Read operations against one meta.
pub(crate) struct View<'a> {
    pub db: &'a Inner,
    pub meta: Arc<Meta>,
    pub stable: bool,
}

impl View<'_> {
    pub fn entry(&self, symbol: u64) -> Result<Option<DirEntry>> {
        self.db.dir_entry(&self.meta, symbol, self.stable)
    }

    pub fn get(&self, symbol: u64, key: Key) -> Result<Option<Value>> {
        let Some(e) = self.entry(symbol)? else {
            return Ok(None);
        };
        let t = Tree::symbol(symbol, &e);
        if t.root.is_null() {
            return Ok(None);
        }
        let leaf = self.db.find_leaf(&t, key, self.stable)?;
        let lay = Leaf::of(&leaf.buf);
        let (i, exact) = lay.search(&leaf.buf, key);
        Ok(exact.then(|| {
            let (off, len) = lay.val(&leaf.buf, i);
            Value::new(leaf.clone(), off, len)
        }))
    }

    /// The answer usually lies in the landing leaf; only a lookup that
    /// falls off its edge pays for a path to step to the neighbour.
    fn near(&self, symbol: u64, key: Key, before: bool) -> Result<Option<Entry>> {
        let Some(e) = self.entry(symbol)? else {
            return Ok(None);
        };
        let t = Tree::symbol(symbol, &e);
        if t.root.is_null() {
            return Ok(None);
        }
        let leaf = self.db.find_leaf(&t, key, self.stable)?;
        let lay = Leaf::of(&leaf.buf);
        let n = count(&leaf.buf);
        let (i, exact) = lay.search(&leaf.buf, key);
        let hit = match (before, exact) {
            (_, true) => Some(i),
            (true, false) => i.checked_sub(1),
            (false, false) => (i < n).then_some(i),
        };
        if let Some(i) = hit {
            let k = lay.key(&leaf.buf, i);
            let (off, len) = lay.val(&leaf.buf, i);
            return Ok(Some(Entry {
                symbol,
                time: k.time,
                seq: k.seq,
                value: Value::new(leaf.clone(), off, len),
            }));
        }
        let c = if before {
            self.db.seek_le(&t, key, self.stable, true)?
        } else {
            self.db.seek_ge(&t, key, self.stable, true)?
        };
        Ok(c.map(|c| Entry::from_cursor(symbol, &c)))
    }

    pub fn get_le(&self, symbol: u64, key: Key) -> Result<Option<Entry>> {
        self.near(symbol, key, true)
    }

    pub fn get_ge(&self, symbol: u64, key: Key) -> Result<Option<Entry>> {
        self.near(symbol, key, false)
    }

    pub fn count_range(&self, symbol: u64, lo: Key, hi: Key) -> Result<u64> {
        let Some(e) = self.entry(symbol)? else {
            return Ok(0);
        };
        if lo > hi || e.count == 0 {
            return Ok(0);
        }
        let t = Tree::symbol(symbol, &e);
        let below_hi = match hi.succ() {
            Some(k) => self.db.rank(&t, k, self.stable)?,
            None => e.count,
        };
        Ok(below_hi.saturating_sub(self.db.rank(&t, lo, self.stable)?))
    }

    pub fn symbols(&self) -> Result<Vec<u64>> {
        let t = Tree::dir(&self.meta);
        let mut out = Vec::with_capacity(self.meta.dir.symbols as usize);
        let Some(mut c) = self.db.seek_ge(&t, Key::MIN, self.stable, true)? else {
            return Ok(out);
        };
        loop {
            out.push(c.key().time);
            c.idx += 1;
            if c.idx >= c.n && !self.db.next_leaf(&mut c, self.stable, true)? {
                return Ok(out);
            }
        }
    }

    pub fn meta_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(umeta_entries(&self.meta.umeta)?
            .into_iter()
            .find(|(k, _)| *k == key.as_bytes())
            .map(|(_, v)| v.to_vec()))
    }

    pub fn meta_all(&self) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(umeta_entries(&self.meta.umeta)?
            .into_iter()
            .map(|(k, v)| (String::from_utf8_lossy(k).into_owned(), v.to_vec()))
            .collect())
    }

    pub fn read_columns(&self, symbol: u64, lo: Key, hi: Key, out: &mut Columns) -> Result<usize> {
        let Some(e) = self.entry(symbol)? else {
            return Ok(0);
        };
        if e.rs == 0 {
            return Err(Error::invalid(format!(
                "symbol {symbol} is variable-width; read_columns needs a fixed-width symbol"
            )));
        }
        out.bind(e.rs as usize)?;
        let mark = out.len();
        if lo > hi {
            return Ok(0);
        }
        let r = self.columns_into(symbol, &e, lo, hi, out);
        if r.is_err() {
            out.truncate(mark);
        }
        r.map(|()| out.len() - mark)
    }

    fn columns_into(&self, symbol: u64, e: &DirEntry, lo: Key, hi: Key, out: &mut Columns) -> Result<()> {
        let t = Tree::symbol(symbol, e);
        let Some(mut c) = self.db.seek_ge(&t, lo, self.stable, false)? else {
            return Ok(());
        };
        loop {
            let b = &c.leaf.buf;
            let Leaf::Fixed(f) = c.lay else {
                return Err(Error::corrupt(page_id(b), "variable leaf in a fixed-width symbol"));
            };
            // Records [idx, end) of this leaf are in range.
            let mut end = c.n;
            let last_in = Key::new(rd_u64(b, f.times + 8 * (c.n - 1)), rd_u32(b, f.seqs + 4 * (c.n - 1))) <= hi;
            if !last_in {
                end = c.lay.search(b, hi.succ().unwrap_or(Key::MAX)).0;
            }
            if end > c.idx {
                let (i, j) = (c.idx, end);
                out.times.extend((i..j).map(|k| rd_u64(b, f.times + 8 * k)));
                out.seqs.extend((i..j).map(|k| rd_u32(b, f.seqs + 4 * k)));
                out.payloads.extend_from_slice(&b[f.pay + f.rs * i..f.pay + f.rs * j]);
            }
            if !last_in || !self.db.next_leaf(&mut c, self.stable, false)? {
                return Ok(());
            }
        }
    }
}

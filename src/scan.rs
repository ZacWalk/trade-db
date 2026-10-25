//! Range scans and merged multi-symbol replay.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use crate::cache::Page;
use crate::db::Inner;
use crate::error::{Error, Result};
use crate::layout::{Key, Leaf, page_id};
use crate::meta::Meta;
use crate::read::{Cursor, Tree};
use crate::record::Entry;

/// How many consecutive stale snapshots an optimistic read tolerates
/// before reporting corruption.
pub(crate) const MAX_STALE: u32 = 64;

pub(crate) fn backoff(attempt: u32) {
    if attempt > 4 {
        std::thread::sleep(std::time::Duration::from_micros(50u64 << (attempt - 4).min(6)));
    } else {
        std::thread::yield_now();
    }
}

/// A run of consecutive records from one leaf page, in ascending key
/// order, returned by [`Scan::next_chunk`]. Values are borrowed from the
/// page with no copying.
pub struct Chunk {
    symbol: u64,
    page: Arc<Page>,
    lay: Leaf,
    start: usize,
    end: usize,
}

impl Chunk {
    /// Symbol the records belong to.
    pub fn symbol(&self) -> u64 {
        self.symbol
    }

    /// Number of records.
    pub fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether the chunk holds no records.
    pub fn is_empty(&self) -> bool {
        self.end == self.start
    }

    /// Time of record `i`.
    #[inline]
    pub fn time(&self, i: usize) -> u64 {
        assert!(i < self.len());
        self.lay.time(&self.page.buf, self.start + i)
    }

    /// Sequence number of record `i`.
    #[inline]
    pub fn seq(&self, i: usize) -> u32 {
        assert!(i < self.len());
        self.lay.key(&self.page.buf, self.start + i).seq
    }

    /// Value of record `i`.
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.len());
        let (off, len) = self.lay.val(&self.page.buf, self.start + i);
        &self.page.buf[off..off + len]
    }

    /// Iterate `(time, seq, value)`.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (u64, u32, &[u8])> + '_ {
        (0..self.len()).map(|i| {
            let k = self.lay.key(&self.page.buf, self.start + i);
            (k.time, k.seq, self.value(i))
        })
    }

    /// Record size of a fixed-width symbol, `None` when variable-width.
    pub fn record_size(&self) -> Option<usize> {
        match self.lay {
            Leaf::Fixed(f) => Some(f.rs),
            Leaf::Var => None,
        }
    }

    /// For a fixed-width symbol, every payload of the chunk as one
    /// contiguous slice (`len() * record_size` bytes) — straight from the
    /// page's payload column.
    pub fn payloads(&self) -> Option<&[u8]> {
        match self.lay {
            Leaf::Fixed(f) => Some(&self.page.buf[f.pay + f.rs * self.start..f.pay + f.rs * self.end]),
            Leaf::Var => None,
        }
    }
}

impl std::fmt::Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chunk")
            .field("symbol", &self.symbol)
            .field("page", &page_id(&self.page.buf))
            .field("len", &self.len())
            .finish()
    }
}

enum State {
    Start,
    At(Cursor, bool),
    Done,
}

/// Iterator over one symbol's records in a time range, oldest first or
/// newest first. Created by [`Db::scan`](crate::Db::scan),
/// [`Db::scan_rev`](crate::Db::scan_rev) and the [`Snapshot`](crate::Snapshot)
/// equivalents.
///
/// A scan from a `Snapshot` sees exactly that snapshot. A scan straight
/// from the `Db` starts at the latest commit and never blocks or delays a
/// writer; if a concurrent commit recycles a page it still needs, it
/// continues from the newest commit just past the last record it returned.
/// It never repeats a record or moves backwards.
pub struct Scan<'a> {
    db: &'a Inner,
    meta: Arc<Meta>,
    stable: bool,
    symbol: u64,
    lo: Key,
    hi: Key,
    start: Key,
    reverse: bool,
    state: State,
    last: Option<Key>,
    stale: u32,
}

impl<'a> Scan<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        db: &'a Inner,
        meta: Arc<Meta>,
        stable: bool,
        symbol: u64,
        bounds: Option<(u64, u64)>,
        reverse: bool,
    ) -> Scan<'a> {
        let (lo, hi, state) = match bounds {
            Some((lo, hi)) => (Key::new(lo, 0), Key::new(hi, u32::MAX), State::Start),
            None => (Key::MAX, Key::MIN, State::Done),
        };
        let start = if reverse { hi } else { lo };
        Scan {
            db,
            meta,
            stable,
            symbol,
            lo,
            hi,
            start,
            reverse,
            state,
            last: None,
            stale: 0,
        }
    }

    /// Reposition at `(time, seq)` and continue in the scan's direction,
    /// within its original range.
    pub fn seek(&mut self, time: u64, seq: u32) {
        let k = Key::new(time, seq);
        self.start = if self.reverse { k.min(self.hi) } else { k.max(self.lo) };
        self.last = None;
        self.state = if self.lo <= self.hi { State::Start } else { State::Done };
    }

    fn position(&mut self) -> Result<()> {
        let start = match (self.last, self.reverse) {
            (None, _) => Some(self.start),
            (Some(k), false) => k.succ(),
            (Some(k), true) => k.pred(),
        };
        let entry = self.db.dir_entry(&self.meta, self.symbol, self.stable)?;
        let c = match (entry, start) {
            (Some(e), Some(s)) => {
                let t = Tree::symbol(self.symbol, &e);
                if self.reverse {
                    self.db.seek_le(&t, s, self.stable, false)?
                } else {
                    self.db.seek_ge(&t, s, self.stable, false)?
                }
            }
            _ => None,
        };
        self.state = match c {
            Some(c) => State::At(c, false),
            None => State::Done,
        };
        Ok(())
    }

    /// Bring the cursor onto the next in-range record, or finish.
    fn settle(&mut self) -> Result<Option<Cursor>> {
        loop {
            match std::mem::replace(&mut self.state, State::Done) {
                State::Done => return Ok(None),
                State::Start => self.position()?,
                State::At(mut c, advance) => {
                    if advance {
                        // On failure the state stays Done; the caller
                        // either retries from `last` or ends the scan.
                        let more = if self.reverse {
                            c.idx > 0 && {
                                c.idx -= 1;
                                true
                            } || self.db.prev_leaf(&mut c, self.stable, false)?
                        } else {
                            c.idx += 1;
                            c.idx < c.n || self.db.next_leaf(&mut c, self.stable, false)?
                        };
                        if !more {
                            return Ok(None);
                        }
                    }
                    let k = c.key();
                    if (self.reverse && k < self.lo) || (!self.reverse && k > self.hi) {
                        return Ok(None);
                    }
                    return Ok(Some(c));
                }
            }
        }
    }

    fn retry(&mut self, e: Error) -> Option<Error> {
        if matches!(e, Error::Stale) && !self.stable && self.stale < MAX_STALE {
            self.stale += 1;
            backoff(self.stale);
            match self.db.current_meta() {
                Ok(m) => {
                    self.meta = m;
                    self.state = State::Start;
                    None
                }
                Err(e) => Some(e),
            }
        } else if matches!(e, Error::Stale) {
            Some(Error::corrupt(None, "scan kept losing its snapshot to page reuse"))
        } else {
            Some(e)
        }
    }

    /// The rest of the current leaf's in-range records as one chunk, then
    /// the next leaf's, and so on. Records inside a chunk are always in
    /// ascending order; a reverse scan yields chunks newest first.
    /// Interleaves freely with [`Iterator::next`].
    pub fn next_chunk(&mut self) -> Option<Result<Chunk>> {
        loop {
            match self.settle() {
                Ok(None) => return None,
                Ok(Some(mut c)) => {
                    let b = &c.leaf.buf;
                    let (start, end) = if self.reverse {
                        let first = c.lay.search(b, self.lo).0;
                        (first, c.idx + 1)
                    } else {
                        let end = self.hi.succ().map_or(c.n, |h| c.lay.search(b, h).0);
                        (c.idx, end)
                    };
                    let chunk = Chunk {
                        symbol: self.symbol,
                        page: c.leaf.clone(),
                        lay: c.lay,
                        start,
                        end,
                    };
                    self.stale = 0;
                    if self.reverse {
                        self.last = Some(c.lay.key(b, start));
                        let more = start == 0;
                        c.idx = 0;
                        self.state = if more { State::At(c, true) } else { State::Done };
                    } else {
                        self.last = Some(c.lay.key(b, end - 1));
                        let more = end == c.n;
                        c.idx = c.n - 1;
                        self.state = if more { State::At(c, true) } else { State::Done };
                    }
                    return Some(Ok(chunk));
                }
                Err(e) => {
                    if let Some(e) = self.retry(e) {
                        self.state = State::Done;
                        return Some(Err(e));
                    }
                }
            }
        }
    }
}

impl Iterator for Scan<'_> {
    type Item = Result<Entry>;

    #[inline]
    fn next(&mut self) -> Option<Result<Entry>> {
        // Fast path: the next record is in the leaf already held.
        if let State::At(c, true) = &mut self.state {
            let step = if self.reverse {
                c.idx > 0 && {
                    c.idx -= 1;
                    true
                }
            } else {
                c.idx + 1 < c.n && {
                    c.idx += 1;
                    true
                }
            };
            if step {
                let k = c.key();
                if (self.reverse && k < self.lo) || (!self.reverse && k > self.hi) {
                    self.state = State::Done;
                    return None;
                }
                self.last = Some(k);
                return Some(Ok(Entry {
                    symbol: self.symbol,
                    time: k.time,
                    seq: k.seq,
                    value: c.value(),
                }));
            }
        }
        self.next_slow()
    }
}

impl Scan<'_> {
    fn next_slow(&mut self) -> Option<Result<Entry>> {
        loop {
            match self.settle() {
                Ok(None) => return None,
                Ok(Some(c)) => {
                    let k = c.key();
                    let e = Entry {
                        symbol: self.symbol,
                        time: k.time,
                        seq: k.seq,
                        value: c.value(),
                    };
                    self.last = Some(k);
                    self.stale = 0;
                    self.state = State::At(c, true);
                    return Some(Ok(e));
                }
                Err(e) => {
                    if let Some(e) = self.retry(e) {
                        self.state = State::Done;
                        return Some(Err(e));
                    }
                }
            }
        }
    }
}

impl std::iter::FusedIterator for Scan<'_> {}

/// Several symbols replayed as one stream in `(time, seq, symbol)` order —
/// what a backtest or a live event loop consumes. Created by
/// [`Db::scan_multi`](crate::Db::scan_multi).
///
/// Memory is one cursor per symbol. The merged stream never moves backwards
/// in time; a scan from the `Db` (not a snapshot) that loses its snapshot
/// to page reuse resumes every symbol from the last key it emitted.
pub struct MultiScan<'a> {
    db: &'a Inner,
    meta: Arc<Meta>,
    stable: bool,
    symbols: Vec<u64>,
    lo: Key,
    hi: Key,
    cursors: Vec<Option<Cursor>>,
    heap: BinaryHeap<Reverse<(Key, u64, usize)>>,
    started: bool,
    done: bool,
    advance: Option<usize>,
    last: Option<(Key, u64, usize)>,
    stale: u32,
}

impl<'a> MultiScan<'a> {
    pub(crate) fn new(
        db: &'a Inner,
        meta: Arc<Meta>,
        stable: bool,
        symbols: &[u64],
        bounds: Option<(u64, u64)>,
    ) -> MultiScan<'a> {
        let (lo, hi) = bounds.map_or((Key::MAX, Key::MIN), |(lo, hi)| {
            (Key::new(lo, 0), Key::new(hi, u32::MAX))
        });
        MultiScan {
            db,
            meta,
            stable,
            symbols: symbols.to_vec(),
            lo,
            hi,
            cursors: Vec::new(),
            heap: BinaryHeap::with_capacity(symbols.len()),
            started: false,
            done: bounds.is_none() || symbols.is_empty(),
            advance: None,
            last: None,
            stale: 0,
        }
    }

    fn push(&mut self, i: usize) {
        if let Some(c) = &self.cursors[i] {
            let k = c.key();
            if k <= self.hi {
                self.heap.push(Reverse((k, self.symbols[i], i)));
            }
        }
    }

    /// Position every cursor just after the last emitted record.
    fn position(&mut self) -> Result<()> {
        self.heap.clear();
        self.cursors.clear();
        self.advance = None;
        for i in 0..self.symbols.len() {
            let symbol = self.symbols[i];
            let start = match self.last {
                None => Some(self.lo),
                Some((k, ls, li)) if (symbol, i) > (ls, li) => Some(k),
                Some((k, _, _)) => k.succ(),
            };
            let c = match (start, self.db.dir_entry(&self.meta, symbol, self.stable)?) {
                (Some(s), Some(e)) => self.db.seek_ge(&Tree::symbol(symbol, &e), s, self.stable, false)?,
                _ => None,
            };
            self.cursors.push(c);
            self.push(i);
        }
        Ok(())
    }

    fn step(&mut self) -> Result<Option<Entry>> {
        if !self.started {
            self.position()?;
            self.started = true;
        }
        if let Some(i) = self.advance.take() {
            let mut c = self.cursors[i].take().expect("advanced cursor exists");
            c.idx += 1;
            if c.idx < c.n || self.db.next_leaf(&mut c, self.stable, false)? {
                self.cursors[i] = Some(c);
                self.push(i);
            }
        }
        let Some(Reverse((k, symbol, i))) = self.heap.pop() else {
            return Ok(None);
        };
        let c = self.cursors[i].as_ref().expect("queued cursor exists");
        let e = Entry {
            symbol,
            time: k.time,
            seq: k.seq,
            value: c.value(),
        };
        self.last = Some((k, symbol, i));
        self.advance = Some(i);
        Ok(Some(e))
    }
}

impl Iterator for MultiScan<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Result<Entry>> {
        while !self.done {
            match self.step() {
                Ok(Some(e)) => {
                    self.stale = 0;
                    return Some(Ok(e));
                }
                Ok(None) => self.done = true,
                Err(Error::Stale) if !self.stable && self.stale < MAX_STALE => {
                    self.stale += 1;
                    backoff(self.stale);
                    match self.db.current_meta() {
                        Ok(m) => {
                            self.meta = m;
                            self.started = false;
                        }
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e));
                        }
                    }
                }
                Err(e) => {
                    self.done = true;
                    let e = if matches!(e, Error::Stale) {
                        Error::corrupt(None, "merged scan kept losing its snapshot to page reuse")
                    } else {
                        e
                    };
                    return Some(Err(e));
                }
            }
        }
        None
    }
}

impl std::iter::FusedIterator for MultiScan<'_> {}

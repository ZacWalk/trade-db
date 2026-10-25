//! The public `Db` methods: reads, scans, writes and operations.

use std::ops::RangeBounds;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::batch::{Batch, time_bounds};
use crate::db::{Db, Durability, Stats};
use crate::error::{Error, Result};
use crate::layout::*;
use crate::record::{Columns, Entry, SymbolInfo, Value};
use crate::scan::{MultiScan, Scan};
use crate::snapshot::Snapshot;
use crate::verify::VerifyReport;

pub(crate) fn bounds(range: &impl RangeBounds<u64>) -> Result<Option<(u64, u64)>> {
    time_bounds(range).map_err(|()| Error::invalid("time range starts after it ends"))
}

pub(crate) fn info(e: &DirEntry) -> SymbolInfo {
    let empty = e.count == 0;
    SymbolInfo {
        min_time: if empty { 0 } else { e.min.time },
        max_time: if empty { 0 } else { e.max.time },
        count: e.count,
        leaf_count: e.leaves,
        record_size: e.rs,
    }
}

impl Db {
    // ---- point reads ----------------------------------------------------

    /// The value at `(symbol, time, seq = 0)`.
    pub fn get(&self, symbol: u64, time: u64) -> Result<Option<Value>> {
        self.get_seq(symbol, time, 0)
    }

    /// The value at `(symbol, time, seq)`.
    pub fn get_seq(&self, symbol: u64, time: u64, seq: u32) -> Result<Option<Value>> {
        self.view(|v| v.get(symbol, Key::new(time, seq)))
    }

    /// As-of lookup: the latest record at or before `time` (the highest seq
    /// at that time). The point-in-time primitive — last trade as of T,
    /// prevailing quote as of T — that keeps lookahead out of a backtest.
    pub fn get_le(&self, symbol: u64, time: u64) -> Result<Option<Entry>> {
        self.view(|v| v.get_le(symbol, Key::new(time, u32::MAX)))
    }

    /// The earliest record at or after `time` (the lowest seq at that time).
    pub fn get_ge(&self, symbol: u64, time: u64) -> Result<Option<Entry>> {
        self.view(|v| v.get_ge(symbol, Key::new(time, 0)))
    }

    /// The symbol's oldest record.
    pub fn first(&self, symbol: u64) -> Result<Option<Entry>> {
        self.get_ge(symbol, 0)
    }

    /// The symbol's newest record.
    pub fn last(&self, symbol: u64) -> Result<Option<Entry>> {
        self.get_le(symbol, u64::MAX)
    }

    // ---- cached statistics ------------------------------------------------

    /// Statistics for `symbol`, from its directory entry alone. `None` when
    /// the symbol has never been written or defined.
    pub fn symbol_info(&self, symbol: u64) -> Result<Option<SymbolInfo>> {
        self.view(|v| Ok(v.entry(symbol)?.map(|e| info(&e))))
    }

    /// Time of the symbol's newest record — the hot "anything new?" poll.
    /// `None` when the symbol is absent or empty.
    pub fn max_time(&self, symbol: u64) -> Result<Option<u64>> {
        Ok(self.symbol_info(symbol)?.filter(|i| i.count > 0).map(|i| i.max_time))
    }

    /// Time of the symbol's oldest record; `None` when absent or empty.
    pub fn min_time(&self, symbol: u64) -> Result<Option<u64>> {
        Ok(self.symbol_info(symbol)?.filter(|i| i.count > 0).map(|i| i.min_time))
    }

    /// Number of records of `symbol` (0 when absent).
    pub fn count(&self, symbol: u64) -> Result<u64> {
        Ok(self.symbol_info(symbol)?.map_or(0, |i| i.count))
    }

    /// Number of records of `symbol` whose time is in `range`, in
    /// `O(log n)` — counts are kept in the index, nothing is scanned.
    pub fn count_range(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<u64> {
        let Some((lo, hi)) = bounds(&range)? else {
            return Ok(0);
        };
        self.view(|v| v.count_range(symbol, Key::new(lo, 0), Key::new(hi, u32::MAX)))
    }

    /// [`max_time`](Db::max_time) for a whole watchlist against one commit.
    pub fn max_times(&self, symbols: &[u64]) -> Result<Vec<Option<u64>>> {
        self.view(|v| {
            symbols
                .iter()
                .map(|&s| Ok(v.entry(s)?.filter(|e| e.count > 0).map(|e| e.max.time)))
                .collect()
        })
    }

    /// Every symbol in the database, ascending.
    pub fn symbols(&self) -> Result<Vec<u64>> {
        self.view(|v| v.symbols())
    }

    /// The newest committed transaction id. Cheap enough to poll: a poller
    /// that remembers it can skip all other work while nothing commits.
    pub fn txn_id(&self) -> Result<u64> {
        Ok(self.inner.current_meta()?.txn)
    }

    /// A metadata value written with [`Batch::set_meta`].
    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.view(|v| v.meta_get(key))
    }

    /// Every metadata key and value.
    pub fn meta_entries(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.view(|v| v.meta_all())
    }

    // ---- bulk reads -------------------------------------------------------

    /// Iterate `symbol`'s records with time in `range`, oldest first. The
    /// range is inclusive of every seq at its end times; a reversed range
    /// (`10..=5`) is an error.
    pub fn scan(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<Scan<'_>> {
        let b = bounds(&range)?;
        Ok(Scan::new(
            &self.inner,
            self.inner.current_meta()?,
            false,
            symbol,
            b,
            false,
        ))
    }

    /// Like [`scan`](Db::scan), newest first — "the last N bars".
    pub fn scan_rev(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<Scan<'_>> {
        let b = bounds(&range)?;
        Ok(Scan::new(
            &self.inner,
            self.inner.current_meta()?,
            false,
            symbol,
            b,
            true,
        ))
    }

    /// Replay several symbols as one stream ordered by `(time, seq, symbol)`.
    /// Duplicate symbols are not filtered.
    pub fn scan_multi(&self, symbols: &[u64], range: impl RangeBounds<u64>) -> Result<MultiScan<'_>> {
        let b = bounds(&range)?;
        Ok(MultiScan::new(
            &self.inner,
            self.inner.current_meta()?,
            false,
            symbols,
            b,
        ))
    }

    /// Append a fixed-width symbol's records in `range` to `out` as columns
    /// (times, seqs, packed payloads) — one copy per leaf rather than per
    /// record. Returns the number of records appended.
    pub fn read_columns(&self, symbol: u64, range: impl RangeBounds<u64>, out: &mut Columns) -> Result<usize> {
        let Some((lo, hi)) = bounds(&range)? else {
            return Ok(0);
        };
        self.view(|v| v.read_columns(symbol, Key::new(lo, 0), Key::new(hi, u32::MAX), out))
    }

    /// A stable, registered view of the newest commit. Everything read
    /// through it is consistent, however long it is held and whatever
    /// commits meanwhile. Holding one does not block writers, but pages it
    /// can see are not recycled until it is dropped, so the file grows while
    /// an old snapshot stays open under heavy writes.
    pub fn snapshot(&self) -> Result<Snapshot<'_>> {
        Ok(Snapshot::new(self, self.inner.acquire()?))
    }

    // ---- writes -------------------------------------------------------------

    /// Apply `batch` atomically with the handle's default durability.
    /// Returns the id of the transaction that holds it (unchanged when the
    /// batch changed nothing).
    pub fn write(&self, batch: &Batch) -> Result<u64> {
        self.write_with(batch, self.inner.durability)
    }

    /// Apply `batch` atomically with an explicit durability.
    pub fn write_with(&self, batch: &Batch, durability: Durability) -> Result<u64> {
        if batch.bad_range {
            return Err(Error::invalid("a delete_range in the batch starts after it ends"));
        }
        if batch.is_empty() {
            return self.txn_id();
        }
        Ok(self.transact(durability, |t| t.apply(batch))?.1)
    }

    /// Declare `symbol` fixed-width: every value is exactly `record_size`
    /// bytes, stored in columnar leaves (time, seq and payload columns).
    /// That stores more records per page and makes
    /// [`read_columns`](Db::read_columns) and [`Chunk::payloads`](crate::Chunk::payloads)
    /// one copy per leaf. The symbol must be empty; repeating the same
    /// declaration is harmless.
    pub fn define_symbol(&self, symbol: u64, record_size: u32) -> Result<()> {
        self.transact(self.inner.durability, |t| t.define(symbol, record_size))
            .map(|_| ())
    }

    /// Delete `symbol`'s records with time in `range`; returns how many.
    /// Whole subtrees inside the range are released without being read.
    pub fn delete_range(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<u64> {
        let Some((lo, hi)) = bounds(&range)? else {
            return Ok(0);
        };
        self.transact(self.inner.durability, |t| {
            t.delete_range(symbol, Key::new(lo, 0), Key::new(hi, u32::MAX))
        })
        .map(|r| r.0)
    }

    /// Retention: delete `symbol`'s records strictly before `time`.
    pub fn truncate_before(&self, symbol: u64, time: u64) -> Result<u64> {
        self.delete_range(symbol, ..time)
    }

    /// Remove `symbol` entirely. Returns whether it existed.
    pub fn drop_symbol(&self, symbol: u64) -> Result<bool> {
        self.transact(self.inner.durability, |t| t.drop_symbol(symbol))
            .map(|r| r.0)
    }

    /// Flush every commit to stable storage (completing
    /// [`Durability::Eventual`] and [`Durability::None`] commits).
    pub fn sync(&self) -> Result<()> {
        if self.inner.read_only {
            return Ok(());
        }
        let mut ws = self.inner.writer.lock().unwrap();
        self.inner.storage.sync()?;
        ws.durable = ws.durable.max(ws.last_commit);
        ws.unsynced = false;
        Ok(())
    }

    // ---- operations ---------------------------------------------------------

    /// Block until the transaction id differs from `last_seen` or `timeout`
    /// passes; returns the new id, or `None` on timeout (`Duration::MAX`
    /// waits indefinitely). Commits from this process wake it immediately;
    /// commits from other processes are polled with a backoff of up to 10 ms.
    pub fn wait_for_txn(&self, last_seen: u64, timeout: Duration) -> Result<Option<u64>> {
        let deadline = Instant::now().checked_add(timeout);
        let mut nap = Duration::from_micros(100);
        let (m, cv) = &self.inner.commits;
        loop {
            // Sample this process's commit counter before looking, so a
            // commit landing in between is not slept through.
            let local = *m.lock().unwrap();
            let t = self.txn_id()?;
            if t != last_seen {
                return Ok(Some(t));
            }
            let now = Instant::now();
            let mut wait = match deadline {
                Some(d) if now >= d => return Ok(None),
                Some(d) => d - now,
                None => Duration::from_secs(3600),
            };
            if self.inner.multi {
                wait = wait.min(nap);
                nap = (nap * 2).min(Duration::from_millis(10));
            }
            let g = m.lock().unwrap();
            if *g == local {
                drop(cv.wait_timeout(g, wait).unwrap());
            }
        }
    }

    /// Load `symbol`'s leaves in `range` into the cache, so a strategy's
    /// first scan is not I/O bound. Stops once it has loaded as many pages
    /// as the cache holds; returns the number loaded.
    pub fn prefetch(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<usize> {
        let mut scan = self.scan(symbol, range)?;
        let cap = self.inner.cache.capacity();
        let mut pages = 0;
        while pages < cap {
            match scan.next_chunk() {
                Some(Ok(_)) => pages += 1,
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        Ok(pages)
    }

    /// Write a consistent copy of the newest commit to `dest` (replacing
    /// any file there) while readers and writers carry on. The copy is a
    /// complete database; its free-page state is rebuilt on its first write.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        let dest = dest.as_ref();
        if let (Some(Ok(src)), Ok(d)) = (self.path().map(std::fs::canonicalize), std::fs::canonicalize(dest))
            && (src == d || crate::snapshot::same_file(&src, &d))
        {
            return Err(Error::invalid("backup destination is the database itself"));
        }
        let snap = self.snapshot()?;
        crate::snapshot::write_backup(&self.inner, snap.meta(), dest)
    }

    /// Check the whole file: every tree's ordering, counts, cached
    /// statistics, page checksums and versions, and that no page is used
    /// twice. Runs against a snapshot, so writers carry on.
    pub fn verify(&self) -> Result<VerifyReport> {
        let snap = self.snapshot()?;
        self.inner.verify(snap.meta())
    }

    /// Counters for this handle.
    pub fn stats(&self) -> Result<Stats> {
        let meta = self.inner.current_meta()?;
        let free_pages = if self.inner.read_only {
            None
        } else {
            // Never wait behind a running write just to report a counter.
            match self.inner.writer.try_lock() {
                Ok(ws) => (ws.free_txn == meta.txn)
                    .then(|| ws.free.as_ref().map(|f| f.free_count()))
                    .flatten(),
                Err(_) => None,
            }
        };
        Ok(Stats {
            page_size: self.inner.ps as u32,
            page_count: meta.page_count,
            free_pages,
            txn_id: meta.txn,
            symbol_count: meta.dir.symbols,
            cache_capacity: self.inner.cache.capacity(),
            cache_used: self.inner.cache.used(),
            cache_hits: self.inner.cache.hits.load(std::sync::atomic::Ordering::Relaxed),
            cache_misses: self.inner.cache.misses.load(std::sync::atomic::Ordering::Relaxed),
            spills: self.inner.spills.load(std::sync::atomic::Ordering::Relaxed),
            open_snapshots: self.inner.readers.lock().unwrap().values().sum(),
        })
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.inner.read_only {
            // Never panic in drop, even if a bug poisoned the writer lock.
            let ws = self.inner.writer.lock().unwrap_or_else(|e| e.into_inner());
            if ws.unsynced {
                let _ = self.inner.storage.sync();
            }
        }
    }
}

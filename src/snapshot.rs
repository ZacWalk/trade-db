//! Registered, stable snapshots, and backup.

use std::io::{Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use crate::api::{bounds, info};
use crate::db::{Db, Inner};
use crate::error::Result;
use crate::layout::*;
use crate::meta::Meta;
use crate::read::View;
use crate::record::{Columns, Entry, SymbolInfo, Value};
use crate::scan::{MultiScan, Scan};

/// A stable view of one commit, from [`Db::snapshot`].
///
/// Everything read through a snapshot reflects exactly that commit, no
/// matter how long it is held or what commits meanwhile, in this process or
/// another. It never blocks a writer; instead, pages it can still see are
/// not recycled until it is dropped.
pub struct Snapshot<'db> {
    db: &'db Db,
    meta: Arc<Meta>,
}

impl<'db> Snapshot<'db> {
    pub(crate) fn new(db: &'db Db, meta: Arc<Meta>) -> Snapshot<'db> {
        Snapshot { db, meta }
    }

    pub(crate) fn meta(&self) -> &Meta {
        &self.meta
    }

    fn view(&self) -> View<'_> {
        View {
            db: &self.db.inner,
            meta: self.meta.clone(),
            stable: true,
        }
    }

    /// The transaction this snapshot shows.
    pub fn txn_id(&self) -> u64 {
        self.meta.txn
    }

    /// See [`Db::get`].
    pub fn get(&self, symbol: u64, time: u64) -> Result<Option<Value>> {
        self.get_seq(symbol, time, 0)
    }

    /// See [`Db::get_seq`].
    pub fn get_seq(&self, symbol: u64, time: u64, seq: u32) -> Result<Option<Value>> {
        self.view().get(symbol, Key::new(time, seq))
    }

    /// See [`Db::get_le`].
    pub fn get_le(&self, symbol: u64, time: u64) -> Result<Option<Entry>> {
        self.view().get_le(symbol, Key::new(time, u32::MAX))
    }

    /// See [`Db::get_ge`].
    pub fn get_ge(&self, symbol: u64, time: u64) -> Result<Option<Entry>> {
        self.view().get_ge(symbol, Key::new(time, 0))
    }

    /// See [`Db::first`].
    pub fn first(&self, symbol: u64) -> Result<Option<Entry>> {
        self.get_ge(symbol, 0)
    }

    /// See [`Db::last`].
    pub fn last(&self, symbol: u64) -> Result<Option<Entry>> {
        self.get_le(symbol, u64::MAX)
    }

    /// See [`Db::symbol_info`].
    pub fn symbol_info(&self, symbol: u64) -> Result<Option<SymbolInfo>> {
        Ok(self.view().entry(symbol)?.map(|e| info(&e)))
    }

    /// See [`Db::max_time`].
    pub fn max_time(&self, symbol: u64) -> Result<Option<u64>> {
        Ok(self.symbol_info(symbol)?.filter(|i| i.count > 0).map(|i| i.max_time))
    }

    /// See [`Db::min_time`].
    pub fn min_time(&self, symbol: u64) -> Result<Option<u64>> {
        Ok(self.symbol_info(symbol)?.filter(|i| i.count > 0).map(|i| i.min_time))
    }

    /// See [`Db::count`].
    pub fn count(&self, symbol: u64) -> Result<u64> {
        Ok(self.symbol_info(symbol)?.map_or(0, |i| i.count))
    }

    /// See [`Db::count_range`].
    pub fn count_range(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<u64> {
        let Some((lo, hi)) = bounds(&range)? else {
            return Ok(0);
        };
        self.view().count_range(symbol, Key::new(lo, 0), Key::new(hi, u32::MAX))
    }

    /// See [`Db::max_times`].
    pub fn max_times(&self, symbols: &[u64]) -> Result<Vec<Option<u64>>> {
        let v = self.view();
        symbols
            .iter()
            .map(|&s| Ok(v.entry(s)?.filter(|e| e.count > 0).map(|e| e.max.time)))
            .collect()
    }

    /// See [`Db::symbols`].
    pub fn symbols(&self) -> Result<Vec<u64>> {
        self.view().symbols()
    }

    /// See [`Db::get_meta`].
    pub fn get_meta(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.view().meta_get(key)
    }

    /// See [`Db::meta_entries`].
    pub fn meta_entries(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.view().meta_all()
    }

    /// See [`Db::scan`]; the scan sees exactly this snapshot.
    pub fn scan(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<Scan<'_>> {
        let b = bounds(&range)?;
        Ok(Scan::new(&self.db.inner, self.meta.clone(), true, symbol, b, false))
    }

    /// See [`Db::scan_rev`]; the scan sees exactly this snapshot.
    pub fn scan_rev(&self, symbol: u64, range: impl RangeBounds<u64>) -> Result<Scan<'_>> {
        let b = bounds(&range)?;
        Ok(Scan::new(&self.db.inner, self.meta.clone(), true, symbol, b, true))
    }

    /// See [`Db::scan_multi`]; the scan sees exactly this snapshot.
    pub fn scan_multi(&self, symbols: &[u64], range: impl RangeBounds<u64>) -> Result<MultiScan<'_>> {
        let b = bounds(&range)?;
        Ok(MultiScan::new(&self.db.inner, self.meta.clone(), true, symbols, b))
    }

    /// See [`Db::read_columns`].
    pub fn read_columns(&self, symbol: u64, range: impl RangeBounds<u64>, out: &mut Columns) -> Result<usize> {
        let Some((lo, hi)) = bounds(&range)? else {
            return Ok(0);
        };
        self.view()
            .read_columns(symbol, Key::new(lo, 0), Key::new(hi, u32::MAX), out)
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        self.db.inner.unregister(self.meta.txn);
    }
}

impl std::fmt::Debug for Snapshot<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").field("txn_id", &self.meta.txn).finish()
    }
}

/// Whether two existing paths name the same file (hard links included,
/// where the platform lets std tell).
pub(crate) fn same_file(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let (Ok(x), Ok(y)) = (std::fs::metadata(a), std::fs::metadata(b)) {
            return x.dev() == y.dev() && x.ino() == y.ino();
        }
    }
    a == b
}

/// Copy the pages of `meta` into a fresh file at `dest`, with `meta` as its
/// only meta and no persisted free state (rebuilt on the copy's first write).
pub(crate) fn write_backup(inner: &Inner, meta: &Meta, dest: &Path) -> Result<()> {
    let ps = inner.ps as u64;
    let mut out = std::fs::File::create(dest)?;
    out.set_len(RESERVED_BYTES)?;
    out.seek(SeekFrom::Start(meta.slot as u64 * META_SLOT_SIZE))?;
    out.write_all(&meta.encode(None))?;
    let start = inner.first_data as u64 * ps;
    let end = meta.page_count * ps;
    out.seek(SeekFrom::Start(start))?;
    let mut buf = vec![0u8; (1 << 20).max(ps as usize)];
    let mut off = start;
    while off < end {
        let n = (end - off).min(buf.len() as u64) as usize;
        if inner.storage.read_at(&mut buf[..n], off).is_err() {
            // A free page past the last write can read short; it holds
            // nothing, so fall back to page by page and zero what is missing.
            for (i, page) in buf[..n].chunks_mut(ps as usize).enumerate() {
                if inner.storage.read_at(page, off + i as u64 * ps).is_err() {
                    page.fill(0);
                }
            }
        }
        out.write_all(&buf[..n])?;
        off += n as u64;
    }
    out.sync_all()?;
    Ok(())
}

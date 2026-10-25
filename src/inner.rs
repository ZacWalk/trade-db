//! Engine state shared by every handle method: meta discovery, the reader
//! registry that keeps snapshots' pages from being recycled, and the
//! writer's begin/commit/abort protocol.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use crate::cache::{Cache, Page};
use crate::db::{Durability, Registration};
use crate::error::{Error, Result};
use crate::free::FreeState;
use crate::layout::*;
use crate::meta::{DirRoot, Header, Meta, NO_FREE, Probe, probe_header};
use crate::scan::backoff;
use crate::storage::Storage;
use crate::txn::Txn;

/// Byte-range lock offsets, far past any page (page ids are 32-bit).
/// Shared by every handle for its lifetime; exclusive for an exclusive open.
pub(crate) const LOCK_OPEN: u64 = 1 << 56;
/// Held exclusively for the duration of a write transaction.
pub(crate) const LOCK_WRITER: u64 = (1 << 56) + 1;
/// A registered snapshot of txn `T` holds a shared lock on `READER_BASE + T`.
pub(crate) const READER_BASE: u64 = 1 << 57;

#[derive(Default)]
pub(crate) struct WriterState {
    /// Free state valid for meta `free_txn`.
    pub free: Option<FreeState>,
    pub free_txn: u64,
    /// Newest txn known to be on stable storage.
    pub durable: u64,
    /// Newest txn this handle knows about as a writer.
    pub last_commit: u64,
    pub file_len: u64,
    /// A relaxed-durability commit has not been synced yet.
    pub unsynced: bool,
}

pub(crate) struct Inner {
    pub storage: Box<dyn Storage>,
    pub ps: usize,
    pub first_data: u32,
    pub cache: Cache,
    pub read_only: bool,
    /// Other processes may use the file: read metas from disk and
    /// coordinate through OS locks.
    pub multi: bool,
    pub durability: Durability,
    pub latest: Mutex<Arc<Meta>>,
    pub readers: Mutex<BTreeMap<u64, usize>>,
    pub writer: Mutex<WriterState>,
    pub commits: (Mutex<u64>, Condvar),
    pub spills: AtomicU64,
    pub path: Option<PathBuf>,
    // Declared last so it drops after `storage` has closed the file.
    pub _registration: Option<Registration>,
}

pub(crate) enum Disk {
    Valid(Meta),
    Blank,
}

/// Classify what is in `storage`: a database, a blank file to format, or
/// something we must not touch.
pub(crate) fn inspect(storage: &dyn Storage) -> Result<Disk> {
    let len = storage.len()?;
    if len == 0 {
        return Ok(Disk::Blank);
    }
    if len < RESERVED_BYTES {
        let mut b = vec![0u8; len as usize];
        storage.read_at(&mut b, 0)?;
        if b.iter().all(|&x| x == 0) {
            return Ok(Disk::Blank);
        }
        return Err(Error::NotADatabase("file is too short to hold a database".into()));
    }
    let mut probes = Vec::with_capacity(2);
    let mut best: Option<Meta> = None;
    for slot in 0..2 {
        let mut h = [0u8; META_HEADER_LEN];
        storage.read_at(&mut h, slot as u64 * META_SLOT_SIZE)?;
        let p = probe_header(&h);
        if let Probe::Valid(hd) = p {
            let mut um = vec![0u8; hd.umeta_len as usize];
            storage.read_at(&mut um, slot as u64 * META_SLOT_SIZE + META_HEADER_LEN as u64)?;
            if let Some(m) = Meta::from_parts(hd, &um, slot)
                && best.as_ref().is_none_or(|b| m.txn > b.txn)
            {
                best = Some(m);
            }
        }
        probes.push((p, h));
    }
    if let Some(m) = best {
        return Ok(Disk::Valid(m));
    }
    for (p, _) in &probes {
        if let Probe::Future(v) = p {
            return Err(Error::UnsupportedFormat(format!(
                "tradedb format v{v}; this build reads v{FORMAT_VERSION}"
            )));
        }
    }
    if probes.iter().all(|(p, _)| matches!(p, Probe::Empty)) && len == RESERVED_BYTES {
        return Ok(Disk::Blank);
    }
    if probes.iter().any(|(_, h)| h[..8] == MAGIC) {
        return Err(Error::corrupt(None, "both meta slots are damaged"));
    }
    Err(Error::NotADatabase("no tradedb meta page".into()))
}

/// Write the first meta of a new database.
pub(crate) fn format(storage: &dyn Storage, page_size: u32) -> Result<()> {
    let meta = Meta {
        txn: 1,
        page_size,
        page_count: RESERVED_BYTES / page_size as u64,
        dir: DirRoot::default(),
        umeta: Arc::from(&[][..]),
        slot: 1,
        free_len: 0,
        free_crc: 0,
        hcrc: 0,
    };
    if storage.len()? < RESERVED_BYTES {
        storage.set_len(RESERVED_BYTES)?;
    }
    storage.write_at(&meta.encode(Some(&FreeState::default())), META_SLOT_SIZE)?;
    storage.sync()?;
    Ok(())
}

impl Inner {
    /// Valid meta headers, newest first.
    fn read_headers(&self) -> Result<Vec<(Header, usize)>> {
        let mut hdrs = Vec::with_capacity(2);
        for slot in 0..2usize {
            let mut h = [0u8; META_HEADER_LEN];
            self.storage.read_at(&mut h, slot as u64 * META_SLOT_SIZE)?;
            if let Probe::Valid(hd) = probe_header(&h)
                && hd.page_size as usize == self.ps
            {
                hdrs.push((hd, slot));
            }
        }
        hdrs.sort_by_key(|h| std::cmp::Reverse(h.0.txn));
        Ok(hdrs)
    }

    /// The newest committed meta, read from the file.
    ///
    /// The header and the metadata area are separate reads, so a commit
    /// rewriting the slot in between makes them disagree; that is retried,
    /// never answered with an older slot. Only when the *same* newest header
    /// fails twice is it taken to be a meta write that never completed — torn
    /// by a crash, or still in flight — so that commit does not exist (yet)
    /// and the other slot is the newest committed state. Snapshot
    /// registration relies on this never returning anything older.
    pub(crate) fn disk_meta(&self) -> Result<Arc<Meta>> {
        let mut suspect: Option<(usize, u32)> = None;
        for attempt in 1..=200u32 {
            let hdrs = self.read_headers()?;
            let newest = hdrs.first().map(|&(h, s)| (s, h.hcrc));
            let pick = if newest.is_some() && newest == suspect {
                hdrs.get(1)
            } else {
                hdrs.first()
            };
            let Some(&(h, slot)) = pick else {
                if attempt >= 3 {
                    return Err(Error::corrupt(None, "no valid meta slot"));
                }
                backoff(attempt);
                continue;
            };
            {
                let l = self.latest.lock().unwrap();
                if l.txn == h.txn && l.slot == slot && l.hcrc == h.hcrc {
                    return Ok(l.clone());
                }
            }
            let mut um = vec![0u8; h.umeta_len as usize];
            self.storage
                .read_at(&mut um, slot as u64 * META_SLOT_SIZE + META_HEADER_LEN as u64)?;
            if let Some(m) = Meta::from_parts(h, &um, slot) {
                let m = Arc::new(m);
                let mut l = self.latest.lock().unwrap();
                if m.txn >= l.txn {
                    *l = m.clone();
                }
                return Ok(m);
            }
            if newest == Some((slot, h.hcrc)) {
                suspect = newest;
            }
            backoff(attempt);
        }
        Err(Error::corrupt(None, "meta slots kept changing while being read"))
    }

    /// The newest committed meta: from memory when this process is the only
    /// user of the file, otherwise from disk (two 128-byte reads).
    pub(crate) fn current_meta(&self) -> Result<Arc<Meta>> {
        if self.multi {
            self.disk_meta()
        } else {
            Ok(self.latest.lock().unwrap().clone())
        }
    }

    fn register(&self, txn: u64) -> Result<()> {
        let mut r = self.readers.lock().unwrap();
        let first = !r.contains_key(&txn);
        if first && self.multi {
            self.storage.lock(READER_BASE + txn, 1, false, true)?;
        }
        *r.entry(txn).or_insert(0) += 1;
        Ok(())
    }

    pub(crate) fn unregister(&self, txn: u64) {
        let mut r = self.readers.lock().unwrap();
        if let Some(n) = r.get_mut(&txn) {
            *n -= 1;
            if *n == 0 {
                r.remove(&txn);
                if self.multi {
                    let _ = self.storage.unlock(READER_BASE + txn, 1);
                }
            }
        }
    }

    /// Register a snapshot of the newest commit: its pages will not be
    /// recycled, by this process or any other, until it is released.
    pub(crate) fn acquire(&self) -> Result<Arc<Meta>> {
        if !self.multi {
            // Holding `latest` orders this against the writer publishing a
            // newer meta, so the writer always sees the registration.
            let latest = self.latest.lock().unwrap();
            *self.readers.lock().unwrap().entry(latest.txn).or_insert(0) += 1;
            return Ok(latest.clone());
        }
        let mut attempt = 0;
        loop {
            let m = self.disk_meta()?;
            self.register(m.txn)?;
            // Pages of `m` could only have been recycled by a writer that
            // started after a newer commit; if the file has not moved on, our
            // lock is in place before any such writer can look for it.
            match self.disk_meta() {
                Ok(again) if again.txn == m.txn && again.hcrc == m.hcrc => return Ok(m),
                Ok(_) => {}
                Err(e) => {
                    self.unregister(m.txn);
                    return Err(e);
                }
            }
            self.unregister(m.txn);
            attempt += 1;
            backoff(attempt);
        }
    }

    /// Release groups of freed pages that no snapshot anywhere can still
    /// read and whose freeing commit is durable.
    fn release(&self, free: &mut FreeState, durable: u64) -> Result<()> {
        let inproc = self.readers.lock().unwrap().keys().next().copied().unwrap_or(u64::MAX);
        let limit = durable.min(inproc);
        let cands: Vec<u64> = free
            .pending
            .iter()
            .map(|(t, _)| *t)
            .take_while(|&t| t <= limit)
            .collect();
        let Some(&newest) = cands.last() else {
            return Ok(());
        };
        if !self.multi {
            free.release_upto(newest);
            return Ok(());
        }
        // Group `t` is safe when no process holds a snapshot older than `t`,
        // i.e. nobody has locked any byte in [READER_BASE, READER_BASE + t).
        // Our own registrations are all >= `limit`, outside every probe.
        let clear = |t: u64| -> Result<bool> {
            if self.storage.lock(READER_BASE, t, true, false)? {
                self.storage.unlock(READER_BASE, t)?;
                Ok(true)
            } else {
                Ok(false)
            }
        };
        if clear(newest)? {
            free.release_upto(newest);
            return Ok(());
        }
        let (mut a, mut b) = (0usize, cands.len() - 1);
        while a < b {
            let m = (a + b) / 2;
            if clear(cands[m])? { a = m + 1 } else { b = m }
        }
        if a > 0 {
            free.release_upto(cands[a - 1]);
        }
        Ok(())
    }

    fn load_free(&self, ws: &mut WriterState, meta: &Meta) -> Result<FreeState> {
        if ws.free_txn == meta.txn
            && let Some(f) = ws.free.take()
        {
            return Ok(f);
        }
        ws.free = None;
        if meta.free_len != NO_FREE {
            let mut area = vec![0u8; meta.free_len as usize];
            self.storage
                .read_at(&mut area, meta.slot as u64 * META_SLOT_SIZE + META_FREE_OFF as u64)?;
            if let Some(f) = meta.decode_free(&area) {
                return Ok(f);
            }
        }
        self.rebuild_free(meta)
    }

    /// Reconstruct the free state by walking every tree: whatever is not
    /// reachable from `meta` is free (which also reclaims pages leaked by a
    /// crash). They become reusable once no older snapshot remains.
    pub(crate) fn rebuild_free(&self, meta: &Meta) -> Result<FreeState> {
        let used = self.mark_reachable(meta)?;
        let pages: Vec<u32> = (self.first_data..meta.page_count as u32)
            .filter(|&p| !used.get(p as usize))
            .collect();
        let mut f = FreeState::default();
        if !pages.is_empty() {
            f.pending.push_back((meta.txn, pages));
        }
        Ok(f)
    }

    /// Start a write transaction.
    pub(crate) fn begin(&self) -> Result<(WriteGuard<'_>, Txn<'_>)> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let ws = self.writer.lock().unwrap();
        let mut g = WriteGuard {
            inner: self,
            ws,
            os_locked: false,
        };
        if self.multi {
            self.storage.lock(LOCK_WRITER, 1, true, true)?;
            g.os_locked = true;
        }
        let meta = self.current_meta()?;
        let ws = &mut *g.ws;
        if meta.txn != ws.last_commit {
            // Another process committed since this handle last wrote. Make
            // its work durable before recycling anything it freed.
            self.storage.sync()?;
            ws.durable = ws.durable.max(meta.txn);
            ws.last_commit = meta.txn;
            ws.file_len = self.storage.len()?;
        }
        let mut free = self.load_free(ws, &meta)?;
        self.release(&mut free, ws.durable)?;
        let spill_limit = self.cache.capacity().max(1024);
        Ok((g, Txn::new(self, meta, free, spill_limit)))
    }

    /// Make a commit visible to this process.
    fn publish(&self, meta: Arc<Meta>, pages: Vec<(u32, Box<[u8]>)>) {
        let budget = self.cache.capacity() / 2;
        let mut leaves = 0;
        for (id, buf) in pages {
            let hot = kind(&buf) == KIND_BRANCH || flags(&buf) & FLAG_DIR != 0;
            if !hot {
                if leaves >= budget {
                    continue;
                }
                leaves += 1;
            }
            self.cache.insert(id, Arc::new(Page { buf }), hot);
        }
        let txn = meta.txn;
        *self.latest.lock().unwrap() = meta;
        let (m, cv) = &self.commits;
        *m.lock().unwrap() = txn;
        cv.notify_all();
    }
}

/// The writer's exclusive hold on the database for one transaction.
pub(crate) struct WriteGuard<'a> {
    inner: &'a Inner,
    pub ws: MutexGuard<'a, WriterState>,
    os_locked: bool,
}

impl WriteGuard<'_> {
    pub(crate) fn finish(&mut self, txn: Txn<'_>, durability: Durability) -> Result<u64> {
        let inner = self.inner;
        if txn.is_noop() {
            let base = txn.base.txn;
            self.abort(txn);
            return Ok(base);
        }
        let (id, base) = (txn.id, txn.base.txn);
        let ws = &mut *self.ws;
        match txn.commit(durability, &mut ws.file_len) {
            Ok(c) => {
                ws.free = Some(c.free);
                ws.free_txn = id;
                ws.last_commit = id;
                ws.durable = match durability {
                    Durability::Immediate | Durability::None => id,
                    // The data flush also made every earlier meta durable.
                    Durability::Eventual => ws.durable.max(base),
                };
                ws.unsynced |= durability != Durability::Immediate;
                inner.publish(c.meta, c.pages);
                Ok(id)
            }
            Err(error) => {
                // Whether or not the meta landed, adopt what the file says
                // and rebuild the free state from it next time.
                ws.free = None;
                ws.free_txn = 0;
                if let Ok(m) = inner.disk_meta() {
                    ws.last_commit = m.txn;
                }
                Err(error)
            }
        }
    }

    pub(crate) fn abort(&mut self, txn: Txn<'_>) {
        let base = txn.base.txn;
        self.ws.free = Some(txn.rollback());
        self.ws.free_txn = base;
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if self.os_locked {
            let _ = self.inner.storage.unlock(LOCK_WRITER, 1);
        }
    }
}

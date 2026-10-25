use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};

use crate::cache::Cache;
use crate::error::{Error, Result};
pub(crate) use crate::inner::Inner;
use crate::inner::{Disk, LOCK_OPEN, LOCK_WRITER, WriterState, format, inspect};
use crate::layout::*;
use crate::read::View;
use crate::scan::{MAX_STALE, backoff};
use crate::storage::{FileStorage, MemStorage, Storage};
use crate::txn::Txn;

/// How far a commit is pushed towards stable storage before
/// [`Db::write`] returns. Every level is atomic: after a crash the
/// database holds all of a batch or none of it.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub enum Durability {
    /// Flush the data, then the meta (two fsyncs). A commit that returned
    /// survives a power cut.
    #[default]
    Immediate,
    /// Flush the data only (one fsync); the meta reaches stable storage
    /// with the next commit or [`Db::sync`]. A power cut can lose the most
    /// recent commit — never part of one. The usual choice for a live
    /// ingester, which resumes from its cursor (see [`Batch::set_meta`](crate::Batch::set_meta)).
    Eventual,
    /// No fsync: durable to the operating system only. Survives a process
    /// crash; an OS crash or power cut can lose recent commits or damage
    /// the file. For bulk loads that end with [`Db::sync`].
    None,
}

/// Configuration for opening a database.
///
/// ```no_run
/// let db = tradedb::Options::new().cache_size(256 << 20).open("ticks.tradedb")?;
/// # Ok::<(), tradedb::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct Options {
    page_size: u32,
    cache_bytes: usize,
    read_only: bool,
    create: bool,
    exclusive: bool,
    durability: Durability,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            page_size: DEFAULT_PAGE_SIZE,
            cache_bytes: 64 << 20,
            read_only: false,
            create: true,
            exclusive: false,
            durability: Durability::Immediate,
        }
    }
}

const MIN_CACHE_PAGES: usize = 16;

impl Options {
    /// Default options: 64 KiB pages, 64 MiB cache, read-write, create if
    /// missing, multi-process, [`Durability::Immediate`].
    pub fn new() -> Options {
        Options::default()
    }

    /// Page size of a *new* database: a power of two from 4 KiB to 64 KiB
    /// (default). Larger pages scan faster and hold larger values; smaller
    /// pages make small commits and sparse symbols cheaper. Ignored when
    /// opening an existing file.
    pub fn page_size(mut self, bytes: u32) -> Options {
        self.page_size = bytes;
        self
    }

    /// Page-cache budget in bytes (at least 16 pages).
    pub fn cache_size(mut self, bytes: usize) -> Options {
        self.cache_bytes = bytes;
        self
    }

    /// Open read-only. The file must exist; every write returns
    /// [`Error::ReadOnly`].
    pub fn read_only(mut self, yes: bool) -> Options {
        self.read_only = yes;
        self
    }

    /// Create the file if it does not exist (default `true`).
    pub fn create(mut self, yes: bool) -> Options {
        self.create = yes;
        self
    }

    /// Claim the file for this process alone. Other processes cannot open it
    /// while this handle is open (and this open fails if one already has),
    /// which lets every read skip the on-disk meta check and OS locking.
    pub fn exclusive(mut self, yes: bool) -> Options {
        self.exclusive = yes;
        self
    }

    /// Durability used by [`Db::write`] and the single-operation methods.
    pub fn durability(mut self, d: Durability) -> Options {
        self.durability = d;
        self
    }

    fn validate(&self) -> Result<()> {
        if !self.page_size.is_power_of_two() || !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&self.page_size) {
            return Err(Error::invalid(format!(
                "page size must be a power of two from {MIN_PAGE_SIZE} to {MAX_PAGE_SIZE}, got {}",
                self.page_size
            )));
        }
        if self.read_only && self.exclusive {
            return Err(Error::invalid("an exclusive open needs write access"));
        }
        Ok(())
    }

    /// Open (or create) the database at `path`.
    ///
    /// A file may be opened only once per process at a time — share the
    /// [`Db`] between threads instead (it is `Send + Sync`); a second open
    /// returns [`Error::AlreadyOpen`].
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Db> {
        self.validate()?;
        let path = path.as_ref();
        let mut reg = OPEN_FILES.lock().unwrap();
        // Check before opening anything: on Unix systems without
        // per-descriptor locks, closing a second descriptor for the file
        // would drop the first handle's locks.
        let existing = std::fs::canonicalize(path).ok();
        if let Some(p) = &existing
            && reg.contains(p)
        {
            return Err(Error::AlreadyOpen(p.clone()));
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(!self.read_only)
            .create(!self.read_only && self.create)
            .open(path)?;
        let canon = match existing {
            Some(p) => p,
            None => std::fs::canonicalize(path)?,
        };
        if reg.contains(&canon) {
            return Err(Error::AlreadyOpen(canon));
        }
        reg.push(canon.clone());
        drop(reg);
        let registration = Registration(canon.clone());
        let shown = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        Db::from_storage(Box::new(FileStorage::new(file)), self, Some(shown), Some(registration))
    }

    /// A database held entirely in memory, for tests and scratch work.
    pub fn open_memory(&self) -> Result<Db> {
        self.validate()?;
        Db::from_storage(
            Box::new(MemStorage::new(Vec::new())),
            &Options {
                read_only: false,
                ..self.clone()
            },
            None,
            None,
        )
    }
}

static OPEN_FILES: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Marks a file as open in this process until dropped.
pub(crate) struct Registration(PathBuf);

impl Drop for Registration {
    fn drop(&mut self) {
        OPEN_FILES.lock().unwrap().retain(|p| p != &self.0);
    }
}

/// Statistics about a database handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Page size in bytes.
    pub page_size: u32,
    /// Pages allocated in the file, including the reserved meta pages.
    pub page_count: u64,
    /// Pages available for reuse, when this handle has written and knows.
    pub free_pages: Option<u64>,
    /// Newest committed transaction.
    pub txn_id: u64,
    /// Number of symbols.
    pub symbol_count: u64,
    /// Cache capacity in pages.
    pub cache_capacity: usize,
    /// Pages currently cached.
    pub cache_used: usize,
    /// Page loads served from the cache.
    pub cache_hits: u64,
    /// Page loads that read the file.
    pub cache_misses: u64,
    /// Times a large transaction wrote pages out early.
    pub spills: u64,
    /// Snapshots currently open on this handle.
    pub open_snapshots: usize,
}

/// An open database. `Send + Sync`: share it across threads with `&Db` or
/// `Arc<Db>`. Readers never block the writer and the writer never blocks
/// readers, within a process or across processes.
pub struct Db {
    pub(crate) inner: Inner,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("path", &self.inner.path)
            .field("page_size", &self.inner.ps)
            .field("read_only", &self.inner.read_only)
            .finish_non_exhaustive()
    }
}

impl Db {
    /// Open (or create) the database at `path` with default [`Options`].
    pub fn open(path: impl AsRef<Path>) -> Result<Db> {
        Options::new().open(path)
    }

    /// Open an existing database read-only.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Db> {
        Options::new().read_only(true).open(path)
    }

    /// A database held entirely in memory, with default options.
    pub fn memory() -> Db {
        Options::new()
            .open_memory()
            .expect("formatting an in-memory database cannot fail")
    }

    pub(crate) fn from_storage(
        storage: Box<dyn Storage>,
        opts: &Options,
        path: Option<PathBuf>,
        registration: Option<Registration>,
    ) -> Result<Db> {
        match Self::init(&*storage, opts) {
            Ok((meta, multi)) => {
                let ps = meta.page_size as usize;
                let file_len = storage.len()?;
                let ws = WriterState {
                    durable: meta.txn,
                    last_commit: meta.txn,
                    file_len,
                    ..WriterState::default()
                };
                let txn = meta.txn;
                Ok(Db {
                    inner: Inner {
                        ps,
                        first_data: (RESERVED_BYTES / ps as u64) as u32,
                        cache: Cache::new((opts.cache_bytes / ps).max(MIN_CACHE_PAGES)),
                        read_only: opts.read_only,
                        multi,
                        durability: opts.durability,
                        latest: Mutex::new(Arc::new(meta)),
                        readers: Mutex::new(BTreeMap::new()),
                        writer: Mutex::new(ws),
                        commits: (Mutex::new(txn), Condvar::new()),
                        spills: AtomicU64::new(0),
                        path,
                        storage,
                        _registration: registration,
                    },
                })
            }
            Err(e) => {
                // Close the file before forgetting the registration.
                drop(storage);
                drop(registration);
                Err(e)
            }
        }
    }

    fn init(storage: &dyn Storage, opts: &Options) -> Result<(crate::meta::Meta, bool)> {
        let shared = storage.shared_across_processes();
        let multi = shared && !opts.exclusive;
        // Released when the file is closed, including on the error paths.
        if shared && !storage.lock(LOCK_OPEN, 1, opts.exclusive, false)? {
            return Err(Error::Locked);
        }
        let meta = match inspect(storage)? {
            Disk::Valid(m) => m,
            Disk::Blank if opts.read_only => {
                return Err(Error::NotADatabase("file is empty".into()));
            }
            Disk::Blank => {
                if multi {
                    storage.lock(LOCK_WRITER, 1, true, true)?;
                }
                let r = (|| match inspect(storage)? {
                    Disk::Valid(m) => Ok(m),
                    Disk::Blank => {
                        format(storage, opts.page_size)?;
                        match inspect(storage)? {
                            Disk::Valid(m) => Ok(m),
                            Disk::Blank => Err(Error::corrupt(None, "format did not persist")),
                        }
                    }
                })();
                if multi {
                    let _ = storage.unlock(LOCK_WRITER, 1);
                }
                r?
            }
        };
        if !opts.read_only {
            // Whatever a crashed writer left in the OS cache becomes durable
            // before this handle recycles any page it freed.
            storage.sync()?;
        }
        Ok((meta, multi))
    }

    /// Run `f` against the newest commit, retrying if a concurrent writer
    /// recycles a page out from under it.
    pub(crate) fn view<T>(&self, mut f: impl FnMut(&View<'_>) -> Result<T>) -> Result<T> {
        let mut attempt = 0;
        loop {
            let v = View {
                db: &self.inner,
                meta: self.inner.current_meta()?,
                stable: false,
            };
            match f(&v) {
                Err(Error::Stale) if attempt < MAX_STALE => {
                    attempt += 1;
                    backoff(attempt);
                }
                Err(Error::Stale) => {
                    return Err(Error::corrupt(None, "read kept losing its snapshot to page reuse"));
                }
                r => return r,
            }
        }
    }

    pub(crate) fn transact<T>(
        &self,
        durability: Durability,
        f: impl FnOnce(&mut Txn<'_>) -> Result<T>,
    ) -> Result<(T, u64)> {
        let (mut g, mut txn) = self.inner.begin()?;
        match f(&mut txn) {
            Ok(v) => {
                let id = g.finish(txn, durability)?;
                Ok((v, id))
            }
            Err(e) => {
                g.abort(txn);
                Err(e)
            }
        }
    }

    /// Path the database was opened from (`None` for an in-memory database).
    pub fn path(&self) -> Option<&Path> {
        self.inner.path.as_deref()
    }

    /// Page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.inner.ps as u32
    }

    /// Largest value a variable-width symbol accepts.
    pub fn max_value_len(&self) -> usize {
        max_value_len(self.inner.ps)
    }

    /// Largest record size [`define_symbol`](Db::define_symbol) accepts.
    pub fn max_record_size(&self) -> usize {
        max_record_size(self.inner.ps)
    }
}

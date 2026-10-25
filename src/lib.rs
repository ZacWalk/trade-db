//! # tradedb
//!
//! An embedded, ACID time-series store for financial market data — ticks,
//! quotes, candles — in a single file.
//!
//! Records are keyed by `(symbol: u64, time: u64, seq: u32)` and kept in
//! `(time, seq)` order per symbol; `seq` breaks ties between events that
//! share a timestamp. Values are opaque bytes, or fixed-width records stored
//! column-wise when a symbol is declared with [`Db::define_symbol`].
//!
//! ```
//! use tradedb::{Batch, Db};
//!
//! let db = Db::memory();
//! const BTC: u64 = 1;
//!
//! let mut batch = Batch::new();
//! for t in 0..1000u64 {
//!     batch.put(BTC, t * 1_000, &t.to_le_bytes());
//! }
//! batch.set_meta("cursor", b"exchange-seq-1000"); // commits with the data
//! db.write(&batch)?; // atomic: all or nothing, even across a crash
//!
//! assert_eq!(db.max_time(BTC)?, Some(999_000));
//! // As-of lookup: the last record at or before a time.
//! let e = db.get_le(BTC, 5_500)?.unwrap();
//! assert_eq!(e.time, 5_000);
//! // Range scans, either direction; values are zero-copy.
//! let n = db.scan(BTC, 10_000..=19_000)?.count();
//! assert_eq!(n, 10);
//! let newest = db.scan_rev(BTC, ..)?.next().unwrap()?;
//! assert_eq!(&*newest.value, &999u64.to_le_bytes());
//! # Ok::<(), tradedb::Error>(())
//! ```
//!
//! ## Design
//!
//! - **Copy-on-write B+trees.** One tree per symbol plus a directory tree.
//!   A commit never overwrites a page that a committed state can reach; it
//!   writes new pages and then flips one of two checksummed meta slots.
//!   That flip is the commit point, so a crash leaves all of a batch or none.
//! - **Every page is checked.** Each page carries a CRC-32C, and each
//!   pointer records the transaction that wrote its target, which catches
//!   torn, lost and misdirected writes as well as bit rot.
//! - **Readers never block the writer, and vice versa**, across threads and
//!   processes. Snapshots are registered with byte-range locks so pages they
//!   can see are not recycled.
//! - **No mmap.** An explicit page cache over positional reads keeps memory
//!   bounded on files far larger than RAM.
//! - **Counted index.** Branch entries carry subtree record counts, so
//!   [`Db::count_range`] is `O(log n)`.
//!
//! See `docs/design.md` in the repository for the full design.

mod api;
mod batch;
mod cache;
mod crc;
mod db;
mod error;
mod free;
mod inner;
mod layout;
mod meta;
mod read;
mod record;
mod scan;
mod snapshot;
mod storage;
mod sys;
mod txn;
mod verify;

#[cfg(test)]
mod tests;

pub use batch::Batch;
pub use db::{Db, Durability, Options, Stats};
pub use error::{Error, Result};
pub use layout::{DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, META_CAPACITY, MIN_PAGE_SIZE};
pub use record::{Columns, Entry, SymbolInfo, Value};
pub use scan::{Chunk, MultiScan, Scan};
pub use snapshot::Snapshot;
pub use verify::VerifyReport;

/// On-disk format version written by this build. Files of another format
/// version are refused with [`Error::UnsupportedFormat`].
pub const FORMAT_VERSION: u32 = layout::FORMAT_VERSION;

/// A stable 64-bit id for a symbol name, such as `"AAPL/1Min"` (FNV-1a).
///
/// Symbols are opaque `u64`s to the engine; this is a convenience for
/// mapping names onto them. The function is frozen: the same name gives the
/// same id on every platform and in every version. With 64-bit ids,
/// collisions are vanishingly unlikely for any realistic universe of names,
/// but they are not impossible — keep a name→id table if you must prove
/// uniqueness.
///
/// ```
/// assert_eq!(tradedb::symbol_id("AAPL/1Min"), tradedb::symbol_id("AAPL/1Min"));
/// assert_ne!(tradedb::symbol_id("AAPL/1Min"), tradedb::symbol_id("AAPL/1Day"));
/// ```
pub const fn symbol_id(name: &str) -> u64 {
    let b = name.as_bytes();
    let mut h = 0xCBF2_9CE4_8422_2325u64;
    let mut i = 0;
    while i < b.len() {
        h ^= b[i] as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
        i += 1;
    }
    h
}

#[cfg(test)]
mod api_shape {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}

    #[test]
    fn handles_are_thread_safe() {
        send_sync::<crate::Db>();
        send_sync::<crate::Snapshot<'static>>();
        send_sync::<crate::Value>();
        send_sync::<crate::Entry>();
        send_sync::<crate::Batch>();
        send::<crate::Scan<'static>>();
        send::<crate::MultiScan<'static>>();
        send::<crate::Chunk>();
    }

    #[test]
    fn symbol_id_is_frozen() {
        // FNV-1a test vectors; changing these would re-key every database.
        assert_eq!(crate::symbol_id(""), 0xCBF2_9CE4_8422_2325);
        assert_eq!(crate::symbol_id("a"), 0xAF63_DC4C_8601_EC8C);
        assert_eq!(crate::symbol_id("foobar"), 0x8594_4171_F739_67E8);
    }
}

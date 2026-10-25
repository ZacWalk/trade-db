//! Public record types.

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use crate::cache::Page;
use crate::error::{Error, Result};
use crate::read::Cursor;

/// A stored value. It borrows the page it was read from (no copy) and
/// dereferences to `[u8]`; it stays valid for as long as you keep it, even
/// after the database moves on or is closed.
///
/// Because it shares the whole page (up to 64 KiB), keeping many `Value`s
/// from different pages keeps all those pages in memory. Copy out
/// (`to_vec`, or decode into your own type) whatever you hold long-term.
#[derive(Clone)]
pub struct Value {
    page: Arc<Page>,
    off: u32,
    len: u32,
}

impl Value {
    pub(crate) fn new(page: Arc<Page>, off: usize, len: usize) -> Value {
        Value {
            page,
            off: off as u32,
            len: len as u32,
        }
    }

    /// The value's bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.page.buf[self.off as usize..(self.off + self.len) as usize]
    }
}

impl Deref for Value {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Value {}

impl PartialEq<[u8]> for Value {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

impl PartialEq<&[u8]> for Value {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_bytes() == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for Value {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_bytes() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for Value {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_bytes() == *other
    }
}

impl From<Value> for Vec<u8> {
    fn from(v: Value) -> Vec<u8> {
        v.as_bytes().to_vec()
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Value({:02x?})", self.as_bytes())
    }
}

/// One record: its key and value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Symbol the record belongs to.
    pub symbol: u64,
    /// Timestamp (any monotonic unit; nanoseconds in practice).
    pub time: u64,
    /// Tiebreaker among records sharing a timestamp.
    pub seq: u32,
    /// The payload.
    pub value: Value,
}

impl Entry {
    pub(crate) fn from_cursor(symbol: u64, c: &Cursor) -> Entry {
        let k = c.key();
        Entry {
            symbol,
            time: k.time,
            seq: k.seq,
            value: c.value(),
        }
    }
}

/// Cached statistics of one symbol, read without touching its records.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct SymbolInfo {
    /// Earliest record time; 0 when the symbol is empty.
    pub min_time: u64,
    /// Latest record time; 0 when the symbol is empty.
    pub max_time: u64,
    /// Number of records.
    pub count: u64,
    /// Number of leaf pages holding them.
    pub leaf_count: u64,
    /// Declared record size of a fixed-width symbol, 0 when variable-width.
    pub record_size: u32,
}

/// Columnar output of [`Db::read_columns`](crate::Db::read_columns): parallel
/// time, seq and payload columns for a fixed-width symbol.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Columns {
    /// Record times.
    pub times: Vec<u64>,
    /// Record sequence numbers.
    pub seqs: Vec<u32>,
    /// Payloads, packed back to back, `record_size` bytes each.
    pub payloads: Vec<u8>,
    record_size: usize,
}

impl Columns {
    /// Empty columns.
    pub fn new() -> Columns {
        Columns::default()
    }

    /// Number of records held.
    pub fn len(&self) -> usize {
        self.times.len()
    }

    /// Whether no records are held.
    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    /// Payload size of the records held (0 before the first read).
    pub fn record_size(&self) -> usize {
        self.record_size
    }

    /// Payload of record `i`.
    pub fn payload(&self, i: usize) -> &[u8] {
        &self.payloads[i * self.record_size..(i + 1) * self.record_size]
    }

    /// Iterate `(time, seq, payload)`.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (u64, u32, &[u8])> + '_ {
        (0..self.len()).map(|i| (self.times[i], self.seqs[i], self.payload(i)))
    }

    /// Remove every record (the record size is forgotten as well).
    pub fn clear(&mut self) {
        self.times.clear();
        self.seqs.clear();
        self.payloads.clear();
        self.record_size = 0;
    }

    pub(crate) fn bind(&mut self, rs: usize) -> Result<()> {
        if self.is_empty() {
            self.record_size = rs;
        } else if self.record_size != rs {
            return Err(Error::invalid(format!(
                "columns hold {}-byte records; cannot append {rs}-byte records",
                self.record_size
            )));
        }
        Ok(())
    }

    pub(crate) fn truncate(&mut self, n: usize) {
        self.times.truncate(n);
        self.seqs.truncate(n);
        self.payloads.truncate(n * self.record_size);
    }
}

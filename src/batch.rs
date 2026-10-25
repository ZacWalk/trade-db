use std::borrow::Cow;
use std::ops::{Bound, RangeBounds};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum OpKind {
    Put,
    Overwrite,
    Delete,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Op {
    pub symbol: u64,
    pub time: u64,
    pub seq: u32,
    pub kind: OpKind,
    pub off: usize,
    pub len: usize,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum StructOp {
    Define(u64, u32),
    /// Inclusive time range.
    DeleteRange(u64, u64, u64),
    Drop(u64),
}

/// A set of changes applied atomically by [`Db::write`](crate::Db::write):
/// after a crash the database holds all of them or none.
///
/// Records may be added in any order; the engine sorts them. Within a
/// batch, changes apply in this order:
///
/// 1. structural changes ([`define_symbol`](Batch::define_symbol),
///    [`delete_range`](Batch::delete_range),
///    [`truncate_before`](Batch::truncate_before),
///    [`drop_symbol`](Batch::drop_symbol)) in the order they were added;
/// 2. record puts and deletes in `(symbol, time, seq)` order — entries for
///    the same key apply in the order they were added, so with
///    [`overwrite`](Batch::overwrite) the last one wins;
/// 3. metadata updates in the order they were added.
///
/// So `delete_range` followed by puts replaces a range in one commit.
///
/// [`Db::write`](crate::Db::write) does not consume or clear the batch;
/// call [`clear`](Batch::clear) before reusing it.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub(crate) ops: Vec<Op>,
    pub(crate) data: Vec<u8>,
    pub(crate) structs: Vec<StructOp>,
    pub(crate) meta: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub(crate) bad_range: bool,
}

/// Normalise a time range to inclusive bounds; `None` when it is empty.
/// `Err` when it is reversed (start past end + 1), as slicing would panic.
pub(crate) fn time_bounds(r: &impl RangeBounds<u64>) -> Result<Option<(u64, u64)>, ()> {
    let lo = match r.start_bound() {
        Bound::Included(&x) => Some(x),
        Bound::Excluded(&x) => x.checked_add(1),
        Bound::Unbounded => Some(0),
    };
    let hi = match r.end_bound() {
        Bound::Included(&x) => Some(x),
        Bound::Excluded(&x) => x.checked_sub(1),
        Bound::Unbounded => Some(u64::MAX),
    };
    let start = match r.start_bound() {
        Bound::Included(&x) => x as u128,
        Bound::Excluded(&x) => x as u128 + 1,
        Bound::Unbounded => 0,
    };
    let end_excl = match r.end_bound() {
        Bound::Included(&x) => x as u128 + 1,
        Bound::Excluded(&x) => x as u128,
        Bound::Unbounded => u64::MAX as u128 + 1,
    };
    if start > end_excl {
        return Err(());
    }
    Ok(match (lo, hi) {
        (Some(lo), Some(hi)) if lo <= hi => Some((lo, hi)),
        _ => None,
    })
}

impl Batch {
    /// An empty batch.
    pub fn new() -> Batch {
        Batch::default()
    }

    /// An empty batch with room for `records` records holding `bytes` of
    /// values in total.
    pub fn with_capacity(records: usize, bytes: usize) -> Batch {
        Batch {
            ops: Vec::with_capacity(records),
            data: Vec::with_capacity(bytes),
            ..Batch::default()
        }
    }

    fn push(&mut self, symbol: u64, time: u64, seq: u32, kind: OpKind, value: &[u8]) -> &mut Self {
        let off = self.data.len();
        self.data.extend_from_slice(value);
        self.ops.push(Op {
            symbol,
            time,
            seq,
            kind,
            off,
            len: value.len(),
        });
        self
    }

    /// Insert a record with `seq = 0`. Inserting a key that already exists
    /// fails the whole batch with [`Error::DuplicateKey`](crate::Error::DuplicateKey).
    pub fn put(&mut self, symbol: u64, time: u64, value: &[u8]) -> &mut Self {
        self.push(symbol, time, 0, OpKind::Put, value)
    }

    /// Insert a record with an explicit sequence number, for events that
    /// share a timestamp.
    pub fn put_seq(&mut self, symbol: u64, time: u64, seq: u32, value: &[u8]) -> &mut Self {
        self.push(symbol, time, seq, OpKind::Put, value)
    }

    /// Insert a record, replacing any existing value for the key — for
    /// restatements, busted trades and idempotent replay.
    pub fn overwrite(&mut self, symbol: u64, time: u64, seq: u32, value: &[u8]) -> &mut Self {
        self.push(symbol, time, seq, OpKind::Overwrite, value)
    }

    /// Delete one record. Deleting a key that is not present is not an error.
    pub fn delete(&mut self, symbol: u64, time: u64, seq: u32) -> &mut Self {
        self.push(symbol, time, seq, OpKind::Delete, &[])
    }

    /// Delete every record of `symbol` whose time falls in `range`, at every seq.
    /// A reversed range (`10..=5`) fails the write with
    /// [`Error::InvalidArgument`](crate::Error::InvalidArgument).
    pub fn delete_range(&mut self, symbol: u64, range: impl RangeBounds<u64>) -> &mut Self {
        match time_bounds(&range) {
            Ok(Some((lo, hi))) => self.structs.push(StructOp::DeleteRange(symbol, lo, hi)),
            Ok(None) => {}
            Err(()) => self.bad_range = true,
        }
        self
    }

    /// Delete every record of `symbol` strictly before `time` (retention).
    pub fn truncate_before(&mut self, symbol: u64, time: u64) -> &mut Self {
        self.delete_range(symbol, ..time)
    }

    /// Declare `symbol` fixed-width: every value is exactly `record_size`
    /// bytes, stored in columnar leaves (see [`Db::define_symbol`](crate::Db::define_symbol)).
    pub fn define_symbol(&mut self, symbol: u64, record_size: u32) -> &mut Self {
        self.structs.push(StructOp::Define(symbol, record_size));
        self
    }

    /// Remove `symbol` entirely: its records, pages and directory entry.
    pub fn drop_symbol(&mut self, symbol: u64) -> &mut Self {
        self.structs.push(StructOp::Drop(symbol));
        self
    }

    /// Set a metadata key in the same transaction as the data — the place
    /// for an ingest cursor (exchange sequence, offset, file position), so
    /// a restart resumes without a gap or a double apply.
    pub fn set_meta(&mut self, key: &str, value: &[u8]) -> &mut Self {
        self.meta.push((key.as_bytes().to_vec(), Some(value.to_vec())));
        self
    }

    /// Remove a metadata key.
    pub fn remove_meta(&mut self, key: &str) -> &mut Self {
        self.meta.push((key.as_bytes().to_vec(), None));
        self
    }

    /// Number of record puts and deletes queued.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether nothing at all is queued.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.structs.is_empty() && self.meta.is_empty() && !self.bad_range
    }

    /// Remove everything, keeping the allocations.
    pub fn clear(&mut self) {
        self.ops.clear();
        self.data.clear();
        self.structs.clear();
        self.meta.clear();
        self.bad_range = false;
    }

    /// The record operations in apply order. Time-ordered ingest is
    /// already sorted and is used in place.
    pub(crate) fn sorted_ops(&self) -> Cow<'_, [Op]> {
        let key = |o: &Op| (o.symbol, o.time, o.seq);
        if self.ops.windows(2).all(|w| key(&w[0]) <= key(&w[1])) {
            Cow::Borrowed(&self.ops)
        } else {
            let mut v = self.ops.clone();
            v.sort_by_key(key);
            Cow::Owned(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn bounds_follow_slice_semantics() {
        assert_eq!(time_bounds(&(5..10)), Ok(Some((5, 9))));
        assert_eq!(time_bounds(&(5..=10)), Ok(Some((5, 10))));
        assert_eq!(time_bounds(&(5..5)), Ok(None));
        assert_eq!(time_bounds(&(6..=5)), Ok(None));
        assert_eq!(time_bounds(&(7..=5)), Err(()));
        assert_eq!(time_bounds(&(..)), Ok(Some((0, u64::MAX))));
        assert_eq!(time_bounds(&(..0)), Ok(None));
        assert_eq!(time_bounds(&(u64::MAX..)), Ok(Some((u64::MAX, u64::MAX))));
    }

    #[test]
    fn sorting_is_stable() {
        let mut b = Batch::new();
        b.overwrite(1, 5, 0, b"second-last")
            .put(1, 3, b"x")
            .overwrite(1, 5, 0, b"last");
        let ops = b.sorted_ops();
        assert_eq!(ops[0].time, 3);
        assert_eq!(&b.data[ops[2].off..ops[2].off + ops[2].len], b"last");
    }
}

//! On-disk format: constants, page headers, and the leaf, branch and
//! directory-entry encodings. Everything is little-endian and read through
//! explicit byte offsets, so the format is the same on every host.
//!
//! ```text
//! file:   [meta slot A: 64 KiB][meta slot B: 64 KiB][page]...
//! page:   [header: 32 B][body ...][CRC-32C of everything before it: 4 B]
//! ```

use crate::crc::crc32c;
use crate::error::{Error, Result};

pub(crate) const MAGIC: [u8; 8] = *b"TRADEDB\0";
pub(crate) const FORMAT_VERSION: u32 = 1;

pub(crate) const META_SLOT_SIZE: u64 = 64 * 1024;
pub(crate) const RESERVED_BYTES: u64 = 2 * META_SLOT_SIZE;
pub(crate) const META_HEADER_LEN: usize = 128;
/// Capacity of the transactional metadata area, in encoded bytes.
pub const META_CAPACITY: usize = 4096;
pub(crate) const META_FREE_OFF: usize = META_HEADER_LEN + META_CAPACITY;
pub(crate) const META_FREE_CAP: usize = META_SLOT_SIZE as usize - META_FREE_OFF;

/// Smallest page size accepted by [`Options::page_size`](crate::Options::page_size).
pub const MIN_PAGE_SIZE: u32 = 4096;
/// Largest page size accepted by [`Options::page_size`](crate::Options::page_size).
pub const MAX_PAGE_SIZE: u32 = 65536;
/// Page size of new databases unless configured otherwise.
pub const DEFAULT_PAGE_SIZE: u32 = 65536;

pub(crate) const HDR: usize = 32;
pub(crate) const KIND_VAR_LEAF: u8 = 1;
pub(crate) const KIND_FIXED_LEAF: u8 = 2;
pub(crate) const KIND_BRANCH: u8 = 3;
pub(crate) const FLAG_DIR: u8 = 1;
pub(crate) const SLOT: usize = 16;
pub(crate) const BENTRY: usize = 32;
pub(crate) const DIR_ENTRY_SIZE: usize = 64;
pub(crate) const MAX_HEIGHT: u8 = 12;
/// Fixed-width leaves must hold at least this many records.
pub(crate) const MIN_FIXED_CAP: usize = 4;

const H_KIND: usize = 0;
const H_FLAGS: usize = 1;
const H_LEVEL: usize = 2;
const H_COUNT: usize = 4;
const H_VER: usize = 8;
const H_OWNER: usize = 16;
const H_PAGE: usize = 24;
const H_AUX: usize = 28;

#[inline(always)]
pub(crate) fn rd_u16(b: &[u8], o: usize) -> u16 {
    u16::from_le_bytes([b[o], b[o + 1]])
}
#[inline(always)]
pub(crate) fn rd_u32(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}
#[inline(always)]
pub(crate) fn rd_u64(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
}
#[inline(always)]
pub(crate) fn wr_u16(b: &mut [u8], o: usize, v: u16) {
    b[o..o + 2].copy_from_slice(&v.to_le_bytes());
}
#[inline(always)]
pub(crate) fn wr_u32(b: &mut [u8], o: usize, v: u32) {
    b[o..o + 4].copy_from_slice(&v.to_le_bytes());
}
#[inline(always)]
pub(crate) fn wr_u64(b: &mut [u8], o: usize, v: u64) {
    b[o..o + 8].copy_from_slice(&v.to_le_bytes());
}

/// Record key within one symbol: `(time, seq)`, ordered by time then seq.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub(crate) struct Key {
    pub time: u64,
    pub seq: u32,
}

impl Key {
    pub const MIN: Key = Key { time: 0, seq: 0 };
    pub const MAX: Key = Key {
        time: u64::MAX,
        seq: u32::MAX,
    };

    #[inline]
    pub const fn new(time: u64, seq: u32) -> Key {
        Key { time, seq }
    }

    /// The smallest key strictly greater than `self`.
    pub fn succ(self) -> Option<Key> {
        if self.seq < u32::MAX {
            Some(Key::new(self.time, self.seq + 1))
        } else if self.time < u64::MAX {
            Some(Key::new(self.time + 1, 0))
        } else {
            None
        }
    }

    /// The largest key strictly smaller than `self`.
    pub fn pred(self) -> Option<Key> {
        if self.seq > 0 {
            Some(Key::new(self.time, self.seq - 1))
        } else if self.time > 0 {
            Some(Key::new(self.time - 1, u32::MAX))
        } else {
            None
        }
    }
}

/// Reference to a page plus the transaction that wrote it. A page read
/// through a ref must carry that generation in its header, which detects
/// recycled pages, lost writes and misdirected writes.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct PageRef {
    pub page: u32,
    pub ver: u64,
}

impl PageRef {
    pub const NULL: PageRef = PageRef { page: 0, ver: 0 };

    #[inline]
    pub fn is_null(self) -> bool {
        self.page == 0
    }
}

// ---- page header ---------------------------------------------------------

#[inline(always)]
pub(crate) fn kind(b: &[u8]) -> u8 {
    b[H_KIND]
}
#[inline(always)]
pub(crate) fn flags(b: &[u8]) -> u8 {
    b[H_FLAGS]
}
#[inline(always)]
pub(crate) fn level(b: &[u8]) -> u16 {
    rd_u16(b, H_LEVEL)
}
#[inline(always)]
pub(crate) fn count(b: &[u8]) -> usize {
    rd_u32(b, H_COUNT) as usize
}
#[inline(always)]
pub(crate) fn page_ver(b: &[u8]) -> u64 {
    rd_u64(b, H_VER)
}
#[inline(always)]
pub(crate) fn owner(b: &[u8]) -> u64 {
    rd_u64(b, H_OWNER)
}
#[inline(always)]
pub(crate) fn page_id(b: &[u8]) -> u32 {
    rd_u32(b, H_PAGE)
}
#[inline(always)]
pub(crate) fn aux(b: &[u8]) -> u32 {
    rd_u32(b, H_AUX)
}
#[inline(always)]
pub(crate) fn set_count(b: &mut [u8], n: usize) {
    wr_u32(b, H_COUNT, n as u32);
}
#[inline(always)]
pub(crate) fn set_aux(b: &mut [u8], v: u32) {
    wr_u32(b, H_AUX, v);
}
#[inline(always)]
pub(crate) fn set_page_ver(b: &mut [u8], g: u64) {
    wr_u64(b, H_VER, g);
}
#[inline(always)]
pub(crate) fn set_page_id(b: &mut [u8], p: u32) {
    wr_u32(b, H_PAGE, p);
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn init_header(b: &mut [u8], kind: u8, flags: u8, level: u16, owner: u64, page: u32, ver: u64, aux: u32) {
    b[..HDR].fill(0);
    b[H_KIND] = kind;
    b[H_FLAGS] = flags;
    wr_u16(b, H_LEVEL, level);
    wr_u64(b, H_VER, ver);
    wr_u64(b, H_OWNER, owner);
    wr_u32(b, H_PAGE, page);
    wr_u32(b, H_AUX, aux);
}

#[inline]
pub(crate) fn body_end(ps: usize) -> usize {
    ps - 4
}

pub(crate) fn stamp_crc(b: &mut [u8]) {
    let end = b.len() - 4;
    let c = crc32c(&b[..end]);
    wr_u32(b, end, c);
}

pub(crate) fn crc_ok(b: &[u8]) -> bool {
    let end = b.len() - 4;
    rd_u32(b, end) == crc32c(&b[..end])
}

// ---- leaves --------------------------------------------------------------

/// Column layout of a fixed-width leaf.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Fixed {
    pub rs: usize,
    pub cap: usize,
    pub times: usize,
    pub seqs: usize,
    pub pay: usize,
}

#[inline]
fn align64(x: usize) -> usize {
    (x + 63) & !63
}

impl Fixed {
    pub fn new(ps: usize, rs: usize) -> Fixed {
        let end = body_end(ps);
        let mut cap = if rs == 0 { 0 } else { (end - HDR) / (12 + rs) };
        while cap > 0 && align64(HDR + 12 * cap) + cap * rs > end {
            cap -= 1;
        }
        Fixed {
            rs,
            cap,
            times: HDR,
            seqs: HDR + 8 * cap,
            pay: align64(HDR + 12 * cap),
        }
    }
}

/// Largest record size a fixed-width symbol may declare for page size `ps`.
pub(crate) fn max_record_size(ps: usize) -> usize {
    let mut rs = (body_end(ps) - HDR - 64) / MIN_FIXED_CAP;
    while rs > 0 && Fixed::new(ps, rs).cap < MIN_FIXED_CAP {
        rs -= 1;
    }
    rs
}

/// Largest value a variable-width leaf can hold for page size `ps`.
pub(crate) fn max_value_len(ps: usize) -> usize {
    body_end(ps) - HDR - SLOT
}

/// Leaf layout, decoded from the page header.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Leaf {
    Var,
    Fixed(Fixed),
}

impl Leaf {
    #[inline]
    pub fn for_record_size(ps: usize, rs: u32) -> Leaf {
        if rs == 0 {
            Leaf::Var
        } else {
            Leaf::Fixed(Fixed::new(ps, rs as usize))
        }
    }

    #[inline]
    pub fn of(b: &[u8]) -> Leaf {
        if kind(b) == KIND_FIXED_LEAF {
            Leaf::Fixed(Fixed::new(b.len(), aux(b) as usize))
        } else {
            Leaf::Var
        }
    }

    #[inline]
    pub fn kind(self) -> u8 {
        match self {
            Leaf::Var => KIND_VAR_LEAF,
            Leaf::Fixed(_) => KIND_FIXED_LEAF,
        }
    }

    #[inline(always)]
    pub fn key(self, b: &[u8], i: usize) -> Key {
        match self {
            Leaf::Var => {
                let o = HDR + SLOT * i;
                Key::new(rd_u64(b, o), rd_u32(b, o + 8))
            }
            Leaf::Fixed(f) => Key::new(rd_u64(b, f.times + 8 * i), rd_u32(b, f.seqs + 4 * i)),
        }
    }

    #[inline(always)]
    pub fn time(self, b: &[u8], i: usize) -> u64 {
        match self {
            Leaf::Var => rd_u64(b, HDR + SLOT * i),
            Leaf::Fixed(f) => rd_u64(b, f.times + 8 * i),
        }
    }

    /// Byte range of record `i`'s value.
    #[inline(always)]
    pub fn val(self, b: &[u8], i: usize) -> (usize, usize) {
        match self {
            Leaf::Var => {
                let o = HDR + SLOT * i;
                (rd_u16(b, o + 12) as usize, rd_u16(b, o + 14) as usize)
            }
            Leaf::Fixed(f) => (f.pay + f.rs * i, f.rs),
        }
    }

    /// First index whose key is `>= key`, and whether it is an exact match.
    #[inline]
    pub fn search(self, b: &[u8], key: Key) -> (usize, bool) {
        let n = count(b);
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.key(b, mid) < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo, lo < n && self.key(b, lo) == key)
    }
}

// ---- branches ------------------------------------------------------------

#[inline]
pub(crate) fn branch_cap(ps: usize) -> usize {
    (body_end(ps) - HDR) / BENTRY
}

#[inline(always)]
pub(crate) fn br_key(b: &[u8], i: usize) -> Key {
    let o = HDR + BENTRY * i;
    Key::new(rd_u64(b, o), rd_u32(b, o + 8))
}
#[inline(always)]
pub(crate) fn br_child(b: &[u8], i: usize) -> PageRef {
    let o = HDR + BENTRY * i;
    PageRef {
        page: rd_u32(b, o + 12),
        ver: rd_u64(b, o + 16),
    }
}
#[inline(always)]
pub(crate) fn br_count(b: &[u8], i: usize) -> u64 {
    rd_u64(b, HDR + BENTRY * i + 24)
}
#[inline(always)]
pub(crate) fn br_set(b: &mut [u8], i: usize, key: Key, child: PageRef, n: u64) {
    let o = HDR + BENTRY * i;
    wr_u64(b, o, key.time);
    wr_u32(b, o + 8, key.seq);
    wr_u32(b, o + 12, child.page);
    wr_u64(b, o + 16, child.ver);
    wr_u64(b, o + 24, n);
}
#[inline(always)]
pub(crate) fn br_set_key(b: &mut [u8], i: usize, key: Key) {
    let o = HDR + BENTRY * i;
    wr_u64(b, o, key.time);
    wr_u32(b, o + 8, key.seq);
}
#[inline(always)]
pub(crate) fn br_set_child(b: &mut [u8], i: usize, child: PageRef) {
    let o = HDR + BENTRY * i;
    wr_u32(b, o + 12, child.page);
    wr_u64(b, o + 16, child.ver);
}
#[inline(always)]
pub(crate) fn br_set_count(b: &mut [u8], i: usize, n: u64) {
    wr_u64(b, HDR + BENTRY * i + 24, n);
}

/// Index of the child whose subtree may hold `key`: the last entry whose
/// key is `<= key`, or 0 when `key` precedes them all.
#[inline]
pub(crate) fn br_search(b: &[u8], key: Key) -> usize {
    let n = count(b);
    let (mut lo, mut hi) = (1usize, n);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if br_key(b, mid) <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo - 1
}

// ---- directory entries ---------------------------------------------------

/// A symbol's directory entry: its tree root and cached statistics.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct DirEntry {
    pub root: PageRef,
    pub height: u8,
    pub rs: u32,
    pub count: u64,
    pub leaves: u64,
    pub min: Key,
    pub max: Key,
}

impl DirEntry {
    pub fn decode(b: &[u8]) -> DirEntry {
        DirEntry {
            root: PageRef {
                page: rd_u32(b, 0),
                ver: rd_u64(b, 4),
            },
            height: b[12],
            rs: rd_u32(b, 16),
            count: rd_u64(b, 24),
            leaves: rd_u64(b, 32),
            min: Key::new(rd_u64(b, 40), rd_u32(b, 56)),
            max: Key::new(rd_u64(b, 48), rd_u32(b, 60)),
        }
    }

    pub fn encode(&self) -> [u8; DIR_ENTRY_SIZE] {
        let mut b = [0u8; DIR_ENTRY_SIZE];
        wr_u32(&mut b, 0, self.root.page);
        wr_u64(&mut b, 4, self.root.ver);
        b[12] = self.height;
        wr_u32(&mut b, 16, self.rs);
        wr_u64(&mut b, 24, self.count);
        wr_u64(&mut b, 32, self.leaves);
        wr_u64(&mut b, 40, self.min.time);
        wr_u64(&mut b, 48, self.max.time);
        wr_u32(&mut b, 56, self.min.seq);
        wr_u32(&mut b, 60, self.max.seq);
        b
    }
}

// ---- validation ----------------------------------------------------------

/// Structural checks on a page read from disk, after its checksum passed.
/// They guarantee that every accessor above stays inside the page, so a
/// damaged or hostile file produces `Error::Corrupt`, never a panic.
pub(crate) fn validate(b: &[u8], id: u32) -> Result<()> {
    let ps = b.len();
    let bad = |what: &str| Err(Error::corrupt(id, what.to_string()));
    let n = count(b);
    match kind(b) {
        KIND_VAR_LEAF => {
            let heap = aux(b) as usize;
            let end = body_end(ps);
            if HDR + SLOT * n > heap || heap > end {
                return bad("leaf slot directory overlaps its value heap");
            }
            let mut total = 0usize;
            for i in 0..n {
                let (off, len) = Leaf::Var.val(b, i);
                if off < heap || off + len > end {
                    return bad("leaf value outside its heap");
                }
                total += len;
            }
            // Values never overlap, so together they fit the heap. Splits
            // rely on this: any subset of a leaf's records fits a new leaf.
            if total > end - heap {
                return bad("leaf values overlap");
            }
        }
        KIND_FIXED_LEAF => {
            let rs = aux(b) as usize;
            if rs == 0 || rs > max_record_size(ps) {
                return bad("bad fixed record size");
            }
            if n > Fixed::new(ps, rs).cap {
                return bad("fixed leaf over capacity");
            }
        }
        KIND_BRANCH => {
            if n == 0 || n > branch_cap(ps) {
                return bad("bad branch entry count");
            }
            let lv = level(b);
            if lv == 0 || lv >= MAX_HEIGHT as u16 {
                return bad("bad branch level");
            }
        }
        _ => return bad("unknown page kind"),
    }
    if page_id(b) != id {
        return bad("page header names a different page");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_capacity_stops_short_of_trailer() {
        for ps in [4096usize, 8192, 16384, 32768, 65536] {
            for rs in [1usize, 8, 24, 56, 64, 100, 1000] {
                let f = Fixed::new(ps, rs);
                if f.cap == 0 {
                    continue;
                }
                assert!(f.pay + f.cap * f.rs <= body_end(ps), "ps {ps} rs {rs}");
                assert!(f.seqs + 4 * f.cap <= f.pay);
                assert_eq!(f.pay % 64, 0);
            }
            let m = max_record_size(ps);
            assert!(Fixed::new(ps, m).cap >= MIN_FIXED_CAP);
            assert!(Fixed::new(ps, m + 1).cap < MIN_FIXED_CAP);
        }
        assert!(Fixed::new(65536, 64).cap > 800);
        assert!(branch_cap(65536) > 2000);
    }

    #[test]
    fn key_succ_pred() {
        assert_eq!(Key::new(5, 1).succ(), Some(Key::new(5, 2)));
        assert_eq!(Key::new(5, u32::MAX).succ(), Some(Key::new(6, 0)));
        assert_eq!(Key::MAX.succ(), None);
        assert_eq!(Key::new(5, 0).pred(), Some(Key::new(4, u32::MAX)));
        assert_eq!(Key::MIN.pred(), None);
    }

    #[test]
    fn dir_entry_round_trips() {
        let e = DirEntry {
            root: PageRef { page: 77, ver: 1234 },
            height: 3,
            rs: 56,
            count: 999,
            leaves: 12,
            min: Key::new(1, 2),
            max: Key::new(u64::MAX, u32::MAX),
        };
        assert_eq!(DirEntry::decode(&e.encode()), e);
    }
}

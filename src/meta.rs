//! Meta slots: the two double-buffered roots of the file.
//!
//! ```text
//! slot (64 KiB):  [header 128 B][user metadata, 4 KiB][free state ...]
//! ```
//!
//! The header carries its own CRC so a poller can read just 128 bytes; the
//! user metadata and free state are covered by separate CRCs stored in the
//! header. A slot counts as committed only when its header and metadata
//! CRCs both pass; a damaged free-state area merely forces a rebuild.

use std::sync::Arc;

use crate::crc::crc32c;
use crate::error::{Error, Result};
use crate::free::FreeState;
use crate::layout::*;

const M_MAGIC: usize = 0;
const M_FORMAT: usize = 8;
const M_PAGE_SIZE: usize = 12;
const M_TXN: usize = 16;
const M_PAGE_COUNT: usize = 24;
const M_DIR_ROOT: usize = 32;
const M_DIR_GEN: usize = 36;
const M_DIR_HEIGHT: usize = 44;
const M_SYMBOLS: usize = 48;
const M_DIR_LEAVES: usize = 56;
const M_UMETA_LEN: usize = 64;
const M_UMETA_CRC: usize = 68;
const M_FREE_LEN: usize = 72;
const M_FREE_CRC: usize = 76;
const M_HEADER_CRC: usize = 124;

pub(crate) const NO_FREE: u32 = u32::MAX;

/// Root of the symbol directory tree.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct DirRoot {
    pub root: PageRef,
    pub height: u8,
    pub symbols: u64,
    pub leaves: u64,
}

/// A decoded header, before the metadata area has been checked.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Header {
    pub page_size: u32,
    pub txn: u64,
    pub page_count: u64,
    pub dir: DirRoot,
    pub umeta_len: u32,
    pub umeta_crc: u32,
    pub free_len: u32,
    pub free_crc: u32,
    pub hcrc: u32,
}

/// One committed state of the database.
#[derive(Clone, Debug)]
pub(crate) struct Meta {
    pub txn: u64,
    pub page_size: u32,
    pub page_count: u64,
    pub dir: DirRoot,
    /// Encoded user metadata (`[u16 klen][u16 vlen][key][value]...`).
    pub umeta: Arc<[u8]>,
    /// Which slot it came from (0 or 1).
    pub slot: usize,
    pub free_len: u32,
    pub free_crc: u32,
    /// Header CRC: identifies this exact meta, even across a txn id that
    /// was reused after a failed commit.
    pub hcrc: u32,
}

/// What the first bytes of a slot turned out to be.
pub(crate) enum Probe {
    Valid(Header),
    /// All zero: never written.
    Empty,
    /// A tradedb header in a newer format.
    Future(u32),
    Invalid,
}

pub(crate) fn probe_header(b: &[u8]) -> Probe {
    debug_assert!(b.len() >= META_HEADER_LEN);
    if b[..META_HEADER_LEN].iter().all(|&x| x == 0) {
        return Probe::Empty;
    }
    if b[M_MAGIC..M_MAGIC + 8] != MAGIC {
        return Probe::Invalid;
    }
    if rd_u32(b, M_HEADER_CRC) != crc32c(&b[..M_HEADER_CRC]) {
        return Probe::Invalid;
    }
    let format = rd_u32(b, M_FORMAT);
    if format != FORMAT_VERSION {
        return Probe::Future(format);
    }
    let h = Header {
        page_size: rd_u32(b, M_PAGE_SIZE),
        txn: rd_u64(b, M_TXN),
        page_count: rd_u64(b, M_PAGE_COUNT),
        dir: DirRoot {
            root: PageRef {
                page: rd_u32(b, M_DIR_ROOT),
                ver: rd_u64(b, M_DIR_GEN),
            },
            height: b[M_DIR_HEIGHT],
            symbols: rd_u64(b, M_SYMBOLS),
            leaves: rd_u64(b, M_DIR_LEAVES),
        },
        umeta_len: rd_u32(b, M_UMETA_LEN),
        umeta_crc: rd_u32(b, M_UMETA_CRC),
        free_len: rd_u32(b, M_FREE_LEN),
        free_crc: rd_u32(b, M_FREE_CRC),
        hcrc: rd_u32(b, M_HEADER_CRC),
    };
    let ps_ok = h.page_size.is_power_of_two() && (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&h.page_size);
    if !ps_ok || h.umeta_len as usize > META_CAPACITY || h.dir.height > MAX_HEIGHT || h.page_count > u32::MAX as u64 {
        return Probe::Invalid;
    }
    if h.free_len != NO_FREE && h.free_len as usize > META_FREE_CAP {
        return Probe::Invalid;
    }
    Probe::Valid(h)
}

impl Meta {
    /// Complete a header with its metadata area, which must already have
    /// been read into `umeta`.
    pub fn from_parts(h: Header, umeta: &[u8], slot: usize) -> Option<Meta> {
        if umeta.len() != h.umeta_len as usize || crc32c(umeta) != h.umeta_crc {
            return None;
        }
        Some(Meta {
            txn: h.txn,
            page_size: h.page_size,
            page_count: h.page_count,
            dir: h.dir,
            umeta: umeta.into(),
            slot,
            free_len: h.free_len,
            free_crc: h.free_crc,
            hcrc: h.hcrc,
        })
    }

    /// The meta these slot bytes encode (as produced by [`Meta::encode`]).
    pub fn from_encoded(b: &[u8], slot: usize) -> Option<Meta> {
        let Probe::Valid(h) = probe_header(b) else {
            return None;
        };
        Meta::from_parts(h, &b[META_HEADER_LEN..META_HEADER_LEN + h.umeta_len as usize], slot)
    }

    /// Encode this meta, with `free` inline when it fits, as the bytes to
    /// write at the start of its slot.
    pub fn encode(&self, free: Option<&FreeState>) -> Vec<u8> {
        let free_bytes = free.map(|f| f.encode()).filter(|b| b.len() <= META_FREE_CAP);
        let mut b = vec![0u8; META_FREE_OFF + free_bytes.as_ref().map_or(0, |f| f.len())];
        b[M_MAGIC..M_MAGIC + 8].copy_from_slice(&MAGIC);
        wr_u32(&mut b, M_FORMAT, FORMAT_VERSION);
        wr_u32(&mut b, M_PAGE_SIZE, self.page_size);
        wr_u64(&mut b, M_TXN, self.txn);
        wr_u64(&mut b, M_PAGE_COUNT, self.page_count);
        wr_u32(&mut b, M_DIR_ROOT, self.dir.root.page);
        wr_u64(&mut b, M_DIR_GEN, self.dir.root.ver);
        b[M_DIR_HEIGHT] = self.dir.height;
        wr_u64(&mut b, M_SYMBOLS, self.dir.symbols);
        wr_u64(&mut b, M_DIR_LEAVES, self.dir.leaves);
        wr_u32(&mut b, M_UMETA_LEN, self.umeta.len() as u32);
        wr_u32(&mut b, M_UMETA_CRC, crc32c(&self.umeta));
        b[META_HEADER_LEN..META_HEADER_LEN + self.umeta.len()].copy_from_slice(&self.umeta);
        match &free_bytes {
            Some(f) => {
                wr_u32(&mut b, M_FREE_LEN, f.len() as u32);
                wr_u32(&mut b, M_FREE_CRC, crc32c(f));
                b[META_FREE_OFF..].copy_from_slice(f);
            }
            None => {
                wr_u32(&mut b, M_FREE_LEN, NO_FREE);
                wr_u32(&mut b, M_FREE_CRC, 0);
            }
        }
        let c = crc32c(&b[..M_HEADER_CRC]);
        wr_u32(&mut b, M_HEADER_CRC, c);
        b
    }

    /// Decode the inline free state, if this slot carries a valid one.
    pub fn decode_free(&self, area: &[u8]) -> Option<FreeState> {
        if self.free_len == NO_FREE || area.len() != self.free_len as usize || crc32c(area) != self.free_crc {
            return None;
        }
        FreeState::decode(area, self.page_count)
    }
}

// ---- user metadata -------------------------------------------------------

/// Iterate the encoded `(key, value)` pairs; `Err` on a malformed area.
pub(crate) fn umeta_entries(area: &[u8]) -> Result<Vec<(&[u8], &[u8])>> {
    let mut out = Vec::new();
    let mut o = 0;
    while o < area.len() {
        if o + 4 > area.len() {
            return Err(Error::corrupt(None, "truncated metadata entry"));
        }
        let kl = rd_u16(area, o) as usize;
        let vl = rd_u16(area, o + 2) as usize;
        let end = o + 4 + kl + vl;
        if end > area.len() {
            return Err(Error::corrupt(None, "metadata entry overruns its area"));
        }
        out.push((&area[o + 4..o + 4 + kl], &area[o + 4 + kl..end]));
        o = end;
    }
    Ok(out)
}

/// Apply `updates` (`None` removes) in order and re-encode.
pub(crate) fn umeta_apply(area: &[u8], updates: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<Vec<u8>> {
    let mut map: Vec<(Vec<u8>, Vec<u8>)> = umeta_entries(area)?
        .into_iter()
        .map(|(k, v)| (k.to_vec(), v.to_vec()))
        .collect();
    for (k, v) in updates {
        let pos = map.iter().position(|(mk, _)| mk == k);
        match (pos, v) {
            (Some(i), Some(v)) => map[i].1 = v.clone(),
            (None, Some(v)) => map.push((k.clone(), v.clone())),
            (Some(i), None) => {
                map.remove(i);
            }
            (None, None) => {}
        }
    }
    let needed: usize = map.iter().map(|(k, v)| 4 + k.len() + v.len()).sum();
    if needed > META_CAPACITY {
        return Err(Error::MetaFull {
            needed,
            capacity: META_CAPACITY,
        });
    }
    let mut out = Vec::with_capacity(needed);
    for (k, v) in &map {
        out.extend_from_slice(&(k.len() as u16).to_le_bytes());
        out.extend_from_slice(&(v.len() as u16).to_le_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(v);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta {
            txn: 42,
            page_size: 4096,
            page_count: 100,
            dir: DirRoot {
                root: PageRef { page: 50, ver: 41 },
                height: 2,
                symbols: 7,
                leaves: 3,
            },
            umeta: Arc::from(&b"\x01\x00\x02\x00kvv"[..]),
            slot: 0,
            free_len: 0,
            free_crc: 0,
            hcrc: 0,
        }
    }

    #[test]
    fn slot_round_trips() {
        let m = meta();
        let mut free = FreeState::default();
        free.pool.extend([40, 41, 60]);
        let b = m.encode(Some(&free));
        let Probe::Valid(h) = probe_header(&b) else {
            panic!("header did not validate")
        };
        let d = Meta::from_parts(h, &b[META_HEADER_LEN..META_HEADER_LEN + h.umeta_len as usize], 0).unwrap();
        assert_eq!((d.txn, d.page_count, d.dir), (42, 100, m.dir));
        assert_eq!(&*d.umeta, &*m.umeta);
        assert_eq!(d.decode_free(&b[META_FREE_OFF..]).unwrap(), free);
        let mut torn = b.clone();
        torn[META_HEADER_LEN] ^= 1;
        assert!(Meta::from_parts(h, &torn[META_HEADER_LEN..META_HEADER_LEN + 7], 0).is_none());
        torn[20] ^= 1;
        assert!(matches!(probe_header(&torn), Probe::Invalid));
    }

    #[test]
    fn user_meta_updates() {
        let a = umeta_apply(&[], &[(b"cursor".to_vec(), Some(b"10".to_vec()))]).unwrap();
        let b = umeta_apply(
            &a,
            &[
                (b"cursor".to_vec(), Some(b"11".to_vec())),
                (b"x".to_vec(), Some(vec![])),
            ],
        )
        .unwrap();
        let e = umeta_entries(&b).unwrap();
        assert_eq!(e, vec![(&b"cursor"[..], &b"11"[..]), (&b"x"[..], &b""[..])]);
        let c = umeta_apply(&b, &[(b"cursor".to_vec(), None)]).unwrap();
        assert_eq!(umeta_entries(&c).unwrap().len(), 1);
        let big = vec![0u8; META_CAPACITY];
        assert!(matches!(
            umeta_apply(&[], &[(b"k".to_vec(), Some(big))]),
            Err(Error::MetaFull { .. })
        ));
    }
}

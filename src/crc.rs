//! CRC-32C (Castagnoli), the page and meta checksum.
//!
//! The hardware paths (SSE4.2 on x86-64, the CRC extension on AArch64) run
//! three independent streams so the instruction's latency is hidden, then
//! stitch the partial CRCs together with a precomputed "append N zero
//! bytes" operator. The software path is slicing-by-8. All paths must agree
//! bit for bit — a divergence would make a file written on one machine
//! unreadable on another — and the test suite checks that they do.

const POLY: u32 = 0x82F6_3B78;

#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const LONG: usize = 8192;
#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const SHORT: usize = 256;

/// Software tables for slicing-by-8.
static SW_TABLE: [[u32; 256]; 8] = build_sw_table();

/// Shift operators applying `LONG` and `SHORT` zero bytes to a raw CRC.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
static LONG_SHIFT: [[u32; 256]; 4] = shift_table(LONG);
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
static SHORT_SHIFT: [[u32; 256]; 4] = shift_table(SHORT);

const fn build_sw_table() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut j = 0;
        while j < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            j += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            i += 1;
        }
        k += 1;
    }
    t
}

#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const fn gf2_times(mat: &[u32; 32], mut vec: u32) -> u32 {
    let mut sum = 0;
    let mut i = 0;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    sum
}

#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const fn gf2_square(mat: &[u32; 32]) -> [u32; 32] {
    let mut sq = [0u32; 32];
    let mut n = 0;
    while n < 32 {
        sq[n] = gf2_times(mat, mat[n]);
        n += 1;
    }
    sq
}

/// Operator that appends `len` zero bytes to a raw (non-inverted) CRC.
/// `len` must be a power of two.
#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const fn zeros_op(mut len: usize) -> [u32; 32] {
    let mut odd = [0u32; 32];
    odd[0] = POLY;
    let mut row = 1u32;
    let mut n = 1;
    while n < 32 {
        odd[n] = row;
        row <<= 1;
        n += 1;
    }
    let mut even = gf2_square(&odd); // two zero bits
    odd = gf2_square(&even); // four zero bits
    loop {
        even = gf2_square(&odd);
        len >>= 1;
        if len == 0 {
            return even;
        }
        odd = gf2_square(&even);
        len >>= 1;
        if len == 0 {
            return odd;
        }
    }
}

#[cfg_attr(not(any(target_arch = "x86_64", target_arch = "aarch64")), allow(dead_code))]
const fn shift_table(len: usize) -> [[u32; 256]; 4] {
    let op = zeros_op(len);
    let mut t = [[0u32; 256]; 4];
    let mut n = 0;
    while n < 256 {
        let v = n as u32;
        t[0][n] = gf2_times(&op, v);
        t[1][n] = gf2_times(&op, v << 8);
        t[2][n] = gf2_times(&op, v << 16);
        t[3][n] = gf2_times(&op, v << 24);
        n += 1;
    }
    t
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline]
fn shift(table: &[[u32; 256]; 4], crc: u32) -> u32 {
    table[0][(crc & 0xFF) as usize]
        ^ table[1][((crc >> 8) & 0xFF) as usize]
        ^ table[2][((crc >> 16) & 0xFF) as usize]
        ^ table[3][(crc >> 24) as usize]
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[inline]
fn le64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b[..8].try_into().unwrap())
}

/// Table-driven CRC-32C, continuing from a previous result `crc`.
pub(crate) fn crc32c_sw(crc: u32, data: &[u8]) -> u32 {
    let t = &SW_TABLE;
    let mut c = !crc;
    let (chunks, rest) = data.as_chunks::<8>();
    for b in chunks {
        let lo = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) ^ c;
        let hi = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
        c = t[7][(lo & 0xFF) as usize]
            ^ t[6][((lo >> 8) & 0xFF) as usize]
            ^ t[5][((lo >> 16) & 0xFF) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xFF) as usize]
            ^ t[2][((hi >> 8) & 0xFF) as usize]
            ^ t[1][((hi >> 16) & 0xFF) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in rest {
        c = (c >> 8) ^ t[0][((c ^ b as u32) & 0xFF) as usize];
    }
    !c
}

#[cfg(target_arch = "x86_64")]
mod hw {
    use super::{LONG, LONG_SHIFT, SHORT, SHORT_SHIFT, le64, shift};
    use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    #[inline]
    pub(super) fn available() -> bool {
        std::arch::is_x86_feature_detected!("sse4.2")
    }

    /// # Safety
    /// The CPU must support SSE4.2 (checked by [`available`]).
    #[target_feature(enable = "sse4.2")]
    pub(super) unsafe fn crc32c(crc: u32, data: &[u8]) -> u32 {
        let mut c0 = !crc as u64;
        let mut p = data;
        while p.len() >= LONG * 3 {
            let (mut c1, mut c2) = (0u64, 0u64);
            let mut i = 0;
            while i < LONG {
                c0 = _mm_crc32_u64(c0, le64(&p[i..]));
                c1 = _mm_crc32_u64(c1, le64(&p[LONG + i..]));
                c2 = _mm_crc32_u64(c2, le64(&p[2 * LONG + i..]));
                i += 8;
            }
            c0 = (shift(&LONG_SHIFT, c0 as u32) ^ c1 as u32) as u64;
            c0 = (shift(&LONG_SHIFT, c0 as u32) ^ c2 as u32) as u64;
            p = &p[LONG * 3..];
        }
        while p.len() >= SHORT * 3 {
            let (mut c1, mut c2) = (0u64, 0u64);
            let mut i = 0;
            while i < SHORT {
                c0 = _mm_crc32_u64(c0, le64(&p[i..]));
                c1 = _mm_crc32_u64(c1, le64(&p[SHORT + i..]));
                c2 = _mm_crc32_u64(c2, le64(&p[2 * SHORT + i..]));
                i += 8;
            }
            c0 = (shift(&SHORT_SHIFT, c0 as u32) ^ c1 as u32) as u64;
            c0 = (shift(&SHORT_SHIFT, c0 as u32) ^ c2 as u32) as u64;
            p = &p[SHORT * 3..];
        }
        let (chunks, rest) = p.as_chunks::<8>();
        for b in chunks {
            c0 = _mm_crc32_u64(c0, le64(b));
        }
        let mut c = c0 as u32;
        for &b in rest {
            c = _mm_crc32_u8(c, b);
        }
        !c
    }
}

#[cfg(target_arch = "aarch64")]
mod hw {
    use super::{LONG, LONG_SHIFT, SHORT, SHORT_SHIFT, le64, shift};
    use core::arch::aarch64::{__crc32cb, __crc32cd};

    #[inline]
    pub(super) fn available() -> bool {
        std::arch::is_aarch64_feature_detected!("crc")
    }

    /// # Safety
    /// The CPU must support the CRC extension (checked by [`available`]).
    #[target_feature(enable = "crc")]
    pub(super) unsafe fn crc32c(crc: u32, data: &[u8]) -> u32 {
        let mut c0 = !crc;
        let mut p = data;
        while p.len() >= LONG * 3 {
            let (mut c1, mut c2) = (0u32, 0u32);
            let mut i = 0;
            while i < LONG {
                c0 = __crc32cd(c0, le64(&p[i..]));
                c1 = __crc32cd(c1, le64(&p[LONG + i..]));
                c2 = __crc32cd(c2, le64(&p[2 * LONG + i..]));
                i += 8;
            }
            c0 = shift(&LONG_SHIFT, c0) ^ c1;
            c0 = shift(&LONG_SHIFT, c0) ^ c2;
            p = &p[LONG * 3..];
        }
        while p.len() >= SHORT * 3 {
            let (mut c1, mut c2) = (0u32, 0u32);
            let mut i = 0;
            while i < SHORT {
                c0 = __crc32cd(c0, le64(&p[i..]));
                c1 = __crc32cd(c1, le64(&p[SHORT + i..]));
                c2 = __crc32cd(c2, le64(&p[2 * SHORT + i..]));
                i += 8;
            }
            c0 = shift(&SHORT_SHIFT, c0) ^ c1;
            c0 = shift(&SHORT_SHIFT, c0) ^ c2;
            p = &p[SHORT * 3..];
        }
        let (chunks, rest) = p.as_chunks::<8>();
        for b in chunks {
            c0 = __crc32cd(c0, le64(b));
        }
        for &b in rest {
            c0 = __crc32cb(c0, b);
        }
        !c0
    }
}

/// CRC-32C of `data`, continuing from a previous result `crc` (0 to start).
#[inline]
pub(crate) fn crc32c_append(crc: u32, data: &[u8]) -> u32 {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    if hw::available() {
        // SAFETY: the required CPU feature was detected at runtime.
        return unsafe { hw::crc32c(crc, data) };
    }
    crc32c_sw(crc, data)
}

/// CRC-32C of `data`.
#[inline]
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    crc32c_append(0, data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_value() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c_sw(0, b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn paths_agree_at_every_length_and_offset() {
        let mut data = vec![0u8; 3 * 8192 * 3 + 4096];
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        for b in &mut data {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *b = x as u8;
        }
        let lens = (0..1100).chain([2303, 2304, 2305, 24575, 24576, 24577, 65532, 65536, data.len() - 8]);
        for len in lens {
            for off in [0usize, 1, 3, 7] {
                let s = &data[off..off + len];
                assert_eq!(crc32c(s), crc32c_sw(0, s), "len {len} off {off}");
            }
        }
    }

    #[test]
    fn append_is_incremental() {
        let data: Vec<u8> = (0..70_000u32).map(|i| (i * 31 % 251) as u8).collect();
        let whole = crc32c(&data);
        for split in [0, 1, 255, 8191, 24576, 40000, data.len()] {
            let (a, b) = data.split_at(split);
            assert_eq!(crc32c_append(crc32c(a), b), whole, "split {split}");
            assert_eq!(crc32c_sw(crc32c_sw(0, a), b), whole, "sw split {split}");
        }
    }
}

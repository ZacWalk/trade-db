//! Internal tests: crash simulation, fault injection and a model-based
//! randomized workload, all on the recording in-memory storage.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::batch::{OpKind, StructOp};
use crate::layout::{FORMAT_VERSION, MAGIC, META_SLOT_SIZE, RESERVED_BYTES};
use crate::storage::{FaultHandle, Op, mem_write};
use crate::{Batch, Db, Durability, Error, Options};

/// SplitMix64: small, seedable, good enough for test workloads.
pub(crate) struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Rng {
        Rng(seed)
    }
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    pub fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}

fn opts() -> Options {
    Options::new().page_size(4096).cache_size(16 * 4096)
}

fn open(h: &FaultHandle) -> Db {
    Db::from_storage(h.storage(), &opts(), None, None).unwrap()
}

type Rec = (u64, u64, u32, Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct State {
    recs: Vec<Rec>,
    meta: Vec<(String, Vec<u8>)>,
}

fn dump(db: &Db) -> State {
    let snap = db.snapshot().unwrap();
    let mut recs = Vec::new();
    for s in snap.symbols().unwrap() {
        for e in snap.scan(s, ..).unwrap() {
            let e = e.unwrap();
            recs.push((s, e.time, e.seq, e.value.to_vec()));
        }
    }
    State {
        recs,
        meta: snap.meta_entries().unwrap(),
    }
}

fn assert_clean(db: &Db) {
    let r = db.verify().unwrap();
    assert!(r.is_ok(), "verify failed: {:?}", r.problems);
}

/// Commits that exercise appends, splits across several tree levels,
/// out-of-order inserts, overwrites, range deletes, symbol drops,
/// fixed-width symbols and the metadata cursor.
fn workload(step: usize) -> Batch {
    let mut b = Batch::new();
    let s = step as u64;
    match step % 4 {
        0 => {
            for t in 0..1500u64 {
                b.put(1, s * 10_000 + t, &t.to_le_bytes());
            }
            b.define_symbol(2, 16);
            for t in 0..400u64 {
                b.put(2, t * 7 + s, &[s as u8; 16]);
            }
        }
        1 => {
            for t in (0..800u64).rev() {
                b.overwrite(1, t * 3 + s, 1, &[t as u8; 40]);
            }
            b.delete_range(2, 100..900);
        }
        2 => {
            b.truncate_before(1, s * 1_000);
            for t in 0..300u64 {
                b.put_seq(3, 5, t as u32, &[1, 2, 3]);
            }
        }
        _ => {
            b.drop_symbol(3);
            for t in 0..200u64 {
                b.delete(1, s * 10_000 + t * 5, 0);
            }
        }
    }
    b.set_meta("cursor", format!("step-{step}").as_bytes());
    b
}

struct Recorded {
    before: Vec<u8>,
    log: Vec<Op>,
    /// State before the recorded commits, then after each of them.
    states: Vec<State>,
}

fn record(durability: Durability, commits: usize) -> Recorded {
    let h = FaultHandle::default();
    let db = open(&h);
    db.write(&workload(0)).unwrap();
    db.write(&workload(1)).unwrap();
    let mut states = vec![dump(&db)];
    let before = h.start_recording();
    for step in 2..2 + commits {
        db.write_with(&workload(step), durability).unwrap();
        states.push(dump(&db));
    }
    let log = h.take_log();
    Recorded { before, log, states }
}

fn is_meta_write(op: &Op) -> bool {
    matches!(op, Op::Write(off, _) if *off < RESERVED_BYTES)
}

fn apply(image: &mut Vec<u8>, op: &Op) {
    match op {
        Op::Write(off, data) => mem_write(image, data, *off).unwrap(),
        Op::SetLen(len) => image.resize(*len as usize, 0),
        Op::Sync => {}
    }
}

/// Open a crash image and check it holds exactly one committed state in
/// `allowed`, verifies clean and keeps taking writes.
fn check_image(image: Vec<u8>, rec: &Recorded, allowed: std::ops::RangeInclusive<usize>, what: &str) {
    let h = FaultHandle::with_data(image);
    let db = Db::from_storage(h.storage(), &opts(), None, None).unwrap_or_else(|e| panic!("{what}: open failed: {e}"));
    let s = dump(&db);
    let j = rec
        .states
        .iter()
        .position(|x| *x == s)
        .unwrap_or_else(|| panic!("{what}: not a committed state"));
    assert!(allowed.contains(&j), "{what}: state {j} outside {allowed:?}");
    let r = db.verify().unwrap();
    assert!(r.is_ok(), "{what}: {:?}", r.problems);
    db.write(&workload(9))
        .unwrap_or_else(|e| panic!("{what}: write after recovery failed: {e}"));
    assert_clean(&db);
}

#[test]
fn every_process_crash_point_recovers_exactly() {
    for durability in [Durability::Immediate, Durability::Eventual, Durability::None] {
        let rec = record(durability, 4);
        let mut image = rec.before.clone();
        let mut metas = 0;
        for k in 0..=rec.log.len() {
            // The process died after `k` operations reached the OS, which
            // keeps them: the file shows exactly the commits whose meta
            // write was among them.
            check_image(
                image.clone(),
                &rec,
                metas..=metas,
                &format!("{durability:?} crash after op {k}"),
            );
            if k < rec.log.len() {
                apply(&mut image, &rec.log[k]);
                if is_meta_write(&rec.log[k]) {
                    metas += 1;
                }
            }
        }
        assert_eq!(metas, 4);
    }
}

#[test]
fn power_loss_never_leaves_a_partial_commit() {
    let mut rng = Rng::new(0x5EED_0001);
    for durability in [Durability::Immediate, Durability::Eventual] {
        let rec = record(durability, 4);
        let metas_before = |end: usize| rec.log[..end].iter().filter(|o| is_meta_write(o)).count();
        for trial in 0..1500 {
            let k = rng.below(rec.log.len() as u64 + 1) as usize;
            // Everything before the last completed barrier is on the platter.
            let barrier = rec.log[..k]
                .iter()
                .rposition(|o| matches!(o, Op::Sync))
                .map_or(0, |i| i + 1);
            let mut image = rec.before.clone();
            for op in &rec.log[..barrier] {
                apply(&mut image, op);
            }
            // After it, any subset of writes survives, some of them torn at
            // 512-byte sector granularity.
            for op in &rec.log[barrier..k] {
                match op {
                    Op::Write(off, data) if rng.chance(50) => {
                        if rng.chance(30) {
                            for (i, sector) in data.chunks(512).enumerate() {
                                if rng.chance(50) {
                                    mem_write(&mut image, sector, off + i as u64 * 512).unwrap();
                                }
                            }
                        } else {
                            mem_write(&mut image, data, *off).unwrap();
                        }
                    }
                    Op::Write(..) => {}
                    other => apply(&mut image, other),
                }
            }
            let what = format!("{durability:?} trial {trial} crash at {k}");
            check_image(image, &rec, metas_before(barrier)..=metas_before(k), &what);
        }
    }
}

#[test]
fn spilled_transactions_are_atomic_too() {
    let h = FaultHandle::default();
    let db = open(&h);
    db.write(&workload(0)).unwrap();
    let mut states = vec![dump(&db)];
    let before = h.start_recording();
    let mut b = Batch::new();
    for t in 0..120_000u64 {
        b.put(5, t, &[t as u8; 32]);
    }
    b.overwrite(1, 3, 0, b"rewritten").set_meta("cursor", b"spilled");
    db.write(&b).unwrap();
    assert!(db.stats().unwrap().spills > 0, "the batch must spill");
    states.push(dump(&db));
    let log = h.take_log();
    let rec = Recorded { before, log, states };
    let meta_at = rec.log.iter().position(is_meta_write).unwrap();
    // Sample crash points through the spills and data writes, and every
    // point around the commit.
    let mut image = rec.before.clone();
    for k in 0..=rec.log.len() {
        if k % 97 == 0 || k + 3 >= meta_at && k <= meta_at + 3 || k == rec.log.len() {
            let committed = usize::from(k > meta_at);
            check_image(
                image.clone(),
                &rec,
                committed..=committed,
                &format!("spill crash after op {k}"),
            );
        }
        if k < rec.log.len() {
            apply(&mut image, &rec.log[k]);
        }
    }
}

#[test]
fn overlapping_leaf_values_are_corruption_not_a_panic() {
    // Regression: a crafted variable leaf whose slots overlap (with a valid
    // checksum) passed validation, and a split of it underflowed.
    let h = FaultHandle::default();
    let db = open(&h);
    let mut b = Batch::new();
    for t in [10u64, 20, 30] {
        b.put(9, t, &[t as u8; 1000]);
    }
    db.write(&b).unwrap();
    let meta = db.inner.current_meta().unwrap();
    let leaf = db.inner.dir_entry(&meta, 9, true).unwrap().unwrap().root.page;
    drop(db);
    let mut img = h.data();
    let off = leaf as usize * 4096;
    {
        let p = &mut img[off..off + 4096];
        let heap = crate::layout::aux(p) as usize;
        let span = 4092 - heap;
        for i in 0..3 {
            let o = crate::layout::HDR + crate::layout::SLOT * i;
            crate::layout::wr_u16(p, o + 12, heap as u16);
            crate::layout::wr_u16(p, o + 14, span as u16);
        }
        crate::layout::stamp_crc(p);
    }
    let db = open(&FaultHandle::with_data(img));
    assert!(matches!(db.get(9, 10), Err(Error::Corrupt { .. })));
    let mut b = Batch::new();
    b.put(9, 15, &[7u8; 1000]);
    assert!(matches!(db.write(&b), Err(Error::Corrupt { .. })));
    assert!(!db.verify().unwrap().is_ok());
    // The handle is still usable for other symbols.
    let mut b = Batch::new();
    b.put(10, 1, b"fine");
    db.write(&b).unwrap();
}

#[test]
fn other_format_versions_are_refused_untouched() {
    let h = FaultHandle::default();
    let db = open(&h);
    let mut b = Batch::new();
    b.put(1, 1, b"x");
    db.write(&b).unwrap();
    drop(db);
    // Both slots now hold a header; restamp each as the next format version.
    let mut img = h.data();
    for slot in img.chunks_mut(META_SLOT_SIZE as usize).take(2) {
        assert_eq!(slot[..8], MAGIC);
        slot[8..12].copy_from_slice(&(FORMAT_VERSION + 1).to_le_bytes());
        let crc = crate::crc::crc32c(&slot[..124]);
        slot[124..128].copy_from_slice(&crc.to_le_bytes());
    }
    let h = FaultHandle::with_data(img.clone());
    let r = Db::from_storage(h.storage(), &opts(), None, None);
    assert!(matches!(r, Err(Error::UnsupportedFormat(_))));
    assert_eq!(h.data(), img);
}

#[test]
fn failed_io_unwinds_and_the_handle_keeps_working() {
    let after = {
        let h = FaultHandle::default();
        let d = open(&h);
        d.write(&workload(0)).unwrap();
        d.write(&workload(1)).unwrap();
        dump(&d)
    };
    for kind in ["write", "sync", "read"] {
        let mut n = 1;
        loop {
            let h = FaultHandle::default();
            let db = open(&h);
            db.write(&workload(0)).unwrap();
            let before = dump(&db);
            // Start from a cold cache so the transaction has to read.
            drop(db);
            let db = open(&h);
            h.arm(|s| match kind {
                "write" => s.fail_write = Some(n),
                "sync" => s.fail_sync = Some(n),
                _ => s.fail_read = Some(n),
            });
            let r = db.write(&workload(1));
            let (reads, writes, syncs) = h.counts();
            h.arm(|_| {});
            let used = match kind {
                "write" => writes,
                "sync" => syncs,
                _ => reads,
            };
            if r.is_ok() {
                assert!(n > used, "{kind} {n}: injected fault was not reported");
                break;
            }
            assert!(matches!(r, Err(Error::Io(_))), "{kind} {n}: {r:?}");
            // The handle answers with the old or the new state, never a mix,
            // and keeps working.
            let now = dump(&db);
            assert!(
                now == before || now == after,
                "{kind} {n}: handle shows a partial commit"
            );
            assert_clean(&db);
            db.write(&workload(2)).unwrap();
            assert_clean(&db);
            // The file on its own reopens consistent too.
            assert_clean(&open(&FaultHandle::with_data(h.data())));
            n += 1;
        }
        assert!(n > 2, "{kind}: the workload performed no {kind}s");
    }
}

#[test]
fn free_state_is_rebuilt_when_it_does_not_fit_inline() {
    let h = FaultHandle::default();
    let db = open(&h);
    let mut b = Batch::new();
    for s in 0..20_000u64 {
        for t in 0..10u64 {
            b.put(s, t, &[0u8; 60]);
        }
    }
    db.write(&b).unwrap();
    // Free ~10,000 scattered pages: far more than the meta slot's inline
    // free-state area can describe.
    let mut b = Batch::new();
    for s in (0..20_000u64).step_by(2) {
        b.drop_symbol(s);
    }
    db.write(&b).unwrap();
    let pages = db.stats().unwrap().page_count;
    drop(db);
    // A new handle must rebuild the free state by walking the trees, then
    // reuse those pages rather than grow the file.
    let db = open(&h);
    let mut b = Batch::new();
    for s in (0..20_000u64).step_by(2) {
        for t in 0..10u64 {
            b.put(s, t, &[1u8; 60]);
        }
    }
    db.write(&b).unwrap();
    let grown = db.stats().unwrap().page_count;
    assert!(grown <= pages + 64, "file grew from {pages} to {grown} pages");
    assert_clean(&db);
}

#[test]
fn large_batches_spill_and_still_commit_atomically() {
    let h = FaultHandle::default();
    let db = open(&h);
    let mut b = Batch::new();
    for t in 0..200_000u64 {
        b.put(t % 7, t, &t.to_le_bytes());
    }
    db.write(&b).unwrap();
    assert!(db.stats().unwrap().spills > 0, "a 16-page cache must spill");
    assert_eq!(db.count(3).unwrap(), 200_000 / 7);
    assert_clean(&db);
    // A failing spill-heavy batch leaves nothing behind.
    let mut b = Batch::new();
    for t in 0..100_000u64 {
        b.put(9, t, &[0u8; 32]);
    }
    b.put(3, 3, b"duplicate");
    assert!(db.write(&b).is_err());
    assert!(db.symbol_info(9).unwrap().is_none());
    assert_clean(&db);
    drop(db);
    assert_clean(&open(&h));
}

// ---- model-based randomized testing --------------------------------------

#[derive(Clone, Default)]
struct Model {
    recs: BTreeMap<(u64, u64, u32), Vec<u8>>,
    rs: HashMap<u64, u32>,
    known: BTreeSet<u64>,
    meta: BTreeMap<String, Vec<u8>>,
}

impl Model {
    fn range(&self, s: u64, lo: u64, hi: u64) -> Vec<(u64, u32, Vec<u8>)> {
        self.recs
            .range((s, lo, 0)..=(s, hi, u32::MAX))
            .map(|(&(_, t, q), v)| (t, q, v.clone()))
            .collect()
    }

    /// The batch semantics, independently: `None` when it must be refused
    /// as a duplicate.
    fn apply(&self, b: &Batch) -> Option<Model> {
        let mut n = self.clone();
        for op in &b.structs {
            match *op {
                StructOp::Define(s, rs) => {
                    n.rs.insert(s, rs);
                    n.known.insert(s);
                }
                StructOp::DeleteRange(s, lo, hi) => n.recs.retain(|k, _| !(k.0 == s && (lo..=hi).contains(&k.1))),
                StructOp::Drop(s) => {
                    n.recs.retain(|k, _| k.0 != s);
                    n.rs.remove(&s);
                    n.known.remove(&s);
                }
            }
        }
        for o in b.sorted_ops().iter() {
            let key = (o.symbol, o.time, o.seq);
            let v = b.data[o.off..o.off + o.len].to_vec();
            match o.kind {
                OpKind::Delete => {
                    n.recs.remove(&key);
                }
                OpKind::Overwrite => {
                    n.recs.insert(key, v);
                    n.known.insert(o.symbol);
                }
                OpKind::Put => {
                    if n.recs.insert(key, v).is_some() {
                        return None;
                    }
                    n.known.insert(o.symbol);
                }
            }
        }
        for (k, v) in &b.meta {
            let k = String::from_utf8(k.clone()).unwrap();
            match v {
                Some(v) => n.meta.insert(k, v.clone()),
                None => n.meta.remove(&k),
            };
        }
        Some(n)
    }
}

fn rand_value(rng: &mut Rng, rs: Option<u32>) -> Vec<u8> {
    let len = match rs {
        Some(n) => n as usize,
        None if rng.chance(3) => rng.below(3000) as usize,
        None => rng.below(48) as usize,
    };
    let x = rng.next() as u8;
    (0..len).map(|i| x.wrapping_add(i as u8)).collect()
}

type Row = (u64, u32, Vec<u8>);

fn rows(it: impl Iterator<Item = crate::Result<crate::Entry>>) -> Vec<Row> {
    it.map(|e| e.map(|e| (e.time, e.seq, e.value.to_vec())).unwrap())
        .collect()
}

fn check_against_model(db: &Db, m: &Model, rng: &mut Rng, full: bool) {
    let syms: Vec<u64> = m.known.iter().copied().collect();
    assert_eq!(db.symbols().unwrap(), syms);
    let horizon = m.recs.keys().map(|k| k.1).max().unwrap_or(0) + 100;
    for &s in &syms {
        let want = m.range(s, 0, u64::MAX);
        let info = db.symbol_info(s).unwrap().unwrap();
        assert_eq!(info.count, want.len() as u64, "count of {s}");
        assert_eq!(info.record_size, *m.rs.get(&s).unwrap_or(&0), "record size of {s}");
        if let (Some(f), Some(l)) = (want.first(), want.last()) {
            assert_eq!((info.min_time, info.max_time), (f.0, l.0));
        }
        let (a, b) = (rng.below(horizon), rng.below(horizon));
        let (lo, hi) = (a.min(b), a.max(b));
        let want_r = m.range(s, lo, hi);
        assert_eq!(rows(db.scan(s, lo..=hi).unwrap()), want_r, "scan {s} {lo}..={hi}");
        let mut rev = rows(db.scan_rev(s, lo..=hi).unwrap());
        rev.reverse();
        assert_eq!(rev, want_r, "scan_rev {s}");
        assert_eq!(db.count_range(s, lo..=hi).unwrap(), want_r.len() as u64);
        let t = rng.below(horizon);
        let le = m
            .recs
            .range((s, 0, 0)..=(s, t, u32::MAX))
            .next_back()
            .map(|(&(_, t, q), _)| (t, q));
        assert_eq!(db.get_le(s, t).unwrap().map(|e| (e.time, e.seq)), le, "get_le {s} {t}");
        let ge = m
            .recs
            .range((s, t, 0)..=(s, u64::MAX, u32::MAX))
            .next()
            .map(|(&(_, t, q), _)| (t, q));
        assert_eq!(db.get_ge(s, t).unwrap().map(|e| (e.time, e.seq)), ge, "get_ge {s} {t}");
        if let Some((t, q, v)) = want_r.first() {
            assert_eq!(db.get_seq(s, *t, *q).unwrap().unwrap(), &v[..]);
        }
        if let Some(&rs) = m.rs.get(&s) {
            let mut cols = crate::Columns::new();
            db.read_columns(s, lo..=hi, &mut cols).unwrap();
            assert_eq!(cols.len(), want_r.len());
            if !cols.is_empty() {
                assert_eq!(cols.record_size(), rs as usize);
            }
            for (i, (t, q, v)) in want_r.iter().enumerate() {
                assert_eq!((cols.times[i], cols.seqs[i], cols.payload(i)), (*t, *q, &v[..]));
            }
        }
        if full {
            assert_eq!(rows(db.scan(s, ..).unwrap()), want, "full scan {s}");
        }
    }
    if syms.len() >= 2 {
        let pick = [syms[0], syms[syms.len() - 1]];
        let (lo, hi) = (horizon / 4, horizon / 2);
        let mut want: Vec<(u64, u32, u64)> = Vec::new();
        for &s in &pick {
            want.extend(m.range(s, lo, hi).into_iter().map(|(t, q, _)| (t, q, s)));
        }
        want.sort();
        let got: Vec<_> = db
            .scan_multi(&pick, lo..=hi)
            .unwrap()
            .map(|e| e.map(|e| (e.time, e.seq, e.symbol)).unwrap())
            .collect();
        assert_eq!(got, want, "scan_multi");
    }
    let meta: Vec<(String, Vec<u8>)> = m.meta.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut got = db.meta_entries().unwrap();
    got.sort();
    assert_eq!(got, meta);
    if full {
        assert_clean(db);
    }
}

fn model_run(seed: u64, steps: usize) {
    let mut rng = Rng::new(seed);
    let h = FaultHandle::default();
    let mut db = open(&h);
    let mut m = Model::default();
    for step in 0..steps {
        let mut b = Batch::new();
        let s = rng.below(6);
        let mut rs = m.rs.clone();
        match rng.below(100) {
            0..6 if !m.recs.keys().any(|k| k.0 == s) => {
                let size = [8u32, 24, 100][rng.below(3) as usize];
                b.define_symbol(s, size);
                rs.insert(s, size);
            }
            6..10 => {
                b.drop_symbol(s);
                rs.remove(&s);
            }
            10..18 => {
                let a = rng.below(30_000);
                b.delete_range(s, a..=a + rng.below(8_000));
            }
            18..22 => {
                b.truncate_before(s, rng.below(30_000));
            }
            _ => {}
        }
        let n = if rng.chance(10) { 2000 } else { rng.below(200) };
        let base = m
            .recs
            .range((s, 0, 0)..=(s, u64::MAX, u32::MAX))
            .next_back()
            .map_or(0, |(k, _)| k.1);
        for i in 0..n {
            let sym = if rng.chance(70) { s } else { rng.below(6) };
            let size = rs.get(&sym).copied();
            let t = match rng.below(10) {
                0..6 => base + i + 1,
                6 => base.saturating_sub(rng.below(50)),
                _ => rng.below(30_000),
            };
            let q = if rng.chance(15) { rng.below(4) as u32 } else { 0 };
            match rng.below(10) {
                0 => {
                    b.delete(sym, t, q);
                }
                1 | 2 => {
                    b.overwrite(sym, t, q, &rand_value(&mut rng, size));
                }
                _ => {
                    b.put_seq(sym, t, q, &rand_value(&mut rng, size));
                }
            }
        }
        if rng.chance(20) {
            let k = format!("k{}", rng.below(4));
            if rng.chance(25) {
                b.remove_meta(&k);
            } else {
                let len = rng.below(40) as u32;
                b.set_meta(&k, &rand_value(&mut rng, Some(len)));
            }
        }
        let want = m.apply(&b);
        let d = if rng.chance(50) {
            Durability::Immediate
        } else {
            Durability::Eventual
        };
        let r = db.write_with(&b, d);
        match want {
            None => assert!(
                matches!(r, Err(Error::DuplicateKey { .. })),
                "seed {seed:#x} step {step}: expected a duplicate, got {r:?}"
            ),
            Some(next) => {
                r.unwrap_or_else(|e| panic!("seed {seed:#x} step {step}: {e}"));
                m = next;
            }
        }
        if rng.chance(5) {
            drop(db);
            db = open(&h);
        }
        check_against_model(&db, &m, &mut rng, step % 5 == 4);
    }
    check_against_model(&db, &m, &mut rng, true);
}

#[test]
fn randomized_model_equivalence() {
    let seeds = std::env::var("TRADEDB_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6u64);
    for i in 0..seeds {
        model_run(0xC0FFEE + i * 7919, 150);
    }
}

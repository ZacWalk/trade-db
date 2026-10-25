//! Behaviour through the public API, on real files.

use std::sync::Arc;
use std::time::Duration;

use tradedb::{Batch, Columns, Db, Durability, Error, Options};

const A: u64 = 0xA;
const B: u64 = 0xB;

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn small(dir: &tempfile::TempDir, name: &str) -> Db {
    Options::new()
        .page_size(4096)
        .cache_size(1 << 20)
        .open(dir.path().join(name))
        .unwrap()
}

fn val(t: u64) -> [u8; 8] {
    t.to_le_bytes()
}

fn put_range(db: &Db, symbol: u64, times: impl IntoIterator<Item = u64>) {
    let mut b = Batch::new();
    for t in times {
        b.put(symbol, t, &val(t));
    }
    db.write(&b).unwrap();
}

fn times(db: &Db, symbol: u64, range: impl std::ops::RangeBounds<u64>) -> Vec<u64> {
    db.scan(symbol, range).unwrap().map(|e| e.unwrap().time).collect()
}

#[test]
fn put_get_and_persist_across_reopen() {
    let dir = tmp();
    let path = dir.path().join("p.tradedb");
    {
        let db = Db::open(&path).unwrap();
        let mut b = Batch::new();
        b.put(A, 100, b"hello").put(A, 200, b"world").put(B, 100, b"other");
        let txn = db.write(&b).unwrap();
        assert!(txn > 1);
        assert_eq!(db.get(A, 100).unwrap().unwrap(), b"hello");
        assert!(db.get(A, 150).unwrap().is_none());
        assert!(db.get(0xC, 100).unwrap().is_none());
    }
    let db = Db::open(&path).unwrap();
    assert_eq!(db.get(A, 200).unwrap().unwrap(), b"world");
    assert_eq!(db.get(B, 100).unwrap().unwrap(), b"other");
    assert_eq!(db.symbols().unwrap(), vec![A, B]);
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn scans_are_ordered_bounded_and_reversible() {
    let dir = tmp();
    let db = small(&dir, "s");
    // Out of order and across many leaves.
    let mut ts: Vec<u64> = (0..5000).map(|i| i * 10).collect();
    ts.reverse();
    put_range(&db, A, ts);
    put_range(&db, B, [5, 15]);
    assert_eq!(times(&db, A, ..).len(), 5000);
    assert_eq!(times(&db, A, 100..=150), vec![100, 110, 120, 130, 140, 150]);
    assert_eq!(times(&db, A, 100..150), vec![100, 110, 120, 130, 140]);
    assert_eq!(times(&db, A, 101..109), Vec::<u64>::new());
    assert_eq!(times(&db, A, 49_990..), vec![49_990]);
    assert!(times(&db, A, 60_000..).is_empty());
    let rev: Vec<u64> = db.scan_rev(A, 100..=150).unwrap().map(|e| e.unwrap().time).collect();
    assert_eq!(rev, vec![150, 140, 130, 120, 110, 100]);
    let last3: Vec<u64> = db.scan_rev(A, ..).unwrap().take(3).map(|e| e.unwrap().time).collect();
    assert_eq!(last3, vec![49_990, 49_980, 49_970]);
    assert_eq!(times(&db, B, ..), vec![5, 15]);
    // A reversed range is a caller mistake, an empty one is not.
    #[allow(clippy::reversed_empty_ranges)]
    let reversed = db.scan(A, 10..=5);
    assert!(matches!(reversed, Err(Error::InvalidArgument(_))));
    assert_eq!(times(&db, A, 5..5), Vec::<u64>::new());
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn chunks_cover_the_range_exactly() {
    let dir = tmp();
    let db = small(&dir, "c");
    put_range(&db, A, 0..3000);
    for reverse in [false, true] {
        let mut s = if reverse {
            db.scan_rev(A, 250..=2750).unwrap()
        } else {
            db.scan(A, 250..=2750).unwrap()
        };
        let mut got = Vec::new();
        // Mix single steps and chunks.
        got.push(s.next().unwrap().unwrap().time);
        while let Some(c) = s.next_chunk() {
            let c = c.unwrap();
            let mut ts: Vec<u64> = (0..c.len()).map(|i| c.time(i)).collect();
            if reverse {
                ts.reverse();
            }
            for (i, t) in ts.iter().enumerate() {
                let idx = if reverse { c.len() - 1 - i } else { i };
                assert_eq!(c.value(idx), &val(*t));
            }
            got.extend(ts);
        }
        let mut want: Vec<u64> = (250..=2750).collect();
        if reverse {
            want.reverse();
        }
        assert_eq!(got, want, "reverse={reverse}");
    }
}

#[test]
fn seq_breaks_ties_and_as_of_lookups() {
    let db = Db::memory();
    let mut b = Batch::new();
    for seq in [2u32, 0, 1] {
        b.put_seq(A, 1000, seq, &[seq as u8]);
    }
    b.put(A, 900, b"before").put(A, 1100, b"after");
    db.write(&b).unwrap();
    let all: Vec<(u64, u32)> = db
        .scan(A, ..)
        .unwrap()
        .map(|e| e.map(|e| (e.time, e.seq)).unwrap())
        .collect();
    assert_eq!(all, vec![(900, 0), (1000, 0), (1000, 1), (1000, 2), (1100, 0)]);
    assert_eq!(db.get_seq(A, 1000, 1).unwrap().unwrap(), [1u8]);
    // As-of takes the highest seq at the time; get_ge the lowest.
    let e = db.get_le(A, 1000).unwrap().unwrap();
    assert_eq!((e.time, e.seq), (1000, 2));
    let e = db.get_le(A, 1050).unwrap().unwrap();
    assert_eq!((e.time, e.seq), (1000, 2));
    let e = db.get_ge(A, 950).unwrap().unwrap();
    assert_eq!((e.time, e.seq), (1000, 0));
    assert!(db.get_le(A, 899).unwrap().is_none());
    assert!(db.get_ge(A, 1101).unwrap().is_none());
    assert_eq!(db.first(A).unwrap().unwrap().value, b"before");
    assert_eq!(db.last(A).unwrap().unwrap().value, b"after");
    assert_eq!(db.count_range(A, 1000..=1000).unwrap(), 3);
}

#[test]
fn as_of_crosses_leaf_boundaries() {
    let dir = tmp();
    let db = small(&dir, "asof");
    put_range(&db, A, (0..20_000).map(|i| i * 2));
    for t in [1u64, 999, 1001, 20_001, 39_997] {
        assert_eq!(db.get_le(A, t).unwrap().unwrap().time, t - 1, "le {t}");
        assert_eq!(db.get_ge(A, t).unwrap().unwrap().time, t + 1, "ge {t}");
    }
    assert_eq!(db.get_le(A, u64::MAX).unwrap().unwrap().time, 39_998);
}

#[test]
fn statistics_are_cached_and_counted() {
    let dir = tmp();
    let db = small(&dir, "stats");
    assert!(db.symbol_info(A).unwrap().is_none());
    assert_eq!(db.max_time(A).unwrap(), None);
    put_range(&db, A, (1..=30_000).map(|i| i * 3));
    let i = db.symbol_info(A).unwrap().unwrap();
    assert_eq!((i.min_time, i.max_time, i.count), (3, 90_000, 30_000));
    assert!(i.leaf_count > 100);
    assert_eq!(db.max_times(&[A, B]).unwrap(), vec![Some(90_000), None]);
    assert_eq!(db.count_range(A, ..).unwrap(), 30_000);
    assert_eq!(db.count_range(A, 3..=30).unwrap(), 10);
    assert_eq!(db.count_range(A, 31..33).unwrap(), 0);
    assert_eq!(db.count_range(A, 45_000..).unwrap(), 15_001);
    let t0 = db.txn_id().unwrap();
    put_range(&db, A, [1]);
    assert_eq!(db.min_time(A).unwrap(), Some(1));
    assert!(db.txn_id().unwrap() > t0);
    assert_eq!(db.stats().unwrap().symbol_count, 1);
}

#[test]
fn duplicate_keys_roll_back_the_whole_batch() {
    let db = Db::memory();
    put_range(&db, A, [10, 20]);
    let t0 = db.txn_id().unwrap();
    let mut b = Batch::new();
    b.put(B, 1, b"x").put(A, 15, b"y").put(A, 20, b"dup");
    match db.write(&b) {
        Err(Error::DuplicateKey { symbol, time, seq }) => {
            assert_eq!((symbol, time, seq), (A, 20, 0))
        }
        other => panic!("expected a duplicate, got {other:?}"),
    }
    assert_eq!(db.txn_id().unwrap(), t0);
    assert!(db.get(B, 1).unwrap().is_none());
    assert!(db.get(A, 15).unwrap().is_none());
    // Duplicates inside one batch are rejected too.
    let mut b = Batch::new();
    b.put(B, 1, b"x").put(B, 1, b"x");
    assert!(matches!(db.write(&b), Err(Error::DuplicateKey { .. })));
    // The handle keeps working afterwards.
    put_range(&db, B, [1]);
    assert_eq!(db.count(B).unwrap(), 1);
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn overwrites_deletes_and_last_one_wins() {
    let dir = tmp();
    let db = small(&dir, "ow");
    put_range(&db, A, 0..2000);
    let mut b = Batch::new();
    b.overwrite(A, 5, 0, b"first").overwrite(A, 5, 0, b"second");
    // Growing a value past what fits in place.
    b.overwrite(A, 6, 0, &[7u8; 1500]);
    b.overwrite(A, 9999, 0, b"new"); // overwrite of an absent key inserts
    b.delete(A, 7, 0).delete(A, 123_456, 0).delete(0xDEAD, 1, 0);
    db.write(&b).unwrap();
    assert_eq!(db.get(A, 5).unwrap().unwrap(), b"second");
    assert_eq!(db.get(A, 6).unwrap().unwrap().len(), 1500);
    assert_eq!(db.get(A, 9999).unwrap().unwrap(), b"new");
    assert!(db.get(A, 7).unwrap().is_none());
    assert_eq!(db.count(A).unwrap(), 2000);
    // Deleting an unknown symbol does not create it.
    assert!(db.symbol_info(0xDEAD).unwrap().is_none());
    // Delete everything one by one, then the symbol is empty but present.
    let mut b = Batch::new();
    for t in times(&db, A, ..) {
        b.delete(A, t, 0);
    }
    db.write(&b).unwrap();
    let i = db.symbol_info(A).unwrap().unwrap();
    assert_eq!((i.count, i.leaf_count, i.min_time, i.max_time), (0, 0, 0, 0));
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn range_deletes_retention_and_drop() {
    let dir = tmp();
    let db = small(&dir, "ret");
    put_range(&db, A, 0..50_000);
    put_range(&db, B, 0..100);
    assert_eq!(db.delete_range(A, 10_000..20_000).unwrap(), 10_000);
    assert_eq!(db.truncate_before(A, 5_000).unwrap(), 5_000);
    assert_eq!(db.delete_range(A, 49_990..).unwrap(), 10);
    assert_eq!(db.delete_range(A, 30_000..=30_000).unwrap(), 1);
    assert_eq!(db.delete_range(0xC, ..).unwrap(), 0);
    assert_eq!(db.count(A).unwrap(), 50_000 - 10_000 - 5_000 - 10 - 1);
    assert_eq!(db.min_time(A).unwrap(), Some(5_000));
    assert_eq!(db.max_time(A).unwrap(), Some(49_989));
    assert_eq!(times(&db, A, 9_998..20_002), vec![9_998, 9_999, 20_000, 20_001]);
    assert_eq!(db.count_range(A, 29_999..=30_001).unwrap(), 2);
    assert!(db.verify().unwrap().is_ok());
    // Replace a range atomically: delete then put in one batch.
    let mut b = Batch::new();
    b.delete_range(B, 10..20);
    b.put(B, 15, b"restated");
    db.write(&b).unwrap();
    assert_eq!(times(&db, B, 9..=20), vec![9, 15, 20]);
    assert!(db.drop_symbol(A).unwrap());
    assert!(!db.drop_symbol(A).unwrap());
    assert!(db.symbol_info(A).unwrap().is_none());
    assert_eq!(db.symbols().unwrap(), vec![B]);
    let r = db.verify().unwrap();
    assert!(r.is_ok(), "{:?}", r.problems);
    assert_eq!(r.records, 100 - 10 + 1);
}

#[test]
fn freed_pages_are_reused() {
    let dir = tmp();
    let db = small(&dir, "reuse");
    for round in 0..20u64 {
        put_range(&db, A, (0..5_000).map(|i| round * 1_000_000 + i));
        db.truncate_before(A, round * 1_000_000).unwrap();
    }
    let pages = db.stats().unwrap().page_count;
    for round in 20..40u64 {
        put_range(&db, A, (0..5_000).map(|i| round * 1_000_000 + i));
        db.truncate_before(A, round * 1_000_000).unwrap();
    }
    let after = db.stats().unwrap().page_count;
    assert!(after <= pages + 8, "file kept growing: {pages} -> {after} pages");
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn ingest_cursor_commits_with_the_data() {
    let dir = tmp();
    let path = dir.path().join("cursor");
    {
        let db = Db::open(&path).unwrap();
        let mut b = Batch::new();
        b.put(A, 1, b"tick").set_meta("cursor", b"seq-1");
        db.write(&b).unwrap();
        // A failed batch does not move the cursor either.
        let mut b = Batch::new();
        b.put(A, 1, b"dup").set_meta("cursor", b"seq-2");
        assert!(db.write(&b).is_err());
        assert_eq!(db.get_meta("cursor").unwrap().unwrap(), b"seq-1");
        let mut b = Batch::new();
        b.set_meta("other", b"").remove_meta("missing");
        db.write(&b).unwrap();
    }
    let db = Db::open(&path).unwrap();
    assert_eq!(db.get_meta("cursor").unwrap().unwrap(), b"seq-1");
    assert_eq!(db.get_meta("other").unwrap().unwrap(), b"");
    assert_eq!(db.meta_entries().unwrap().len(), 2);
    let mut b = Batch::new();
    b.remove_meta("cursor");
    db.write(&b).unwrap();
    assert!(db.get_meta("cursor").unwrap().is_none());
    let mut b = Batch::new();
    b.set_meta("big", &vec![0u8; tradedb::META_CAPACITY]);
    assert!(matches!(db.write(&b), Err(Error::MetaFull { .. })));
}

#[test]
fn fixed_width_symbols_read_as_columns() {
    let dir = tmp();
    let db = small(&dir, "fixed");
    #[derive(Debug, PartialEq, Clone, Copy)]
    struct Bar {
        o: f64,
        h: f64,
        l: f64,
        c: f64,
        v: f64,
    }
    let enc = |b: &Bar| -> [u8; 40] {
        let mut out = [0u8; 40];
        for (i, x) in [b.o, b.h, b.l, b.c, b.v].iter().enumerate() {
            out[i * 8..i * 8 + 8].copy_from_slice(&x.to_le_bytes());
        }
        out
    };
    db.define_symbol(A, 40).unwrap();
    db.define_symbol(A, 40).unwrap(); // repeating is harmless
    let mut b = Batch::new();
    for t in (0..10_000u64).rev() {
        let x = t as f64;
        b.put(
            A,
            t * 60,
            &enc(&Bar {
                o: x,
                h: x + 1.0,
                l: x - 1.0,
                c: x + 0.5,
                v: 100.0,
            }),
        );
    }
    db.write(&b).unwrap();
    assert_eq!(db.symbol_info(A).unwrap().unwrap().record_size, 40);
    let mut b = Batch::new();
    b.put(A, 1, &[0u8; 39]);
    assert!(matches!(
        db.write(&b),
        Err(Error::RecordSize {
            expected: 40,
            actual: 39,
            ..
        })
    ));
    assert!(matches!(db.define_symbol(A, 48), Err(Error::InvalidArgument(_))));
    let mut cols = Columns::new();
    let n = db.read_columns(A, 600..=6_000, &mut cols).unwrap();
    assert_eq!(n, 91);
    assert_eq!(cols.times[0], 600);
    assert_eq!(*cols.times.last().unwrap(), 6_000);
    assert_eq!(
        cols.payload(0),
        &enc(&Bar {
            o: 10.0,
            h: 11.0,
            l: 9.0,
            c: 10.5,
            v: 100.0
        })
    );
    // Appends; a variable-width symbol is refused.
    db.read_columns(A, 0..=0, &mut cols).unwrap();
    assert_eq!(cols.len(), 92);
    put_range(&db, B, [1]);
    assert!(matches!(
        db.read_columns(B, .., &mut cols),
        Err(Error::InvalidArgument(_))
    ));
    // Chunks expose the payload column directly.
    let mut s = db.scan(A, ..).unwrap();
    let c = s.next_chunk().unwrap().unwrap();
    assert_eq!(c.record_size(), Some(40));
    assert_eq!(c.payloads().unwrap().len(), c.len() * 40);
    // A populated variable symbol cannot change layout; an empty one can.
    assert!(db.define_symbol(B, 8).is_err());
    db.delete_range(B, ..).unwrap();
    db.define_symbol(B, 8).unwrap();
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn merged_replay_orders_by_time_seq_symbol() {
    let dir = tmp();
    let db = small(&dir, "multi");
    let syms = [3u64, 1, 2];
    let mut b = Batch::new();
    for (k, &s) in syms.iter().enumerate() {
        for t in 0..2_000u64 {
            if !(t + k as u64).is_multiple_of(3) {
                b.put(s, t, &[s as u8]);
            }
        }
        b.put_seq(s, 5_000, 1, &[s as u8]);
        b.put_seq(s, 5_000, 0, &[s as u8]);
    }
    db.write(&b).unwrap();
    let got: Vec<(u64, u32, u64)> = db
        .scan_multi(&syms, 100..=5_000)
        .unwrap()
        .map(|e| e.map(|e| (e.time, e.seq, e.symbol)).unwrap())
        .collect();
    let mut want = Vec::new();
    for t in 100..2_000u64 {
        for (k, s) in [(1usize, 1u64), (2, 2), (0, 3)] {
            if !(t + k as u64).is_multiple_of(3) {
                want.push((t, 0, s));
            }
        }
    }
    for seq in 0..2 {
        for s in 1..=3 {
            want.push((5_000, seq, s));
        }
    }
    assert_eq!(got, want);
    assert_eq!(db.scan_multi(&[], ..).unwrap().count(), 0);
    assert_eq!(db.scan_multi(&[A, A], ..).unwrap().count(), 0);
    assert_eq!(db.scan_multi(&[1, 1], 0..=1).unwrap().count(), 2 * 2);
}

#[test]
fn snapshots_are_stable_while_writers_continue() {
    let dir = tmp();
    let db = small(&dir, "snap");
    put_range(&db, A, 0..10_000);
    let snap = db.snapshot().unwrap();
    let mut scan = snap.scan(A, ..).unwrap();
    let first: Vec<u64> = scan.by_ref().take(10).map(|e| e.unwrap().time).collect();
    // Rewrite everything underneath the open scan, many times over, so the
    // writer would love to recycle the snapshot's pages.
    for round in 0..5u64 {
        let mut b = Batch::new();
        for t in 0..10_000 {
            b.overwrite(A, t, 0, &(t + round * 1_000_000).to_le_bytes());
        }
        db.write(&b).unwrap();
        db.truncate_before(A, 1).unwrap();
    }
    let rest: Vec<(u64, Vec<u8>)> = scan.map(|e| e.map(|e| (e.time, e.value.to_vec())).unwrap()).collect();
    assert_eq!(first, (0..10).collect::<Vec<_>>());
    assert_eq!(rest.len(), 9_990);
    assert!(
        rest.iter().all(|(t, v)| v == &t.to_le_bytes()),
        "snapshot saw a later write"
    );
    assert_eq!(snap.count(A).unwrap(), 10_000);
    assert_eq!(db.count(A).unwrap(), 9_999);
    assert!(snap.get(A, 0).unwrap().is_some());
    assert!(db.get(A, 0).unwrap().is_none());
    assert_eq!(db.stats().unwrap().open_snapshots, 1);
    drop(snap);
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn readers_and_writer_in_parallel_threads() {
    let dir = tmp();
    let db = Arc::new(small(&dir, "threads"));
    put_range(&db, A, 0..1_000);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut scans = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut prev = None;
                    for e in db.scan(A, ..).unwrap() {
                        let e = e.unwrap();
                        assert!(prev < Some(e.time), "went backwards");
                        assert_eq!(&*e.value, &e.time.to_le_bytes()[..]);
                        prev = Some(e.time);
                    }
                    let snap = db.snapshot().unwrap();
                    let n = snap.count(A).unwrap();
                    assert_eq!(snap.scan(A, ..).unwrap().count() as u64, n);
                    scans += 1;
                }
                scans
            })
        })
        .collect();
    for round in 1..200u64 {
        let base = round * 1_000;
        put_range(&db, A, base..base + 1_000);
        if round % 10 == 0 {
            db.truncate_before(A, base - 5_000).unwrap();
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn read_only_handles_refuse_writes() {
    let dir = tmp();
    let path = dir.path().join("ro");
    {
        let db = Db::open(&path).unwrap();
        put_range(&db, A, 0..10);
    }
    let db = Db::open_read_only(&path).unwrap();
    let t0 = db.txn_id().unwrap();
    assert_eq!(db.count(A).unwrap(), 10);
    let mut b = Batch::new();
    b.put(A, 99, b"x");
    assert!(matches!(db.write(&b), Err(Error::ReadOnly)));
    assert!(matches!(db.define_symbol(B, 8), Err(Error::ReadOnly)));
    assert!(matches!(db.truncate_before(A, 5), Err(Error::ReadOnly)));
    assert!(matches!(db.drop_symbol(A), Err(Error::ReadOnly)));
    db.backup(dir.path().join("ro-copy")).unwrap();
    assert_eq!(db.txn_id().unwrap(), t0);
    assert!(Db::open_read_only(dir.path().join("missing")).is_err());
}

#[test]
fn boundary_keys_and_values() {
    let dir = tmp();
    let db = Db::open(dir.path().join("edge")).unwrap();
    let max = db.max_value_len();
    let mut b = Batch::new();
    b.put(0, 0, b"")
        .put(u64::MAX, u64::MAX, b"max")
        .put_seq(1, u64::MAX, u32::MAX, &vec![9u8; max]);
    db.write(&b).unwrap();
    assert_eq!(db.get(0, 0).unwrap().unwrap().len(), 0);
    assert_eq!(db.get(u64::MAX, u64::MAX).unwrap().unwrap(), b"max");
    assert_eq!(db.get_seq(1, u64::MAX, u32::MAX).unwrap().unwrap().len(), max);
    assert_eq!(times(&db, u64::MAX, u64::MAX..), vec![u64::MAX]);
    assert_eq!(db.get_le(1, u64::MAX).unwrap().unwrap().seq, u32::MAX);
    let mut b = Batch::new();
    b.put(2, 1, &vec![0u8; max + 1]);
    assert!(matches!(db.write(&b), Err(Error::ValueTooLarge { .. })));
    // Values of every size around a leaf's capacity.
    let mut b = Batch::new();
    for (i, len) in [max, max - 1, 1, max / 2, max / 2 + 1, 0, max].iter().enumerate() {
        b.put(3, i as u64, &vec![i as u8; *len]);
    }
    db.write(&b).unwrap();
    for (i, len) in [max, max - 1, 1, max / 2, max / 2 + 1, 0, max].iter().enumerate() {
        assert_eq!(db.get(3, i as u64).unwrap().unwrap().len(), *len);
    }
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn foreign_files_are_left_alone() {
    let dir = tmp();
    let text = dir.path().join("notes.txt");
    std::fs::write(&text, b"just some text, definitely not a database").unwrap();
    assert!(matches!(Db::open(&text), Err(Error::NotADatabase(_))));
    assert_eq!(
        std::fs::read(&text).unwrap(),
        b"just some text, definitely not a database"
    );
    // An empty file is formatted.
    let e = dir.path().join("empty");
    std::fs::write(&e, b"").unwrap();
    Db::open(&e).unwrap();
}

#[test]
fn a_file_opens_once_per_process() {
    let dir = tmp();
    let path = dir.path().join("once");
    let db = Db::open(&path).unwrap();
    // path() reports the path as given (absolute), not a canonical form.
    assert_eq!(db.path().unwrap(), std::path::absolute(&path).unwrap());
    assert!(matches!(Db::open(&path), Err(Error::AlreadyOpen(_))));
    assert!(matches!(Db::open_read_only(&path), Err(Error::AlreadyOpen(_))));
    drop(db);
    Db::open(&path).unwrap();
}

#[test]
fn damaged_pages_are_reported_not_returned() {
    let dir = tmp();
    let path = dir.path().join("rot");
    {
        let db = small(&dir, "rot");
        put_range(&db, A, 0..5_000);
    }
    // Flip one byte in every data page past the meta slots.
    let mut bytes = std::fs::read(&path).unwrap();
    let start = 128 * 1024;
    for p in (start..bytes.len()).step_by(4096) {
        bytes[p + 100] ^= 0x40;
    }
    std::fs::write(&path, &bytes).unwrap();
    let db = Db::open(&path).unwrap();
    assert!(matches!(db.get(A, 10), Err(Error::Corrupt { .. })));
    let mut s = db.scan(A, ..).unwrap();
    assert!(matches!(s.next(), Some(Err(Error::Corrupt { .. }))));
    assert!(s.next().is_none());
    let r = db.verify().unwrap();
    assert!(!r.is_ok());
    assert!(!r.problems.is_empty());
}

#[test]
fn backup_is_a_consistent_database() {
    let dir = tmp();
    let path = dir.path().join("src");
    let db = Db::open(&path).unwrap();
    put_range(&db, A, 0..20_000);
    let mut b = Batch::new();
    b.set_meta("cursor", b"42");
    db.write(&b).unwrap();
    assert!(matches!(db.backup(&path), Err(Error::InvalidArgument(_))));
    let copy = dir.path().join("copy");
    db.backup(&copy).unwrap();
    put_range(&db, A, [99_999]);
    let c = Db::open(&copy).unwrap();
    assert_eq!(c.count(A).unwrap(), 20_000);
    assert_eq!(c.get_meta("cursor").unwrap().unwrap(), b"42");
    assert!(c.verify().unwrap().is_ok());
    // The copy takes writes (its free state is rebuilt on the first one).
    put_range(&c, B, 0..10);
    assert!(c.verify().unwrap().is_ok());
}

#[test]
fn utf8_paths() {
    let dir = tmp();
    let path = dir.path().join("données-ü-日本.tradedb");
    let db = Db::open(&path).unwrap();
    put_range(&db, A, [1]);
    db.backup(dir.path().join("копия.tradedb")).unwrap();
    drop(db);
    assert!(path.exists());
    assert_eq!(Db::open(&path).unwrap().count(A).unwrap(), 1);
}

#[test]
fn durability_modes_all_commit_atomically() {
    let dir = tmp();
    let path = dir.path().join("dur");
    {
        let db = Options::new().durability(Durability::Eventual).open(&path).unwrap();
        put_range(&db, A, 0..100);
        let mut b = Batch::new();
        b.put(A, 100, b"x");
        db.write_with(&b, Durability::None).unwrap();
        db.write_with(Batch::new().put(A, 101, b"y"), Durability::Immediate)
            .unwrap();
        db.sync().unwrap();
    }
    let db = Db::open(&path).unwrap();
    assert_eq!(db.count(A).unwrap(), 102);
}

#[test]
fn wait_for_txn_wakes_on_commit() {
    let db = Arc::new(Db::memory());
    let t0 = db.txn_id().unwrap();
    assert_eq!(db.wait_for_txn(t0, Duration::ZERO).unwrap(), None);
    let w = db.clone();
    let h = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        put_range(&w, A, [1]);
    });
    // Duration::MAX means "wait for ever" and must not overflow.
    let t = db.wait_for_txn(t0, Duration::MAX).unwrap();
    assert!(t.unwrap() > t0);
    h.join().unwrap();
}

#[test]
fn several_range_deletes_in_one_batch() {
    // Regression: a second range delete on a symbol whose pages the first
    // had already copied mistook the changed child for an unchanged one,
    // leaving the cached count, min and max stale.
    let dir = tmp();
    let db = small(&dir, "ranges");
    put_range(&db, A, 0..10_000);
    let mut b = Batch::new();
    b.delete_range(A, 10..=20).delete_range(A, 30..=40);
    db.write(&b).unwrap();
    assert_eq!(db.count(A).unwrap(), 9_978);
    assert_eq!(db.scan(A, ..).unwrap().count(), 9_978);
    let mut b = Batch::new();
    b.delete_range(A, 9_990..).delete_range(A, 9_980..9_990);
    db.write(&b).unwrap();
    assert_eq!(db.max_time(A).unwrap(), Some(9_979));
    assert_eq!(db.last(A).unwrap().unwrap().time, 9_979);
    assert_eq!(db.count_range(A, 9_900..).unwrap(), 80);
    // Retention plus a correction across a three-level tree.
    put_range(&db, B, 0..60_000);
    let mut b = Batch::new();
    b.truncate_before(B, 1_000)
        .delete_range(B, 20_000..=29_999)
        .delete_range(B, 30_000..=30_000);
    db.write(&b).unwrap();
    assert_eq!(db.count(B).unwrap(), 60_000 - 1_000 - 10_001);
    assert_eq!(db.count_range(B, ..).unwrap(), db.scan(B, ..).unwrap().count() as u64);
    // Point edits after a structural op in the same batch move min/max.
    let mut b = Batch::new();
    b.delete_range(A, 5_000..6_000)
        .delete(A, 0, 0)
        .put(A, 20_000, b"new max");
    db.write(&b).unwrap();
    assert_eq!(db.min_time(A).unwrap(), Some(1));
    assert_eq!(db.max_time(A).unwrap(), Some(20_000));
    let r = db.verify().unwrap();
    assert!(r.is_ok(), "{:?}", r.problems);
    // Everything left can still be removed one record at a time.
    let mut b = Batch::new();
    for t in times(&db, A, ..) {
        b.delete(A, t, 0);
    }
    db.write(&b).unwrap();
    assert_eq!(db.count(A).unwrap(), 0);
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn snapshots_and_reads_race_cleanly_with_commits() {
    // Regression: a meta read racing a commit that rewrote the slot's user
    // metadata fell back to an older slot or failed with "no valid meta
    // slot". Default options (multi-process mode), no fsync, the cursor
    // changing on every commit.
    let dir = tmp();
    let db = Arc::new(
        Options::new()
            .page_size(4096)
            .durability(Durability::None)
            .open(dir.path().join("race"))
            .unwrap(),
    );
    put_range(&db, A, 0..1_000);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                let mut n = 0u64;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let snap = db.snapshot().unwrap();
                    let cursor = u64::from_le_bytes(
                        snap.get_meta("cursor")
                            .unwrap()
                            .unwrap_or(vec![0; 8])
                            .try_into()
                            .unwrap(),
                    );
                    for e in snap.scan(A, ..).unwrap() {
                        let e = e.unwrap();
                        assert_eq!(&*e.value, &e.time.to_le_bytes());
                    }
                    if cursor > 0 {
                        assert_eq!(snap.max_time(A).unwrap(), Some(cursor));
                    }
                    drop(snap);
                    db.last(A).unwrap();
                    db.txn_id().unwrap();
                    n += 1;
                }
                n
            })
        })
        .collect();
    let start = std::time::Instant::now();
    let mut t = 1_000u64;
    while start.elapsed() < Duration::from_millis(1500) {
        let mut b = Batch::new();
        for _ in 0..20 {
            b.put(A, t, &t.to_le_bytes());
            t += 1;
        }
        b.truncate_before(A, t - 1_000)
            .set_meta("cursor", &(t - 1).to_le_bytes());
        db.write(&b).unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }
    assert!(db.verify().unwrap().is_ok());
}

#[test]
fn every_page_size_works() {
    for ps in [4096u32, 8192, 16384, 32768, 65536] {
        let db = Options::new().page_size(ps).open_memory().unwrap();
        assert_eq!(db.page_size(), ps);
        put_range(&db, A, (0..20_000).rev());
        db.define_symbol(B, 24).unwrap();
        let mut b = Batch::new();
        for t in 0..20_000u64 {
            b.put(B, t, &[t as u8; 24]);
        }
        db.write(&b).unwrap();
        assert_eq!(times(&db, A, ..).len(), 20_000);
        assert_eq!(db.count_range(B, 100..200).unwrap(), 100);
        assert!(db.verify().unwrap().is_ok(), "page size {ps}");
    }
    assert!(Options::new().page_size(1000).open_memory().is_err());
    assert!(Options::new().page_size(131_072).open_memory().is_err());
}

#[test]
fn prefetch_and_stats() {
    let dir = tmp();
    let db = small(&dir, "pf");
    put_range(&db, A, 0..10_000);
    assert!(db.prefetch(A, ..).unwrap() > 0);
    let s = db.stats().unwrap();
    assert_eq!(s.page_size, 4096);
    assert!(s.cache_used > 0 && s.cache_used <= s.cache_capacity);
}

#[test]
fn exclusive_open_excludes_and_reads_from_memory() {
    let dir = tmp();
    let path = dir.path().join("excl");
    let db = Options::new().exclusive(true).open(&path).unwrap();
    put_range(&db, A, 0..1000);
    assert_eq!(db.count(A).unwrap(), 1000);
    assert!(db.verify().unwrap().is_ok());
    assert!(
        Options::new()
            .read_only(true)
            .exclusive(true)
            .open(dir.path().join("x"))
            .is_err()
    );
}

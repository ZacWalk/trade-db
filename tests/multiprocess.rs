//! Real multi-process behaviour: a child process writes while the parent
//! reads, through OS byte-range locks.
//!
//! The test binary re-runs itself as the child, selected by an environment
//! variable, so no helper executable is needed.

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tradedb::{Batch, Db, Durability, Error, Options};

const CHILD: &str = "TRADEDB_CHILD_PATH";
const SYM: u64 = 7;
const ROUNDS: u64 = 15_000;
const PER_ROUND: u64 = 200;
const KEEP: u64 = 20;

/// Child: an ingester committing as fast as it can (no fsync), retiring old
/// rounds so pages are recycled constantly, with the cursor changing on
/// every commit.
fn child(path: &str) {
    let db = Options::new()
        .page_size(4096)
        .durability(Durability::None)
        .open(path)
        .unwrap();
    for round in 0..ROUNDS {
        let mut b = Batch::new();
        for i in 0..PER_ROUND {
            let t = round * PER_ROUND + i;
            b.put(SYM, t, &t.to_le_bytes());
        }
        if round >= KEEP {
            b.truncate_before(SYM, (round - KEEP) * PER_ROUND);
        }
        b.set_meta("round", &round.to_le_bytes());
        db.write(&b).unwrap();
    }
    db.sync().unwrap();
}

#[test]
fn child_entry_point() {
    if let Ok(path) = std::env::var(CHILD) {
        child(&path);
    }
}

/// A registered snapshot must show exactly the child's commit for its round,
/// however the child recycles pages meanwhile.
fn check_snapshot(db: &Db, last_round: &mut u64) -> bool {
    let snap = db.snapshot().unwrap();
    let Some(r) = snap.get_meta("round").unwrap() else {
        return false;
    };
    let round = u64::from_le_bytes(r.try_into().unwrap());
    assert!(round >= *last_round, "rounds went backwards");
    *last_round = round;
    let lo = round.saturating_sub(KEEP) * PER_ROUND;
    let hi = (round + 1) * PER_ROUND - 1;
    assert_eq!(snap.min_time(SYM).unwrap(), Some(lo));
    assert_eq!(snap.max_time(SYM).unwrap(), Some(hi));
    let mut expect = lo;
    for e in snap.scan(SYM, ..).unwrap() {
        let e = e.unwrap();
        assert_eq!(e.time, expect);
        assert_eq!(&*e.value, &e.time.to_le_bytes());
        expect += 1;
    }
    assert_eq!(expect, hi + 1);
    true
}

#[test]
fn reader_process_sees_consistent_commits_from_writer_process() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shared.tradedb");
    // Create the file first so both sides agree on the page size.
    drop(Options::new().page_size(4096).open(&path).unwrap());
    let db = Arc::new(Db::open(&path).unwrap());

    let mut writer = Command::new(std::env::current_exe().unwrap())
        .args(["child_entry_point", "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, &path)
        .spawn()
        .unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (db, done) = (db.clone(), done.clone());
            std::thread::spawn(move || {
                let (mut checks, mut round) = (0u64, 0u64);
                while !done.load(Ordering::Relaxed) {
                    if check_snapshot(&db, &mut round) {
                        checks += 1;
                    }
                    // Optimistic reads straight from the handle stay correct too.
                    if let Some(e) = db.last(SYM).unwrap() {
                        assert_eq!(&*e.value, &e.time.to_le_bytes());
                    }
                }
                checks
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(300);
    let status = loop {
        if let Some(status) = writer.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline || readers.iter().any(|r| r.is_finished()) {
            let _ = writer.kill();
            break writer.wait().unwrap();
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    done.store(true, Ordering::Relaxed);
    let checks: u64 = readers
        .into_iter()
        .map(|r| r.join().expect("reader thread failed"))
        .sum();
    assert!(status.success(), "writer process failed");
    assert!(checks > 0);

    // After the writer is gone, the final state is all there and the file
    // verifies; this process can now write too.
    let final_round = u64::from_le_bytes(db.get_meta("round").unwrap().unwrap().try_into().unwrap());
    assert_eq!(final_round, ROUNDS - 1);
    assert_eq!(db.count(SYM).unwrap(), (KEEP + 1) * PER_ROUND);
    let r = db.verify().unwrap();
    assert!(r.is_ok(), "{:?}", r.problems);
    let mut b = Batch::new();
    b.put(SYM + 1, 1, b"parent");
    db.write(&b).unwrap();
    assert!(db.verify().unwrap().is_ok());
    // An exclusive open by another process works once this one has closed,
    // and excludes everybody else.
    drop(db);
    let probe = Command::new(std::env::current_exe().unwrap())
        .args(["exclusive_probe", "--exact", "--nocapture"])
        .env("TRADEDB_EXCLUSIVE_PROBE", &path)
        .status()
        .unwrap();
    assert!(probe.success());
}

/// Child for the exclusive-open check: opens exclusively (works, the parent
/// closed its handle), and a second process could not then open it.
#[test]
fn exclusive_probe() {
    if let Ok(path) = std::env::var("TRADEDB_EXCLUSIVE_PROBE") {
        let db = Options::new().exclusive(true).open(&path).unwrap();
        assert_eq!(db.count(SYM + 1).unwrap(), 1);
        let out = Command::new(std::env::current_exe().unwrap())
            .args(["exclusive_blocked", "--exact", "--nocapture"])
            .env("TRADEDB_BLOCKED_PATH", &path)
            .status()
            .unwrap();
        assert!(out.success());
    }
}

#[test]
fn exclusive_blocked() {
    if let Ok(path) = std::env::var("TRADEDB_BLOCKED_PATH") {
        assert!(matches!(Db::open(&path), Err(Error::Locked)));
    }
}

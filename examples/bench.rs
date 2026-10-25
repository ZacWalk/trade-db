//! Throughput benchmark: `cargo run --release --example bench [records]`.
//!
//! Ingest and scans of 64-byte records with a warm cache, plus the
//! candle-oriented paths: fixed-width columns, as-of lookups, polling and
//! merged replay.

use std::hint::black_box;
use std::time::Instant;

use tradedb::{Batch, Columns, Durability, Options};

fn rate(n: u64, start: Instant) -> String {
    let s = start.elapsed().as_secs_f64();
    let r = n as f64 / s;
    if r >= 1e6 {
        format!("{:>8.2} M/s  ({s:.3} s)", r / 1e6)
    } else {
        format!("{:>8.1} K/s  ({s:.3} s)", r / 1e3)
    }
}

fn main() -> tradedb::Result<()> {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(1_000_000);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("bench.tradedb");
    let db = Options::new().cache_size(512 << 20).exclusive(true).open(&path)?;
    println!(
        "tradedb bench: {n} records, page size {} B, file {}",
        db.page_size(),
        path.display()
    );

    // ---- ingest ---------------------------------------------------------
    const SYM: u64 = 1;
    let value = [7u8; 64];
    let t = Instant::now();
    let mut b = Batch::with_capacity(10_000, 10_000 * 64);
    for chunk in 0..n.div_ceil(10_000) {
        b.clear();
        for i in chunk * 10_000..((chunk + 1) * 10_000).min(n) {
            b.put(SYM, i * 1_000, &value);
        }
        db.write_with(&b, Durability::Eventual)?;
    }
    db.sync()?;
    println!("ingest 64 B, 10k/batch, eventual  {}", rate(n, t));

    let mut big = Batch::with_capacity(n as usize, n as usize * 64);
    for i in 0..n {
        big.put(2, i * 1_000, &value);
    }
    let t = Instant::now();
    db.write(&big)?;
    println!("ingest 64 B, one batch, immediate {}", rate(n, t));
    drop(big);

    // ---- scans ------------------------------------------------------------
    let _ = db.scan(SYM, ..)?.count(); // warm the cache
    let t = Instant::now();
    let mut sum = 0usize;
    for e in db.scan(SYM, ..)? {
        sum += e?.value.len();
    }
    black_box(sum);
    println!("scan (iterator, warm)             {}", rate(n, t));

    let t = Instant::now();
    let mut s = db.scan(SYM, ..)?;
    let mut sum = 0u64;
    while let Some(c) = s.next_chunk() {
        let c = c?;
        for i in 0..c.len() {
            sum = sum.wrapping_add(c.time(i)) + c.value(i).len() as u64;
        }
    }
    black_box(sum);
    println!("scan (chunks, warm)               {}", rate(n, t));

    let t = Instant::now();
    let k = db.scan_rev(SYM, ..)?.take(1000).count();
    black_box(k);
    println!(
        "last 1000 records (scan_rev)      {:>8.1} µs",
        t.elapsed().as_secs_f64() * 1e6
    );

    // ---- fixed-width candles ------------------------------------------------
    const BARS: u64 = 3;
    db.define_symbol(BARS, 40)?;
    let mut b = Batch::with_capacity(n as usize, n as usize * 40);
    let bar = [1u8; 40];
    for i in 0..n {
        b.put(BARS, i * 60, &bar);
    }
    let t = Instant::now();
    db.write(&b)?;
    println!("ingest 40 B bars (fixed width)    {}", rate(n, t));
    drop(b);
    let mut cols = Columns::new();
    db.read_columns(BARS, .., &mut cols)?;
    let t = Instant::now();
    cols.clear();
    let got = db.read_columns(BARS, .., &mut cols)?;
    println!("read_columns (warm)               {}", rate(got as u64, t));
    let t = Instant::now();
    let mut s = db.scan(BARS, ..)?;
    let mut bytes = 0usize;
    while let Some(c) = s.next_chunk() {
        bytes += black_box(c?.payloads().unwrap()).len();
    }
    black_box(bytes);
    println!("payload column slices (warm)      {}", rate(n, t));
    let info = db.symbol_info(BARS)?.unwrap();
    println!(
        "  {} bars in {} leaves ({:.0}/leaf); {} 64 B ticks in {} leaves",
        info.count,
        info.leaf_count,
        info.count as f64 / info.leaf_count as f64,
        n,
        db.symbol_info(SYM)?.unwrap().leaf_count
    );

    // ---- point reads ------------------------------------------------------------
    let lookups = 1_000_000u64;
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let t = Instant::now();
    for _ in 0..lookups {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        black_box(db.get_le(SYM, x % (n * 1_000))?);
    }
    println!("get_le (as-of), random            {}", rate(lookups, t));
    let t = Instant::now();
    for _ in 0..lookups {
        black_box(db.max_time(SYM)?);
    }
    println!("max_time poll, exclusive handle   {}", rate(lookups, t));
    let t = Instant::now();
    for i in 0..lookups {
        black_box(db.count_range(SYM, i..i + 10_000_000)?);
    }
    println!("count_range (O(log n))            {}", rate(lookups, t));
    drop(db);

    let db = Options::new().cache_size(512 << 20).open(&path)?;
    let polls = 200_000u64;
    let t = Instant::now();
    for _ in 0..polls {
        black_box(db.max_time(SYM)?);
    }
    println!("max_time poll, multi-process      {}", rate(polls, t));

    // ---- merged replay ------------------------------------------------------------
    let symbols: Vec<u64> = (100..200).collect();
    let mut b = Batch::new();
    for i in 0..(n / 100) {
        for &s in &symbols {
            b.put(s, i * 1_000 + s, &value[..16]);
        }
    }
    db.write(&b)?;
    let total = n / 100 * symbols.len() as u64;
    let _ = db.scan_multi(&symbols, ..)?.count();
    let t = Instant::now();
    let k = db.scan_multi(&symbols, ..)?.count();
    black_box(k);
    println!("merged replay, 100 symbols        {}", rate(total, t));
    let st = db.stats()?;
    println!(
        "file: {} pages ({:.1} MiB); cache {}/{} pages, {} hits, {} misses",
        st.page_count,
        (st.page_count * st.page_size as u64) as f64 / (1u64 << 20) as f64,
        st.cache_used,
        st.cache_capacity,
        st.cache_hits,
        st.cache_misses
    );
    Ok(())
}

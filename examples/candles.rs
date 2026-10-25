//! Storing OHLCV candles: `cargo run --example candles`.
//!
//! One fixed-width symbol per (ticker, timeframe). Bars are 48-byte records
//! stored column-wise, so a chart window is a single `read_columns` call and
//! "the last N bars" is a short reverse scan. The ingest cursor commits in
//! the same transaction as the bars, so a restart resumes exactly where the
//! last commit ended.

use tradedb::{Batch, Columns, Db, Durability, Options, symbol_id};

/// A candle as stored: six little-endian fields, 48 bytes.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Bar {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trades: u64,
}

impl Bar {
    const SIZE: u32 = 48;

    fn encode(&self) -> [u8; 48] {
        let mut b = [0u8; 48];
        for (i, x) in [self.open, self.high, self.low, self.close, self.volume]
            .iter()
            .enumerate()
        {
            b[i * 8..i * 8 + 8].copy_from_slice(&x.to_le_bytes());
        }
        b[40..48].copy_from_slice(&self.trades.to_le_bytes());
        b
    }

    fn decode(b: &[u8]) -> Bar {
        let f = |i: usize| f64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().unwrap());
        Bar {
            open: f(0),
            high: f(1),
            low: f(2),
            close: f(3),
            volume: f(4),
            trades: u64::from_le_bytes(b[40..48].try_into().unwrap()),
        }
    }
}

const MINUTE_NS: u64 = 60_000_000_000;

fn main() -> tradedb::Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = Options::new()
        // A live feed commits every minute; Eventual durability is one fsync
        // per commit and never leaves a partial one.
        .durability(Durability::Eventual)
        .open(dir.path().join("candles.tradedb"))?;

    let tickers = ["AAPL", "MSFT", "NVDA"];
    for t in tickers {
        db.define_symbol(symbol_id(&format!("{t}/1Min")), Bar::SIZE)?;
    }

    // Backfill a trading day of 1-minute bars, with the cursor in the same
    // transaction.
    let open_ns = 1_767_265_200 * 1_000_000_000u64; // 2026-01-01 14:30 UTC
    let mut batch = Batch::new();
    for (k, t) in tickers.iter().enumerate() {
        let sym = symbol_id(&format!("{t}/1Min"));
        for m in 0..390u64 {
            let p = 100.0 * (k + 1) as f64 + m as f64 * 0.01;
            let bar = Bar {
                open: p,
                high: p + 0.5,
                low: p - 0.5,
                close: p + 0.1,
                volume: 1_000.0 + m as f64,
                trades: 50 + m,
            };
            batch.put(sym, open_ns + m * MINUTE_NS, &bar.encode());
        }
    }
    batch.set_meta("cursor", &(open_ns + 389 * MINUTE_NS).to_le_bytes());
    db.write(&batch)?;

    let aapl = symbol_id("AAPL/1Min");

    // A chart window: one call, three columns.
    let mut cols = Columns::new();
    let n = db.read_columns(aapl, open_ns..open_ns + 30 * MINUTE_NS, &mut cols)?;
    let closes: Vec<f64> = (0..n).map(|i| Bar::decode(cols.payload(i)).close).collect();
    println!("first 30 AAPL closes: {:.2?}", &closes[..5]);

    // The last 5 bars, newest first.
    for e in db.scan_rev(aapl, ..)?.take(5) {
        let e = e?;
        println!("  {}  {:?}", e.time, Bar::decode(&e.value));
    }

    // As-of: the bar in force at an arbitrary instant (no lookahead).
    let t = open_ns + 100 * MINUTE_NS + 17_000_000_000;
    let e = db.get_le(aapl, t)?.expect("a bar before t");
    assert_eq!(e.time, open_ns + 100 * MINUTE_NS);

    // A restatement replaces a window atomically.
    let mut fix = Batch::new();
    fix.delete_range(aapl, open_ns..open_ns + 10 * MINUTE_NS);
    for m in 0..10u64 {
        let bar = Bar {
            open: 1.0,
            high: 1.0,
            low: 1.0,
            close: 1.0,
            volume: 0.0,
            trades: 0,
        };
        fix.put(aapl, open_ns + m * MINUTE_NS, &bar.encode());
    }
    db.write(&fix)?;

    // Replay every ticker as one time-ordered stream, as a backtest would.
    let syms: Vec<u64> = tickers.iter().map(|t| symbol_id(&format!("{t}/1Min"))).collect();
    let events = db.scan_multi(&syms, open_ns..open_ns + 60 * MINUTE_NS)?.count();
    println!("replayed {events} bar events across {} tickers", syms.len());

    // Where to resume after a restart.
    let cursor = u64::from_le_bytes(db.get_meta("cursor")?.unwrap().try_into().unwrap());
    println!("resume after {cursor}; {} bars stored", db.count(aapl)?);

    let report = db.verify()?;
    println!(
        "verify: {} symbols, {} records, ok = {}",
        report.symbols,
        report.records,
        report.is_ok()
    );
    let _ = Db::memory(); // in-memory databases are handy in tests
    Ok(())
}

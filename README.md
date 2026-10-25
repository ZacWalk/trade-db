# trade-db

An embedded time-series database for market data (ticks, quotes and candles)
in a single file. It is crash-safe, lets many readers work alongside one
writer, and scans hundreds of millions of records a second. Pure Rust.

```rust
use tradedb::{Batch, Db, symbol_id};

let db = Db::open("market.tradedb")?;
let btc = symbol_id("BTCUSDT");

// Ingest atomically: all of it or none of it, even across a crash.
let mut batch = Batch::new();
for tick in ticks {
    batch.put(btc, tick.time_ns, &tick.payload);
}
db.write(&batch)?;

// The last trade as of a bar's close: no lookahead.
let last = db.get_le(btc, bar_close_ns)?;

// Stream a time range; values are zero-copy.
for e in db.scan(btc, t0..t1)? {
    let e = e?;
    process(e.time, &e.value);
}
```

- **Crash-safe:** a batch commits entirely or not at all, through process
  crashes and power loss, and the database opens instantly afterwards.
- **Concurrent:** one writer and any number of readers, across threads and
  processes; readers never block the writer.
- **Made for market data:** `(symbol, time, seq)` keys, as-of lookups, merged
  multi-symbol replay and atomic corrections.
- **Fast:** 4–5 million records a second ingested and up to 470 million
  scanned, on a desktop PC.

## Use it

The crate is `tradedb`. It is not on crates.io yet:

```toml
[dependencies]
tradedb = { git = "https://github.com/ZacWalk/trade-db" }
```

The [guide](docs/guide.md) covers the guarantees, every feature, concurrency
and performance. The [design](docs/design.md) explains how it works inside.

## Build and test

```sh
cargo test
cargo run --release --example bench
```

trade-db is pre-1.0, so its API and file format may still change. It stores
every bar of [trade-rl](https://github.com/ZacWalk/trade-rl).

## License

MIT

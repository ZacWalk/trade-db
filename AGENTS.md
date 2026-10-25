# AGENTS.md — trade-db

Embedded ACID time-series store for market data, keyed by
`(symbol: u64, time: u64, seq: u32)`. One library crate, `tradedb`, built on
a copy-on-write B+tree engine.

| Question | Read |
|---|---|
| What is it, what is the public API? | [README.md](README.md), [docs/guide.md](docs/guide.md), rustdoc in [src/lib.rs](src/lib.rs) |
| How does it work inside (formats, commit protocol, locks, free space)? | [docs/design.md](docs/design.md) |

Do not restate design details here — update docs/design.md instead.

## Commands

```sh
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test                                        # unit, crash, model, API, multi-process
TRADEDB_SEEDS=200 cargo test --lib randomized     # longer randomized run
cargo run --release --example bench               # throughput
cargo run --example candles                       # OHLCV usage
```

The minimum supported Rust is 1.88 (`rust-version` in Cargo.toml); CI builds
it on Windows, Linux and macOS.

## Layout

```
src/lib.rs        crate docs, re-exports, symbol_id
src/db.rs         Options, Durability, open/format, Db struct, registry
src/api.rs        public Db methods (reads, scans, writes, operations)
src/snapshot.rs   Snapshot, backup
src/inner.rs      meta discovery, reader registry, writer begin/commit/abort
src/txn.rs        write transaction: copy-on-write, B+tree mutation, commit
src/read.rs       verified page loads, cursors, point lookups, rank
src/scan.rs       Scan, Chunk, MultiScan
src/layout.rs     on-disk page format and encodings
src/meta.rs       meta slot and user-metadata encoding
src/free.rs       free-page state
src/cache.rs      sharded CLOCK page cache
src/verify.rs     verify and the reachability walk
src/storage.rs    Storage trait: file, memory, and the test FaultStorage
src/sys.rs        OS positional I/O and byte-range locks (the FFI lives here)
src/crc.rs        CRC-32C, hardware and software
src/tests.rs      crash simulation, fault injection, model-based tests
tests/            public-API and multi-process tests
```

## Invariants that break things silently

- **Never modify a committed page.** Writes go through `Txn::cow` (copy and
  record the old page as freed) or touch pages the transaction allocated
  (`ver == txn id`). Writing a committed page in place breaks atomicity and
  every snapshot that can see it.
- **Every pointer is `(page, ver)`, and a loaded page must carry that `ver`.**
  When you copy a page, rewrite the parent's pointer — and when a
  transaction moves a tree's root, store the new root in the directory even
  if no records changed.
- **A page this transaction already copied keeps its reference when edited
  again.** So "the reference did not change" or "the tree descriptor did not
  change" never means "nothing changed" — decide by what was removed or
  inserted. Two review-found bugs (stale counts and min/max after two range
  deletes, or a range delete plus point edits, in one batch) came from this.
- **Freed pages are not reusable until no snapshot can see them and the
  freeing commit is durable** (`Inner::release`). Pages a transaction
  allocated and frees again may be reused at once (`Txn::free_page`).
- **All I/O goes through `Storage`.** That is what lets the test suite fail
  any call and replay crash images; do not touch `std::fs::File` elsewhere.
- **All page bytes are read through `layout` accessors after `validate`**,
  which guarantees accessors stay in bounds. A new page layout needs new
  validation, or a damaged file can panic the process.
- **Branch keys are exact subtree minimums**, and branch counts exact subtree
  record counts. Insert, delete and range delete maintain both; `verify`
  checks both.
- **Changing any on-disk layout changes the format.** Bump
  `layout::FORMAT_VERSION`, update docs/design.md, and keep refusing other
  versions cleanly — never misread them.
- **`unsafe` lives only in `sys.rs` (FFI) and `crc.rs` (CPU intrinsics)**,
  each block with a `SAFETY:` comment.

## Tests

- Every behaviour change needs a test; every bug fix a test that failed
  before it.
- Crash safety claims are proved in `src/tests.rs` by replaying recorded
  write logs (every process-crash point; random power-loss subsets with torn
  sectors). Extend `workload` when adding a mutating operation.
- `verify()` after mutations catches structural bugs that reads miss.
- Tests write only to `tempfile` directories or the in-memory backends.

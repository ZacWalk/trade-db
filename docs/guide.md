# trade-db guide

What trade-db guarantees and offers, how it behaves under concurrency, and how
fast it is. [design.md](design.md) explains how it works inside.

## Why

Market data has a shape general-purpose key-value stores handle poorly:

- **Composite keys.** Every record is `(symbol, time)`, and the natural query
  is "all of `BTCUSDT` between `t0` and `t1`".
- **Timestamps are not unique.** One aggressive order prints several fills in
  the same nanosecond, so the key carries a `seq` tiebreaker:
  `(symbol: u64, time: u64, seq: u32)`.
- **Point-in-time reads.** "Last trade as of `T`" underlies every feature
  pipeline; getting it wrong is how lookahead gets into a backtest.
- **Append-dominant, mostly ordered**, with occasional late or restated data:
  overwrites, deletes and range replacement must be first-class and atomic.
- **One writer, many readers**, often in other processes, some scanning for
  hours. Readers must never stall the ingester.
- **Files larger than RAM.** No `mmap`; an explicit page cache bounds memory.

## Guarantees

| | |
|---|---|
| **Atomic** | A batch commits entirely or not at all — across process crashes, OS crashes and power loss. Committed pages are never overwritten; a commit becomes visible only when one of two checksummed meta slots flips. There is no partial state to recover, so opening after a crash is instant. |
| **Consistent** | Every page carries a CRC-32C, and every pointer records the transaction that wrote its target, so torn, lost, misdirected and bit-rotted pages are detected rather than returned. `Db::verify` checks the whole file. |
| **Isolated** | One writer at a time (across processes); readers see committed states only. A `Snapshot` is a stable view of one commit for as long as it is held. |
| **Durable** | Per write: `Immediate` (default, two fsyncs), `Eventual` (one fsync; a power cut may lose the latest commit, never part of one) or `None` (OS cache only, for bulk loads finished with `sync`). |

The crash claims are tested by replaying every prefix of a commit's write
log and thousands of simulated power cuts that keep random subsets of
unsynced writes and tear pages at sector granularity; every image must open
as exactly one committed state and verify clean (see [Testing](#testing)).

## Features

- **Range scans** forward and reverse, inclusive of every `seq` at the
  boundary times, with zero-copy values (`Value` borrows the page it came
  from). `Scan::next_chunk` hands out whole runs of a leaf for bulk loops.
- **As-of lookups**: `get_le` (latest at or before `t`, highest `seq`) and
  `get_ge`, plus `first` / `last`.
- **Fixed-width symbols**: `define_symbol(sym, size)` stores records
  column-wise (`times[] | seqs[] | payloads[]`): 5–20% more records per page
  (most for small records), and `read_columns` / `Chunk::payloads` copy whole
  leaves at once — the natural layout for OHLCV bars.
- **Merged replay**: `scan_multi` streams many symbols as one sequence
  ordered by `(time, seq, symbol)`, as a backtest or live loop consumes it.
- **Counted index**: `count_range` is `O(log n)`; nothing is scanned.
- **Hot polling**: `max_time`, `symbol_info`, `max_times` (a whole watchlist)
  and `txn_id` read cached statistics; `wait_for_txn` blocks for the next
  commit.
- **Corrections and retention**: `overwrite`, `delete`, `delete_range`,
  `truncate_before` and `drop_symbol`; a batch may delete a range and
  re-insert it atomically (restatements). Whole subtrees inside a deleted
  range are released without being read.
- **Transactional ingest cursor**: `Batch::set_meta` commits with the data,
  so a restarted ingester resumes with no gap and no double apply.
- **Operations**: online `backup`, `prefetch`, `verify`, `stats`,
  in-memory databases (`Db::memory`) for tests.

## Concurrency

A `Db` is `Send + Sync`: open the file once per process and share it between
threads (opening the same file twice in one process returns
`Error::AlreadyOpen`). Any number of processes may open the same file.

- **Readers never block the writer, and the writer never blocks readers** —
  within a process or across processes. Writers are serialized by an OS
  byte-range lock held only for the duration of a write.
- Reads on `Db` run against the newest commit and need no locks. If a
  concurrent writer recycles a page such a read still needs, the read
  notices (the page's version no longer matches) and retries; a scan resumes
  from the newest commit just past the last record it returned, never
  repeating one or moving backwards.
- A `Snapshot` registers itself with a shared byte-range lock, so pages it
  can see are not recycled — by this process or any other — until it is
  dropped. Hold snapshots for consistency, not forever: under heavy writes
  the file grows while an old snapshot pins the pages it can see.
- Other processes' commits do not flush a reader's cache: pages are
  immutable once committed and referenced by version, so only pages that
  actually changed are re-read.
- `Options::exclusive(true)` claims the file for one process, letting reads
  skip the on-disk meta check entirely.

Locks are open-file-description locks on Linux and handle locks on Windows.
On macOS and the BSDs they are per-process `fcntl` locks, which the
one-open-per-process rule makes safe; do not open the database file through
other means while a `Db` is open on those systems.

## Performance

One million records, warm cache, single process (`cargo run --release --example bench`):

| Operation | Windows 11 (NTFS) | Linux (WSL2, ext4) |
|---|---|---|
| Ingest, 64 B values, one batch | 4.0 M rec/s | 4.8 M rec/s |
| Ingest, 64 B values, 10k per commit (`Eventual`) | 2.4 M rec/s | 1.4 M rec/s |
| Ingest, 40 B fixed-width bars | 4.9 M rec/s | 5.3 M rec/s |
| Scan, per-record iterator | 82 M rec/s | 94 M rec/s |
| Scan, `next_chunk` | 413 M rec/s | 469 M rec/s |
| `read_columns` (fixed width) | 159 M rec/s | 136 M rec/s |
| Last 1,000 records (`scan_rev`) | 13 µs | 13 µs |
| `get_le`, random times | 1.6 M/s | 2.5 M/s |
| `max_time` poll, exclusive / multi-process | 14 M/s / 262 K/s | 23 M/s / 704 K/s |
| Merged replay, 100 symbols | 15 M rec/s | 21 M rec/s |

**Page size** is chosen at creation (`Options::page_size`, 4–64 KiB, default
64 KiB). Large pages scan fastest and hold values up to 65,484 bytes. Every
commit rewrites the pages it touches, so many small commits spread across
many symbols — or many sparse symbols — favour smaller pages.

## Design in brief

```
file   [meta slot A 64 KiB][meta slot B 64 KiB][page][page]...
page   [32 B header: kind, level, count, version, owner, id][body][CRC-32C]

meta ─► directory B+tree (symbol → root, height, counts, min/max, record size)
                    └─► per-symbol B+tree of (time, seq) → value
```

- **Copy-on-write B+trees.** A write copies each page it changes, and the
  path to the root, into newly allocated pages; the meta flip publishes the
  new roots. Branch entries carry `(first key, child page, child version,
  subtree count)`; leaves are variable-width (slot directory + value heap)
  or fixed-width (columns).
- **Sorted ingest packs pages.** Appending past the newest leaf starts a new
  leaf instead of splitting, so time-ordered data fills every page.
- **Free space** is recycled once no snapshot anywhere can still read it and
  the commit that freed it is durable. The free list rides inline in the
  meta slot; when too large it is rebuilt by walking the index (leaves are
  not read), which also reclaims pages leaked by a crash.
- **Page cache**: sharded CLOCK over positional I/O, handing out `Arc`'d
  immutable pages; scans insert leaves cold so they do not flush the index.
  Large transactions spill freshly written pages early.
- **CRC-32C** uses SSE4.2 or the ARMv8 CRC instructions with three
  interleaved streams, falling back to slicing-by-8; all paths are tested to
  agree.

Formats, algorithms, the commit protocol, the reader registry and its proof
sketch are in [design.md](design.md).

## Testing

```sh
cargo test                     # 50+ tests, a few seconds
TRADEDB_SEEDS=200 cargo test --lib randomized   # longer randomized run
cargo run --release --example bench
```

The suite covers the public API on real files, a real two-process
writer/reader run, and on an in-memory recording backend:

- **every process-crash point** of multi-commit workloads (all three
  durability modes) recovers to exactly the committed prefix;
- **power-loss simulation**: random subsets of unsynced writes, torn at
  512-byte granularity, never yield a partial commit or lose a durable one;
- **fault injection**: failing the Nth write, fsync or read reports the
  error, leaves the handle on the old or new state and still writable;
- **model-based randomized testing** against a `BTreeMap` reference, with
  structural verification, reopen and both durability modes.

The on-disk format is little-endian on every host. Files carry a format
version and are refused, never misread, across versions.

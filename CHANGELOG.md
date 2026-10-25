# Changelog

## 0.1.0 — unreleased

First release, on a copy-on-write B+tree engine.

- Composite key `(symbol, time, seq)`; puts, overwrites, deletes, range
  deletes, retention and symbol drop in atomic batches with a transactional
  metadata cursor.
- Crash-atomic commits under process crash, OS crash and power loss;
  `Immediate`, `Eventual` and `None` durability.
- Snapshot isolation; readers and the writer never block each other across
  threads and processes.
- Forward, reverse, chunked and merged multi-symbol scans; as-of lookups;
  `O(log n)` range counts; fixed-width columnar symbols.
- CRC-32C on every page and versioned page references; `verify`, `backup`,
  `prefetch`, `stats`, `wait_for_txn`; in-memory databases.
- Page size 4–64 KiB per file.

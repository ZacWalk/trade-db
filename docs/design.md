# trade-db design

Reference for how the code in [`src/`](../src) works. It describes what is
implemented, not a roadmap. The [guide](guide.md) covers the workload, the
guarantees and the public API.

---

## 1. Data model

```
Key:    (symbol: u64, time: u64, seq: u32)   unique; ordered by (time, seq) within a symbol
Value:  bytes, 0 ..= page_size - 52   (variable-width symbols)
        exactly record_size bytes     (fixed-width symbols)
```

`symbol` is opaque. `seq` breaks ties between events sharing a timestamp;
series unique per timestamp use `seq = 0`. Inserting an existing key with
`Batch::put` fails the batch with `DuplicateKey`; `Batch::overwrite` replaces.

---

## 2. File layout

```
offset 0        meta slot A (64 KiB)
offset 64 KiB   meta slot B (64 KiB)
offset 128 KiB  first data page
```

Pages are `page_size` bytes (a power of two, 4–64 KiB, fixed at creation and
recorded in the meta). Page ids are `u32` offsets in units of `page_size`;
ids below `128 KiB / page_size` are reserved, so id 0 can mean "none". The
file holds at most 2³² pages (256 TiB at 64 KiB).

Every page:

```
[0]   kind     u8   1 = variable leaf, 2 = fixed leaf, 3 = branch
[1]   flags    u8   bit 0: page belongs to the symbol directory
[2]   level    u16  0 for leaves; a branch over leaves is level 1
[4]   count    u32  records (leaf) or entries (branch)
[8]   ver      u64  id of the transaction that wrote this image
[16]  owner    u64  symbol id (0 for directory pages)
[24]  page     u32  this page's own id
[28]  aux      u32  variable leaf: heap bottom; fixed leaf: record size
[32]  body ...
[ps-4] CRC-32C of bytes [0, ps-4)
```

All integers are little-endian and read through explicit byte offsets, so
the format is the same on every host and nothing depends on alignment.

### 2.1 Leaves

**Variable width** (`kind = 1`): a slot directory grows up from byte 32 and
a value heap grows down from the trailer.

```
slot (16 B): time u64 | seq u32 | value offset u16 | value length u16
```

Slots are sorted by `(time, seq)`; values are not. Inserting writes the
value at the heap bottom and shifts the slot tail; overwriting reuses the old
bytes when the new value fits there. Garbage left by overwrites and deletes
is reclaimed by compacting the heap when an insert would otherwise not fit.
A record costs `16 + len` bytes; 818 records of 64 bytes fit a 64 KiB page.

**Fixed width** (`kind = 2`, `aux = record_size`): three columns of a fixed
capacity `cap`:

```
times[cap] u64 @ 32 | seqs[cap] u32 | (pad to 64) payloads[cap * record_size]
```

`cap` is the largest value with the payload column, aligned to 64 bytes,
ending before the trailer. A record costs `12 + record_size` bytes (861 at
64 B, 1,259 at 40 B per 64 KiB page) and the payloads of any run of records
are one contiguous slice, which is what `read_columns` and
`Chunk::payloads` copy or expose.

### 2.2 Branches

```
entry (32 B): first time u64 | first seq u32 | child page u32 | child ver u64 | subtree records u64
```

Entry `i`'s key is the **exact** smallest key in child `i`'s subtree; the
first entry's key is the subtree minimum too, and is also what a lookup
falls back to when its key precedes everything. Searching takes the last
entry whose key is `<=` the target. Keeping separators exact (rather than
lower bounds) lets `verify` check them and lets range deletes decide whether
a child is wholly covered. A 64 KiB branch holds 2,046 entries, so a symbol
of a billion records is three levels deep. The per-entry record counts make
`rank` — and therefore `count_range` — `O(height × fanout)` with no leaf
reads beyond the two boundary leaves.

### 2.3 The symbol directory

A B+tree of exactly the same shape, flagged `FLAG_DIR`, whose leaves are
fixed-width with 64-byte records keyed `(symbol, 0)`:

```
root page u32 | root ver u64 | height u8 | pad | record_size u32 | pad
records u64 | leaf pages u64 | min time u64 | max time u64 | min seq u32 | max seq u32
```

`height = 0` and `root = 0` mean an empty symbol (defined, or all records
deleted). `max_time`, `count` and `symbol_info` read only this entry.

---

## 3. Meta slots

```
slot:  [header 128 B][user metadata, up to 4096 B][free state, up to 61,312 B]

header:
  0   magic "TRADEDB\0"     8   format version u32    12  page size u32
  16  txn u64               24  page count u64        32  directory root u32
  36  directory root ver    44  directory height u8   48  symbols u64
  56  directory leaves u64  64  user meta len u32     68  user meta CRC u32
  72  free len u32 (u32::MAX = not persisted)         76  free CRC u32
  124 header CRC u32 (over bytes 0..124)
```

Transaction `T` is written to slot `T % 2`, so the other slot always holds
the previous commit. A slot is **committed** when its header CRC and its
user-metadata CRC both pass. Open (and every multi-process read) picks the
committed slot with the higher `txn`. A free-state area that fails its CRC
does not invalidate the slot; it only means the free state must be rebuilt.

The header has its own CRC so a poller can read just 128 bytes per slot to
learn whether anything changed. The header and the metadata area are read
separately, so a commit rewriting the slot in between makes them disagree;
the reader then re-reads rather than answering with the older slot. Only
when the *same* newest header fails twice is it taken to be a meta write
that never completed — torn by a crash, or still in flight — and the other
slot is used: that commit does not exist (yet). Snapshot registration
(§7.2) depends on this never returning anything older than the newest
committed state.

User metadata is `[u16 key len][u16 value len][key][value]...`. The free
state is a list of extents `(start u32, len u32)` for the reusable pool,
then pending groups `(freeing txn u64, extents)`.

An empty file, or one holding nothing but a zeroed reserved area (an
interrupted format), is formatted; a header of another format version is
refused with `UnsupportedFormat`; anything else is refused untouched.

---

## 4. Versioned references

Every pointer — branch entries, directory entries, the meta's directory root
— is a `(page, ver)` pair, and a loaded page must carry that `ver` in its
header (plus its own id and a valid CRC). This one check does several jobs:

- **Recycled pages.** A page freed by one commit and rewritten by a later one
  has a new `ver`; a reader still following an old pointer sees the mismatch
  instead of foreign data.
- **Cross-process cache coherence.** Cached pages are immutable images keyed
  by id and version. When another process commits, a reader does not flush
  its cache: a lookup for `(page, ver)` misses only if that page changed.
- **Lost and misdirected writes**, which a CRC alone cannot see.

---

## 5. Writing

### 5.1 Begin

1. Take the in-process writer mutex, then (unless exclusive) the OS writer
   lock — an exclusive byte-range lock held for the whole transaction.
2. Read the newest meta. If another process committed since this handle's
   last write, fsync once so its commits are durable before anything they
   freed is recycled.
3. Obtain the free state for that meta: in memory if this handle made the
   commit, else decoded from the meta slot, else rebuilt (§6.3).
4. Release pending groups that are safe to reuse (§6.2).

### 5.2 Copy on write

The transaction owns a map of *dirty* pages — pages it allocated. To modify
a committed page it allocates a new id, copies the image, stamps the new
`ver` (= its own txn id) and records the old id as *freed by this txn*.
Descending to modify a leaf copies the root-to-leaf path and rewrites each
parent's child pointer; pages already dirty are modified in place. A dirty
page that the transaction frees again goes straight back to the pool.

### 5.3 Insert

Descend copying the path, then binary-search the leaf:

1. **In place** if the record fits (after compacting garbage if needed).
2. **Append** — the key sorts after every record of the tree's rightmost
   leaf, which is full: start a new leaf holding just the record. The full
   leaf is left untouched, so time-ordered ingest packs every page. The
   mirror case — prepending before the tree's leftmost leaf — likewise gives
   the new record a leaf of its own.
3. **Split** otherwise: fixed leaves halve; variable leaves take the most
   even byte split for which both halves fit, or — when one huge record
   makes that impossible — split three ways with the new record alone.

New siblings propagate up: each ancestor's entry gets its new count (exact
after a split, `+1` otherwise) and, along a leftmost path, its new first key;
an ancestor without room splits likewise (the rightmost branch appends rather
than halves); a split root grows a new root.

### 5.4 Delete

A point delete first checks the key exists (deleting an absent key is a
no-op and copies nothing), then copies the path, removes the slot and
repairs upward: counts `-1`; a new first key when slot 0 went; an emptied
leaf is freed and its entry removed, cascading through emptied branches; a
root branch left with one child is replaced by that child.

`delete_range` recurses over the entries overlapping `[lo, hi]`. A child is
**covered** when its first key is `>= lo` and the next entry's key is
`<= hi + 1`; covered subtrees are freed by walking their branch pages — leaf
pages are released by id, never read. Boundary children recurse. A branch is
copied only once something below it actually changed. `truncate_before` is
`delete_range(..t)`.

Underfull leaves are not merged (deletes are corrections and retention,
not the hot path).

### 5.5 Batches

`Db::write` applies, in order: structural operations (`define_symbol`,
`delete_range` / `truncate_before`, `drop_symbol`) in submission order; then
record operations sorted by `(symbol, time, seq)` with submission order as
the tiebreak (a stable sort, skipped when the batch is already ordered);
then metadata updates. In the record phase each symbol's directory entry is
read once and written once. Any error rolls the whole transaction back.

### 5.6 Spilling

When the dirty set exceeds `max(cache pages, 1024)`, dirty leaves are written
to their (fresh) locations early and dropped from memory, re-read if touched
again. This is always safe: nothing committed can reach a fresh page.

### 5.7 Commit

1. Stamp CRCs; write dirty pages in id order, coalescing runs of consecutive
   ids into single writes of up to 1 MiB; extend the file to cover
   `page_count` if needed.
2. Data barrier (fsync), unless `Durability::None`.
3. Append this txn's freed pages as a pending group; write the meta for
   `T + 1` into slot `(T + 1) % 2` — the **commit point**.
4. Meta barrier, for `Durability::Immediate` only.
5. Publish in-process: the new meta becomes current; dirty pages enter the
   cache as committed images; waiters in `wait_for_txn` wake.

A crash before step 3 lands leaves only unreachable pages behind. A torn
meta write fails its CRC and the previous slot stays current. With
`Eventual`, the next commit's data barrier also makes this meta durable.

If anything fails, the transaction is rolled back: the free state is
restored from its undo log (failures during the batch) or discarded and
re-read from the file (failures during commit, where the meta may or may not
have landed — the handle adopts whatever the file says).

---

## 6. Free space

### 6.1 Allocation

Pages come from the reusable pool (lowest id first, keeping the file
compact) and otherwise from the end of the file (`page_count`). The file
never shrinks.

### 6.2 When a freed page may be reused

Pages freed by commit `F` join a pending group `F`. The group moves to the
pool only when

- **no snapshot older than `F` exists** in any process — such a snapshot can
  still reach those pages; and
- **commit `F` is durable** — otherwise a power cut could revert to a meta
  that still points at pages a later commit had already overwritten.

In-process snapshots are counted in a map. Other processes' snapshots are
found by probing locks (§7.2).

### 6.3 Persistence and rebuild

After a commit the free state is encoded inline in the meta slot when it
fits (about 7,600 extents), so the next writer — in this or another process
— loads it directly. When it does not fit, or its CRC fails, the next writer
rebuilds it: it walks the directory and every symbol's branch pages (leaves
are known from their parents and are not read) marking reachable pages; the
rest of `[first data page, page_count)` is free. Rebuilt pages are tagged as
a pending group of the current txn, so snapshots taken before the rebuild
stay protected. A rebuild also reclaims pages leaked by a crash.

---

## 7. Reading and concurrency

### 7.1 Optimistic reads

Methods on `Db` read the newest meta (from memory when exclusive, else two
128-byte reads) and run without registering. If a page they load fails its
version or CRC check, a concurrent writer recycled it: the read retries
against a fresh meta with backoff (up to 64 times before reporting
corruption). Scans resume just past the last key they returned — forward,
from its successor; reverse, from its predecessor — so they never repeat or
go backwards, and may see newer commits ahead of the cursor.

### 7.2 Snapshots

`Db::snapshot` registers the snapshot's txn `T`:

1. Read the newest meta, `T`.
2. Count it in-process; the first registration of `T` in this process takes
   a **shared** OS lock on byte `READER_BASE + T`.
3. Re-read the newest meta. If it is still `T`, done; otherwise unregister
   and retry.

The writer, about to release group `F`, takes a non-blocking **exclusive**
lock on `[READER_BASE, READER_BASE + F)` and releases it at once: success
means no process holds a snapshot older than `F` (a binary search over the
candidate groups finds the newest safe one).

*Why the re-check suffices:* pages reachable from `T` can only be recycled
after some commit `F > T` freed them and a later writer probed. If the
re-read still sees `T`, no such `F` has committed yet, so any later probe
happens after our lock is in place. *Why the probe never disturbs our own
locks:* the writer only probes `F` at or below the oldest in-process
snapshot, so its range never covers this process's own reader locks (which
matters where locks are per process).

Exclusive opens skip all of this: the handle's in-memory meta is
authoritative and registration is a counter under the same mutex that
publishes commits.

### 7.3 Locks

| Offset | Mode | Held by |
|---|---|---|
| `2^56` | shared / exclusive | every handle for its lifetime / an exclusive open |
| `2^56 + 1` | exclusive | the writer, for one transaction |
| `2^57 + T` | shared | snapshots of txn `T` in a process |

The offsets lie far beyond any page, so they never collide with I/O (Windows
enforces byte-range locks against reads and writes). Linux uses
open-file-description locks (falling back to process locks on kernels
without them), Windows `LockFileEx` on the handle, other Unix systems
process-associated `fcntl` locks. Because those are per process and released
when *any* descriptor for the file closes, a file may be opened only once per
process; a process-wide registry of canonical paths enforces it.

### 7.4 Page cache

A sharded CLOCK cache (up to 64 shards) maps page ids to `Arc` images and the
version each holds. Readers clone the `Arc`; the reference count is the only
pin, so eviction never invalidates a page a reader holds. Index and
directory pages enter hot; scan leaves enter cold, so a long scan does not
flush the index. Committed pages are published straight from the writer.
Pages are read from disk with positional I/O (`pread` / `ReadFile` with an
offset); there is no `mmap`.

### 7.5 Scans

A cursor keeps the root-to-leaf path of `Arc` pages, so stepping across
leaves needs no sibling pointers. Steps within a leaf are an index
increment. `next_chunk` hands out the in-range run of the current leaf.
`scan_multi` keeps one cursor per symbol in a binary heap ordered by
`(time, seq, symbol, position)`; after a stale page it re-seeks every cursor:
symbols after the last emitted one (in that order) at the last key
inclusive, the rest just past it, so the merged stream stays strictly
increasing.

---

## 8. Verification

`Db::verify` walks a snapshot: every page is in range, read once (no page
reachable twice), passes CRC, version, kind, level and owner checks; keys
increase within leaves and across branches; every separator equals its
subtree's first key and lies inside its parent's range; every subtree count
matches; every directory entry's count, leaf count and min/max match its
tree; leaf layouts match the symbol's record size; the persisted free list
names no reachable page. It reports the number of unreachable pages (free or
awaiting reuse).

Pages read from disk are also validated structurally on every load (slot
and value extents, capacities, levels, page id), so a damaged or hostile file
produces `Error::Corrupt`, never a panic or an out-of-bounds read.

---

## 9. Limits

| | 64 KiB pages | 4 KiB pages |
|---|---|---|
| Largest value (variable width) | 65,484 B | 4,044 B |
| Largest fixed record | 16,351 B | 991 B |
| Records per leaf, 64 B variable / fixed | 818 / 861 | 50 / 52 |
| Branch fanout | 2,046 | 126 |
| Largest file | 256 TiB | 16 TiB |
| Transactional user metadata | 4,096 B | 4,096 B |
| Minimum cache | 16 pages | 16 pages |
| Tree height limit | 12 | 12 |

---

## 10. Not implemented

- Merging underfull leaves after heavy deletes, and shrinking the file.
- Asynchronous read-ahead (`prefetch` warms the cache synchronously).
- Compression, secondary indexes, multi-batch transactions.
- Network filesystems: byte-range locks and fsync semantics vary too much.
- Big-endian hosts: the format is explicit little-endian, so they should
  work, but they are not tested.

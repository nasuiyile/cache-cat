# Data Structure Design

## Data Structure Documentation

> For simplicity, Redis's basic data structures are used as headings. Most operations are still in the development adaptation phase.

### String

Currently implemented via moka. All key-value operations for the following data structures use moka as the foundation.

### Hash

Redis Hash fields use `ScanMap<Bytes, HashValue>` inside the existing `Arc<Mutex<_>>`. Entries are dense; bucket chains store entry indices and cached hashes without allocating a node per field. It supports the lookup, insert, remove, iteration, clone and serde APIs used by existing Hash commands. The outer mocha cache and other data types retain their existing maps.

Bucket counts are powers of two, with a maximum load factor of 0.5; the lower load only enlarges the compact bucket array, not the dense entries allocation. The default keyed hasher is `ahash::RandomState`. Each bucket maintains a fingerprint bitset to reject most misses without reading entries. Removal refreshes the filter so churn cannot accumulate stale bits. Growth rebuilds all bucket chains synchronously; there is no incremental rehash or automatic shrinking on removal. Removal swaps in the last entry and repairs its incoming bucket link, keeping ordinary iteration proportional to the number of fields. Growth holds the Hash mutex and can delay serial Raft writes.

`HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]` implements `ReadCommand`. Ordinary reads use a bit-reversed cursor and visit entire buckets, preserving coverage of fields present throughout a full iteration despite growth and insertion/removal of other fields. COUNT defaults to 10 and is a work hint counted before MATCH filtering. A call visits at most `10 * COUNT` buckets, never splitting a collision chain: replies can exceed COUNT or contain no matches with a nonzero cursor. Zero starts or completes an iteration; there is no per-client scan state.

Lua/EXEC reads applied through Raft can affect subsequent writes. Since replicas have different local hash seeds and capacities, this path returns every matching field sorted by field bytes and cursor 0. It also accepts ordinary scan cursors and may repeat previously returned fields. Its cost is O(N + M log M), where N is the field count and M the matching count; ordinary HSCAN does not sort the whole hash. Snapshots serialize only fields and values, rebuilding indexes and randomized seeds on restore. Ordinary cursors are not guaranteed to survive restart, snapshot installation or leader changes; clients should restart at cursor 0 in those cases.

MEMORY USAGE accounts for the allocated entry and bucket arrays. `cache_cat/examples/hash_map_bench.rs` compares lookup, update, deletion, insertion and iteration against the former standard HashMap.

### List

Implemented using `std::collections::LinkedList`. For concurrent read and write, crossbeam's SegQueue can be used.

### Set

An unordered set, currently implemented using Rust's hashset. For concurrent read and write, only flurry can be used.

### Sorted Set

Also called Zset in Redis. Currently implemented using Rust's built-in BTreeSet instead of crossbeam's skiplist.

### Others

It is known that Redis also supports bitmap, hyperloglog, Geospatial, Streams, Bitfield, etc. There are currently no plans to support them.

## Read-Write Concurrency Model

In Redis, there is a core execution queue, the dispatcher, and all commands are executed in a single thread. However, this is not suitable in Raft, as there is a significant latency gap between read and write commands. Read commands can typically return directly within the lease period, while write commands must go through a full round of the consensus algorithm. Additionally, the core Raft execution is also single-threaded.
Naturally, cache-cat uses a read-write concurrent Hashmap.

Rust's moka library ensures that each single-key operation is atomic. Read-read conflicts obviously do not occur. Write-write operations are serialized, so atomicity issues do not arise. (Write here includes delete operations)

|                 | Single-key Write (multiple) | Multi-key Write | Single-key Read | Multi-key Read |
| --------------- | --------------------------- | --------------- | --------------- | -------------- |
| Single-key Read (multiple)  | ✓                           | x               | ✓               | ✓              |
| Multi-key Read              | x                           | x               | ✓               | ✓              |

When a multi-key read encounters multiple single-key writes — for example, reading k1 and k2 via mget: read k1 -> write k1 -> write k2 -> read k2 — atomicity issues arise. The same applies to multi-key writes.

When a single-key read encounters a multi-key write: write k1 -> read k1 -> read k2 -> write k2 — an intermediate state is read.

Cache-cat uses read_lock and write_lock to optimize this issue.

|                 | read_lock (RwLock) | write_lock (Mutex) |
| --------------- | ------------------ | ------------------ |
| Single-key Read (multiple)  | Read lock          | x                  |
| Multi-key Read              | Read lock          | ✓                  |
| Single-key Write (multiple) | x                  | ✓                  |
| Multi-key Write             | Write lock         | ✓                  |

Since multi-key reads and writes are absolute minority operations, both locks only need to be acquired for multi-key operations.

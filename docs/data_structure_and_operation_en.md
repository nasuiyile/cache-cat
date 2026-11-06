# Data Structure Design

## Data Structure Documentation

> For simplicity, Redis's basic data structures are used as headings. Most operations are still in the development adaptation phase.

### String

All outer key/value operations use `mocha::map::HashMap`; `mocha::Mocha` wraps it with logical clocks and timing-wheel expiration. The local concurrent table in `cache_cat/src/mocha/map` derives from Papaya 0.2.4. Production no longer depends on the Papaya crate; it remains a dev dependency for comparison benchmarks. Atomic publication, incremental resizing and Seize reclamation are retained, supporting serial Raft writes alongside concurrent reads, background expiration and lazy deletion.

`SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` implements `MultiReadCommand` through a database traversal hook, without materializing a database-wide input-key list or cloning values. A bit-reversed cursor visits logical home hash buckets, completing each selected bucket's quadratic probe sequence and corresponding buckets in migration successors. Growth, shrinking, same-size rehashing and changes to other keys do not omit keys present throughout an iteration. Migration may produce duplicates. COUNT defaults to 10, counts candidates before expiration/MATCH/TYPE filtering, and limits each page to `10 * COUNT` root logical buckets. Collision groups are never split, so replies may exceed COUNT or be empty with a nonzero cursor. Filtering fixes read/write clocks for each call; TYPE shares the existing TYPE command's names, and unknown type names legally match nothing.

SCAN keeps no server-side sessions, extra key index or per-entry fields and adds no locks to ordinary GET/SET. It recomputes candidate hashes and resolves copied entries through current lookups, so it is slower than ordinary linear iteration; migration may require visiting several tables. In one table, each bucket examines at most `min(capacity, 1 + probe_limit)` slots. Frontier discovery and copied-entry lookups retain the underlying lock-free, not wait-free, progress qualification. Snapshots continue to use the original duplicate-free physical iterator and revision/delta protocol, not SCAN.

Lua/EXEC returns all matching keys sorted by bytes with cursor 0, avoiding replica-local hash layout influencing subsequent writes. This costs O(N + M log M); ordinary reads do not sort the database. Clients must restart at cursor 0 after restart, snapshot installation or leader changes. `cache_cat/examples/mocha_map_bench.rs` compares allocation bytes and common operations; `cache_cat/src/mocha/map/README.md` documents the algorithm and inherited code.

Empty databases, including after FLUSHDB, return cursor zero. For allocated tables, a zero sharded count only triggers verification: SCAN visits physical slots and validates copied entries against current state so transient counter values cannot omit stable keys. This exceptional emptiness check visits allocated slots and is not bounded by COUNT; it adds no counter operations to ordinary reads/writes. See `docs/mocha_lock_optimization.md` for validation and benchmark methodology.

Resizing is incremental only, with a default batch of 64 slots. Copy/promotion waiters use shared address-keyed parking queues; each table retains only an atomic pending count instead of a waiter-registry mutex and nested hash maps. No queue lock is touched when no one waits. A compact `parking_lot::Mutex<()>` still serializes successor allocation to prevent competing helpers allocating duplicate large tables; ordinary lookup and point writes outside migration do not take it.

### Hash

Redis Hash fields use `ScanMap<Bytes, HashValue>` inside the existing `Arc<Mutex<_>>`. Entries are dense; bucket chains store entry indices and cached hashes without allocating a node per field. It supports the lookup, insert, remove, iteration, clone and serde APIs used by existing Hash commands. ZSET also uses ScanMap for its member index, as described below; the outer mocha cache retains its existing map.

Bucket counts are powers of two, with a maximum load factor of 0.5; the lower load only enlarges the compact bucket array, not the dense entries allocation. The default keyed hasher is `ahash::RandomState`. Each bucket maintains a fingerprint bitset to reject most misses without reading entries. Removal refreshes the filter so churn cannot accumulate stale bits. Growth rebuilds all bucket chains synchronously; there is no incremental rehash or automatic shrinking on removal. Removal swaps in the last entry and repairs its incoming bucket link, keeping ordinary iteration proportional to the number of fields. Growth holds the Hash mutex and can delay serial Raft writes.

`HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]` implements `ReadCommand`. Ordinary reads use a bit-reversed cursor and visit entire buckets, preserving coverage of fields present throughout a full iteration despite growth and insertion/removal of other fields. COUNT defaults to 10 and is a work hint counted before MATCH filtering. A call visits at most `10 * COUNT` buckets, never splitting a collision chain: replies can exceed COUNT or contain no matches with a nonzero cursor. Zero starts or completes an iteration; there is no per-client scan state.

Lua/EXEC reads applied through Raft can affect subsequent writes. Since replicas have different local hash seeds and capacities, this path returns every matching field sorted by field bytes and cursor 0. It also accepts ordinary scan cursors and may repeat previously returned fields. Its cost is O(N + M log M), where N is the field count and M the matching count; ordinary HSCAN does not sort the whole hash. Snapshots serialize only fields and values, rebuilding indexes and randomized seeds on restore. Ordinary cursors are not guaranteed to survive restart, snapshot installation or leader changes; clients should restart at cursor 0 in those cases.

MEMORY USAGE accounts for the allocated entry and bucket arrays. `cache_cat/examples/hash_map_bench.rs` compares lookup, update, deletion, insertion and iteration against the former standard HashMap.

### List

Implemented using `std::collections::LinkedList`. For concurrent read and write, crossbeam's SegQueue can be used.

### Set

Sets use `ScanSet<Bytes>`, a set API over ScanMap with zero-sized `()` values, inside the existing `Arc<Mutex<_>>`. Sets select a maximum bucket load of 0.75 at compile time; Hash/ZSET retain the original 0.5 configuration. Insertions, removals, membership queries, dense iteration and bucket scans share ScanMap's implementation. `retain` compacts surviving entries and rebuilds bucket links in dense tables, while sparse tables remove entries individually to avoid traversing empty buckets. Both paths reuse cached hashes, allocate no temporary collection and keep indexes valid during unwinding. Set store operations build their destination ScanSet directly; read-only SINTER/SDIFF filter member vectors without copying or maintaining a temporary bucket index.

`SSCAN key cursor [MATCH pattern] [COUNT count]` implements `ReadCommand`. Ordinary reads use bit-reversed bucket cursors. COUNT defaults to 10 and counts entries before MATCH filtering; each call attempts at most `10 * COUNT` buckets without splitting collision chains. Replies may exceed COUNT or be empty with a nonzero cursor. Growth and insertion/removal of other members preserve coverage of members present throughout a full iteration. Both RESP2 and RESP3 return an array `[cursor, [member, ...]]`.

Lua/EXEC returns every matching member sorted by member bytes and cursor 0, avoiding replica-local hash seeds and capacities affecting later writes. This path costs O(N + M log M); ordinary reads do not sort the full set. Snapshot-time clones preserve hash seeds and bounded entry headroom so a subsequent single-member SADD does not move the entire entry array again. When buckets outnumber members by more than eight to one, clones rebuild a smaller bucket array using cached hashes. Bit-reversed cursors support synchronous growth and contraction: shrinking may repeat previously scanned members, but preserves coverage of members present throughout the iteration. Snapshots persist only member sequences and rebuild tables on restore; clients must restart at cursor 0 after restart, snapshot installation or leader changes.

MEMORY USAGE accounts for allocated bucket and entry arrays. The Set bucket load factor is at most 0.75 and entries/buckets carry index metadata: zero-sized `()` does not make its table cost equal to standard HashSet. Synchronous growth can still delay serial Raft writes. `cache_cat/examples/set_bench.rs` compares common reads/writes, iteration, retain, cloning, writes following cloning and table memory against standard HashSet. It supports operation/cardinality/member-width filters, fixed seeds within each sample, alternating run order and minimum accumulated timing to reduce noise. These are container microbenchmarks, not complete Raft request latency measurements.

### Sorted Set

ZSET uses an indexed skiplist ordered by `(score, member)` and a `ScanMap<Bytes, f64>` hash index for member lookup and cursor scans. Each skiplist node holds a small sorted block of members to reduce dependent memory accesses and allocations. Links carry spans measured in members, and level-zero backward links support reverse ranges. Nodes use stable arena indices and recycled slots, with at most 32 levels and a promotion probability of 1/4. Blocks split when full and merge with adjacent blocks when space permits; score updates staying within a block reuse its allocation. The existing per-value mutex protects both indexes; no additional locks are required.

`ZRANK`, `ZREVRANK`, and `ZCOUNT` take expected O(log N). Rank and score ranges seek their boundaries using spans and then visit the returned members, taking expected O(log N + M), including reverse ranges and large LIMIT offsets. BYLEX also uses indexed boundaries when all scores are equal, as required by Redis; mixed-score BYLEX retains deterministic filtering and may scan the whole set. Member lookup remains expected O(1), while insertions and removals take expected O(log N).

`ZSCAN key cursor [MATCH pattern] [COUNT count]` implements `ReadCommand` and uses the hash index's bit-reversed bucket cursor. COUNT defaults to 10 and is a work hint before MATCH filtering; complete collision chains are returned, and a page may be empty with a nonzero cursor. Ordinary scans do not traverse or sort the whole set. Members present throughout an iteration remain covered despite growth, other member insertions/removals, or score updates. Replies contain a cursor and a flat member/score array; scores are bulk strings under both RESP2 and RESP3.

For reads applied through Raft (Lua/EXEC), ZSCAN returns all matching members sorted by member bytes with cursor 0, independent of local hash seeds, capacity and skiplist layout. This path costs O(N + M log M); ordinary reads do not pay that cost. As with HSCAN, restart, snapshot installation and leader changes require restarting ordinary scans from cursor 0.

Snapshots serialize each logical `(member, score)` once in score/member order. Restoration validates that order. For sequences declaring more than 4096 members, a bounded buffer directly builds level links, member spans and backward links in order, while rebuilding the member hash index, in expected O(N) time. Construction preserves the ordinary sorted-insertion layout: at 65 buffered members, the first 32 form a node and the remainder stays in the tail. Nodes and the tail keep their usual spare capacity so subsequent insertions and merges need no extra reallocations. Smaller sets and encodings without a length hint retain per-member insertion. Initial preallocation is bounded; duplicate members, NaN scores and out-of-order entries are rejected, with signed zeros comparing as equal scores. This replaces the previous alpha ZSET encoding. MEMORY USAGE accounts for the hash arrays, skiplist arena and level allocations, counting shared member payloads once. `cache_cat/examples/zset_bench.rs` compares queries and writes against the former BTreeSet + HashMap implementation. `cache_cat/examples/zset_restore_bench.rs` measures snapshot decoding and reads/writes on the restored layout.

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

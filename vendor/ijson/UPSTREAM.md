# Redis ijson in Cache-cat

Source: <https://github.com/RedisJSON/ijson>

Pinned commit: `711bdce15fad668c83399b74bd76b455b4207408`, used by RedisJSON
8.10.0 in Redis 8.10.3. The upstream MIT and Apache-2.0 license files are
included unchanged. `README.md` is upstream documentation; its old benchmark
numbers are not measurements of Cache-cat.

This is a vendored Rust dependency so the following changes are reviewable and
reproducible. Keep upstream layout and tests when updating it.

## Local adaptations

- The compact string implementation always uses a shared
  `OnceLock<Mutex<HashSet<WeakIString>>>`. It never creates a mutable reference to
  a global before locking. Shared string headers are accessed through `ThinRef`,
  and reference counts remain atomic. Short strings retain their seven-byte
  inline representation. Weak handles are only accessed under the cache mutex;
  final-reference removal and deallocation happen under that same lock.
- The `thread_safe` feature is retained as a compatibility alias. It uses the
  same compact, synchronized implementation instead of selecting the older
  string representation. Initialization always selects the synchronized cache,
  including when upstream callers pass `false`.
- String interning no longer heap-allocates a boxed insertion closure. String,
  number, array and object allocation check null returns before initializing
  memory, using Rust's standard `handle_alloc_error` behavior on exhaustion.
- Object lookups and removals accept borrowed string content directly. Looking
  up a long field name does not intern it, allocate a temporary string, or lock
  the global string table; field insertion still interns the stored name.
- `IArray::replace` updates one element with lossless type promotion, without
  shifting the rest of the array. `push_heterogeneous` permits snapshot decoding
  to preserve an existing heterogeneous representation, even for numeric values.
- Unused upstream CBOR/compression and alternative string source files are
  omitted, along with their dependencies and upstream examples/fuzz workspace.
  In particular there are no floating git dependencies on the ciborium fork.
  Cache-cat's `JsonDocument` supplies explicit tagged, streaming binary serde
  for snapshots, preserving typed-array widths and float bits. JSON parsing and
  JSON response serialization continue to use the upstream implementation.

## Validation

Run upstream unit tests independently:

```text
cargo test --manifest-path vendor/ijson/Cargo.toml --lib -- --test-threads=1
```

The single test thread avoids inter-test interference in upstream global
allocation accounting. Cache-cat's `structure::json` tests separately exercise
concurrent owners, cloning, lookups, serialization and last-reference drops.
They also verify tagged snapshot round trips, malformed-input rejection and
typed float-array preservation. The project lockfile pins production dependency
versions; the vendor's standalone test lockfile is ignored.

## Memory and concurrency scope

`IValue` is one machine word on the supported 64-bit targets; arrays and objects
retain upstream compact layouts. Long strings share allocations. Upstream
`mem_allocated()` is a reachable-allocation estimate: it can count shared strings
more than once and excludes the shared table, allocator metadata and RSS.

Cloning still copies containers recursively and shares string storage. It is
not subtree copy-on-write. Cache-cat uses this clone only where its existing
snapshot protocol requires an independent document; ordinary partial updates
mutate the live document under its per-key mutex. The string-table mutex applies
only to JSON string interning/reclamation, never to other Redis data types.

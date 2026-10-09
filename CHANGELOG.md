# Changelog

## [0.3.0] - 2026-10-09

- Breaking: `push` accepts `Box<T>` and `PushError::into_inner` returns `Box<T>`.
  Crossbeam `Owned` conversion is internal and preserves the original allocation.
- Reexport `AtomicLink` for the minimal next/prev traits, without requiring an
  epoch ownership wrapper at call sites. Add a matching stack `is_empty` query.
- Keep only `ConcurrentShardedQueue` and `ConcurrentShardedStack` public.
- Normalize release metadata to UTF-8 and check its encoding in CI.

## [0.2.0] - 2026-10-09

- Breaking: rename `ConcurrentQueue` to `ConcurrentShardedQueue`, without an alias.
- Breaking: hide the strict FIFO implementation. Its history/lifetime tests remain.
- Update benchmarks to compare public queue configurations at 1, 8 and 32 shards.
- A locale-encoded character in the changelog prevented automated GitHub release
  notes; the compiled library and its tests were unaffected. Corrected in 0.3.0.


## [0.1.0] - 2026-10-09

Initial release.

- Minimal unsafe `SinglyLinked` / `DoublyLinked` traits exposing Crossbeam epoch
  atomic links to complete nodes, without adapters or `intrusive-collections`.
- `ConcurrentShardedStack`: per-shard LIFO, local push, XOR probing, close/drain.
- `ConcurrentQueue`: per-shard FIFO with the same locality and probing strategy.
- `ConcurrentFifoQueue`: globally linearizable Michael–Scott FIFO and atomic close.
- `Owned<T>` ownership transfer, shared-borrow callbacks, and epoch reclamation.
- Rust 1.85 minimum; default `std` collections and allocator-backed `no_std` traits.
- Dependency floors exclude RUSTSEC-2026-0204 and RUSTSEC-2022-0041 affected versions.
- Tests for unique IDs, FIFO histories, races, callbacks, and exact node destruction;
  benchmarks and allocation/CPU profiling with recorded tradeoffs.

Queue shards retain their most recently consumed node as a dummy. Sharded
collections provide neither global ordering nor a global atomic close operation.
See the README and safety documentation before implementing the unsafe link traits.

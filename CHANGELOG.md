# Changelog

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

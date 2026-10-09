# concurrent-intrusive-collections

[![CI](https://github.com/zhongyi51/concurrent-intrusive-collections/actions/workflows/ci.yml/badge.svg)](https://github.com/zhongyi51/concurrent-intrusive-collections/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/concurrent-intrusive-collections.svg)](https://crates.io/crates/concurrent-intrusive-collections)
[![docs.rs](https://docs.rs/concurrent-intrusive-collections/badge.svg)](https://docs.rs/concurrent-intrusive-collections)

Intrusive concurrent collections with Crossbeam epoch reclamation. Nodes contain
their own atomic links; there is no adapter, separate link-pointer representation,
or `intrusive-collections` dependency.

## Installation and selection

```toml
[dependencies]
concurrent-intrusive-collections = "0.3"
```

Requires **Rust 1.85+** and pointer-width atomics. The default `std` feature enables
all collections. Disabling it provides allocator-backed link traits and the epoch
reexport, not the concurrent containers themselves.

| Collection | Ordering | Close | Typical use |
| --- | --- | --- | --- |
| `ConcurrentShardedQueue` | FIFO per shard | Closes shards individually | Concurrent work with relaxed global ordering |
| `ConcurrentShardedStack` | LIFO per shard | Closes shards individually | Locality-oriented work stacks |

Push accepts a standard `Box<T>` and internally converts it to epoch ownership
without reallocating or moving the node. `AtomicLink` is a reexport of Crossbeam
Atomic for the link field; neither push nor errors require Crossbeam `Owned`.

These APIs transfer `Box<T>` nodes and expose popped data through shared-borrow
callbacks. They do not yield an owned node for immediate reuse. Queue shards can
retain their last popped node as a dummy. Read the
[safety and ownership contract](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/docs/SAFETY.md)
before implementing link traits. A complete producer/consumer example is in
[`examples/work_queue.rs`](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/examples/work_queue.rs):

```sh
cargo run --example work_queue
```

## Link traits

- `SinglyLinked`: an unsafe `next(&self) -> &AtomicLink<Self>` accessor.
- `DoublyLinked: SinglyLinked`: adds an unsafe
  `prev(&self) -> &AtomicLink<Self>` accessor.

Links point to **complete nodes**, not embedded link objects. Each accessor must
return a stable, private atomic field within its node. Link fields are non-owning:
node destructors must not follow or reclaim successors or predecessors. See the
traits' `Safety` documentation for the implementation contract.

`DoublyLinked` supplies the backward-link capability for future collections; a
concurrent doubly linked collection is not implemented yet.

## Concurrent sharded stack

`ConcurrentShardedStack<T>` adapts the sharded Treiber algorithm from
[`concurrent-sharded-stack` 0.2.1, revision `9ce4f96`](https://github.com/zhongyi51/concurrent-sharded-stack/tree/9ce4f967c0c4679540a2bb4312a2fdfcf2435a9b).
It retains cache-padded shards, thread-local shard selection, XOR scanning, and
per-shard closed head tags. The adapted code is distributed under the MIT license.

```rust
use concurrent_intrusive_collections::{
    ConcurrentShardedStack, PopError, SinglyLinked,
    AtomicLink as Atomic,
};

struct Job {
    next: Atomic<Self>,
    name: String,
}

// SAFETY: next is a fixed, private, non-owning field of each Job.
unsafe impl SinglyLinked for Job {
    unsafe fn next(&self) -> &Atomic<Self> {
        &self.next
    }
}

let stack = ConcurrentShardedStack::with_concurrency(4);
stack.push(Box::new(Job {
    next: Atomic::null(),
    name: "compile".into(),
})).unwrap();

let name = stack.pop_with(|job| job.name.clone()).unwrap();
assert_eq!(name, "compile");
assert_eq!(stack.pop_with(|_| ()), Err(PopError::Empty));

assert!(stack.close());
assert_eq!(stack.pop_with(|_| ()), Err(PopError::Closed));
```

The stack is `Send + Sync` for nodes implementing
`SinglyLinked + Send + Sync + 'static`, so it can be shared through `Arc`.

| Operation | Behavior |
| --- | --- |
| `new()` | Uses available parallelism, rounded up to a power of two. |
| `with_concurrency(n)` | Uses exactly `n` shards; `n` must be a nonzero power of two. |
| `push(Box<T>)` | Transfers ownership of the original node allocation, without adding a wrapper. |
| `pop_with(f)` | Removes a node and returns the result of `f(&T)`; returns `Empty` or `Closed` if none was found. |
| `close()` | Closes all shards, allowing existing nodes to drain. |
| `is_empty()` | Observes empty shards during a scan; not a global snapshot. |
| `is_closed()` | Reports whether all shards were observed closed. |
| `shard_count()` | Returns the configured shard count. |

A rejected push returns the original allocation in `PushError::Closed`; use
`into_inner()` to recover its original `Box<T>`. Its forward link is cleared.
Pointer tags are internal; callers do not construct epoch ownership wrappers.

### Ordering and closing

Ordering is LIFO **within a shard**, not globally. Threads push locally and scan
`local ^ 0, local ^ 1, ...` on pop. An `Empty` result describes a scan, not a
simultaneous snapshot of every shard.

`close()` returns whether this call closed at least one shard. Multiple concurrent
callers may return `true`. It does not close every shard in one atomic step:
overlapping pushes may still succeed on an open shard. After `close()` returns,
new pushes fail. Closed shards retain their nodes until popped or dropped.

### Ownership and reclamation

```text
Box<T> --push--> linked node --pop_with--> shared callback access
                                               |
                                               v
                                  epoch-deferred node destruction
```

The stack pins Crossbeam's default collector internally. `pop_with` schedules
destruction before invoking the callback, so callback panics also leave the node
queued for reclamation. The active guard keeps it alive throughout the callback.
Callbacks may copy or clone data, return independently owned results, and reenter
the stack; they cannot return a borrowed node reference or obtain `&mut T`.

Unlike the old non-intrusive stack, this API does **not** move an arbitrary payload
out of a private wrapper. The caller owns the node layout. Removed nodes are
destroyed after a grace period, not returned for immediate reinsertion. This
avoids reusing a link while another reader still holds its old address.

Keep callbacks short: they run while pinned and can delay reclamation. Reclamation
has no fixed completion time, and `flush()` does not wait for a grace period.
Dropping the stack synchronously destroys nodes still linked in it; previously
popped nodes remain the collector's responsibility.

Only the link-update algorithm is lock-free. Allocation, collector work, user
accessors, and callbacks need not be lock-free.

## Concurrent sharded queue

`ConcurrentShardedQueue<T>` uses independent Michael–Scott FIFO shards, local enqueue,
and the same XOR probing as the stack. `new()` uses available parallelism rounded
up to a power of two; `with_concurrency(n)` selects a nonzero power-of-two count.
`shard_count()` reports that count. A single guard protects each full pop scan.

Consumers probe `local ^ 0`, `local ^ 1`, ... . This visits successively larger
binary groups around the local shard, but it is still an **O(n) worst-case scan**,
not an O(log n) search or a hardware-topology-aware tree.

FIFO is guaranteed **within each shard**, including successive enqueues by the
same thread. There is no global FIFO guarantee or fixed bound on how far an item
may be overtaken. A busy local shard can starve remote shards. `is_empty()` and
an `Empty` pop describe observations made during a scan, not one atomic snapshot.

`close()` closes every shard and returns whether this caller changed any shard.
Several concurrent callers may return `true`; pushes overlapping close may
succeed on still-open shards. All later pushes fail after `close()` returns.
Existing nodes remain drainable. Each used shard can retain its last popped node
as a dummy until its next successful pop or collection destruction.

The API is `push(Box<T>)`, `pop_with(f)`, `close()`, `is_closed()`, and
`is_empty()`, with the same node bounds and callback restrictions as the stack.
Use `with_concurrency(1)` for globally ordered FIFO through the same public API.

The default was changed to sharding after paired throughput benchmarks showed
substantial gains under multi-producer contention. Single-thread and empty-scan
costs increase; this is not a universal performance improvement. See
[benchmark methodology and measured results](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/benches/README.md). The strict FIFO shard is an internal implementation detail.
The subsequent [profiling report](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/benches/PROFILING.md) explains allocation costs,
the smaller shard layout, and the measured empty-probe optimization.

## Internal helpers

`helper` holds the thread-local shard ID, default concurrency, node-alignment
check, and closed-pointer tag. All collections share `PushError` and `PopError`;
their implementations remain separate because ordering and reclamation differ.

## Features

The default `std` feature enables the sharded stack and queue, and Crossbeam's default
collector. With `default-features = false`, the crate exposes the two link traits
and reexports `crossbeam-epoch` with `alloc`, without requiring `std` itself.

## Validation

```sh
cargo test --all-features
cargo test --release --all-features
cargo check --no-default-features
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

Tests cover per-shard LIFO, per-shard/global queue FIFO, XOR probe order,
ownership on rejection, input pointer
tags, shutdown races, reentrant/panicking callbacks, deferred destruction,
moving-dummy retention, and concurrent unique IDs with no duplication or loss.
Paused-reader tests exercise stale snapshots while another thread removes a
node. Queue unit tests model a producer paused after publication but before
tail advancement. Concurrent workloads are reduced under Miri.

```powershell
$env:MIRIFLAGS = "-Zmiri-tree-borrows -Zmiri-ignore-leaks"
cargo +nightly miri test --lib --tests -- --test-threads=1
```

Crossbeam 0.9.21's default collector leaves internal allocations at process exit;
this also reproduces with only `pin`, `defer`, and `flush`, without this crate.
The Miri command therefore disables its process-exit leak check. The tests still
explicitly wait for every test node's destructor and check destruction counts.
The tested Windows nightly also reports a Stacked Borrows violation in
Crossbeam's internal participant list during `pin`; the same standalone baseline
reproduces it. The command above uses Tree Borrows instead.
Crossbeam's integer-to-pointer conversions also limit Miri's provenance coverage;
passing this suite is not a proof of all possible concurrent executions.

CI runs native tests on Linux, Windows, and macOS, the Rust 1.85 MSRV, bare-metal
`no_std` compilation, Miri, documentation checks, and crate packaging. Benchmark
results are manual measurements, not CI performance guarantees.

## Project information

- [API documentation](https://docs.rs/concurrent-intrusive-collections)
- [Changelog](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/CHANGELOG.md)
- [Contributing](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/CONTRIBUTING.md)
- [Release process](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/RELEASING.md)
- [Security reporting](https://github.com/zhongyi51/concurrent-intrusive-collections/blob/main/SECURITY.md)

Licensed under MIT. The stack adapts `concurrent-sharded-stack` under its MIT
license; its original copyright notice is retained in LICENSE and the source.

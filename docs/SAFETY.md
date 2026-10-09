# Ownership, ordering, and memory safety

This document records the implementation invariants for version 0.3.0. It is an
audit aid, not a formal proof or a substitute for the unsafe trait contracts.

## Node contract

`SinglyLinked::next` and `DoublyLinked::prev` return stable, distinct atomic fields
inside a complete node. They cannot panic or modify links. The implementing
module must prevent safe code, including callbacks and interior-mutable methods,
from changing links while a collection owns the node or retirement is pending.
Keeping the fields private is part of maintaining this boundary, not sufficient
by itself. Destructors must not follow or reclaim the non-owning links.

Collections require `Send + Sync + 'static`. `Box<T>` transfers an allocation
exactly once. Internally `Owned::<T>::from(boxed)` retains the original allocation
and address without allocating again. Rejection converts it back with `into_box`. The forward link is cleared before publication;
only one low tag bit is used for closure. Node alignment is checked. A rejected
push returns the original allocation. A successful pop yields no `&mut T`, `Box<T>`, or escaping
borrow available from a successful pop. Payloads can be copied or cloned inside
the callback, which may reenter the collection.

All public operations pin Crossbeam's default collector internally. The internal
guard-taking shard helpers receive that same collector's guard. No arbitrary
user collector can be mixed into these paths.

## Stack

Push publishes a node and its initialized forward link with a Release CAS.
Pop loads the head with Acquire, reads its immutable next link, and replaces the
head with an AcqRel CAS. Exactly the CAS winner retires the removed complete node
before calling user code. The guard covers the callback and competing stale
readers, preventing reuse of that address while a stale CAS can still occur.

`close` atomically tags each shard head, retaining its pointer. Pop carries that
tag to the new head. Closed shards never reopen. A pop that observes all shards
closed and empty can return `Closed`; an open `Empty` scan is not a global snapshot.

## FIFO shard

Every shard uses a moving dummy. Before its first pop, null head/tail pointers
identify a virtual dummy whose next field is stored in the queue. No virtual
pointer is cast to a `T`. The queue can move when exclusively owned because no
pointer refers into that queue object.

Publication changes the terminal next link from null to a node exactly once.
A later tail CAS is helping: any thread may perform it. A dequeue advances head
to its successor and retires the **previous** real dummy. Head cannot advance
past tail; the pop path first helps when head and tail name the same node.
Acquire reads observe initialized data, and Release tail updates carry that
publication to threads entering through tail. Head CAS is AcqRel.

The empty fast path reads head, reads its next link, and validates head again.
If that next link is null, no successor was available at that observation; tail
does not need to be read. Nonempty paths still help tail and win the head CAS
before retiring anything. A real dummy is never retired twice.

The initial next link is never reset. It may contain a dangling old first-node
pointer after head advances. Such an old snapshot is never dereferenced without
winning the head CAS from null; head and tail never return to null. A guard
protects any operation that observed that first node while it was reachable.
Drop follows the current head once it is real, never that stale initial link.

Close and enqueue compete on the terminal next link: null becomes either a real
successor or a tagged closed null. A close winner therefore prevents subsequent
enqueues. A strict FIFO has exactly one successful close caller.

The consumed node becomes the next dummy and remains readable through the
callback's guard even if another pop retires it. Each shard can retain one such
node while empty or closed, until another successful pop or queue destruction.
This is intentional retention, separate from delayed epoch reclamation.

## Sharding and observable semantics

| Type | Ordering | Successful-operation linearization | Close |
| --- | --- | --- | --- |
| `ConcurrentShardedStack` | LIFO within each shard | Successful shard-head CAS | Per-shard tagged-head RMW |
| `ConcurrentShardedQueue` | FIFO within each shard | Corresponding FIFO shard operation | Per-shard terminal-link CAS |
| Internal FIFO shard | Global FIFO | Enqueue: terminal-link CAS; dequeue: head CAS | One terminal-link CAS |

Sharded collections choose a stable per-thread shard. Numeric thread IDs may
collide or wrap; only locality depends on their uniqueness, never safety.
XOR probing visits every shard once, with O(shards) worst-case work. It is not a
global FIFO/LIFO order, a hardware-topology mapping, or a fairness guarantee.
A continuously busy local shard can starve remote shards. Several concurrent
sharded close callers may return true, but all shards are closed after any one
close call returns. Callback completion order is not pop linearization order.

## Destruction and progress

Drop has exclusive ownership and destroys still-linked nodes synchronously.
Already-retired nodes belong to the collector. Node destructors may run on another
thread. Callback panic still consumes its node, and the guard drops during unwind.
As with ordinary Rust destructors, a panicking user destructor can interrupt
cleanup; the library does not promise recovery from it.

Only link-update algorithms are lock-free. Allocation, collector work, trait
accessors, and callbacks may block. There is no fixed reclamation deadline, and
a pinned thread or long callback can delay the whole collector's grace period.
Flushing the collector is not waiting for all destructors to finish.

## Evidence and limits

The dependency minimums exclude the affected releases in
[RUSTSEC-2026-0204](https://rustsec.org/advisories/RUSTSEC-2026-0204.html)
and [RUSTSEC-2022-0041](https://rustsec.org/advisories/RUSTSEC-2022-0041.html).
Keep reviewing upstream advisories when updating dependencies or releasing.

Tests cover exact ID sets, per-producer order, rejected ownership, stale readers,
paused publication/tail helping, concurrent close, callback panic/reentrancy,
dummy retention, and complete node destruction. A bounded-history checker tries
all legal sequential orderings of overlapping strict FIFO calls while respecting
their real-time precedence. It checks enqueue, dequeue, empty/closed results, and
close against a `VecDeque` model. This is sampled execution testing, not exhaustive
verification of every weak-memory execution.

CI tests Linux, Windows, macOS, Rust 1.85, a pointer-atomic bare-metal `no_std`
target, and Miri. The Miri invocation uses Tree Borrows and disables process-exit
leak checking because the standalone Crossbeam baseline reproduces the documented
collector issues. Tests separately count node destruction. Crossbeam's pointer/
integer conversions limit provenance coverage. Passing these checks is not a
claim of a formally verified algorithm.

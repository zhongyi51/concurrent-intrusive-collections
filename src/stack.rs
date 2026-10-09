// Adapted from concurrent-sharded-stack 0.2.1 (9ce4f96), under the MIT license.
// Copyright (c) 2026 concurrent-sharded-stack contributors

use crate::helper::{CLOSED_TAG, assert_node_alignment, current_thread_id, default_concurrency};
use crate::{PopError, PushError, SinglyLinked, epoch};
use crossbeam_utils::{Backoff, CachePadded};
use epoch::{Atomic, Owned, Shared};
use std::sync::atomic::Ordering;

/// An intrusive Treiber stack distributed across cache-padded shards.
///
/// Each thread pushes to its assigned shard. Pops scan the local shard first,
/// then `local ^ 1`, `local ^ 2`, and so on. Ordering is LIFO within each shard,
/// not across the entire collection. Shards may be shared by multiple threads.
///
/// The stack owns complete nodes supplied as [`Box<T>`], without allocating
/// a wrapper node. All operations use Crossbeam's default collector internally.
/// A successful [`pop_with`](Self::pop_with) borrows the removed node only for
/// its callback, then epoch GC eventually destroys it. It never returns an
/// owned node that could be freed or reinserted while old readers still use it.
///
/// Link updates use lock-free atomics, with bounded spinning followed by
/// yielding under contention. Allocation, reclamation, trait accessors, and
/// user callbacks are not promised to be lock-free.
///
/// # Example
///
/// ```
/// use concurrent_intrusive_collections::{
///     ConcurrentShardedStack, PopError, SinglyLinked, AtomicLink as Atomic,
/// };
///
/// struct Job {
///     next: Atomic<Self>,
///     name: String,
/// }
///
/// // SAFETY: next is a private, fixed, non-owning field of each Job.
/// unsafe impl SinglyLinked for Job {
///     unsafe fn next(&self) -> &Atomic<Self> { &self.next }
/// }
///
/// let stack = ConcurrentShardedStack::with_concurrency(4);
/// stack.push(Box::new(Job {
///     next: Atomic::null(),
///     name: "compile".into(),
/// })).unwrap();
/// assert_eq!(stack.pop_with(|job| job.name.clone()).unwrap(), "compile");
/// assert_eq!(stack.pop_with(|_| ()), Err(PopError::Empty));
/// assert!(stack.close());
/// assert_eq!(stack.pop_with(|_| ()), Err(PopError::Closed));
/// ```
pub struct ConcurrentShardedStack<T: SinglyLinked + Send + Sync + 'static> {
    shards: Box<[CachePadded<Atomic<T>>]>,
    shard_index_mask: usize,
}

impl<T: SinglyLinked + Send + Sync + 'static> Default for ConcurrentShardedStack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SinglyLinked + Send + Sync + 'static> ConcurrentShardedStack<T> {
    /// Creates a stack using the available parallelism, rounded up to a power
    /// of two, or four shards if the parallelism cannot be determined.
    pub fn new() -> Self {
        Self::with_concurrency(default_concurrency())
    }

    /// Creates a stack with exactly `shard_count` shards.
    ///
    /// # Panics
    ///
    /// Panics if `shard_count` is zero or is not a power of two. Also rejects
    /// node alignments smaller than two, since one pointer tag bit is reserved
    /// for closing. A node with an embedded `Atomic<Self>` meets this alignment.
    pub fn with_concurrency(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be non-zero");
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of two"
        );
        assert_node_alignment::<T>();

        let shards = (0..shard_count)
            .map(|_| CachePadded::new(Atomic::null()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            shards,
            shard_index_mask: shard_count - 1,
        }
    }

    /// Transfers ownership of a node into this thread's shard.
    ///
    /// On rejection, the error returns the same allocation with a null forward
    /// link. Pointer tags are internal; the stack owns the head tag.
    /// The node's previous forward-link contents are overwritten, not reclaimed.
    pub fn push(&self, node: Box<T>) -> Result<(), PushError<T>> {
        let shard = &self.shards[self.current_shard_index()];
        let guard = &epoch::pin();
        let mut node = Owned::<T>::from(node);
        let backoff = Backoff::new();

        loop {
            let head = shard.load(Ordering::Acquire, guard);
            // SAFETY: node is exclusively owned and has not been published.
            let next = unsafe { node.next() };
            if head.tag() == CLOSED_TAG {
                next.store(Shared::null(), Ordering::Relaxed);
                return Err(PushError::Closed(node.into_box()));
            }
            next.store(head, Ordering::Relaxed);

            match shard.compare_exchange_weak(
                head,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(_) => return Ok(()),
                Err(error) => {
                    // The failed CAS returns ownership of the unpublished node.
                    node = error.new;
                    backoff.snooze();
                }
            }
        }
    }

    /// Removes one node and calls `f` with a shared borrow of that node.
    ///
    /// The callback is called exactly once on success and never on error. Its
    /// result can own or copy data, but cannot borrow from the node. The node
    /// is retired before the callback runs, so unwinding also leaves it queued
    /// for reclamation. The guard keeps it alive until the callback finishes.
    ///
    /// The callback may reenter the stack. It runs while the current thread is
    /// pinned, so long-running callbacks delay reclamation. Neither returning
    /// from this method nor flushing the collector guarantees immediate Drop.
    ///
    /// Node references cannot escape the callback:
    ///
    /// ```compile_fail
    /// use concurrent_intrusive_collections::{ConcurrentShardedStack, SinglyLinked};
    /// fn escape<T: SinglyLinked + Send + Sync + 'static>(
    ///     stack: &ConcurrentShardedStack<T>,
    /// ) -> &T {
    ///     stack.pop_with(|node| node).unwrap()
    /// }
    /// ```
    pub fn pop_with<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, PopError> {
        let guard = &epoch::pin();
        let start = self.current_shard_index();
        let mut all_closed = true;
        let backoff = Backoff::new();

        for mask in 0..self.shards.len() {
            let shard = &self.shards[start ^ mask];
            loop {
                let head = shard.load(Ordering::Acquire, guard);
                if head.is_null() {
                    all_closed &= head.tag() == CLOSED_TAG;
                    break;
                }

                // SAFETY: Acquire observes publication, and this default-
                // collector guard prevents the head node from being reclaimed.
                let node = unsafe { head.deref() };
                // SAFETY: only push writes next, before publication. Neither
                // pop nor retirement changes it while old readers may use it.
                let next = unsafe { node.next() }.load(Ordering::Relaxed, guard);
                let new_head = next.with_tag(head.tag());

                match shard.compare_exchange(
                    head,
                    new_head,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                ) {
                    Ok(_) => {
                        // SAFETY: exactly this CAS winner retires the complete
                        // allocation originally transferred via Box<T>.
                        // New traversals cannot reach it; old readers and the
                        // callback remain protected by their guards. T: Send
                        // + 'static permits deferred destruction on any thread.
                        unsafe { guard.defer_destroy(head.with_tag(0)) };
                        return Ok(f(node));
                    }
                    Err(_) => backoff.snooze(),
                }
            }
        }

        Err(if all_closed {
            PopError::Closed
        } else {
            PopError::Empty
        })
    }

    /// Closes every shard. Already inserted nodes can still be popped.
    ///
    /// Returns true if this call closed at least one shard. Multiple concurrent
    /// callers can return true. Closing all shards is not one atomic operation:
    /// overlapping pushes may succeed on shards not closed yet. Once this call
    /// returns, all later pushes are rejected.
    pub fn close(&self) -> bool {
        let guard = &epoch::pin();
        let mut changed = false;
        for shard in &self.shards {
            // One atomic RMW preserves the current pointer and marks it closed.
            let previous = shard.fetch_or(CLOSED_TAG, Ordering::AcqRel, guard);
            changed |= previous.tag() != CLOSED_TAG;
        }
        changed
    }

    /// Returns whether every shard has been observed closed.
    pub fn is_closed(&self) -> bool {
        let guard = &epoch::pin();
        self.shards
            .iter()
            .all(|shard| shard.load(Ordering::Acquire, guard).tag() == CLOSED_TAG)
    }

    /// Returns whether every shard was observed empty during a scan.
    ///
    /// This is not a simultaneous snapshot; concurrent pushes can invalidate it.
    pub fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        self.shards
            .iter()
            .all(|shard| shard.load(Ordering::Acquire, guard).is_null())
    }

    /// Returns the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn current_shard_index(&self) -> usize {
        current_thread_id() & self.shard_index_mask
    }
}

impl<T: SinglyLinked + Send + Sync + 'static> Drop for ConcurrentShardedStack<T> {
    fn drop(&mut self) {
        // SAFETY: exclusive ownership excludes in-flight methods and callback
        // borrows. Callbacks cannot return safe node references; any raw
        // pointers they construct require unsafe access after returning.
        let guard = unsafe { epoch::unprotected() };
        for shard in &self.shards {
            let mut current = shard.swap(Shared::null(), Ordering::Relaxed, guard);
            while !current.is_null() {
                // SAFETY: this is a still-owned, unretired complete node.
                // Read its non-owning link before destroying the allocation.
                let next = unsafe { current.deref().next() }.load(Ordering::Relaxed, guard);
                // SAFETY: no readers remain for nodes still in this stack.
                // Nodes removed by pop_with are instead owned by the collector.
                unsafe { drop(current.with_tag(0).into_owned()) };
                current = next;
            }
        }
    }
}

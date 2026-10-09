use crate::helper::{CLOSED_TAG, assert_node_alignment};
use crate::{PopError, PushError, SinglyLinked, epoch};
use crossbeam_utils::{Backoff, CachePadded};
use epoch::{Atomic, Guard, Owned, Shared};
use std::sync::atomic::Ordering;

/// Internal, unbounded multiple-producer/multiple-consumer FIFO shard.
///
/// Uses the [Michael–Scott algorithm][ms] with epoch reclamation and helping:
/// any thread can advance a lagging tail after another thread links a node.
/// Unlike the sharded collections, enqueue and dequeue are globally FIFO and
/// linearizable. Overlapping operations may take effect in either order;
/// callback completion order need not be dequeue order.
///
/// [ms]: https://www.cs.rochester.edu/research/synchronization/pseudocode/queues.html
///
/// No wrapper allocation, `T: Default`, or user-supplied dummy node is needed.
/// The initial dummy is represented by null head/tail pointers and a separate
/// atomic link inside the queue. It is never dereferenced as a `T`. After the
/// first successful pop, the most recently popped node is the moving dummy.
///
/// # Node lifetime
///
/// [`pop_with`](Self::pop_with) logically consumes a node but keeps it as the
/// new dummy. The **previous dummy** is retired through Crossbeam's default
/// collector. Consequently, the last popped node stays allocated until the
/// next successful pop or queue destruction, even when the queue is empty or
/// closed. Other retired nodes are destroyed only after their grace period.
/// Callbacks receive shared access, cannot return node borrows, and may reenter
/// the queue; their guards protect nodes even if another pop retires them.
///
/// Only the link-update algorithm is lock-free. Allocation, collector work,
/// user accessors, and callbacks are not promised to be lock-free.
///
pub(crate) struct FifoQueue<T: SinglyLinked + Send + Sync + 'static> {
    // Null represents the initial, virtual dummy. Once non-null, neither
    // pointer becomes null again during the queue's lifetime.
    head: CachePadded<Head<T>>,
    tail: CachePadded<Atomic<T>>,
}

// The initial dummy's link is read with head. Keep both in one CachePadded
// block so initial_next does not require a third padded block by itself.
struct Head<T> {
    pointer: Atomic<T>,
    // The virtual dummy's next field. After first publication this must NOT
    // be reset: a pinned thread may still be working with the initial dummy.
    // It may eventually dangle, but after head advances it is never used to
    // reclaim or dereference a node without first winning a head CAS from null.
    initial_next: Atomic<T>,
}

impl<T: SinglyLinked + Send + Sync + 'static> Default for FifoQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SinglyLinked + Send + Sync + 'static> FifoQueue<T> {
    /// Creates an empty queue without allocating a dummy `T`.
    ///
    /// # Panics
    ///
    /// Rejects node alignments smaller than two: a low bit of a terminal null
    /// link encodes closure. An embedded `Atomic<Self>` meets this alignment.
    pub fn new() -> Self {
        assert_node_alignment::<T>();
        Self {
            head: CachePadded::new(Head {
                pointer: Atomic::null(),
                initial_next: Atomic::null(),
            }),
            tail: CachePadded::new(Atomic::null()),
        }
    }

    /// Appends the original node allocation, transferring its ownership.
    ///
    /// The successful CAS linking the node at the end is the enqueue
    /// linearization point. Updating the tail afterwards is only helping.
    /// On a closed queue the original allocation is returned in the error,
    /// with a null next link. Incoming tags and next contents are overwritten.
    pub fn push(&self, node: Owned<T>) -> Result<(), PushError<T>> {
        let mut node = node.with_tag(0);
        // SAFETY: the node is exclusively owned and not yet published.
        unsafe { node.next() }.store(Shared::null(), Ordering::Relaxed);
        let guard = &epoch::pin();
        let backoff = Backoff::new();

        loop {
            let (tail, link, next) = self.tail_slot(guard);
            if next.tag() == CLOSED_TAG {
                return Err(PushError::Closed(node));
            }
            match link.compare_exchange_weak(
                next,
                node,
                Ordering::Release,
                Ordering::Relaxed,
                guard,
            ) {
                Ok(published) => {
                    self.help_tail(tail, published, guard);
                    return Ok(());
                }
                Err(error) => {
                    node = error.new;
                    backoff.snooze();
                }
            }
        }
    }

    /// Consumes the oldest queued node and calls `f` once with a shared borrow.
    ///
    /// The head CAS is the dequeue linearization point. A panic in the callback
    /// still consumes that item; it is never returned by a later pop. The node
    /// remains the moving dummy until another pop retires it or the queue drops.
    /// An empty/closed result never invokes the callback.
    ///
    /// Callbacks run while pinned, so keep them short. They may copy/clone data
    /// or return independently owned results, but may not return node borrows:
    ///
    #[cfg(test)]
    pub fn pop_with<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, PopError> {
        let guard = &epoch::pin();
        self.pop_with_guard(f, guard)
    }

    #[inline]
    pub(crate) fn pop_with_guard<R>(
        &self,
        f: impl FnOnce(&T) -> R,
        guard: &Guard,
    ) -> Result<R, PopError> {
        let backoff = Backoff::new();
        loop {
            let head = self.head.pointer.load(Ordering::Acquire, guard);
            // SAFETY: a real head is protected by this guard; a null head is
            // the virtual dummy, whose link is stored in the queue itself.
            let next = unsafe { self.next_link(head) }.load(Ordering::Acquire, guard);
            if head != self.head.pointer.load(Ordering::Acquire, guard) {
                backoff.snooze();
                continue;
            }
            // A validated head with no successor is empty regardless of tail.
            // Avoid reading another cache line for every empty shard probe.
            if next.is_null() {
                return Err(if next.tag() == CLOSED_TAG {
                    PopError::Closed
                } else {
                    PopError::Empty
                });
            }
            let tail = self.tail.load(Ordering::Acquire, guard);
            if head == tail {
                // An enqueuer published next but has not advanced tail yet.
                // Move tail before permitting that old dummy to be retired.
                self.help_tail(tail, next, guard);
                backoff.snooze();
                continue;
            }

            if self
                .head
                .pointer
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Relaxed, guard)
                .is_ok()
            {
                // SAFETY: this CAS alone removes the old dummy. It is no
                // longer reachable from head or tail. A real dummy came from
                // Owned<T>; guards protect all old users, and T: Send permits
                // its eventual destructor to run on another thread.
                if !head.is_null() {
                    unsafe { guard.defer_destroy(head) };
                }
                // SAFETY: next is a real node, acquired from the published
                // link and now the new dummy. This guard also protects it if
                // a second consumer retires it before our callback finishes.
                return Ok(f(unsafe { next.deref() }));
            }
            backoff.snooze();
        }
    }

    /// Atomically closes the queue against further pushes; queued items remain.
    ///
    /// Closure competes with enqueue on the terminal next link. Exactly one
    /// concurrent closer returns true. An overlapping push succeeds only if
    /// its link CAS precedes the successful close CAS.
    #[cfg(test)]
    pub fn close(&self) -> bool {
        let guard = &epoch::pin();
        self.close_with_guard(guard)
    }

    pub(crate) fn close_with_guard(&self, guard: &Guard) -> bool {
        let backoff = Backoff::new();
        loop {
            let (_, link, next) = self.tail_slot(guard);
            if next.tag() == CLOSED_TAG {
                return false;
            }
            if link
                .compare_exchange(
                    next,
                    Shared::null().with_tag(CLOSED_TAG),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                    guard,
                )
                .is_ok()
            {
                return true;
            }
            backoff.snooze();
        }
    }

    /// Returns whether the terminal next link has been marked closed.
    ///
    /// May help advance a lagging tail while concurrent producers make progress.
    #[cfg(test)]
    pub fn is_closed(&self) -> bool {
        self.is_closed_with_guard(&epoch::pin())
    }

    pub(crate) fn is_closed_with_guard(&self, guard: &Guard) -> bool {
        self.tail_slot(guard).2.tag() == CLOSED_TAG
    }

    /// Observes whether no unconsumed nodes remain, whether open or closed.
    ///
    /// Another operation can change this result immediately after it is read.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        self.is_empty_with_guard(guard)
    }

    pub(crate) fn is_empty_with_guard(&self, guard: &Guard) -> bool {
        let backoff = Backoff::new();
        loop {
            let head = self.head.pointer.load(Ordering::Acquire, guard);
            // SAFETY: as in pop_with, guard protects any real dummy node.
            let next = unsafe { self.next_link(head) }.load(Ordering::Acquire, guard);
            if head == self.head.pointer.load(Ordering::Acquire, guard) {
                return next.is_null();
            }
            backoff.snooze();
        }
    }

    // A null snapshot names the initial dummy; it is never dereferenced as T.
    // For a real node, callers must keep the matching collector guard alive.
    unsafe fn next_link<'g>(&'g self, node: Shared<'g, T>) -> &'g Atomic<T> {
        if node.is_null() {
            &self.head.initial_next
        } else {
            unsafe { node.deref().next() }
        }
    }

    // Observe a terminal null link (possibly tagged closed), helping tail over
    // every already-linked node. The returned slot may change after this read;
    // callers that mutate it must compare_exchange the exact observed value.
    fn tail_slot<'g>(&'g self, guard: &'g Guard) -> (Shared<'g, T>, &'g Atomic<T>, Shared<'g, T>) {
        let backoff = Backoff::new();
        loop {
            let tail = self.tail.load(Ordering::Acquire, guard);
            // SAFETY: tail is either the virtual dummy or a protected node.
            let link = unsafe { self.next_link(tail) };
            let next = link.load(Ordering::Acquire, guard);
            if tail == self.tail.load(Ordering::Acquire, guard) {
                if next.is_null() {
                    return (tail, link, next);
                }
                self.help_tail(tail, next, guard);
            }
            backoff.snooze();
        }
    }

    fn help_tail<'g>(&self, tail: Shared<'g, T>, next: Shared<'g, T>, guard: &'g Guard) {
        // next has already been acquired from a published link. The release
        // carries that initialization to future threads acquiring the new tail.
        let _ = self
            .tail
            .compare_exchange(tail, next, Ordering::Release, Ordering::Relaxed, guard);
    }
}

impl<T: SinglyLinked + Send + Sync + 'static> Drop for FifoQueue<T> {
    fn drop(&mut self) {
        // SAFETY: no methods or callback borrows can still use this queue.
        let guard = unsafe { epoch::unprotected() };
        let head = self.head.pointer.load(Ordering::Relaxed, guard);
        // A real head is the last consumed node and still belongs to the queue.
        // Before any dequeue, start after the virtual dummy instead.
        let mut current = if head.is_null() {
            self.head.initial_next.load(Ordering::Relaxed, guard)
        } else {
            head
        };
        while !current.is_null() {
            // SAFETY: this is an owned, unretired complete allocation. Save
            // its link before destruction; a tagged terminal null ends the loop.
            let next = unsafe { current.deref().next() }.load(Ordering::Relaxed, guard);
            unsafe { drop(current.into_owned()) };
            current = next;
        }
    }
}

#[cfg(test)]
mod helping_tests {
    use super::*;
    use std::sync::{Arc, atomic::AtomicUsize};

    struct Node {
        next: Atomic<Self>,
        id: usize,
        drops: Arc<AtomicUsize>,
    }

    // SAFETY: each node owns its fixed, private, non-owning next field.
    unsafe impl SinglyLinked for Node {
        unsafe fn next(&self) -> &Atomic<Self> {
            &self.next
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn node(id: usize, drops: &Arc<AtomicUsize>) -> Owned<Node> {
        Owned::new(Node {
            next: Atomic::null(),
            id,
            drops: Arc::clone(drops),
        })
    }

    // Model an enqueuer paused immediately after its publication CAS, before
    // help_tail. Public operations must finish without that enqueuer resuming.
    fn publish_without_advancing_tail(queue: &FifoQueue<Node>, node: Owned<Node>) {
        let guard = &epoch::pin();
        let (_, link, next) = queue.tail_slot(guard);
        assert!(next.is_null());
        assert_eq!(next.tag(), 0);
        assert!(
            link.compare_exchange(next, node, Ordering::Release, Ordering::Relaxed, guard)
                .is_ok()
        );
    }

    #[test]
    fn pop_helps_tail_past_virtual_dummy() {
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = FifoQueue::new();
        publish_without_advancing_tail(&queue, node(1, &drops));
        assert!(queue.tail.load(Ordering::Acquire, &epoch::pin()).is_null());
        assert_eq!(queue.pop_with(|node| node.id), Ok(1));
        assert!(queue.is_empty());
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        drop(queue);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn push_and_close_help_tail_past_real_nodes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = FifoQueue::new();
        queue.push(node(1, &drops)).unwrap();
        publish_without_advancing_tail(&queue, node(2, &drops));
        queue.push(node(3, &drops)).unwrap();
        publish_without_advancing_tail(&queue, node(4, &drops));
        assert!(queue.close());
        {
            let guard = &epoch::pin();
            let tail = queue.tail.load(Ordering::Acquire, guard);
            // SAFETY: tail points to a published node protected by guard.
            assert_eq!(unsafe { tail.deref() }.id, 4);
        }
        assert!(queue.is_closed());
        // No dequeue occurred, so Drop owns and destroys the entire chain.
        drop(queue);
        assert_eq!(drops.load(Ordering::Relaxed), 4);
    }
}

#[cfg(test)]
mod tests;

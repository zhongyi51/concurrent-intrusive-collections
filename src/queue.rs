use crate::fifo_queue::FifoQueue;
use crate::helper::{current_thread_id, default_concurrency};
use crate::{PopError, PushError, SinglyLinked, epoch};
use epoch::Owned;

/// An intrusive MPMC queue distributed across independent FIFO shards.
///
/// Producers enqueue in their thread's shard. Consumers probe `local ^ 0`,
/// `local ^ 1`, ... as in [`crate::ConcurrentShardedStack`]. This groups probes
/// by increasing XOR distance; it still scans up to every shard, not O(log n).
/// FIFO is guaranteed only within each shard, not globally. Repeated pushes
/// by one thread therefore preserve order, but a busy local shard can starve
/// remote shards. An empty result describes a scan, not an atomic snapshot.
///
/// Each shard uses the Michael–Scott queue and retains its last consumed node
/// as a dummy until its next successful pop or collection destruction. At most
/// one such dummy per shard is retained, in addition to epoch-retired nodes.
/// Callbacks run while pinned and cannot return borrowed node references.
/// A single shard (`with_concurrency(1)`) provides globally ordered FIFO.
/// Only the per-shard link-update algorithm is lock-free; allocation, collector
/// work, accessors, and callbacks need not be lock-free.
///
/// # Example
///
/// ```
/// use concurrent_intrusive_collections::{ConcurrentShardedQueue, PopError, SinglyLinked,
///     epoch::{Atomic, Owned}};
/// struct Job { next: Atomic<Self>, id: usize }
/// // SAFETY: each node has a fixed, private, non-owning forward link.
/// unsafe impl SinglyLinked for Job {
///     unsafe fn next(&self) -> &Atomic<Self> { &self.next }
/// }
/// let queue = ConcurrentShardedQueue::with_concurrency(4);
/// // One producer thread always enqueues into the same shard.
/// for id in 0..3 { queue.push(Owned::new(Job { next: Atomic::null(), id })).unwrap(); }
/// queue.close();
/// for id in 0..3 { assert_eq!(queue.pop_with(|job| job.id), Ok(id)); }
/// assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
/// ```
pub struct ConcurrentShardedQueue<T: SinglyLinked + Send + Sync + 'static> {
    // FifoQueue already cache-pads its head and tail.
    shards: Box<[FifoQueue<T>]>,
    shard_index_mask: usize,
}

impl<T: SinglyLinked + Send + Sync + 'static> Default for ConcurrentShardedQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: SinglyLinked + Send + Sync + 'static> ConcurrentShardedQueue<T> {
    /// Creates a queue using available parallelism rounded up to a power of two.
    pub fn new() -> Self {
        Self::with_concurrency(default_concurrency())
    }

    /// Creates exactly `shard_count` independently ordered FIFO shards.
    ///
    /// # Panics
    ///
    /// Panics unless `shard_count` is a nonzero power of two, or if the node
    /// alignment is smaller than two (one tag bit is required for closing).
    pub fn with_concurrency(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be non-zero");
        assert!(
            shard_count.is_power_of_two(),
            "shard_count must be a power of two"
        );
        Self {
            shards: (0..shard_count)
                .map(|_| FifoQueue::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            shard_index_mask: shard_count - 1,
        }
    }

    /// Transfers the original node allocation into this thread's shard.
    ///
    /// A closed shard returns the original allocation with its next link cleared.
    /// Incoming pointer tags and link contents are discarded as in the FIFO queue.
    pub fn push(&self, node: Owned<T>) -> Result<(), PushError<T>> {
        self.shards[self.current_shard_index()].push(node)
    }

    /// Probes local then remote shards, invoking `f` exactly once on success.
    ///
    /// One guard protects the entire scan and callback, including reentrant
    /// calls. A panic still consumes the node; its shard retains it as a dummy
    /// until the next pop, or retires it if another consumer already advanced.
    ///
    /// ```compile_fail
    /// use concurrent_intrusive_collections::{ConcurrentShardedQueue, SinglyLinked};
    /// fn escape<T: SinglyLinked + Send + Sync + 'static>(q: &ConcurrentShardedQueue<T>) -> &T {
    ///     q.pop_with(|node| node).unwrap()
    /// }
    /// ```
    pub fn pop_with<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, PopError> {
        let guard = &epoch::pin();
        let start = self.current_shard_index();
        let mut all_closed = true;
        let mut f = Some(f);
        for mask in 0..self.shards.len() {
            match self.shards[start ^ mask].pop_with_guard(|node| f.take().unwrap()(node), guard) {
                Ok(value) => return Ok(value),
                Err(PopError::Empty) => all_closed = false,
                Err(PopError::Closed) => {}
            }
        }
        Err(if all_closed {
            PopError::Closed
        } else {
            PopError::Empty
        })
    }

    /// Closes all shards, retaining existing items for draining.
    ///
    /// Returns whether this call closed at least one shard. Multiple concurrent
    /// closers may return true; there is no single global close linearization
    /// point. After return every subsequent push is rejected.
    pub fn close(&self) -> bool {
        let guard = &epoch::pin();
        let mut changed = false;
        for shard in &self.shards {
            changed |= shard.close_with_guard(guard);
        }
        changed
    }

    /// Returns whether every shard was observed closed.
    pub fn is_closed(&self) -> bool {
        let guard = &epoch::pin();
        self.shards
            .iter()
            .all(|shard| shard.is_closed_with_guard(guard))
    }

    /// Returns whether every shard was observed empty during a scan.
    ///
    /// This is not a simultaneous snapshot; concurrent pushes can invalidate it.
    pub fn is_empty(&self) -> bool {
        let guard = &epoch::pin();
        self.shards
            .iter()
            .all(|shard| shard.is_empty_with_guard(guard))
    }

    /// Returns the configured number of shards.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn current_shard_index(&self) -> usize {
        current_thread_id() & self.shard_index_mask
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use epoch::Atomic;

    struct Node {
        next: Atomic<Self>,
        id: usize,
    }
    // SAFETY: fixed private non-owning link.
    unsafe impl SinglyLinked for Node {
        unsafe fn next(&self) -> &Atomic<Self> {
            &self.next
        }
    }

    #[test]
    fn probes_local_then_xor_groups_preserving_shard_fifo() {
        let queue = ConcurrentShardedQueue::with_concurrency(8);
        let local = queue.current_shard_index();
        for shard in 0..8 {
            for offset in 0..2 {
                queue.shards[shard]
                    .push(Owned::new(Node {
                        next: Atomic::null(),
                        id: shard * 2 + offset,
                    }))
                    .unwrap();
            }
        }
        for mask in 0..8 {
            for offset in 0..2 {
                assert_eq!(
                    queue.pop_with(|node| node.id),
                    Ok((local ^ mask) * 2 + offset)
                );
            }
        }
        assert_eq!(queue.pop_with(|_| ()), Err(PopError::Empty));
    }

    #[test]
    fn partially_closed_shards_do_not_report_globally_closed() {
        let queue = ConcurrentShardedQueue::<Node>::with_concurrency(4);
        queue.shards[queue.current_shard_index()].close();
        assert!(!queue.is_closed());
        assert_eq!(queue.pop_with(|_| ()), Err(PopError::Empty));
        assert!(queue.close());
        assert!(!queue.close());
        assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
    }
}

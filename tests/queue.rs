#![cfg(feature = "std")]
mod common;
use common::collect_until;
use concurrent_intrusive_collections::{
    ConcurrentShardedQueue, PopError, SinglyLinked,
    epoch::{Atomic, Owned},
};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

struct Node {
    next: Atomic<Self>,
    id: usize,
    drops: Arc<AtomicUsize>,
}
// SAFETY: fixed private non-owning link.
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

#[test]
fn constructors_and_single_thread_fifo() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<ConcurrentShardedQueue<Node>>();
    assert!(
        ConcurrentShardedQueue::<Node>::default()
            .shard_count()
            .is_power_of_two()
    );
    let queue = ConcurrentShardedQueue::with_concurrency(8);
    assert_eq!(queue.shard_count(), 8);
    assert!(queue.is_empty());
    let drops = Arc::new(AtomicUsize::new(0));
    for id in 0..10 {
        queue.push(node(id, &drops).with_tag(1)).unwrap();
    }
    for id in 0..10 {
        assert_eq!(queue.pop_with(|node| node.id), Ok(id));
    }
    assert!(queue.is_empty());
    assert_eq!(
        queue.pop_with(|_| panic!("empty callback")),
        Err(PopError::Empty)
    );
    drop(queue);
    collect_until(&drops, 10);
}

#[test]
#[should_panic(expected = "shard_count must be non-zero")]
fn rejects_zero_shards() {
    let _ = ConcurrentShardedQueue::<Node>::with_concurrency(0);
}

#[test]
#[should_panic(expected = "shard_count must be a power of two")]
fn rejects_non_power_of_two_shards() {
    let _ = ConcurrentShardedQueue::<Node>::with_concurrency(3);
}

#[test]
fn rejection_preserves_original_allocation() {
    let queue = ConcurrentShardedQueue::with_concurrency(4);
    let drops = Arc::new(AtomicUsize::new(0));
    let owned = node(3, &drops);
    let address: *const Node = &*owned;
    queue.close();
    assert!(queue.is_closed());
    let rejected = queue.push(owned).unwrap_err().into_inner();
    assert_eq!(&*rejected as *const Node, address);
    assert_eq!(
        queue.pop_with(|_| panic!("closed callback")),
        Err(PopError::Closed)
    );
    drop(rejected);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn callbacks_can_reenter_and_panic_without_losing_nodes() {
    let queue = ConcurrentShardedQueue::with_concurrency(4);
    let drops = Arc::new(AtomicUsize::new(0));
    queue.push(node(0, &drops)).unwrap();
    queue
        .pop_with(|outer| {
            queue.push(node(1, &drops)).unwrap();
            assert_eq!(queue.pop_with(|inner| inner.id), Ok(1));
            assert_eq!(outer.id, 0);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
        })
        .unwrap();
    queue.push(node(2, &drops)).unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            queue.pop_with(|_| panic!("callback"))
        }))
        .is_err()
    );
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Empty));
    drop(queue);
    collect_until(&drops, 3);
}

fn concurrent_ids(shards: usize, producers: usize, consumers: usize, close_early: bool) {
    let per_producer = if cfg!(miri) { 8 } else { 2000 };
    let total = producers * per_producer;
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = Arc::new(ConcurrentShardedQueue::with_concurrency(shards));
    let done = AtomicBool::new(false);
    let barrier = Barrier::new(producers + consumers + usize::from(close_early) * 2 + 1);
    thread::scope(|scope| {
        let mut pushers = Vec::new();
        let mut poppers = Vec::new();
        let mut closers = Vec::new();
        for producer in 0..producers {
            let (queue, drops, barrier) = (&queue, &drops, &barrier);
            pushers.push(scope.spawn(move || {
                barrier.wait();
                let mut accepted = Vec::new();
                let mut rejected = Vec::new();
                for offset in 0..per_producer {
                    let id = producer * per_producer + offset;
                    match queue.push(node(id, drops)) {
                        Ok(()) => accepted.push(id),
                        Err(error) => {
                            assert!(close_early);
                            let value = error.into_inner();
                            assert_eq!(value.id, id);
                            rejected.push(id);
                        }
                    }
                }
                (accepted, rejected)
            }));
        }
        for _ in 0..consumers {
            let (queue, barrier, done) = (&queue, &barrier, &done);
            poppers.push(scope.spawn(move || {
                let mut ids = Vec::new();
                let mut last = vec![None; producers];
                barrier.wait();
                let deadline = Instant::now() + Duration::from_secs(30);
                loop {
                    let finished = done.load(Ordering::Acquire);
                    match queue.pop_with(|node| node.id) {
                        Ok(id) => {
                            let producer = id / per_producer;
                            if let Some(previous) = last[producer] {
                                assert!(id > previous);
                            }
                            last[producer] = Some(id);
                            ids.push(id);
                        }
                        Err(PopError::Closed) => break,
                        Err(PopError::Empty) if !close_early && finished => break,
                        Err(PopError::Empty) => thread::yield_now(),
                    }
                    assert!(Instant::now() < deadline, "consumer stalled");
                }
                ids
            }));
        }
        if close_early {
            for _ in 0..2 {
                let (queue, barrier) = (&queue, &barrier);
                closers.push(scope.spawn(move || {
                    barrier.wait();
                    queue.close()
                }));
            }
        }
        barrier.wait();
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        for p in pushers {
            let (a, r) = p.join().unwrap();
            accepted.extend(a);
            rejected.extend(r);
        }
        done.store(true, Ordering::Release);
        if close_early {
            let changes: usize = closers
                .into_iter()
                .map(|c| usize::from(c.join().unwrap()))
                .sum();
            assert!((1..=2).contains(&changes));
            assert!(queue.is_closed());
            assert!(!queue.close());
        }
        let mut popped: Vec<_> = poppers
            .into_iter()
            .flat_map(|p| p.join().unwrap())
            .collect();
        accepted.sort_unstable();
        popped.sort_unstable();
        assert_eq!(popped, accepted, "lost or duplicated accepted node");
        accepted.extend(rejected);
        accepted.sort_unstable();
        assert_eq!(accepted, (0..total).collect::<Vec<_>>());
    });
    assert!(queue.is_empty());
    drop(queue);
    collect_until(&drops, total);
}

#[test]
fn many_workers_sharing_few_shards() {
    concurrent_ids(2, 4, 4, false);
}
#[test]
fn few_workers_probe_many_shards() {
    concurrent_ids(32, 2, 6, false);
}
#[test]
fn single_consumer_checks_full_per_producer_fifo() {
    concurrent_ids(8, 4, 1, false);
}
#[test]
fn concurrent_close_accounts_for_every_node() {
    concurrent_ids(8, 4, 4, true);
}

#[test]
fn drop_releases_consumed_nodes_from_multiple_threads() {
    let queue = ConcurrentShardedQueue::with_concurrency(8);
    let drops = Arc::new(AtomicUsize::new(0));
    // Different threads use the shared shard-ID helper. Other tests may also
    // allocate IDs, so this makes no assumption about their exact shard mapping.
    for id in 0..8 {
        thread::scope(|scope| {
            scope
                .spawn(|| {
                    queue.push(node(id, &drops)).unwrap();
                    assert_eq!(queue.pop_with(|node| node.id), Ok(id));
                })
                .join()
                .unwrap();
        });
    }
    assert!(queue.is_empty());
    drop(queue);
    collect_until(&drops, 8);
}

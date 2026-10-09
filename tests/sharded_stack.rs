#![cfg(feature = "std")]

mod common;
use common::collect_until;

use concurrent_intrusive_collections::{
    ConcurrentShardedStack, DoublyLinked, PopError, SinglyLinked,
    epoch::{self, Atomic, Owned, Shared},
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[repr(C)]
struct Node {
    // Payload deliberately precedes the link: the stack must preserve the
    // address of the complete allocation, not reinterpret a field as a node.
    id: usize,
    next: Atomic<Self>,
    drops: Arc<AtomicUsize>,
}

impl Node {
    fn new(id: usize, drops: &Arc<AtomicUsize>) -> Self {
        Self {
            id,
            next: Atomic::null(),
            drops: Arc::clone(drops),
        }
    }
}

// SAFETY: a private, fixed, non-owning atomic field in each Node.
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

#[test]
fn constructors_and_auto_traits() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ConcurrentShardedStack<Node>>();
    let stack = ConcurrentShardedStack::<Node>::default();
    assert!(stack.shard_count().is_power_of_two());
    assert!(!stack.is_closed());
    assert_eq!(
        ConcurrentShardedStack::<Node>::with_concurrency(8).shard_count(),
        8
    );
}

#[test]
#[should_panic(expected = "non-zero")]
fn zero_shards_are_rejected() {
    ConcurrentShardedStack::<Node>::with_concurrency(0);
}

#[test]
#[should_panic(expected = "power of two")]
fn non_power_of_two_shards_are_rejected() {
    ConcurrentShardedStack::<Node>::with_concurrency(3);
}

#[test]
fn single_thread_lifo_and_empty_callback() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(4);
    for id in 0..5 {
        stack.push(Owned::new(Node::new(id, &drops))).unwrap();
    }
    for id in (0..5).rev() {
        assert_eq!(stack.pop_with(|node| node.id), Ok(id));
    }
    assert_eq!(
        stack.pop_with(|_| panic!("callback on empty stack")),
        Err(PopError::Empty)
    );
    drop(stack);
    collect_until(&drops, 5);
}

#[test]
fn complete_node_address_is_preserved_and_input_tags_are_cleared() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(1);
    let boxed = Box::new(Node::new(7, &drops));
    let address: *const Node = &*boxed;
    let node = Owned::<Node>::from(boxed).with_tag(1);
    stack.push(node).unwrap();
    assert!(!stack.is_closed(), "input tag must not close the shard");
    stack
        .pop_with(|node| {
            assert_eq!(node as *const Node, address);
            assert_eq!(node.id, 7);
        })
        .unwrap();
    drop(stack);
    collect_until(&drops, 1);
}

#[test]
fn initial_link_is_overwritten_without_owning_its_target() {
    let drops = Arc::new(AtomicUsize::new(0));
    let unrelated = Node::new(99, &drops);
    let node = Owned::new(Node::new(1, &drops));
    // This node is not published; its initial link may contain any raw pointer.
    node.next
        .store(Shared::from(&unrelated as *const Node), Ordering::Relaxed);
    let stack = ConcurrentShardedStack::with_concurrency(1);
    stack.push(node).unwrap();
    assert_eq!(stack.pop_with(|node| node.id), Ok(1));
    assert_eq!(stack.pop_with(|node| node.id), Err(PopError::Empty));
    assert_eq!(unrelated.id, 99);
    drop(unrelated);
    drop(stack);
    collect_until(&drops, 2);
}

#[test]
fn closing_preserves_nodes_and_returns_rejected_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(4);
    for id in 0..3 {
        stack.push(Owned::new(Node::new(id, &drops))).unwrap();
    }
    assert!(stack.close());
    assert!(!stack.close());
    assert!(stack.is_closed());

    let rejected = Owned::new(Node::new(9, &drops));
    let address: *const Node = &*rejected;
    let error = stack.push(rejected).unwrap_err();
    assert_eq!(error.to_string(), "collection is closed");
    let rejected = error.into_inner();
    assert_eq!(&*rejected as *const Node, address);
    assert_eq!(rejected.id, 9);
    assert!(
        rejected
            .next
            .load(Ordering::Relaxed, &epoch::pin())
            .is_null()
    );
    // Rejection did not consume or retire the allocation; another stack can use it.
    let other = ConcurrentShardedStack::with_concurrency(1);
    other.push(rejected).unwrap();
    assert_eq!(other.pop_with(|node| node.id), Ok(9));

    for id in (0..3).rev() {
        assert_eq!(stack.pop_with(|node| node.id), Ok(id));
        assert!(stack.is_closed(), "pop must preserve the closed tag");
    }
    assert_eq!(
        stack.pop_with(|_| panic!("callback on closed stack")),
        Err(PopError::Closed)
    );
    drop(other);
    drop(stack);
    collect_until(&drops, 4);
}

#[test]
fn remaining_nodes_are_destroyed_synchronously() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(4);
    for id in 0..20 {
        stack.push(Owned::new(Node::new(id, &drops))).unwrap();
    }
    stack.close();
    drop(stack);
    assert_eq!(drops.load(Ordering::Relaxed), 20);
}

#[test]
fn partial_drain_and_callback_panic_drop_each_node_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(1);
    for id in 0..20 {
        stack.push(Owned::new(Node::new(id, &drops))).unwrap();
    }
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = stack.pop_with(|node| {
            assert_eq!(node.id, 19);
            panic!("callback failed");
        });
    }));
    assert!(panic.is_err());
    assert_eq!(stack.pop_with(|node| node.id), Ok(18));
    drop(stack);
    collect_until(&drops, 20);
}

#[test]
fn a_pinned_reader_delays_destruction_beyond_stack_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(1);
    stack.push(Owned::new(Node::new(0, &drops))).unwrap();
    let guard = epoch::pin();
    stack
        .pop_with(|node| {
            for _ in 0..8 {
                epoch::pin().flush();
            }
            assert_eq!(node.id, 0);
            assert_eq!(drops.load(Ordering::Relaxed), 0);
        })
        .unwrap();
    drop(stack);
    for _ in 0..16 {
        guard.flush();
        thread::yield_now();
    }
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(guard);
    collect_until(&drops, 1);
}

#[test]
fn callbacks_can_reenter_and_return_owned_data() {
    struct Job {
        name: String,
        next: Atomic<Self>,
        prev: Atomic<Self>,
        drops: Arc<AtomicUsize>,
    }
    // SAFETY: both links are private distinct fields; neither owns a node.
    unsafe impl SinglyLinked for Job {
        unsafe fn next(&self) -> &Atomic<Self> {
            &self.next
        }
    }
    unsafe impl DoublyLinked for Job {
        unsafe fn prev(&self) -> &Atomic<Self> {
            &self.prev
        }
    }
    impl Drop for Job {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = ConcurrentShardedStack::with_concurrency(1);
    let make = |name: &str| {
        Owned::new(Job {
            name: name.into(),
            next: Atomic::null(),
            prev: Atomic::null(),
            drops: Arc::clone(&drops),
        })
    };
    stack.push(make("outer")).unwrap();
    let name = stack
        .pop_with(|outer| {
            stack.push(make("inner")).unwrap();
            assert_eq!(stack.pop_with(|inner| inner.name.clone()).unwrap(), "inner");
            assert!(stack.close());
            assert_eq!(outer.name, "outer");
            outer.name.clone()
        })
        .unwrap();
    assert_eq!(name, "outer");
    assert_eq!(stack.pop_with(|_| ()), Err(PopError::Closed));
    drop(stack);
    collect_until(&drops, 2);
}

fn concurrent_unique_ids(shards: usize, producers: usize, consumers: usize, close: bool) {
    let per_producer = if cfg!(miri) { 8 } else { 2_000 };
    let total = producers * per_producer;
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(shards));
    let start = Arc::new(Barrier::new(producers + consumers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let mut pushers = Vec::new();
    let mut poppers = Vec::new();

    for producer in 0..producers {
        let (stack, start, drops) = (Arc::clone(&stack), Arc::clone(&start), Arc::clone(&drops));
        pushers.push(thread::spawn(move || {
            start.wait();
            for offset in 0..per_producer {
                stack
                    .push(Owned::new(Node::new(
                        producer * per_producer + offset,
                        &drops,
                    )))
                    .unwrap();
            }
        }));
    }
    for _ in 0..consumers {
        let (stack, start, done) = (Arc::clone(&stack), Arc::clone(&start), Arc::clone(&done));
        poppers.push(thread::spawn(move || {
            let mut ids = Vec::new();
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                // Observe producer completion BEFORE scanning: once true,
                // no later publication can race an Empty result.
                let finished = done.load(Ordering::Acquire);
                match stack.pop_with(|node| node.id) {
                    Ok(id) => ids.push(id),
                    Err(PopError::Closed) => break,
                    Err(PopError::Empty) if !close && finished => break,
                    Err(PopError::Empty) => thread::yield_now(),
                }
                assert!(Instant::now() < deadline, "consumer stalled");
            }
            ids
        }));
    }
    start.wait();
    for pusher in pushers {
        pusher.join().unwrap();
    }
    done.store(true, Ordering::Release);
    if close {
        stack.close();
    }

    let mut ids: Vec<_> = poppers
        .into_iter()
        .flat_map(|p| p.join().unwrap())
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        (0..total).collect::<Vec<_>>(),
        "duplicate or missing node"
    );
    assert_eq!(
        stack.pop_with(|_| ()),
        Err(if close {
            PopError::Closed
        } else {
            PopError::Empty
        })
    );
    drop(stack);
    collect_until(&drops, total);
}

#[test]
fn contended_single_shard_has_no_duplicates_or_loss() {
    concurrent_unique_ids(1, 4, 4, true);
}

#[test]
fn asymmetric_open_shards_have_no_duplicates_or_loss() {
    concurrent_unique_ids(16, 2, 6, false);
}

#[test]
fn asymmetric_closed_shards_have_no_duplicates_or_loss() {
    concurrent_unique_ids(16, 2, 6, true);
}

#[test]
fn few_threads_scan_many_shards_without_loss() {
    concurrent_unique_ids(32, 2, 1, false);
}

#[test]
fn close_racing_push_and_pop_accounts_for_every_allocation() {
    let producers = 4;
    let per_producer = if cfg!(miri) { 8 } else { 500 };
    let total = producers * per_producer;
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(8));
    let start = Arc::new(Barrier::new(producers + 2 + 2 + 1));
    let mut pushers = Vec::new();
    let mut poppers = Vec::new();
    let mut closers = Vec::new();
    // Make sure closed-shard draining is exercised even if close wins all races.
    stack.push(Owned::new(Node::new(total, &drops))).unwrap();
    for producer in 0..producers {
        let (stack, start, drops) = (Arc::clone(&stack), Arc::clone(&start), Arc::clone(&drops));
        pushers.push(thread::spawn(move || {
            let mut accepted = Vec::new();
            let mut rejected = Vec::new();
            start.wait();
            for offset in 0..per_producer {
                let id = producer * per_producer + offset;
                match stack.push(Owned::new(Node::new(id, &drops))) {
                    Ok(()) => accepted.push(id),
                    Err(error) => {
                        let node = error.into_inner();
                        assert_eq!(node.id, id);
                        rejected.push(id);
                    }
                }
            }
            (accepted, rejected)
        }));
    }
    for _ in 0..2 {
        let (stack, start) = (Arc::clone(&stack), Arc::clone(&start));
        closers.push(thread::spawn(move || {
            start.wait();
            stack.close()
        }));
    }
    for _ in 0..2 {
        let (stack, start) = (Arc::clone(&stack), Arc::clone(&start));
        poppers.push(thread::spawn(move || {
            let mut ids = Vec::new();
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match stack.pop_with(|node| node.id) {
                    Ok(id) => ids.push(id),
                    Err(PopError::Empty) => thread::yield_now(),
                    Err(PopError::Closed) => break,
                }
                assert!(Instant::now() < deadline, "close did not finish");
            }
            ids
        }));
    }
    start.wait();
    let mut accepted = vec![total];
    let mut rejected = Vec::new();
    for pusher in pushers {
        let (a, r) = pusher.join().unwrap();
        accepted.extend(a);
        rejected.extend(r);
    }
    let changed: Vec<_> = closers.into_iter().map(|c| c.join().unwrap()).collect();
    assert!(changed.iter().any(|&value| value));
    assert!(stack.is_closed());
    assert!(!stack.close());
    let mut popped: Vec<_> = poppers
        .into_iter()
        .flat_map(|p| p.join().unwrap())
        .collect();
    popped.sort_unstable();
    accepted.sort_unstable();
    assert_eq!(popped, accepted);
    accepted.extend(rejected);
    accepted.sort_unstable();
    assert_eq!(accepted, (0..=total).collect::<Vec<_>>());
    assert_eq!(stack.pop_with(|_| ()), Err(PopError::Closed));
    drop(stack);
    collect_until(&drops, total + 1);
}

#[test]
fn a_stale_cas_reader_keeps_the_removed_node_alive() {
    struct PausedNode {
        next: Atomic<Self>,
        id: usize,
        armed: Arc<AtomicBool>,
        entered: Arc<Barrier>,
        resume: Arc<Barrier>,
        drops: Arc<AtomicUsize>,
    }
    // SAFETY: instrumentation only pauses an accessor; it never mutates or
    // substitutes the node's private non-owning link.
    unsafe impl SinglyLinked for PausedNode {
        unsafe fn next(&self) -> &Atomic<Self> {
            if self.armed.swap(false, Ordering::AcqRel) {
                self.entered.wait();
                self.resume.wait();
            }
            &self.next
        }
    }
    impl Drop for PausedNode {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    let armed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    let drops = Arc::new(AtomicUsize::new(0));
    let stack = Arc::new(ConcurrentShardedStack::with_concurrency(1));
    let make = |id| {
        Owned::new(PausedNode {
            next: Atomic::null(),
            id,
            armed: Arc::clone(&armed),
            entered: Arc::clone(&entered),
            resume: Arc::clone(&resume),
            drops: Arc::clone(&drops),
        })
    };
    stack.push(make(1)).unwrap();
    armed.store(true, Ordering::Release);
    let reader_stack = Arc::clone(&stack);
    let reader = thread::spawn(move || reader_stack.pop_with(|node| node.id));
    entered.wait(); // The reader has loaded node 1 and is pinned inside next().
    let first = stack.pop_with(|node| node.id);
    for _ in 0..16 {
        epoch::pin().flush();
        thread::yield_now();
    }
    let drops_while_pinned = drops.load(Ordering::Relaxed);
    stack.push(make(2)).unwrap();
    resume.wait(); // Reader must lose its stale CAS and retry using node 2.
    let second = reader.join().unwrap();
    assert_eq!(first, Ok(1));
    assert_eq!(drops_while_pinned, 0);
    assert_eq!(second, Ok(2));
    assert_eq!(stack.pop_with(|_| ()), Err(PopError::Empty));
    drop(stack);
    collect_until(&drops, 2);
}

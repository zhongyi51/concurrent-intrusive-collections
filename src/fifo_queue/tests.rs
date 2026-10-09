#![cfg(feature = "std")]

#[path = "../../tests/common/mod.rs"]
mod common;
use super::FifoQueue;
use crate::{
    PopError, SinglyLinked,
    epoch::{self, Atomic, Shared},
};
use common::collect_until;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[repr(C)]
struct Node {
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

// SAFETY: next is a fixed, private, non-owning atomic within each node.
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
fn empty_queue_needs_no_dummy_value() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FifoQueue<Node>>();
    let queue = FifoQueue::<Node>::default(); // Node has no Default impl.
    assert!(queue.is_empty());
    assert!(!queue.is_closed());
    assert_eq!(
        queue.pop_with(|_| panic!("empty callback")),
        Err(PopError::Empty)
    );
    assert!(queue.close());
    assert!(!queue.close());
    assert!(queue.is_closed());
    assert!(queue.is_empty());
    assert_eq!(
        queue.pop_with(|_| panic!("closed callback")),
        Err(PopError::Closed)
    );
}

#[test]
fn fifo_across_distinct_producer_threads() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = Arc::new(FifoQueue::new());
    for id in 0..8 {
        let (queue, drops) = (Arc::clone(&queue), Arc::clone(&drops));
        // Join imposes an unambiguous order across different producers.
        thread::spawn(move || queue.push(Box::new(Node::new(id, &drops))).unwrap())
            .join()
            .unwrap();
    }
    assert!(!queue.is_empty());
    for id in 0..8 {
        assert_eq!(queue.pop_with(|node| node.id), Ok(id));
    }
    assert!(queue.is_empty());
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Empty));
    drop(queue);
    collect_until(&drops, 8);
}

#[test]
fn incoming_link_is_overwritten_and_allocation_is_preserved() {
    let drops = Arc::new(AtomicUsize::new(0));
    let unrelated = Node::new(99, &drops);
    let node = Box::new(Node::new(7, &drops));
    let address: *const Node = &*node;
    node.next
        .store(Shared::from(&unrelated as *const Node), Ordering::Relaxed);
    let queue = FifoQueue::new();
    queue.push(node).unwrap();
    assert!(!queue.is_closed());
    queue
        .pop_with(|node| {
            assert_eq!(node as *const Node, address);
            assert_eq!(node.id, 7);
        })
        .unwrap();
    assert!(queue.is_empty());
    drop(queue);
    drop(unrelated);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn closing_virtual_dummy_returns_original_rejected_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    assert!(queue.close());
    let node = Box::new(Node::new(4, &drops));
    let address: *const Node = &*node;
    let error = queue.push(node).unwrap_err();
    assert_eq!(error.to_string(), "collection is closed");
    let node = error.into_inner();
    assert_eq!(&*node as *const Node, address);
    assert!(node.next.load(Ordering::Relaxed, &epoch::pin()).is_null());
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let other = FifoQueue::new();
    other.push(node).unwrap();
    assert_eq!(other.pop_with(|node| node.id), Ok(4));
    drop(other);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn close_preserves_fifo_and_drains_to_closed() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    for id in 0..10 {
        queue.push(Box::new(Node::new(id, &drops))).unwrap();
    }
    assert!(queue.close());
    assert!(!queue.close());
    for id in 0..10 {
        assert!(queue.is_closed());
        assert_eq!(queue.pop_with(|node| node.id), Ok(id));
    }
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
    assert!(queue.is_empty());
    drop(queue);
    collect_until(&drops, 10);
}

#[test]
fn last_popped_node_is_retained_as_dummy() {
    let first_drops = Arc::new(AtomicUsize::new(0));
    let second_drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    queue.push(Box::new(Node::new(1, &first_drops))).unwrap();
    assert_eq!(queue.pop_with(|node| node.id), Ok(1));
    for _ in 0..16 {
        epoch::pin().flush();
    }
    assert_eq!(first_drops.load(Ordering::Relaxed), 0);
    assert!(queue.is_empty());
    queue.push(Box::new(Node::new(2, &second_drops))).unwrap();
    assert_eq!(
        first_drops.load(Ordering::Relaxed),
        0,
        "enqueue cannot retire dummy"
    );
    assert_eq!(queue.pop_with(|node| node.id), Ok(2));
    collect_until(&first_drops, 1);
    assert_eq!(second_drops.load(Ordering::Relaxed), 0);
    assert!(queue.close());
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
    drop(queue);
    assert_eq!(second_drops.load(Ordering::Relaxed), 1);
}

#[test]
fn drop_before_any_dequeue_reclaims_the_entire_chain() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    for id in 0..10 {
        queue.push(Box::new(Node::new(id, &drops))).unwrap();
    }
    queue.close();
    drop(queue);
    assert_eq!(drops.load(Ordering::Relaxed), 10);
}

#[test]
fn partial_drain_and_callback_panic_do_not_duplicate_or_leak_nodes() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    for id in 0..10 {
        queue.push(Box::new(Node::new(id, &drops))).unwrap();
    }
    let panic = catch_unwind(AssertUnwindSafe(|| {
        let _ = queue.pop_with(|node| {
            assert_eq!(node.id, 0);
            panic!("callback failed");
        });
    }));
    assert!(panic.is_err());
    assert_eq!(queue.pop_with(|node| node.id), Ok(1));
    drop(queue);
    collect_until(&drops, 10);
}

#[test]
fn reentrant_callback_is_protected_when_its_node_is_retired() {
    let first_drops = Arc::new(AtomicUsize::new(0));
    let second_drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    queue.push(Box::new(Node::new(1, &first_drops))).unwrap();
    queue
        .pop_with(|first| {
            queue.push(Box::new(Node::new(2, &second_drops))).unwrap();
            assert_eq!(queue.pop_with(|node| node.id), Ok(2));
            for _ in 0..16 {
                epoch::pin().flush();
            }
            assert_eq!(first_drops.load(Ordering::Relaxed), 0);
            assert_eq!(first.id, 1);
            assert!(queue.close());
            assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
        })
        .unwrap();
    collect_until(&first_drops, 1);
    drop(queue);
    assert_eq!(second_drops.load(Ordering::Relaxed), 1);
}

#[test]
fn queue_can_move_and_cycle_after_the_first_node_is_reclaimed() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = FifoQueue::new();
    queue.push(Box::new(Node::new(0, &drops))).unwrap();
    assert_eq!(queue.pop_with(|node| node.id), Ok(0));
    // Moving the collection must not invalidate its virtual initial dummy.
    let queue = Box::new(queue);
    for id in 1..8 {
        queue.push(Box::new(Node::new(id, &drops))).unwrap();
        assert_eq!(queue.pop_with(|node| node.id), Ok(id));
        collect_until(&drops, id);
        assert!(queue.is_empty());
    }
    // initial_next contains the old first pointer; Drop must follow head instead.
    drop(queue);
    assert_eq!(drops.load(Ordering::Relaxed), 8);
}

fn concurrent_unique_ids(producers: usize, consumers: usize, close: bool) {
    let per_producer = if cfg!(miri) { 8 } else { 2_000 };
    let total = producers * per_producer;
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = Arc::new(FifoQueue::new());
    let start = Arc::new(Barrier::new(producers + consumers + 1));
    let done = Arc::new(AtomicBool::new(false));
    let mut pushers = Vec::new();
    let mut poppers = Vec::new();
    for producer in 0..producers {
        let (queue, start, drops) = (Arc::clone(&queue), Arc::clone(&start), Arc::clone(&drops));
        pushers.push(thread::spawn(move || {
            start.wait();
            for offset in 0..per_producer {
                queue
                    .push(Box::new(Node::new(
                        producer * per_producer + offset,
                        &drops,
                    )))
                    .unwrap();
            }
        }));
    }
    for _ in 0..consumers {
        let (queue, start, done) = (Arc::clone(&queue), Arc::clone(&start), Arc::clone(&done));
        poppers.push(thread::spawn(move || {
            let mut ids = Vec::new();
            let mut last = vec![None; producers];
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let finished = done.load(Ordering::Acquire);
                match queue.pop_with(|node| node.id) {
                    Ok(id) => {
                        // Each consumer observes a subsequence of FIFO order.
                        // With a single consumer this checks the full per-
                        // producer order without mistaking callback timing for
                        // the order in which concurrent head CASes succeed.
                        let producer = id / per_producer;
                        assert!(producer < producers);
                        if let Some(previous) = last[producer] {
                            assert!(id > previous);
                        }
                        last[producer] = Some(id);
                        ids.push(id);
                    }
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
        queue.close();
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
    assert!(queue.is_empty());
    assert_eq!(
        queue.pop_with(|_| ()),
        Err(if close {
            PopError::Closed
        } else {
            PopError::Empty
        })
    );
    drop(queue);
    collect_until(&drops, total);
}

#[test]
fn concurrent_producers_preserve_fifo_for_a_single_consumer() {
    concurrent_unique_ids(4, 1, false);
}

#[test]
fn mpmc_open_queue_has_no_duplicates_or_loss() {
    concurrent_unique_ids(4, 4, false);
}

#[test]
fn mpmc_closed_queue_has_no_duplicates_or_loss() {
    concurrent_unique_ids(2, 6, true);
}

#[test]
fn close_racing_push_and_pop_has_one_winner_and_accounts_for_every_node() {
    let producers = 4;
    let per_producer = if cfg!(miri) { 8 } else { 500 };
    let total = producers * per_producer;
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = Arc::new(FifoQueue::new());
    let start = Arc::new(Barrier::new(producers + 2 + 2 + 1));
    let mut pushers = Vec::new();
    let mut poppers = Vec::new();
    let mut closers = Vec::new();
    queue.push(Box::new(Node::new(total, &drops))).unwrap();
    for producer in 0..producers {
        let (queue, start, drops) = (Arc::clone(&queue), Arc::clone(&start), Arc::clone(&drops));
        pushers.push(thread::spawn(move || {
            let mut accepted = Vec::new();
            let mut rejected = Vec::new();
            start.wait();
            for offset in 0..per_producer {
                let id = producer * per_producer + offset;
                match queue.push(Box::new(Node::new(id, &drops))) {
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
        let (queue, start) = (Arc::clone(&queue), Arc::clone(&start));
        closers.push(thread::spawn(move || {
            start.wait();
            queue.close()
        }));
    }
    for _ in 0..2 {
        let (queue, start) = (Arc::clone(&queue), Arc::clone(&start));
        poppers.push(thread::spawn(move || {
            let mut ids = Vec::new();
            start.wait();
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                match queue.pop_with(|node| node.id) {
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
    let changed = closers
        .into_iter()
        .map(|c| usize::from(c.join().unwrap()))
        .sum::<usize>();
    assert_eq!(changed, 1, "close has one global linearization point");
    assert!(queue.is_closed());
    assert!(!queue.close());
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
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
    drop(queue);
    collect_until(&drops, total + 1);
}

struct PausedNode {
    next: Atomic<Self>,
    id: usize,
    armed: Arc<AtomicBool>,
    entered: Arc<Barrier>,
    resume: Arc<Barrier>,
    drops: Arc<AtomicUsize>,
}

// SAFETY: instrumentation only pauses access to node 0's private next field.
unsafe impl SinglyLinked for PausedNode {
    unsafe fn next(&self) -> &Atomic<Self> {
        if self.id == 0 && self.armed.swap(false, Ordering::AcqRel) {
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

#[test]
fn stale_head_snapshot_is_protected_and_retried() {
    let (armed, entered, resume, drops) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(Barrier::new(2)),
        Arc::new(Barrier::new(2)),
        Arc::new(AtomicUsize::new(0)),
    );
    let queue = Arc::new(FifoQueue::new());
    let make = |id| {
        Box::new(PausedNode {
            next: Atomic::null(),
            id,
            armed: Arc::clone(&armed),
            entered: Arc::clone(&entered),
            resume: Arc::clone(&resume),
            drops: Arc::clone(&drops),
        })
    };
    queue.push(make(0)).unwrap();
    assert_eq!(queue.pop_with(|node| node.id), Ok(0)); // real dummy
    queue.push(make(1)).unwrap();
    armed.store(true, Ordering::Release);
    let reader_queue = Arc::clone(&queue);
    let reader = thread::spawn(move || reader_queue.pop_with(|node| node.id));
    entered.wait();
    let first = queue.pop_with(|node| node.id); // removes dummy 0
    for _ in 0..16 {
        epoch::pin().flush();
        thread::yield_now();
    }
    let before_resume = drops.load(Ordering::Relaxed);
    queue.push(make(2)).unwrap();
    resume.wait();
    let second = reader.join().unwrap();
    assert_eq!(before_resume, 0);
    assert_eq!(first, Ok(1));
    assert_eq!(second, Ok(2));
    drop(queue);
    collect_until(&drops, 3);
}

#[test]
fn close_rejects_a_paused_enqueuer_without_losing_ownership() {
    let (armed, entered, resume, drops) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(Barrier::new(2)),
        Arc::new(Barrier::new(2)),
        Arc::new(AtomicUsize::new(0)),
    );
    let queue = Arc::new(FifoQueue::new());
    let make = |id| {
        Box::new(PausedNode {
            next: Atomic::null(),
            id,
            armed: Arc::clone(&armed),
            entered: Arc::clone(&entered),
            resume: Arc::clone(&resume),
            drops: Arc::clone(&drops),
        })
    };
    queue.push(make(0)).unwrap();
    queue.pop_with(|_| ()).unwrap();
    let node = make(1);
    armed.store(true, Ordering::Release);
    let writer_queue = Arc::clone(&queue);
    let writer = thread::spawn(move || writer_queue.push(node));
    entered.wait();
    let closed = queue.close();
    resume.wait();
    let rejected = writer.join().unwrap().unwrap_err().into_inner();
    assert!(closed);
    assert_eq!(rejected.id, 1);
    assert!(
        rejected
            .next
            .load(Ordering::Relaxed, &epoch::pin())
            .is_null()
    );
    drop(rejected);
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Closed));
    drop(queue);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

#[test]
fn concurrent_histories_match_a_linearizable_fifo_model() {
    use std::collections::VecDeque;

    #[derive(Clone, Copy, Debug)]
    enum Operation {
        Push { id: usize, accepted: bool },
        Pop(Result<usize, PopError>),
        Close(bool),
    }
    #[derive(Clone, Copy, Debug)]
    struct Event {
        start: usize,
        end: usize,
        operation: Operation,
    }
    fn record(clock: &AtomicUsize, f: impl FnOnce() -> Operation) -> Event {
        let start = clock.fetch_add(1, Ordering::SeqCst);
        let operation = f();
        let end = clock.fetch_add(1, Ordering::SeqCst);
        Event {
            start,
            end,
            operation,
        }
    }
    fn linearizable(events: &[Event], placed: usize, queue: VecDeque<usize>, closed: bool) -> bool {
        if placed == (1 << events.len()) - 1 {
            return true;
        }
        for (index, event) in events.iter().enumerate() {
            if placed & (1 << index) != 0 {
                continue;
            }
            // Every call completed before this invocation must precede it.
            if events
                .iter()
                .enumerate()
                .any(|(j, earlier)| earlier.end < event.start && placed & (1 << j) == 0)
            {
                continue;
            }
            let mut next = queue.clone();
            let mut next_closed = closed;
            let agrees = match event.operation {
                Operation::Push { id, accepted } => {
                    if accepted && !closed {
                        next.push_back(id);
                    }
                    accepted != closed
                }
                Operation::Pop(result) => {
                    let expected = next.pop_front().ok_or(if closed {
                        PopError::Closed
                    } else {
                        PopError::Empty
                    });
                    result == expected
                }
                Operation::Close(changed) => {
                    next_closed = true;
                    changed != closed
                }
            };
            if agrees && linearizable(events, placed | (1 << index), next, next_closed) {
                return true;
            }
        }
        false
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let rounds = if cfg!(miri) { 4 } else { 64 };
    for round in 0..rounds {
        let queue = FifoQueue::new();
        let clock = AtomicUsize::new(0);
        let start = Barrier::new(4);
        let events = thread::scope(|scope| {
            let mut workers = Vec::new();
            for worker in 0..4 {
                let (queue, clock, start, drops) = (&queue, &clock, &start, &drops);
                workers.push(scope.spawn(move || {
                    start.wait();
                    for _ in 0..(round + worker) % 4 {
                        thread::yield_now();
                    }
                    if worker < 2 {
                        let id = round * 2 + worker;
                        let push = record(clock, || Operation::Push {
                            id,
                            accepted: queue.push(Box::new(Node::new(id, drops))).is_ok(),
                        });
                        thread::yield_now();
                        vec![
                            push,
                            record(clock, || Operation::Pop(queue.pop_with(|node| node.id))),
                        ]
                    } else if worker == 2 {
                        (0..2)
                            .map(|_| {
                                record(clock, || Operation::Pop(queue.pop_with(|node| node.id)))
                            })
                            .collect()
                    } else {
                        vec![record(clock, || Operation::Close(queue.close()))]
                    }
                }));
            }
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            linearizable(&events, 0, VecDeque::new(), false),
            "nonlinearizable history: {events:?}"
        );
        drop(queue);
    }
    collect_until(&drops, rounds * 2);
}

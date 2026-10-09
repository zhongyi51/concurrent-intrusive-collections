//! Paired, dependency-free queue benchmark. See benches/README.md for methodology.
use concurrent_intrusive_collections::{
    ConcurrentShardedQueue, PopError, SinglyLinked,
    epoch::{self, Atomic},
};
use crossbeam_utils::CachePadded;
use std::hint::black_box;
use std::sync::{
    Barrier,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

// Separate producer counters avoid a global destructor-counter hotspot.
// All nodes must be reclaimed before any counter is reset for another sample.
static DROPS: [CachePadded<AtomicUsize>; 32] =
    [const { CachePadded::new(AtomicUsize::new(0)) }; 32];

struct Node {
    next: Atomic<Self>,
    id: usize,
    producer: usize,
}
// SAFETY: fixed private non-owning link; no other code changes published links.
unsafe impl SinglyLinked for Node {
    unsafe fn next(&self) -> &Atomic<Self> {
        &self.next
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        DROPS[self.producer].fetch_add(1, Ordering::Relaxed);
    }
}
fn node(id: usize, producer: usize) -> Box<Node> {
    Box::new(Node {
        next: Atomic::null(),
        id,
        producer,
    })
}

trait Queue: Sync {
    fn push(&self, node: Box<Node>);
    fn pop(&self) -> Result<usize, PopError>;
}
impl Queue for ConcurrentShardedQueue<Node> {
    fn push(&self, node: Box<Node>) {
        self.push(node).unwrap();
    }
    fn pop(&self) -> Result<usize, PopError> {
        self.pop_with(|node| black_box(node.id))
    }
}

#[derive(Clone, Copy)]
enum Work {
    RoundTrip(usize),
    Transfer(usize, usize),
    Empty(usize),
}
impl Work {
    fn dimensions(self) -> (usize, usize) {
        match self {
            Self::RoundTrip(n) | Self::Empty(n) => (n, 0),
            Self::Transfer(p, c) => (p, c),
        }
    }
    fn label(self) -> String {
        match self {
            Self::RoundTrip(n) => format!("roundtrip-{n}"),
            Self::Transfer(p, c) => format!("transfer-{p}p{c}c"),
            Self::Empty(n) => format!("empty-{n}"),
        }
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Core,
    Allocated,
}
impl Mode {
    fn label(self) -> &'static str {
        match self {
            Self::Core => "preallocated-pinned",
            Self::Allocated => "allocated-ebr",
        }
    }
}

fn trial<Q: Queue>(queue: Q, work: Work, mode: Mode, items: usize) -> (u64, usize, usize) {
    let (producers, consumers) = work.dimensions();
    let workers = producers + consumers;
    let empty = matches!(work, Work::Empty(_));
    // A common multiple lets every consumer take a fixed quota without a
    // contended global progress counter or close() in the measured interval.
    let divisor = producers * consumers.max(1);
    let total = (items / divisor).max(1) * divisor;
    let per_producer = total / producers;
    for counter in &DROPS {
        counter.store(0, Ordering::Relaxed);
    }
    let guard = matches!(mode, Mode::Core).then(epoch::pin);
    let start = Barrier::new(workers + 1);
    let end = Barrier::new(workers + 1);
    let (ready_tx, ready_rx) = mpsc::channel();
    let (elapsed, checksum, count, misses) = thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..workers {
            let (queue, start, end, ready_tx) = (&queue, &start, &end, &ready_tx);
            handles.push(scope.spawn(move || {
                // Serial handshake makes thread IDs contiguous in role order.
                // Also initializes TLS/collector registration before timing.
                assert_eq!(queue.pop(), Err(PopError::Empty));
                let is_producer = worker < producers;
                let mut prepared = if !empty && is_producer && matches!(mode, Mode::Core) {
                    (0..per_producer)
                        .map(|i| node(worker * per_producer + i, worker))
                        .collect::<Vec<_>>()
                        .into_iter()
                } else {
                    Vec::new().into_iter()
                };
                let mut checksum = 0_u64;
                let mut count = 0;
                let mut misses = 0;
                ready_tx.send(()).unwrap();
                start.wait();
                let mut pop = || loop {
                    match queue.pop() {
                        Ok(id) => {
                            checksum = checksum.wrapping_add(id as u64);
                            count += 1;
                            break;
                        }
                        Err(PopError::Empty) => {
                            misses += 1;
                            thread::yield_now();
                        }
                        Err(PopError::Closed) => panic!("benchmark queue unexpectedly closed"),
                    }
                };
                match work {
                    Work::Empty(_) => {
                        for _ in 0..per_producer {
                            assert_eq!(black_box(queue.pop()), Err(PopError::Empty));
                            misses += 1;
                        }
                    }
                    Work::RoundTrip(_) | Work::Transfer(_, _) if is_producer => {
                        for i in 0..per_producer {
                            let value = prepared
                                .next()
                                .unwrap_or_else(|| node(worker * per_producer + i, worker));
                            queue.push(value);
                            if matches!(work, Work::RoundTrip(_)) {
                                pop();
                            }
                        }
                    }
                    Work::Transfer(_, _) => {
                        for _ in 0..total / consumers {
                            pop();
                        }
                    }
                    _ => unreachable!(),
                }
                end.wait();
                (checksum, count, misses)
            }));
            ready_rx.recv().unwrap();
        }
        let begin = Instant::now();
        start.wait();
        end.wait();
        let elapsed = begin.elapsed();
        let mut checksum = 0_u64;
        let mut count = 0;
        let mut misses = 0;
        for handle in handles {
            let (s, c, m) = handle.join().unwrap();
            checksum = checksum.wrapping_add(s);
            count += c;
            misses += m;
        }
        (elapsed, checksum, count, misses)
    });
    assert_eq!(queue.pop(), Err(PopError::Empty));
    if !empty {
        assert_eq!(count, total);
        assert_eq!(checksum, (total as u64) * (total as u64 - 1) / 2);
    }
    drop(queue); // Includes all retained per-shard dummies, outside timing.
    drop(guard);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        epoch::pin().flush();
        let dropped: usize = DROPS.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        let expected = if empty { 0 } else { total };
        assert!(dropped <= expected);
        if dropped == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "GC failed to drain between samples"
        );
        thread::yield_now();
    }
    let operations = if empty { total } else { 2 * total };
    (elapsed.as_nanos().try_into().unwrap(), operations, misses)
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let argument = |name: &str, default: usize| {
        args.windows(2)
            .find(|a| a[0] == name)
            .map_or(default, |a| a[1].parse().unwrap())
    };
    let items = argument("--items", 262_144);
    let samples = argument("--samples", 9);
    assert!(samples > 0);
    eprintln!(
        "available_parallelism={:?}; items={items}; samples={samples}",
        thread::available_parallelism()
    );
    println!("mode,workload,implementation,shards,sample,operations,nanoseconds,empty_attempts");
    let works = [
        Work::RoundTrip(1),
        Work::RoundTrip(4),
        Work::RoundTrip(8),
        Work::RoundTrip(16),
        Work::Transfer(1, 1),
        Work::Transfer(4, 4),
        Work::Transfer(8, 8),
        Work::Transfer(1, 7),
        Work::Transfer(7, 1),
        Work::Empty(1),
        Work::Empty(16),
    ];
    for work in works {
        if let Some(filter) = args.windows(2).find(|a| a[0] == "--work") {
            if work.label() != filter[1] {
                continue;
            }
        }
        for mode in [Mode::Core, Mode::Allocated] {
            if matches!(work, Work::Empty(_)) && matches!(mode, Mode::Core) {
                continue;
            }
            // One discarded warmup, then rotate/reverse implementation order
            // to reduce systematic warmup, thermal, and scheduling bias.
            for sample in 0..=samples {
                for offset in 0..3 {
                    let choice = if sample % 2 == 0 {
                        (offset + sample) % 3
                    } else {
                        (2 - offset + sample) % 3
                    };
                    let shards = [1, 8, 32][choice];
                    let implementation = "sharded";
                    let result = trial(
                        ConcurrentShardedQueue::with_concurrency(shards),
                        work,
                        mode,
                        items,
                    );
                    if sample != 0 {
                        let (ns, ops, misses) = result;
                        println!(
                            "{},{},{implementation},{shards},{sample},{ops},{ns},{misses}",
                            mode.label(),
                            work.label()
                        );
                    }
                }
            }
        }
    }
}

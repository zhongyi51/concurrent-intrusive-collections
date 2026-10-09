//! Allocation attribution and repeatable CPU workloads, separate from throughput benchmarks.
use concurrent_intrusive_collections::{
    ConcurrentShardedQueue, ConcurrentShardedStack, PopError, SinglyLinked,
    epoch::{self, Atomic, Owned},
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct CountingAllocator;
static TRACK: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static FREES: AtomicUsize = AtomicUsize::new(0);
static REALLOCS: AtomicUsize = AtomicUsize::new(0);
static SIZES: [AtomicUsize; 32] = [const { AtomicUsize::new(0) }; 32];
static COUNTS: [AtomicUsize; 32] = [const { AtomicUsize::new(0) }; 32];

fn allocation(size: usize) {
    ALLOCS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size, Ordering::Relaxed);
    // Bounded, allocation-free histogram. Zero-sized entries are unused.
    for (slot, count) in SIZES.iter().zip(&COUNTS) {
        let old = slot.load(Ordering::Relaxed);
        if old == size
            || (old == 0
                && slot
                    .compare_exchange(0, size, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok())
        {
            count.fetch_add(1, Ordering::Relaxed);
            break;
        }
    }
}

// SAFETY: all operations delegate to System with the original pointer/layout.
// Bookkeeping uses only static atomics and cannot allocate or unwind.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if TRACK.load(Ordering::Relaxed) && !result.is_null() {
            allocation(layout.size());
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        if TRACK.load(Ordering::Relaxed) && !result.is_null() {
            allocation(layout.size());
        }
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACK.load(Ordering::Relaxed) {
            FREES.fetch_add(1, Ordering::Relaxed);
        }
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, size) };
        if TRACK.load(Ordering::Relaxed) && !result.is_null() {
            REALLOCS.fetch_add(1, Ordering::Relaxed);
            allocation(size);
        }
        result
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static DROPS: AtomicUsize = AtomicUsize::new(0);
struct Node {
    next: Atomic<Self>,
    value: usize,
}
// SAFETY: fixed, private, non-owning link.
unsafe impl SinglyLinked for Node {
    unsafe fn next(&self) -> &Atomic<Self> {
        &self.next
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
fn node(value: usize) -> Owned<Node> {
    Owned::new(Node {
        next: Atomic::null(),
        value,
    })
}

fn measure<R>(label: &str, operations: usize, f: impl FnOnce() -> R) -> R {
    for counter in [&ALLOCS, &BYTES, &FREES, &REALLOCS]
        .into_iter()
        .chain(SIZES.iter())
        .chain(COUNTS.iter())
    {
        counter.store(0, Ordering::Relaxed);
    }
    TRACK.store(true, Ordering::Relaxed);
    let result = f();
    TRACK.store(false, Ordering::Relaxed);
    print!(
        "{label},{operations},{},{},{},{}",
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
        FREES.load(Ordering::Relaxed),
        REALLOCS.load(Ordering::Relaxed)
    );
    print!(",");
    let mut separator = "";
    for (size, count) in SIZES.iter().zip(&COUNTS) {
        let count = count.load(Ordering::Relaxed);
        if count != 0 {
            print!("{separator}{}:{count}", size.load(Ordering::Relaxed));
            separator = ";";
        }
    }
    println!();
    result
}

fn reclaim(expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while DROPS.load(Ordering::Relaxed) != expected {
        assert!(Instant::now() < deadline, "reclamation did not finish");
        epoch::pin().flush();
        std::thread::yield_now();
    }
}

fn allocations() {
    const N: usize = 8192;
    println!(
        "phase,operations,alloc_calls,requested_bytes,dealloc_calls,realloc_calls,allocation_sizes"
    );
    drop(measure("first-pin", 1, epoch::pin));
    let fifo = measure("construct-queue-1", 1, || {
        ConcurrentShardedQueue::<Node>::with_concurrency(1)
    });
    let queue = measure("construct-queue-32", 1, || {
        ConcurrentShardedQueue::<Node>::with_concurrency(32)
    });
    let stack = measure("construct-stack-32", 1, || {
        ConcurrentShardedStack::<Node>::with_concurrency(32)
    });
    // Initialize shard-selection TLS before measuring steady-state operations.
    assert_eq!(queue.pop_with(|_| ()), Err(PopError::Empty));
    measure("warm-empty-queue-32", N, || {
        for _ in 0..N {
            black_box(queue.pop_with(|_| ())).unwrap_err();
        }
    });
    measure("warm-empty-queue-1", N, || {
        for _ in 0..N {
            black_box(fifo.pop_with(|_| ())).unwrap_err();
        }
    });
    measure("warm-empty-stack-32", N, || {
        for _ in 0..N {
            black_box(stack.pop_with(|_| ())).unwrap_err();
        }
    });
    measure("state-queries-queue-32", 2 * N, || {
        for _ in 0..N {
            black_box(queue.is_empty());
            black_box(queue.is_closed());
        }
    });
    let mut nodes = (0..N).map(node).collect::<Vec<_>>().into_iter();
    measure("preallocated-push-queue-32", N, || {
        for value in nodes.by_ref() {
            queue.push(value).unwrap();
        }
    });
    drop(nodes);
    measure("pop-queue-32", N, || {
        for expected in 0..N {
            assert_eq!(queue.pop_with(|node| node.value), Ok(expected));
        }
    });
    drop(queue);
    reclaim(N);
    // The same retirement count with no collection demonstrates EBR bag overhead.
    let mut nodes = (0..N).map(node).collect::<Vec<_>>().into_iter();
    measure("bare-ebr-defer-destroy", N, || {
        let guard = &epoch::pin();
        for value in nodes.by_ref() {
            // SAFETY: unpublished uniquely owned nodes; retire exactly once.
            unsafe {
                guard.defer_destroy(value.into_shared(guard));
            }
        }
    });
    drop(nodes);
    reclaim(2 * N);
    fifo.push(node(0)).unwrap();
    let capture = [black_box(5_usize); 64];
    measure("large-callback-capture", 1, || {
        assert_eq!(
            fifo.pop_with(move |node| black_box(capture)[node.value]),
            Ok(5)
        );
    });
    let mut nodes = (0..N).map(node).collect::<Vec<_>>().into_iter();
    measure("preallocated-push-stack-32", N, || {
        for value in nodes.by_ref() {
            stack.push(value).unwrap();
        }
    });
    drop(nodes);
    measure("pop-stack-32", N, || {
        for expected in (0..N).rev() {
            assert_eq!(stack.pop_with(|node| node.value), Ok(expected));
        }
    });
    reclaim(3 * N);
    let queue = ConcurrentShardedQueue::<Node>::with_concurrency(32);
    measure("close-queue-32", 1, || queue.close());
    drop(queue);
    drop(fifo);
    drop(stack);
    reclaim(3 * N + 1);
    eprintln!(
        "layout: one-shard handle={} bytes; sharded handle={} bytes; node={} bytes",
        std::mem::size_of::<ConcurrentShardedQueue<Node>>(),
        std::mem::size_of::<ConcurrentShardedQueue<Node>>(),
        std::mem::size_of::<Node>()
    );
}

#[inline(never)]
fn empty_batch(queue: &ConcurrentShardedQueue<Node>) {
    for _ in 0..10000 {
        black_box(queue.pop_with(|_| ())).unwrap_err();
    }
}
#[inline(never)]
fn roundtrip_batch(queue: &ConcurrentShardedQueue<Node>) {
    for value in 0..10000 {
        queue.push(node(value)).unwrap();
        black_box(queue.pop_with(|node| node.value)).unwrap();
    }
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    if let Some(pos) = args.iter().position(|a| a == "--cpu") {
        let work = &args[pos + 1];
        let queue = ConcurrentShardedQueue::with_concurrency(32);
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match work.as_str() {
                "empty" => empty_batch(&queue),
                "roundtrip" => roundtrip_batch(&queue),
                _ => panic!("--cpu must be empty or roundtrip"),
            }
        }
    } else {
        allocations();
    }
}

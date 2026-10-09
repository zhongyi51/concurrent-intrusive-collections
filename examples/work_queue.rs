use concurrent_intrusive_collections::{
    ConcurrentQueue, PopError, SinglyLinked,
    epoch::{Atomic, Owned},
};
use std::sync::Arc;
use std::thread;

struct Job {
    next: Atomic<Self>,
    id: usize,
}

// SAFETY: each Job has a fixed, private, non-owning next field. No safe API
// changes it while a collection owns the node, including during retirement.
unsafe impl SinglyLinked for Job {
    unsafe fn next(&self) -> &Atomic<Self> {
        &self.next
    }
}

fn main() {
    let queue = Arc::new(ConcurrentQueue::<Job>::with_concurrency(4));
    let consumers: Vec<_> = (0..2)
        .map(|_| {
            let queue = Arc::clone(&queue);
            thread::spawn(move || {
                let mut completed = Vec::new();
                loop {
                    match queue.pop_with(|job| job.id) {
                        Ok(id) => completed.push(id),
                        Err(PopError::Empty) => thread::yield_now(),
                        Err(PopError::Closed) => return completed,
                    }
                }
            })
        })
        .collect();
    thread::scope(|scope| {
        for producer in 0..2 {
            let queue = &queue;
            scope.spawn(move || {
                for offset in 0..100 {
                    queue
                        .push(Owned::new(Job {
                            next: Atomic::null(),
                            id: producer * 100 + offset,
                        }))
                        .unwrap();
                }
            });
        }
    });
    queue.close(); // Producers have joined; consumers drain before seeing Closed.
    let mut completed: Vec<_> = consumers
        .into_iter()
        .flat_map(|consumer| consumer.join().unwrap())
        .collect();
    // Completion order is not global FIFO, so verify membership rather than order.
    completed.sort_unstable();
    assert_eq!(completed, (0..200).collect::<Vec<_>>());
    println!("Processed {} jobs", completed.len());
}

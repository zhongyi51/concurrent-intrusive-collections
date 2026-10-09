use crossbeam_epoch as epoch;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

pub fn collect_until(drops: &AtomicUsize, expected: usize) {
    let start = Instant::now();
    for attempt in 0..100_000 {
        // No guard may remain alive while waiting for the next epoch.
        epoch::pin().flush();
        let actual = drops.load(Ordering::Relaxed);
        if actual == expected {
            return;
        }
        assert!(actual < expected, "double drop: {actual} > {expected}");
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "reclamation stalled: {actual}/{expected}"
        );
        if attempt % 64 == 0 {
            thread::sleep(Duration::from_millis(1));
        } else {
            thread::yield_now();
        }
    }
    panic!(
        "nodes were not reclaimed: {}/{}",
        drops.load(Ordering::Relaxed),
        expected
    );
}

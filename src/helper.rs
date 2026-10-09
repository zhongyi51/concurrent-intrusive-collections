//! Internal utilities shared by the concurrent collections.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

pub(crate) const CLOSED_TAG: usize = 1;
static NEXT_THREAD_ID: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_ID: usize = NEXT_THREAD_ID.fetch_add(1, Ordering::Relaxed);
}

/// A stable per-thread index for shard selection, independent of the collection.
pub(crate) fn current_thread_id() -> usize {
    THREAD_ID.with(|id| *id)
}

/// The default power-of-two shard count.
pub(crate) fn default_concurrency() -> usize {
    thread::available_parallelism()
        .map(|count| count.get().next_power_of_two())
        .unwrap_or(4)
}

/// Collections reserve one low pointer bit for their closed marker.
pub(crate) fn assert_node_alignment<T>() {
    assert!(std::mem::align_of::<T>() >= 2, "node requires a tag bit");
}

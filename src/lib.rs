//! Intrusive concurrent collections backed by Crossbeam's epoch collector.
//!
//! Nodes implement [`SinglyLinked`] and, optionally, [`DoublyLinked`]. Links
//! point directly to complete nodes, so there are no adapters or link-to-node
//! conversions. The default `std` feature enables the sharded stack, sharded
//! `ConcurrentShardedQueue`.
//! Disabling it leaves the link traits and the `alloc`-enabled [`epoch`] API.
//!
//! # Choosing a collection
//!
//! - `ConcurrentShardedQueue`: per-shard FIFO with local enqueue and XOR probing.
//! - `ConcurrentShardedStack`: per-shard LIFO with local push and XOR probing.
//!
//! Collections take ownership through `Box<T>` and return results from
//! shared-borrow callbacks. They do not return removed nodes for immediate reuse.
//! Queue shards retain their last consumed node as a dummy until the next pop
//! or destruction. See each collection for ordering and reclamation semantics.
//!
//! Rust 1.85 or newer and pointer-width atomic operations are required.

#![cfg_attr(not(feature = "std"), no_std)]
#![forbid(unsafe_op_in_unsafe_fn)]
#![deny(missing_docs)]

#[cfg(not(target_has_atomic = "ptr"))]
compile_error!("concurrent-intrusive-collections requires pointer-width atomics");

pub use crossbeam_epoch as epoch;

/// An atomic forward/backward link stored privately in an intrusive node.
///
/// This is Crossbeam's atomic pointer type, reexported for implementing the link
/// traits. Link access must follow the owning collection's safety protocol.
pub use crossbeam_epoch::Atomic as AtomicLink;

#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod fifo_queue;
#[cfg(feature = "std")]
mod helper;
#[cfg(feature = "std")]
mod queue;
#[cfg(feature = "std")]
mod stack;
#[cfg(feature = "std")]
pub use error::{PopError, PushError};
#[cfg(feature = "std")]
pub use queue::ConcurrentShardedQueue;
#[cfg(feature = "std")]
pub use stack::ConcurrentShardedStack;

/// A node containing an atomic forward link to another complete node.
///
/// Implement this trait on the node itself. Keep the link field private;
/// collections obtain access through the unsafe accessor below.
///
/// # Safety
///
/// - `next` must always return the same initialized atomic field embedded in
///   `self`. Distinct live nodes must have distinct link fields.
/// - The accessor must not panic, modify the link, or invalidate the node.
/// - Safe APIs must not allow the link to be changed independently while a
///   collection owns the node, including its retirement period. In particular,
///   do not expose the atomic through a public field or another safe accessor.
/// - The link is non-owning. The node's destructor must not follow, dereference,
///   or reclaim its successor: by destruction time the successor may be gone.
///
/// Collections must keep published nodes at stable addresses and synchronize
/// access and reclamation. Implementing this trait alone does not do so.
pub unsafe trait SinglyLinked: Sized {
    /// Returns the node's forward link.
    ///
    /// # Safety
    ///
    /// All uses of the returned atomic must follow the owning collection's
    /// synchronization, ownership, and reclamation protocol. Loading a pointer
    /// does not by itself make that pointer valid to dereference.
    unsafe fn next(&self) -> &AtomicLink<Self>;
}

/// A node that also contains an atomic backward link.
///
/// This only describes storage; updating two atomic links is not one atomic
/// operation. A doubly linked collection must supply its own update protocol.
///
/// # Safety
///
/// `prev` has the same requirements as [`SinglyLinked::next`], and must return
/// a different atomic field from `next` in the same node. Neither link owns
/// the node to which it points.
pub unsafe trait DoublyLinked: SinglyLinked {
    /// Returns the node's backward link.
    ///
    /// # Safety
    ///
    /// All uses of the returned atomic must follow the owning collection's
    /// synchronization, ownership, and reclamation protocol.
    unsafe fn prev(&self) -> &AtomicLink<Self>;
}

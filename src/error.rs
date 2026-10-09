use std::fmt;

/// A rejected push, retaining ownership of the unpublished node.
pub enum PushError<T> {
    /// The collection was closed. The node can be reused or dropped.
    Closed(Box<T>),
}

impl<T> PushError<T> {
    /// Recovers ownership of the rejected node.
    pub fn into_inner(self) -> Box<T> {
        match self {
            Self::Closed(node) => node,
        }
    }
}

impl<T> fmt::Debug for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed(node) => f
                .debug_tuple("Closed")
                .field(&(node.as_ref() as *const T))
                .finish(),
        }
    }
}

impl<T> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("collection is closed")
    }
}

impl<T> std::error::Error for PushError<T> {}

/// The result of an operation that did not find a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// No node was found, and the collection was observed open.
    ///
    /// For a sharded collection this describes a scan, not an atomic snapshot of
    /// all shards. A concurrent push can make another node available.
    Empty,
    /// The collection was observed empty and closed.
    Closed,
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "collection is empty",
            Self::Closed => "collection is closed and empty",
        })
    }
}

impl std::error::Error for PopError {}

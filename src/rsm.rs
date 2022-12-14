use futures::Future;

use crate::proto::Entry;

pub trait ReplicationStateMachine: Send + 'static {
    type ApplyResponse: Send + Default;

    /// GAT trait
    type ApplyFuture<'life0>: Future<Output = Self::ApplyResponse>
    where
        Self: 'life0;

    fn apply(&mut self, entry: Entry) -> Self::ApplyFuture<'_>;
}

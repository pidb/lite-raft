use std::vec::IntoIter;

use futures::Future;

use crate::multiraft::ApplyEvent;

pub trait StateMachine: Send + 'static {
    type ApplyFuture<'life0>: Send + Future<Output = Option<IntoIter<ApplyEvent>>> 
    where
        Self: 'life0;

    fn apply(&mut self, iter: IntoIter<ApplyEvent>) -> Self::ApplyFuture<'_>;
}

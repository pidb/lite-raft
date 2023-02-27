use std::vec::IntoIter;

use futures::Future;

use crate::multiraft::ApplyEvent;

use super::response::AppWriteResponse;

pub trait StateMachine<R>: Send + 'static
where
    R: AppWriteResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = Option<IntoIter<ApplyEvent<R>>>>
    where
        Self: 'life0;

    fn apply(&mut self, iter: IntoIter<ApplyEvent<R>>) -> Self::ApplyFuture<'_>;
}

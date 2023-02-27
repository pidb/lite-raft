use std::vec::IntoIter;

use futures::Future;
use oceanraft::multiraft::{ApplyEvent, StateMachine};
use tokio::sync::mpsc::Sender;

pub struct FixtureStateMachine {
    tx: Sender<Vec<ApplyEvent<()>>>,
}

impl FixtureStateMachine {
    pub fn new(tx: Sender<Vec<ApplyEvent<()>>>) -> Self {
        Self { tx }
    }
}

impl StateMachine<()> for FixtureStateMachine {
    type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<ApplyEvent<()>>>> + 'life0
    where
        Self: 'life0;
    fn apply(&mut self, iter: IntoIter<ApplyEvent<()>>) -> Self::ApplyFuture<'_> {
        async move {
            self.tx.send(iter.collect()).await.unwrap();
            None
        }
    }
}

use std::vec::IntoIter;

use futures::Future;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::GroupState;
use oceanraft::multiraft::StateMachine;
use tokio::sync::mpsc::Sender;

pub struct FixtureStateMachine {
    tx: Sender<Vec<Apply<()>>>,
}

impl FixtureStateMachine {
    pub fn new(tx: Sender<Vec<Apply<()>>>) -> Self {
        Self { tx }
    }
}

impl StateMachine<()> for FixtureStateMachine {
    type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<Apply<()>>>> + 'life0
    where
        Self: 'life0;
    fn apply(
        &self,
        _group_id: u64,
        _state: &GroupState,
        iter: IntoIter<Apply<()>>,
    ) -> Self::ApplyFuture<'_> {
        async move {
            // self.tx.send(iter.collect()).await.unwrap();
            if let Err(_) = self.tx.send(iter.collect()).await {
                // TODO: handle error
            }
            None
        }
    }
}

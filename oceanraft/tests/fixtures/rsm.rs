use std::vec::IntoIter;

use futures::Future;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::GroupState;
use oceanraft::multiraft::StateMachine;
use tokio::sync::mpsc::Sender;

use super::FixtureWriteData;

pub struct FixtureStateMachine {
    tx: Sender<Vec<Apply<FixtureWriteData, ()>>>,
}

impl FixtureStateMachine {
    pub fn new(tx: Sender<Vec<Apply<FixtureWriteData, ()>>>) -> Self {
        Self { tx }
    }
}

impl StateMachine<FixtureWriteData, ()> for FixtureStateMachine {
    type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<Apply<FixtureWriteData, ()>>>> + 'life0
    where
        Self: 'life0;
    fn apply(
        &self,
        _group_id: u64,
        _state: &GroupState,
        iter: IntoIter<Apply<FixtureWriteData, ()>>,
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

use std::future::Future;

use oceanraft::Apply;
use oceanraft::GroupState;
use oceanraft::StateMachine;

use crate::server::{KVData, KVResponse};

pub struct KVStateMachine {}

impl StateMachine<KVData, KVResponse> for KVStateMachine {
    type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0;
    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        state: &oceanraft::GroupState,
        applys: Vec<Apply<KVData, KVResponse>>,
    ) -> Self::ApplyFuture<'life0> {
        async move { todo!() }
    }
}

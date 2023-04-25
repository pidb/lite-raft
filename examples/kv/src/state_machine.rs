use std::future::Future;

use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::RockStore;
use oceanraft::storage::StorageExt;
use oceanraft::Apply;
use oceanraft::GroupState;
use oceanraft::StateMachine;

use crate::server::KVAppType;
use crate::server::{KVData, KVResponse};
use crate::storage::SledStorage;

pub struct KVStateMachine {
    storage: RockStore<SledStorage, SledStorage>,
}

impl KVStateMachine {
    pub fn new(storage: RockStore<SledStorage, SledStorage>) -> Self {
        Self { storage }
    }
}

impl StateMachine<KVData, KVResponse> for KVStateMachine {
    type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0;
    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        state: &oceanraft::GroupState,
        applys: Vec<Apply<KVData, KVResponse>>,
    ) -> Self::ApplyFuture<'life0> {
        async move {
            for apply in applys {
                println!("apply index = {}", apply.get_index());
                let gs = self.storage.group_storage(group_id, 1).await.unwrap();
                gs.set_applied(apply.get_index()).unwrap();
            }
        }
    }
}

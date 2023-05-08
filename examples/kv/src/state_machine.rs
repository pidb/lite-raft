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
        replica_id: u64,
        state: &oceanraft::GroupState,
        applys: Vec<Apply<KVData, KVResponse>>,
    ) -> Self::ApplyFuture<'life0> {
        async move {
            for apply in applys {
                let apply_index = apply.get_index();
                println!("group({}), replica({}) apply index = {}", group_id, replica_id, apply_index);
                match apply {
                    Apply::NoOp(apply) => {}
                    Apply::Normal(apply) => {}
                    Apply::Membership(apply) => {
                        apply.tx.map(|tx| {
                            tx.send(Ok((
                                KVResponse {
                                    index: apply.index,
                                    term: apply.term,
                                },
                                apply.ctx,
                            )))
                        });
                    }
                }
                let gs = self
                    .storage
                    .group_storage(group_id, replica_id)
                    .await
                    .unwrap();
                gs.set_applied(apply_index).unwrap();
            }
        }
    }
}

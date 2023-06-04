use std::future::Future;

use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::RockStore;
use oceanraft::storage::StorageExt;
use oceanraft::Apply;
use oceanraft::StateMachine;

use crate::server::{KVData, KVResponse};
use crate::storage::MemKvStorage;

pub struct KVStateMachine {
    storage: RockStore<MemKvStorage, MemKvStorage>,
    kv_storage: MemKvStorage,
}

impl KVStateMachine {
    pub fn new(storage: RockStore<MemKvStorage, MemKvStorage>, kv_storage: MemKvStorage) -> Self {
        Self {
            storage,
            kv_storage,
        }
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
                println!(
                    "group({}), replica({}) apply index = {}",
                    group_id, replica_id, apply_index
                );
                match apply {
                    Apply::NoOp(_) => {}
                    Apply::Normal(mut apply) => {
                        let res = KVResponse {
                            index: apply_index,
                            term: apply.term,
                        };
                        self.kv_storage.put(apply.data.key, apply.data.value);
                        // TODO: this call as method
                        apply
                            .tx
                            .map(|tx| tx.send(Ok((res, apply.context.take()))).unwrap());
                    }
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
                // TODO: consider more easy api
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

use std::sync::Arc;

use futures::Future;
use oceanraft::multiraft::storage::StateMachineStore;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::ApplyMembership;
use oceanraft::multiraft::ApplyNoOp;
use oceanraft::multiraft::ApplyNormal;
use oceanraft::multiraft::ApplyResult;
use oceanraft::multiraft::GroupState;
use oceanraft::multiraft::StateMachine;
use oceanraft::multiraft::WriteData;
use oceanraft::multiraft::WriteResponse;
use oceanraft::prelude::StoreData;
use slog::info;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MemStoreStateMachine<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    trigger_apply_error: Arc<RwLock<(bool, u64)>>,
    tx: Sender<Vec<Apply<W, R>>>,
}

impl<W, R> MemStoreStateMachine<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub fn new(tx: Sender<Vec<Apply<W, R>>>) -> Self {
        Self {
            tx,
            trigger_apply_error: Default::default(),
        }
    }

    pub async fn trigger_apply_error(&self, enable: bool, which: u64) {
        let mut wl = self.trigger_apply_error.write().await;
        *wl = (enable, which)
    }
}

impl<W, R> StateMachine<W, R> for MemStoreStateMachine<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    type ApplyFuture<'life0> = impl Future<Output = ApplyResult<W, R>> + 'life0
        where
            Self: 'life0;
    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        state: &GroupState,
        mut applys: Vec<Apply<W, R>>,
    ) -> Self::ApplyFuture<'life0> {
        let tx = self.tx.clone();

        let mut send_applys = vec![];
        async move {
            let lock = self.trigger_apply_error.read().await;
            if lock.0 && lock.1 == 0 {
                return ApplyResult {
                    index: 0,
                    term: 0,
                    unapplied: Some(applys),
                    reason: None,
                };
            }

            if let Some(apply) = applys.first() {
                if lock.0 && apply.get_index() == lock.1 {
                    return ApplyResult {
                        index: 0,
                        term: 0,
                        unapplied: Some(applys),
                        reason: None,
                    };
                }
            }
            drop(lock);

            let mut applied_index = 0;
            let mut applied_term = 0;

            while let Some(apply) = applys.pop() {
                let lock = self.trigger_apply_error.read().await;
                if lock.0 && apply.get_index() == lock.1 {
                    tracing::info!("trigger errored at apply {}", apply.get_index());
                    drop(lock);
                    break;
                }
                drop(lock);

                applied_index = apply.get_index();
                applied_term = apply.get_term();
                match apply {
                    Apply::NoOp(noop) => {
                        send_applys.push(Apply::NoOp(ApplyNoOp {
                            group_id: noop.group_id,
                            index: noop.index,
                            term: noop.term,
                        }));
                    }
                    Apply::Normal(mut normal) => {
                        send_applys.push(Apply::Normal(ApplyNormal {
                            group_id: normal.group_id,
                            index: normal.index,
                            term: normal.term,
                            data: normal.data,
                            context: normal.context.take(),
                            is_conf_change: normal.is_conf_change,
                            tx: normal.tx.take(),
                        }));
                    }
                    Apply::Membership(mut membership) => {
                        // TODO: if group is leader, we need save conf state to kv store.
                        membership.tx.take().map(|tx| tx.send(Ok(R::default())));
                        send_applys.push(Apply::Membership(ApplyMembership {
                            group_id: membership.group_id,
                            index: membership.index,
                            term: membership.term,
                            change_data: membership.change_data,
                            conf_state: membership.conf_state,
                            tx: None,
                        }));
                    }
                }
            }

            tracing::info!("return apply index = {}", applied_index);
            let res = ApplyResult {
                index: applied_index,
                term: applied_term,
                unapplied: if applys.is_empty() {
                    None
                } else {
                    Some(applys)
                },
                reason: None,
            };

            tx.send(send_applys).await;

            res
        }
    }
}

#[derive(Clone)]
pub struct RockStoreStateMachine<R>
where
    R: WriteResponse,
{
    kv_store: StateMachineStore<R>,
    tx: Sender<Vec<Apply<StoreData, R>>>,
}

impl<R> RockStoreStateMachine<R>
where
    R: WriteResponse,
{
    pub fn new(kv_store: StateMachineStore<R>, tx: Sender<Vec<Apply<StoreData, R>>>) -> Self {
        Self { kv_store, tx }
    }
}

impl<R> StateMachine<StoreData, R> for RockStoreStateMachine<R>
where
    R: WriteResponse,
{
    type ApplyFuture<'life0> = impl Future<Output = ApplyResult<StoreData, R>> + 'life0
    where
        Self: 'life0;
    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        _state: &GroupState,
        mut applys: Vec<Apply<StoreData, R>>,
    ) -> Self::ApplyFuture<'life0> {
        let tx = self.tx.clone();
        async move {
            let mut batch = self.kv_store.write_batch_for_apply(group_id);
            for apply in applys.iter_mut() {
                match apply {
                    Apply::NoOp(noop) => {
                        batch.set_applied_index(noop.index);
                        batch.set_applied_term(noop.term);
                    }
                    Apply::Normal(normal) => {
                        batch.put_data(&normal.data);
                        batch.set_applied_index(normal.index);
                        batch.set_applied_term(normal.term);
                    }
                    Apply::Membership(membership) => {
                        // membership.done().await.unwrap();
                        // TODO: if group is leader, we need save conf state to kv store.
                        batch.set_applied_index(membership.index);
                        batch.set_applied_term(membership.term);
                        batch.put_conf_state(&membership.conf_state);
                    }
                }
            }
            self.kv_store.write_apply_bath(group_id, batch).unwrap();

            for apply in applys.iter_mut() {
                match apply {
                    Apply::NoOp(_) => {}
                    Apply::Normal(normal) => {
                        normal.tx.take().map(|tx| tx.send(Ok(R::default())));
                    }
                    Apply::Membership(membership) => {
                        membership.tx.take().map(|tx| tx.send(Ok(R::default())));
                    }
                }
            }

            let res = ApplyResult {
                index: applys.last().unwrap().get_index(),
                term: applys.last().unwrap().get_term(),
                unapplied: None,
                reason: None,
            };

            if let Err(_) = tx.send(applys).await {}
            res
        }
    }
}

// #[derive(Clone)]
// pub struct FixtureMultiStateMachine<R>
// where
//     R: WriteResponse,
// {
//     kv_store: KVStore<R>,
//     machines: Arc<RwLock<HashMap<u64, FixtureStateMachine<R>>>>,
//     rxs: Arc<RwLock<HashMap<u64, Arc<Mutex<Receiver<Apply<StoreData, R>>>>>>>,
// }

// impl<R> FixtureMultiStateMachine<R>
// where
//     R: WriteResponse,
// {
//     pub fn new(kv_store: KVStore<R>) -> Self {
//         Self {
//             rxs: Default::default(),
//             machines: Default::default(),
//             kv_store,
//         }
//     }

//     pub fn get_rx(&self, group_id: u64) -> Arc<Mutex<Receiver<Apply<StoreData, R>>>> {
//         let lock = self.rxs.read().unwrap();
//         lock.get(&group_id).unwrap().clone()
//     }
// }

// impl<R> MultiStateMachine<StoreData, R> for FixtureMultiStateMachine<R>
// where
//     R: WriteResponse,
// {
//     type E = FixtureWriteDataError;
//     type S = FixtureStateMachine<R>;

//     fn create_state_machine(&self, group_id: u64) -> Result<Self::S, Self::E> {
//         {
//             let rl = self.machines.read().unwrap();
//             if let Some(machine) = rl.get(&group_id) {
//                 return Ok(machine.clone());
//             }
//         }

//         let (tx, rx) = channel(100);
//         let machine = FixtureStateMachine {
//             group_id,
//             tx,
//             kv_store: self.kv_store.clone(),
//         };

//         {
//             let mut wl = self.machines.write().unwrap();
//             wl.insert(group_id, machine.clone());
//         }

//         let mut wl = self.rxs.write().unwrap();
//         assert_eq!(
//             wl.insert(group_id, Arc::new(Mutex::new(rx))).is_none(),
//             true
//         );

//         Ok(machine)
//     }
// }

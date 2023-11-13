use futures::Future;
use oceanraft::prelude::StoreData;
use oceanraft::rsm_event;
use oceanraft::storage::StateMachineStore;
use oceanraft::GroupState;
use oceanraft::ProposeRequest;
use oceanraft::ProposeResponse;
use oceanraft::StateMachine;
use tokio::sync::mpsc::Sender;
use tracing::info;

use super::port::StateMachineEvent;

#[derive(Clone)]
pub struct MemStoreStateMachine<W>
where
    W: ProposeRequest,
{
    // tx: Sender<Vec<Apply<W, ()>>>,
    event_tx: Sender<StateMachineEvent<W, ()>>,
}

impl<W> StateMachine<W, ()> for MemStoreStateMachine<W>
where
    W: ProposeRequest,
{
    type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
    fn apply<'life0>(
        &'life0 self,
        mut event: rsm_event::ApplyEvent<W, ()>,
    ) -> Self::ApplyFuture<'life0> {
        let tx = self.event_tx.clone();
        async move {
            for apply in event.applys.iter_mut() {
                match apply {
                    rsm_event::Apply::NoOp(noop) => {}
                    rsm_event::Apply::Normal(normal) => {}
                    rsm_event::Apply::Membership(membership) => {
                        // TODO: if group is leader, we need save conf state to kv store.
                        // FIXME: don't use default trait
                        membership
                            .tx
                            .take()
                            .map(|tx| tx.send(Ok(((), membership.ctx.take()))));
                    }
                }
            }

            tx.send(StateMachineEvent::Apply(event.applys)).await;
        }
    }

    type OnLeaderElectionFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
    fn on_leader_election<'life0>(
        &'life0 self,
        event: rsm_event::LeaderElectionEvent,
    ) -> Self::OnLeaderElectionFuture<'life0> {
        async move {}
    }

    type OnGroupCreateFuture<'life0> = impl Future<Output = ()> + 'life0
    where
        Self: 'life0;
    fn on_group_create<'life0>(
        &'life0 self,
        _: rsm_event::GroupCreateEvent,
    ) -> Self::OnGroupCreateFuture<'life0> {
        async move {}
    }
}

impl<W> MemStoreStateMachine<W>
where
    W: ProposeRequest,
{
    pub fn new(event_tx: Sender<StateMachineEvent<W, ()>>) -> Self {
        Self { event_tx }
    }
}

#[derive(Clone)]
pub struct RockStoreStateMachine {
    kv_store: StateMachineStore<()>,
    event_tx: Sender<StateMachineEvent<StoreData, ()>>,
}

impl RockStoreStateMachine {
    pub fn new(
        kv_store: StateMachineStore<()>,
        event_tx: Sender<StateMachineEvent<StoreData, ()>>,
    ) -> Self {
        Self { kv_store, event_tx }
    }
}

impl StateMachine<StoreData, ()> for RockStoreStateMachine {
    type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0
    where
        Self: 'life0;
    fn apply<'life0>(
        &'life0 self,
        mut event: rsm_event::ApplyEvent<StoreData, ()>,
    ) -> Self::ApplyFuture<'life0> {
        let tx = self.event_tx.clone();
        async move {
            let mut batch = self.kv_store.write_batch_for_apply(event.group_id);
            for apply in event.applys.iter_mut() {
                match apply {
                    rsm_event::Apply::NoOp(noop) => {
                        batch.set_applied_index(noop.index);
                        batch.set_applied_term(noop.term);
                    }
                    rsm_event::Apply::Normal(normal) => {
                        batch.put_data(&normal.data);
                        batch.set_applied_index(normal.index);
                        batch.set_applied_term(normal.term);
                    }
                    rsm_event::Apply::Membership(membership) => {
                        // membership.done().await.unwrap();
                        // TODO: if group is leader, we need save conf state to kv store.
                        batch.set_applied_index(membership.index);
                        batch.set_applied_term(membership.term);
                        batch.put_conf_state(&membership.conf_state);
                    }
                }
            }
            self.kv_store
                .write_apply_bath(event.group_id, batch)
                .unwrap();

            for apply in event.applys.iter_mut() {
                match apply {
                    rsm_event::Apply::NoOp(_) => {}
                    rsm_event::Apply::Normal(normal) => {
                        normal.tx.take().map(|tx| tx.send(Ok(((), None))));
                    }
                    rsm_event::Apply::Membership(membership) => {
                        membership
                            .tx
                            .take()
                            .map(|tx| tx.send(Ok(((), membership.ctx.take()))));
                    }
                }
            }

            if let Err(_) = tx.send(StateMachineEvent::Apply(event.applys)).await {}
        }
    }

    type OnLeaderElectionFuture<'life0> = impl Future<Output = ()> + 'life0
    where
        Self: 'life0;
    fn on_leader_election<'life0>(
        &'life0 self,
        event: rsm_event::LeaderElectionEvent,
    ) -> Self::OnLeaderElectionFuture<'life0> {
        let tx = self.event_tx.clone();
        async move {
            info!(
                "group({}), replica({}) become leader, term = {}",
                event.group_id, event.replica_id, event.term
            );
            if let Err(_) = tx.send(StateMachineEvent::LeaderElection(event)).await {
                info!("send leader election event failed");
            }
        }
    }

    type OnGroupCreateFuture<'life0> = impl Future<Output = ()> + 'life0
    where
        Self: 'life0;
    fn on_group_create<'life0>(
        &'life0 self,
        _: rsm_event::GroupCreateEvent,
    ) -> Self::OnGroupCreateFuture<'life0> {
        async move {}
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

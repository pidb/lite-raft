use std::collections::HashMap;
use std::slice::IterMut;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::vec::IntoIter;

use futures::Future;
use oceanraft::multiraft::storage::KVStore;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::ApplyMembership;
use oceanraft::multiraft::ApplyNoOp;
use oceanraft::multiraft::ApplyNormal;
use oceanraft::multiraft::GroupState;
// use oceanraft::multiraft::MultiStateMachine;
use oceanraft::multiraft::StateMachine;
use oceanraft::multiraft::WriteResponse;
use oceanraft::prelude::StoreData;
use rocksdb::WriteBatch;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;


#[derive(Clone)]
pub struct FixtureStateMachine<R>
where
    R: WriteResponse,
{
    kv_store: KVStore<R>,
    tx: Sender<Vec<Apply<StoreData, R>>>,
}

impl<R> FixtureStateMachine<R>
where
    R: WriteResponse,
{
    pub fn new(kv_store: KVStore<R>, tx: Sender<Vec<Apply<StoreData, R>>>) -> Self {
        Self { kv_store, tx }
    }
}

impl<R> StateMachine<StoreData, R> for FixtureStateMachine<R>
where
    R: WriteResponse,
{
    type ApplyFuture<'life0> = impl Future<Output = ()>
    where
        Self: 'life0;
    fn apply(
        &self,
        group_id: u64,
        _state: &GroupState,
        applys: Vec<Apply<StoreData, R>>,
    ) -> Self::ApplyFuture<'_> {
        let tx = self.tx.clone();
        self.kv_store.apply(group_id, &applys);
        async move {
            tx.send(applys).await;
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

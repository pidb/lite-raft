use std::collections::HashMap;
use std::marker::PhantomData;

use raft::Ready;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;

use raft::prelude::Entry;
use raft::prelude::HardState;
use raft::prelude::Snapshot;
use tokio::task::JoinHandle;

use crate::storage::transmute_raft_entries_ref;
use crate::storage::transmute_raft_hard_state;
use crate::storage::transmute_raft_snapshot;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

#[derive(Default, Debug)]
pub struct GroupWriteRequest {
    pub replica_id: u64,
    pub hard_state: Option<HardState>,
    pub entries: Option<Vec<Entry>>,
    pub snapshot: Option<Snapshot>,
}

#[derive(Default, Debug)]
pub struct WriteTaskRequest {
    pub groups: HashMap<u64, GroupWriteRequest>,
}

#[derive(Default, Debug)]
pub struct WriteTaskResponse {}

pub struct WriteAddress {
    // send request to write actor
    pub tx: Sender<WriteTaskRequest>,
    // receive response from write actor
    pub rx: Receiver<WriteTaskResponse>,
}

pub struct WriterActor<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    storage: MRS,
    rx: Receiver<WriteTaskRequest>,
    tx: Sender<WriteTaskResponse>,
    _m1: PhantomData<RS>,
}

impl<RS, MRS> WriterActor<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn spawn(storage: MRS, stop: watch::Receiver<bool>) -> (JoinHandle<()>, WriteAddress) {
        let (request_tx, request_rx) = channel(1);
        let (response_tx, response_rx) = channel(1);

        let actor = WriterActor {
            rx: request_rx,
            tx: response_tx,
            storage,
            _m1: PhantomData,
        };

        let main_loop = async move {
            actor.start(stop).await;
        };

        let join_handle = tokio::spawn(main_loop);

        let address = WriteAddress {
            tx: request_tx,
            rx: response_rx,
        };

        (join_handle, address)
    }

    async fn start(mut self, mut stop_rx: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break;
                    }
                },
                Some(request) = self.rx.recv() => {
                    let res = self.handle_write_request(request).await;
                    if let Err(_error) = self.tx.send(res).await {

                    }
                },
                else => {
                    break
                }
            }
        }
    }

    async fn handle_write_request(&mut self, request: WriteTaskRequest) -> WriteTaskResponse {
        let res = WriteTaskResponse::default();
        // TODO(yuanchang.xu) Disk write flow control
        for (group_id, group_write_request) in request.groups.into_iter() {
            let gs = match self
                .storage
                .group_storage(group_id, group_write_request.replica_id)
                .await
            {
                Ok(gs) => match gs {
                    Some(gs) => gs,
                    None => {
                        return res;
                    }
                },
                Err(error) => {
                    return res;
                }
            };

            // step 2. apply snapshot
            if let Some(snapshot) = group_write_request.snapshot {
                if let Err(_error) = gs.apply_snapshot(transmute_raft_snapshot(snapshot)) {}
            }

            // step 3.2. persistent enteis
            if let Some(entries) = group_write_request.entries {
                if !entries.is_empty() {
                    if let Err(_error) = gs.append_entries(transmute_raft_entries_ref(&entries)) {}
                }
            }

            // step 3.3. persistent hard state
            if let Some(hs) = group_write_request.hard_state {
                if let Err(_error) = gs.set_hardstate(transmute_raft_hard_state(hs)) {}
            }
        }

        res
    }
}

use std::collections::HashMap;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;


use raft::prelude::HardState;
use raft::prelude::Snapshot;
use raft::prelude::Entry;
use tokio::task::JoinHandle;


#[derive(Default, Debug)]
pub struct GroupWriteRequest {
    pub hard_state: Option<HardState>,
    pub entries: Option<Vec<Entry>>,
    pub snapshot: Option<Snapshot>,
}

#[derive(Default, Debug)]
pub struct WriteTaskRequest {
    pub groups: HashMap<u64, GroupWriteRequest>,
}

#[derive(Default, Debug)]
pub struct WriteTaskResponse {

}

pub struct WriteAddress {
    // send request to write actor
    pub tx: Sender<WriteTaskRequest>,
    // receive response from write actor
    pub rx: Receiver<WriteTaskResponse>,
}

pub struct WriterActor {
    rx: Receiver<WriteTaskRequest>,
    tx: Sender<WriteTaskResponse>,
}

impl WriterActor {
    pub fn spawn(stop: watch::Receiver<bool>) -> (JoinHandle<()>, WriteAddress) {
        let (request_tx, request_rx) = channel(1);
        let (response_tx, response_rx) = channel(1);
        
        let actor = WriterActor {
            rx: request_rx,
            tx: response_tx,
        };

        let main_loop = async move {
            actor.start(stop)
        };

        let join_handle = tokio::spawn(main_loop);
        

        let address = WriteAddress {
            tx: request_tx,
            rx: response_rx,
        };

       
        (join_handle, address)
   }

   fn start(mut self, stop: watch::Receiver<bool>) {
        unimplemented!()
   }
}
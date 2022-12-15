use std::{collections::HashMap, fmt::Debug};

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;


use super::apply_command::ApplyCommand;

#[derive(Default)]
pub struct GroupApplyRequest<R> {
    pub term: u64,
    pub commit_index: u64,
    pub entries: Vec<raft::prelude::Entry>,
    pub apply_commands: Vec<ApplyCommand<R>>,
}


#[derive(Default)]
pub struct ApplyTaskRequest<R> {
    pub groups: HashMap<u64, GroupApplyRequest<R>>,
}

pub struct ApplyTaskResponse {

}

pub struct ApplyActorAddress<R> {
    pub tx: Sender<ApplyTaskRequest<R>>,
    pub rx: Receiver<ApplyTaskResponse>,
}

pub struct ApplyActor<R> {
    pub rx: Receiver<ApplyTaskRequest<R>>,
    pub tx: Sender<ApplyTaskResponse>,
    pub pending_apply_commands: Vec<ApplyCommand<R>>,
}

impl<R: Send + 'static> ApplyActor<R> {
    pub fn spawn(mut stop_rx: watch::Receiver<bool>) -> (JoinHandle<()>, ApplyActorAddress<R>) {
        let (request_tx, request_rx) = channel(1);
        let (response_tx, response_rx) = channel(1);

        let address = ApplyActorAddress {
            tx: request_tx,
            rx: response_rx,
        };

        let actor = ApplyActor {
            rx: request_rx,
            tx: response_tx,
            pending_apply_commands: Vec::new(),
        };

        let join_handle = tokio::spawn(async move {
            actor.start(stop_rx).await;
        });
        
        (join_handle, address)
    }

    async fn start(mut self, mut stop_rx: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break
                    }
                },
                Some(request) = self.rx.recv() => {

                }
            }
        }
    }
}
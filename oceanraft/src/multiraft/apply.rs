use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::vec::IntoIter;

use tracing::info;

use futures::Future;
use raft::prelude::ConfChangeV2;
use raft::Storage;
use raft_proto::ConfChangeI;

use prost::Message as ProstMessage;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use raft_proto::prelude::ConfChange;
use raft_proto::prelude::ConfChangeType;
use raft_proto::prelude::Entry;
use raft_proto::prelude::EntryType;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::SingleMembershipChange;

use crate::multiraft::config::Config;

// use super::apply_command::ApplyCommand;
use super::error::Error;
use super::error::ProposalError;
use super::event::ApplyMembershipChangeEvent;
use super::event::ApplyNormalEvent;
use super::event::CallbackEvent;
use super::event::Event;
use super::event::MembershipChangeView;
use super::event::MultiRaftAsyncCb;
use super::multiraft_actor::MultiRaftActorContext;
use super::proposal::Proposal;
use super::storage::MultiRaftStorage;

const MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

pub struct Apply {
    pub replica_id: u64,
    pub group_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entries: Vec<Entry>,
    pub entries_size: usize,
    pub proposals: VecDeque<Proposal>,
}

impl Apply {
    fn try_batch(&mut self, that: &mut Apply) -> bool {
        assert_eq!(self.replica_id, that.replica_id);
        assert_eq!(self.group_id, that.group_id);
        if self.entries_size + that.entries_size > MAX_APPLY_BATCH_SIZE {
            return false;
        }

        assert!(that.term >= self.term);
        assert!(that.commit_index >= self.commit_index);
        assert!(that.commit_term >= self.commit_term);
        self.term = that.term;
        self.commit_index = that.commit_index;
        self.commit_term = that.commit_term;
        self.entries.append(&mut that.entries);
        self.entries_size += that.entries_size;
        self.proposals.append(&mut that.proposals);
        return true;
    }
}

// #[derive(Default)]
// pub struct GroupApplyRequest {
//     pub term: u64,
//     pub commit_index: u64,
//     pub entries: Vec<raft::prelude::Entry>,
//     pub apply_commands: Vec<ApplyCommand>,
// }

pub enum ApplyTask {
    Apply(Apply),
}

#[derive(Debug)]
pub struct ApplyResult {}

#[derive(Default)]
pub struct ApplyTaskRequest {
    pub groups: HashMap<u64, ApplyTask>,
}

#[derive(Debug)]
pub struct ApplyTaskResponse {
    pub apply_results: HashMap<u64, ApplyResult>,
}

pub struct ApplyActorAddress {
    pub tx: Sender<ApplyTaskRequest>,
    pub rx: Receiver<ApplyTaskResponse>,
}

pub struct ApplyActor {
    node_id: u64,
    rx: Receiver<ApplyTaskRequest>,
    tx: Sender<ApplyTaskResponse>,
    event_tx: Sender<Vec<Event>>,
    callback_event_tx: Sender<CallbackEvent>,
    // apply_to_tx: Sender<Vec<ApplyCommand>>,
    group_pending_apply: HashMap<u64, Apply>,
}

impl ApplyActor {
    /// the `event_tx` send apply event to application.
    /// the `committed_tx` send committed event to multiraft actor.
    pub fn spawn(
        config: Config,
        event_tx: Sender<Vec<Event>>,
        callback_event_tx: Sender<CallbackEvent>,
        stop_rx: watch::Receiver<bool>,
    ) -> (JoinHandle<()>, ApplyActorAddress) {
        let (request_tx, request_rx) = channel(1);
        let (response_tx, response_rx) = channel(1);

        let address = ApplyActorAddress {
            tx: request_tx,
            rx: response_rx,
        };

        let actor = ApplyActor {
            node_id: config.node_id,
            event_tx,
            rx: request_rx,
            tx: response_tx,
            callback_event_tx,
            group_pending_apply: HashMap::new(),
        };

        let join_handle = tokio::spawn(async move {
            actor.start(stop_rx).await;
        });

        (join_handle, address)
    }

    #[tracing::instrument(name = "ApplyActor::start", skip(self, stop_rx))]
    async fn start(mut self, mut stop_rx: watch::Receiver<bool>) {
        info!("node ({}) apply actor start", self.node_id);
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        break
                    }
                },
                Some(request) = self.rx.recv() => self.handle_request(request).await,
            }
        }
        info!("node ({}) apply actor stop", self.node_id);
    }

    async fn handle_request(&mut self, request: ApplyTaskRequest) {
        let mut apply_results  = HashMap::new();
        for (group_id, task) in request.groups.into_iter() {
            match task {
                ApplyTask::Apply(mut apply) => {
                    match self.group_pending_apply.get_mut(&group_id) {
                        Some(batch) => {
                            if batch.try_batch(&mut apply) {
                                continue;
                            }

                            let take_batch = self.group_pending_apply.remove(&group_id).unwrap();
                            let res = self.handle_apply(take_batch).await.unwrap();
                            apply_results.insert(group_id, res);
                        }
                        None => {
                            self.group_pending_apply.insert(group_id, apply);
                        }
                    };
                }
            }
        }

        let task_response = ApplyTaskResponse {
            apply_results,
        };

        self.tx.send(task_response).await.unwrap();
    }

    async fn handle_apply(&mut self, apply: Apply) -> Result<ApplyResult, Error> {
        let mut delegate = ApplyDelegate {
            group_id: apply.group_id,
            pending_proposals: apply.proposals,
            staging_events: Vec::new(),
            callback_event_tx: self.callback_event_tx.clone(),
        };

        delegate.handle_committed_entries(apply.entries);
        self.event_tx.send(delegate.staging_events).await.unwrap();

        let res = ApplyResult {};
        Ok(res)

        // delegate.apply_to(&self.apply_to_tx);
        // take entries
        // transervel entry
        //   get proposal if exists
        //
        //   match entry type
        //      if cc apply inner data structure and
        //      if other, together entry and proposal to vec
        // send batch apply to user interface
    }
}

pub struct ApplyDelegate {
    group_id: u64,
    // apply_multiraft_tx: Sender<(MembershipChangeResult, oneshot::Sender<Result<(), Error>>)>,
    callback_event_tx: Sender<CallbackEvent>,
    pending_proposals: VecDeque<Proposal>,
    staging_events: Vec<Event>,
}

impl ApplyDelegate {
    fn pop_normal(&mut self, index: u64, term: u64) -> Option<Proposal> {
        self.pending_proposals.pop_front().and_then(|cmd| {
            if (cmd.term, cmd.index) > (term, index) {
                self.pending_proposals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn find_pending(&mut self, term: u64, index: u64) -> Option<Proposal> {
        while let Some(p) = self.pop_normal(index, term) {
            if p.term == term {
                if p.index == index {
                    return Some(p);
                } else {
                    panic!(
                        "unexpected callback at term {}, found index {}, expected {}",
                        term, p.index, index
                    );
                }
            } else {
                // notify_stale_command(region_id, peer_id, self.term, head);
                p.tx.map(|tx| tx.send(Err(Error::Proposal(ProposalError::Stale(p.term)))));
            }
        }
        return None;
    }

    // fn set_apply_state<'life0>(&'life0 mut self, apply_state: &ApplyState) {
    // }

    fn handle_committed_entries(&mut self, ents: Vec<Entry>) {
        for entry in ents.into_iter() {
            match entry.entry_type() {
                EntryType::EntryNormal => self.handle_committed_normal(entry),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry)
                }
            }
        }
    }

    fn handle_committed_normal(&mut self, entry: Entry) {
        let entry_index = entry.index;
        let entry_term = entry.term;

        if entry.data.is_empty() {
            // TODO: detect user propose empty data and if there any pending proposal, we don't
            // apply it and notify client.
            // info!(
            //     self.ctx.root_logger,
            //     "{} skip no-op log index = {}, term = {}",
            //     self.ctx.peer_id,
            //     entry_index,
            //     entry_term
            // );
            // self.apply_state.applied_term = entry_term;
            // self.apply_state.applied_index = entry_index;
            self.response_stale_proposals(entry_index, entry_term);
            return;
        }
        let tx = self
            .find_pending(entry.term, entry.index)
            .map_or(None, |p| p.tx);

        let apply_command = Event::ApplyNormal(ApplyNormalEvent {
            group_id: self.group_id,
            is_conf_change: false,
            entry,
            tx,
        });
        self.staging_events.push(apply_command);
    }

    fn handle_committed_conf_change(&mut self, entry: Entry) {
        // TODO: empty adta?

        let tx = self
            .find_pending(entry.term, entry.index)
            .map_or(None, |p| p.tx);
        let proposal = self.find_pending(entry.term, entry.index);
        let event = match entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut cc = ConfChange::default();
                cc.merge(entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeRequest::default();
                change_request.merge(entry.context.as_ref()).unwrap();

                let change_view = Some(MembershipChangeView {
                    index: entry.index,
                    conf_change: cc.clone().into_v2(),
                    change_request,
                });

                ApplyMembershipChangeEvent {
                    group_id: self.group_id,
                    entry,
                    tx,
                    change_view,
                    callback_event_tx: self.callback_event_tx.clone(),
                }
            }
            EntryType::EntryConfChangeV2 => {
                let mut cc_v2 = ConfChangeV2::default();
                cc_v2.merge(entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeRequest::default();
                change_request.merge(entry.context.as_ref()).unwrap();

                let change_view = Some(MembershipChangeView {
                    index: entry.index,
                    conf_change: cc_v2,
                    change_request,
                });

                ApplyMembershipChangeEvent {
                    group_id: self.group_id,
                    entry,
                    tx,
                    change_view,
                    callback_event_tx: self.callback_event_tx.clone(),
                }
            }
        };

        let apply_command = Event::ApplyMembershipChange(event);
        self.staging_events.push(apply_command);

        // self.apply_state.applied_index = entry.index;
        // self.apply_state.applied_term = entry.term;
    }

    fn response_stale_proposals(&mut self, index: u64, term: u64) {
        while let Some(p) = self.pop_normal(index, term) {
            p.tx.map(|tx| tx.send(Err(Error::Proposal(ProposalError::Stale(p.term)))));
        }
    }
}

// fn commit_membership_change_cb<'r, RS, MRS>(result: MembershipChangeRequest) -> MultiRaftAsyncCb<'r, RS, MRS>
// where
//     MRS: MultiRaftStorage<RS>,
//     RS: Storage,
// {
//     let cb = move |ctx: &'r mut MultiRaftActorContext<RS, MRS>| -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'r >> {
//         let local_fut = async move {
//             for change in result.changes.iter() {
//             match change.change_type() {
//                 ConfChangeType::AddNode => {
//                     // TODO: this call need transfer to user call, and if user call return errored,
//                     // the membership change should failed and user need to retry.
//                     // we need a channel to provider user notify actor it need call these code.
//                     // and we recv the notify can executing these code, if executed failed, we
//                     // response to user and membership change is failed.
//                     let replica_metadata = ReplicaDesc {
//                         node_id: change.node_id,
//                         replica_id: change.replica_id,
//                     };
//                     ctx.node_manager.add_node(change.node_id, change.group_id);
//                     ctx.replica_cache
//                         .cache_replica_desc(change.group_id, replica_metadata, ctx.sync_replica_cache)
//                         .await
//                         .unwrap();
//                 }
//                 ConfChangeType::RemoveNode => unimplemented!(),
//                 ConfChangeType::AddLearnerNode => unimplemented!(),
//             }
//         }

//         Ok(())
//         };
//         Box::pin(local_fut)

//         // pin!(local_fut)
//     };

//     Box::new(cb)
// }

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use prost::Message as ProstMessage;

use raft::prelude::ConfChangeV2;

use raft_proto::prelude::ConfChange;
use raft_proto::prelude::Entry;
use raft_proto::prelude::EntryType;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::ConfChangeI;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

use tracing::debug;
use tracing::info;
use tracing::trace;
use tracing::error;
use tracing::trace_span;
use tracing::warn;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::multiraft::config::Config;
use crate::util::Stopper;
use crate::util::TaskGroup;

use super::error::Error;
use super::error::ProposalError;
use super::event::ApplyMembershipChangeEvent;
use super::event::ApplyNormalEvent;
use super::event::CallbackEvent;
use super::event::Event;
use super::event::MembershipChangeView;
use super::proposal::Proposal;
use super::raft_group::RaftGroupApplyState;

/// State is used to safely shard the state
/// of an actor between threads.

#[derive(Default)]
struct State {
    stopped: AtomicBool,
}

// #[derive(Clone)]
// pub struct ApplyActorSender {
//     pub tx: Sender<ApplyTaskRequest>,
// }

// pub struct ApplyActorReceiver {
//     pub rx: Receiver<ApplyTaskResponse>,
// }

pub struct ApplyActor {
    // cfg: Config,
    state: Arc<State>,
    runtime: Mutex<Option<ApplyActorRuntime>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl ApplyActor {
    pub fn new(
        cfg: &Config,
        request_rx: UnboundedReceiver<(Span, ApplyRequest)>,
        response_tx: UnboundedSender<ApplyTaskResponse>,
        callback_tx: Sender<CallbackEvent>,
        event_tx: &Sender<Vec<Event>>,
    ) -> Self {
        let state = Arc::new(State::default());
        let runtime = ApplyActorRuntime {
            state: state.clone(),
            multi_groups_apply_state: HashMap::default(), // FIXME: Should be initialized at raft group creation time
            node_id: cfg.node_id,
            event_tx: event_tx.clone(),
            cfg: cfg.clone(),
            // ctx: ctx.clone(),
            rx: request_rx,
            tx: response_tx,
            callback_tx,
            batch_apply: cfg.batch_apply,
            batch_size: cfg.batch_size, // TODO: per-group
            // task_group: task_group.clone(),
            pending_applys: HashMap::new(),
        };

        Self {
            state,
            runtime: Mutex::new(Some(runtime)),
            join: Mutex::default(),
        }
    }

    pub fn start(&self, task_group: &TaskGroup) {
        let runtime = {
            let mut wl = self.runtime.lock().unwrap();
            wl.take().unwrap()
        };

        let stopper = task_group.stopper();
        let jh = task_group.spawn(async move {
            runtime.main_loop(stopper).await;
        });

        *self.join.lock().unwrap() = Some(jh);
    }
}

const MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug)]
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
    fn try_batch(&mut self, that: &mut Apply, max_batch_size: usize) -> bool {
        assert_eq!(self.replica_id, that.replica_id);
        assert_eq!(self.group_id, that.group_id);
        assert!(that.term >= self.term);
        assert!(that.commit_index >= self.commit_index);
        assert!(that.commit_term >= self.commit_term);
        if max_batch_size == 0 || self.entries_size + that.entries_size > max_batch_size {
            return false;
        }
        self.term = that.term;
        self.commit_index = that.commit_index;
        self.commit_term = that.commit_term;
        self.entries.append(&mut that.entries);
        self.entries_size += that.entries_size;
        self.proposals.append(&mut that.proposals);
        return true;
    }
}

pub enum ApplyTask {
    Apply(Apply),
}

#[derive(Default)]
pub struct ApplyRequest {
    pub groups: HashMap<u64, ApplyTask>,
}

#[derive(Debug)]
pub struct ApplyResult {
    pub apply_state: RaftGroupApplyState,
}

#[derive(Debug)]
pub struct ApplyTaskResponse {
    pub apply_results: HashMap<u64, ApplyResult>,
}

pub struct ApplyActorRuntime {
    multi_groups_apply_state: HashMap<u64, RaftGroupApplyState>,
    node_id: u64,
    cfg: Config,
    state: Arc<State>,
    rx: UnboundedReceiver<(tracing::span::Span, ApplyRequest)>,
    tx: UnboundedSender<ApplyTaskResponse>,
    event_tx: Sender<Vec<Event>>,
    callback_tx: Sender<CallbackEvent>,
    pending_applys: HashMap<u64, Apply>,
    batch_apply: bool,
    batch_size: usize,
}

impl ApplyActorRuntime {
    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorInner::start",
    //     fields(node_id=self.node_id)
    //     skip_all
    // )]
    async fn main_loop(mut self, mut stopper: Stopper) {
        info!("node {}: start apply main_loop", self.node_id);
        loop {
            tokio::select! {
                _ = &mut stopper => {
                    break
                },
                Some((span, request)) = self.rx.recv() => self.handle_request(request).instrument(span).await,
            }
        }

        // info!("node ({}) apply actor stop", self.node_id);
        self.do_stop();
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_request",
    //     skip_all
    // )]
    async fn handle_request(&mut self, request: ApplyRequest) {
        let mut apply_results = HashMap::new();
        for (group_id, task) in request.groups.into_iter() {
            match task {
                ApplyTask::Apply(apply) => {
                    if let Some(apply) = self.try_batch(group_id, apply) {
                        match self
                            .handle_apply(group_id, apply)
                            .instrument(tracing::span!(
                                parent: &tracing::span::Span::current(),
                                Level::TRACE,
                                "handle_apply",
                            ))
                            .await
                        {
                            Ok(res) => apply_results.insert(group_id, res),
                            Err(err) => panic!("apply error = {}", err),
                        };
                    }
                    // if self.batch_apply {
                    //     match self.pending_applys.get_mut(&group_id) {
                    //         Some(batch) => {
                    //             if batch.try_batch(&mut apply, self.batch_size) {
                    //                 continue;
                    //             }

                    //             let take_batch = self.pending_applys.remove(&group_id).unwrap();
                    //             let res = self.handle_apply(group_id, take_batch).await.unwrap();
                    //             apply_results.insert(group_id, res);
                    //         }
                    //         None => {
                    //             self.pending_applys.insert(group_id, apply);
                    //         }
                    //     };
                    // } else {
                    //     let res = self
                    //         .handle_apply(group_id, apply)
                    //         .instrument(tracing::span!(
                    //             parent: &tracing::span::Span::current(),
                    //             Level::TRACE,
                    //             "handle_apply",
                    //         ))
                    //         .await
                    //         .unwrap();
                    //     apply_results.insert(group_id, res);
                    // }
                }
            }
        }

        let task_response = ApplyTaskResponse { apply_results };

        if let Err(err) = self.tx.send(task_response) {
            error!("node {}:send apply response {:?} error, the receiver on multiraft actor dropped", self.node_id, err.0);
        }
    }

    /// Return None if the can batched otherwise return Apply that can be applied.
    fn try_batch(&mut self, group_id: u64, mut new_apply: Apply) -> Option<Apply> {
        if !self.batch_apply {
            if let Some(pending_apply) = self.pending_applys.remove(&group_id) {
                self.pending_applys.insert(group_id, new_apply);
                return Some(pending_apply);
            }
            return Some(new_apply);
        }

        if let Some(batch_apply) = self.pending_applys.get_mut(&group_id) {
            if batch_apply.try_batch(&mut new_apply, self.batch_size) {
                None
            } else {
                self.pending_applys.remove(&group_id)
            }
        } else {
            self.pending_applys.insert(group_id, new_apply)
        }
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_apply",
    //     skip_all
    // )]
    async fn handle_apply(&mut self, group_id: u64, apply: Apply) -> Result<ApplyResult, Error> {
        let apply_state = match self.multi_groups_apply_state.get_mut(&group_id) {
            Some(state) => state,
            None => {
                // FIXME: Should be initialized at raft group creation time
                self.multi_groups_apply_state
                    .insert(group_id, RaftGroupApplyState::default());
                self.multi_groups_apply_state.get_mut(&group_id).unwrap()
            }
        };

        let (prev_applied_index, prev_applied_term) =
            (apply_state.applied_index, apply_state.applied_term);
        let (curr_commit_index, curr_commit_term) = (apply.commit_index, apply.commit_term);
        // check if the state machine is backword
        if prev_applied_index > curr_commit_index || prev_applied_term > curr_commit_term {
            panic!(
                "commit state jump backward {:?} -> {:?}",
                (prev_applied_index, prev_applied_term),
                (curr_commit_index, curr_commit_term)
            );
        }

        let mut delegate = ApplyDelegate {
            event_tx: self.event_tx.clone(),
            node_id: self.cfg.node_id,
            group_id: apply.group_id,
            pending_proposals: apply.proposals,
            staging_events: Vec::new(),
            callback_event_tx: self.callback_tx.clone(),
        };

        delegate.handle_committed_entries(apply.entries, apply_state).await;
        // if !delegate.staging_events.is_empty() {
        //     ApplyActorRuntime::notify_apply(&self.event_tx, delegate.staging_events).await;
        // }

        let res = ApplyResult {
            apply_state: apply_state.clone(),
        };
        Ok(res)
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::notify_apply",
    //     skip_all
    // )]
   

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorRuntime::do_stop", 
        skip_all
    )]
    fn do_stop(mut self) {}
}

pub struct ApplyDelegate {
    group_id: u64,
    node_id: u64,
    event_tx: Sender<Vec<Event>>,
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

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_committed_entries",
    //     skip_all
    // )]
    async fn handle_committed_entries(&mut self, ents: Vec<Entry>, state: &mut RaftGroupApplyState) {
        for entry in ents.into_iter() {
            match entry.entry_type() {
                EntryType::EntryNormal => self.handle_committed_normal(entry, state),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry, state)
                }
            }
        }
        Self::notify_apply(&self.event_tx, std::mem::take(&mut self.staging_events)).await;   
    }

    async fn notify_apply(event_tx: &Sender<Vec<Event>>, events: Vec<Event>) {
        if let Err(err) = event_tx.send(events).await {
            warn!("notify apply events {:?}, but receiver dropped", err.0);
        }
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_committed_normal",
    //     skip_all
    // )]
    fn handle_committed_normal(&mut self, entry: Entry, state: &mut RaftGroupApplyState) {
        let entry_index = entry.index;
        let entry_term = entry.term;

        if entry.data.is_empty() {
            // When the new leader online, a no-op log will be send and commit.
            // we will skip this log for the application and set index and term after
            // apply.
            info!(
                "node {}: group = {} skip no-op entry index = {}, term = {}",
                self.node_id, self.group_id, entry_index, entry_term
            );
            if !self.pending_proposals.is_empty() {
                warn!(
                    "node {}: group = {} remove stale proposals because there was a leadership change at term = {}",
                    self.node_id,  self.group_id, entry_term
                );
                self.response_stale_proposals(entry_index, entry_term);
            }
        } else {
            trace!(
                "staging pending apply entry log ({}, {})",
                entry_index,
                entry_term
            );
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

        state.applied_index = entry_index;
        state.applied_term = entry_term;
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_committed_conf_change",
    //     skip_all,
    //     fields(node_id = self.node_id),
    // )]
    fn handle_committed_conf_change(&mut self, entry: Entry, state: &mut RaftGroupApplyState) {
        // TODO: empty adta?
        let entry_index = entry.index;
        let entry_term = entry.term;

        let tx = self
            .find_pending(entry.term, entry.index)
            .map_or(None, |p| p.tx);
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

        state.applied_index = entry_index;
        state.applied_term = entry_term;
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::response_stale_proposals",
    //     skip_all
    // )]
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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::vec::IntoIter;

use serde::__private::de::Content;
use tokio::task::JoinError;
use tracing::info;
use tracing::Level;

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
use tracing::trace;
use tracing::warn;

use crate::multiraft::config::Config;
use crate::util::Stopper;
use crate::util::TaskGroup;

// use super::apply_command::ApplyCommand;
use super::error::Error;
use super::error::ProposalError;
use super::event::ApplyMembershipChangeEvent;
use super::event::ApplyNormalEvent;
use super::event::CallbackEvent;
use super::event::Event;
use super::event::MembershipChangeView;
// use super::event::MultiRaftAsyncCb;
use super::multiraft_actor::MultiRaftActorContext;
use super::proposal::Proposal;
use super::raft_group::RaftGroupApplyState;
use super::storage::MultiRaftStorage;

struct Shard {
    stopped: AtomicBool,
}

impl Shard {
    fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
        }
    }
}

/// ApplyActorContext is used to safely shard the state
/// of an actor between threads.
pub struct ApplyActorContext {
    shard: Arc<Shard>,
}
impl ApplyActorContext {
    pub fn new() -> Self {
        Self {
            shard: Arc::new(Shard::new()),
        }
    }
}

impl Clone for ApplyActorContext {
    fn clone(&self) -> Self {
        Self {
            shard: self.shard.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ApplyActorSender {
    pub tx: Sender<ApplyTaskRequest>,
}

pub struct ApplyActorReceiver {
    pub rx: Receiver<ApplyTaskResponse>,
}

pub struct ApplyActor {
    join: JoinHandle<()>,
    ctx: ApplyActorContext,
}

impl ApplyActor {
    /// clone and return `MultiRaftActorContext`.
    pub fn context(&self) -> ApplyActorContext {
        self.ctx.clone()
    }

    /// join take onwership to join actor.
    async fn join(self) -> Result<(), JoinError> {
        self.join.await
    }

    /// `true` if actor stop.
    pub fn stopped(&self) -> bool {
        self.ctx.shard.stopped.load(Ordering::Acquire)
    }
}

pub fn spawn(
    config: Config,
    event_tx: Sender<Vec<Event>>,
    callback_event_tx: Sender<CallbackEvent>,
    task_group: TaskGroup,
) -> (ApplyActor, ApplyActorSender, ApplyActorReceiver) {
    let (request_tx, request_rx) = channel(1);
    let (response_tx, response_rx) = channel(1);

    let ctx = ApplyActorContext::new();

    let actor_inner = ApplyActorInner {
        multi_groups_apply_state: HashMap::default(), // FIXME: Should be initialized at raft group creation time
        node_id: config.node_id,
        event_tx,
        cfg: config,
        ctx: ctx.clone(),
        rx: request_rx,
        tx: response_tx,
        callback_event_tx,
        task_group: task_group.clone(),
        group_pending_apply: HashMap::new(),
    };

    let join = task_group.spawn(async move {
        actor_inner.start().await;
    });

    (
        ApplyActor { join, ctx },
        ApplyActorSender { tx: request_tx },
        ApplyActorReceiver { rx: response_rx },
    )
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
    fn try_batch(&mut self, that: &mut Apply, config: &Config) -> bool {
        assert_eq!(self.replica_id, that.replica_id);
        assert_eq!(self.group_id, that.group_id);
        if config.batch_size != 0 && self.entries_size + that.entries_size > config.batch_size {
            return false;
        }

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

pub enum ApplyTask {
    Apply(Apply),
}

#[derive(Debug)]
pub struct ApplyResult {
    pub apply_state: RaftGroupApplyState,
}

#[derive(Default)]
pub struct ApplyTaskRequest {
    pub groups: HashMap<u64, ApplyTask>,
}

#[derive(Debug)]
pub struct ApplyTaskResponse {
    pub apply_results: HashMap<u64, ApplyResult>,
}

pub struct ApplyActorInner {
    multi_groups_apply_state: HashMap<u64, RaftGroupApplyState>,

    node_id: u64,
    cfg: Config,
    ctx: ApplyActorContext,
    rx: Receiver<ApplyTaskRequest>,
    tx: Sender<ApplyTaskResponse>,
    event_tx: Sender<Vec<Event>>,
    callback_event_tx: Sender<CallbackEvent>,
    // apply_to_tx: Sender<Vec<ApplyCommand>>,
    group_pending_apply: HashMap<u64, Apply>,
    task_group: TaskGroup,
}

impl ApplyActorInner {
    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorInner::start", 
        fields(node_id=self.node_id)
        skip_all
    )]
    async fn start(mut self) {
        let mut stopper = self.task_group.stopper();
        loop {
            tokio::select! {
                _ = &mut stopper => {
                    break
                },
                Some(request) = self.rx.recv() => self.handle_request(request).await,
            }
        }

        info!("node ({}) apply actor stop", self.node_id);
        self.do_stop();
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorInner::handle_request", 
        skip_all
    )]
    async fn handle_request(&mut self, request: ApplyTaskRequest) {
        let mut apply_results = HashMap::new();
        for (group_id, task) in request.groups.into_iter() {
            match task {
                ApplyTask::Apply(mut apply) => {
                    if self.cfg.batch_apply {
                        match self.group_pending_apply.get_mut(&group_id) {
                            Some(batch) => {
                                if batch.try_batch(&mut apply, &self.cfg) {
                                    continue;
                                }

                                let take_batch =
                                    self.group_pending_apply.remove(&group_id).unwrap();
                                let res = self.handle_apply(group_id, take_batch).await.unwrap();
                                apply_results.insert(group_id, res);
                            }
                            None => {
                                self.group_pending_apply.insert(group_id, apply);
                            }
                        };
                    } else {
                        let res = self.handle_apply(group_id, apply).await.unwrap();
                        apply_results.insert(group_id, res);
                    }
                }
            }
        }

        let task_response = ApplyTaskResponse { apply_results };

        self.tx.send(task_response).await.unwrap();
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorInner::handle_apply", 
        skip_all
    )]
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
            // FIXME: need load curr_commit_term
            // panic!(
            //     "commit state jump backward {:?} -> {:?}",
            //     (prev_applied_index, prev_applied_term),
            //     (curr_commit_index, curr_commit_term)
            // );
        }

        let mut delegate = ApplyDelegate {
            group_id: apply.group_id,
            pending_proposals: apply.proposals,
            staging_events: Vec::new(),
            callback_event_tx: self.callback_event_tx.clone(),
        };

        delegate.handle_committed_entries(apply.entries, apply_state);
        if !delegate.staging_events.is_empty() {
            ApplyActorInner::notify_apply(&self.event_tx, delegate.staging_events).await;
        }

        let res = ApplyResult {
            apply_state: apply_state.clone(),
        };
        Ok(res)
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorInner::notify_apply", 
        skip_all
    )]
    async fn notify_apply(event_tx: &Sender<Vec<Event>>, events: Vec<Event>) {
        if let Err(err) = event_tx.send(events).await {
            warn!("notify apply events {:?}, but receiver dropped", err.0);
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorInner::do_stop", 
        skip_all
    )]
    fn do_stop(mut self) {}
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

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyDelegate::handle_committed_entries", 
        skip_all
    )]
    fn handle_committed_entries(&mut self, ents: Vec<Entry>, state: &mut RaftGroupApplyState) {
        for entry in ents.into_iter() {
            match entry.entry_type() {
                EntryType::EntryNormal => self.handle_committed_normal(entry, state),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry, state)
                }
            }
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyDelegate::handle_committed_normal", 
        skip_all
    )]
    fn handle_committed_normal(&mut self, entry: Entry, state: &mut RaftGroupApplyState) {
        let entry_index = entry.index;
        let entry_term = entry.term;

        if entry.data.is_empty() {
            // When the new leader online, a no-op log will be send and commit.
            // we will skip this log for the application and set index and term after
            // apply.
            info!("skip no-op index = {}, term = {}", entry_index, entry_term);
            if !self.pending_proposals.is_empty() {
                info!(
                    "skip no-op log index = {}, term = {}, remove stale proposals = {:?}",
                    entry_index, entry_term, self.pending_proposals
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

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyDelegate::handle_committed_conf_change", 
        skip_all
    )]
    fn handle_committed_conf_change(&mut self, entry: Entry, state: &mut RaftGroupApplyState) {
        // TODO: empty adta?
        let entry_index = entry.index;
        let entry_term = entry.term;

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

        state.applied_index = entry_index;
        state.applied_term = entry_term;
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyDelegate::response_stale_proposals", 
        skip_all
    )]
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

use std::collections::HashMap;
use std::collections::VecDeque;
use std::mem::take;
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

use futures::stream::FuturesUnordered;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
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
        request_rx: UnboundedReceiver<(Span, Request)>,
        response_tx: UnboundedSender<Response>,
        callback_tx: Sender<CallbackEvent>,
        event_tx: &Sender<Vec<Event>>,
    ) -> Self {
        let state = Arc::new(State::default());
        let runtime =
            ApplyActorRuntime::new(cfg, &state, request_rx, response_tx, callback_tx, event_tx);

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
    pub proposals: Vec<Proposal>,
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

// pub enum ApplyTask {
//     Apply(Apply),
// }

// #[derive(Default)]
// pub struct ApplyRequest {
//     pub groups: HashMap<u64, ApplyTask>,
// }

#[derive(Debug)]
pub struct ApplyResult {}

// #[derive(Debug)]
// pub struct ApplyTaskResponse {
//     pub apply_results: HashMap<u64, ApplyResult>,
// }

pub struct ApplyActorRuntime {
    multi_groups_apply_state: HashMap<u64, RaftGroupApplyState>,
    node_id: u64,
    cfg: Config,
    state: Arc<State>,
    rx: UnboundedReceiver<(tracing::span::Span, Request)>,
    tx: UnboundedSender<Response>,
    event_tx: Sender<Vec<Event>>,
    callback_tx: Sender<CallbackEvent>,

    pending_applys: HashMap<u64, Vec<Apply>>,

    batch_apply: bool,
    batch_size: usize,
}

pub enum Request {
    Apply { applys: HashMap<u64, Apply> },
}

#[derive(Debug)]
pub struct Response {
    pub group_id: u64,
    pub apply_state: RaftGroupApplyState,
    pub apply_results: Vec<ApplyResult>,
}

impl ApplyActorRuntime {
    fn new(
        cfg: &Config,
        state: &Arc<State>,
        request_rx: UnboundedReceiver<(Span, Request)>,
        response_tx: UnboundedSender<Response>,
        callback_tx: Sender<CallbackEvent>,
        event_tx: &Sender<Vec<Event>>,
    ) -> Self {
        Self {
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
        }
    }

    #[inline]
    fn insert_pending_apply(&mut self, group_id: u64, apply: Apply) {
        match self.pending_applys.get_mut(&group_id) {
            Some(applys) => applys.push(apply),
            None => {
                self.pending_applys.insert(group_id, vec![apply]);
            }
        };
    }

    fn handle_requests(&mut self, requests: Vec<Request>) {
        // let mut pending_applys: HashMap<u64, Vec<Apply>> = HashMap::new();
        let mut batch_applys: HashMap<u64, Option<Apply>> = HashMap::new();

        // let mut batcher = Batcher::new(self.cfg.batch_apply, self.cfg.batch_size);
        for request in requests {
            match request {
                Request::Apply { applys } => {
                    for (group_id, mut apply) in applys.into_iter() {
                        if !self.cfg.batch_apply {
                            self.insert_pending_apply(group_id, apply);
                        } else {
                            // 如果可以 batch, 则新的 apply 和之前的 apply batch. 如果不存在之前
                            // 的 batch apply, 则用当前的 apply 设置.
                            // 如果不能 batch, 则之前的 batch_apply 拿到 pending applys 对应的组里面, 当前的 apply 设置为
                            // batch_apply 继续尝试 batch.
                            match batch_applys.get_mut(&group_id) {
                                Some(batch_apply) => {
                                    if let Some(batch) = batch_apply.as_mut() {
                                        if batch.try_batch(&mut apply, self.cfg.batch_size) {
                                            continue;
                                        } else {
                                            self.insert_pending_apply(
                                                group_id,
                                                batch_apply.take().unwrap(),
                                            );
                                        }
                                    }
                                    *batch_apply = Some(apply);
                                }
                                None => {
                                    batch_applys.insert(group_id, Some(apply));
                                }
                            }
                        }
                    }
                }
            }
        }

        for (group_id, mut batch_apply) in batch_applys.into_iter() {
            batch_apply
                .take()
                .map(|batch_apply| self.insert_pending_apply(group_id, batch_apply));
        }
    }

    async fn delegate_handle_applys(&mut self) {
        let mut futs = vec![];
        for (group_id, applys) in self.pending_applys.drain() {
            let apply_state = match self.multi_groups_apply_state.get(&group_id) {
                None => {
                    self.multi_groups_apply_state.insert(
                        group_id,
                        RaftGroupApplyState {
                            commit_index: 0,
                            commit_term: 0,
                            applied_term: 1,
                            applied_index: 1,
                        },
                    );
                    RaftGroupApplyState {
                        commit_index: 0,
                        commit_term: 0,
                        applied_index: 1,
                        applied_term: 1,
                    }
                }
                Some(state) => state.clone(),
            };
            let mut delegate = ApplyDelegate::new(
                self.node_id,
                group_id,
                &self.event_tx,
                &self.callback_tx,
                apply_state,
            );
            futs.push(tokio::spawn(async move {
                // TODO: new delegate
                // let mut response = Response {
                //     group_id,
                //     apply_results: Vec::new(),
                //     apply_state: RaftGroupApplyState::default(),
                // };
                delegate.handle_applys(group_id, applys).await.unwrap()
                // response.apply_results.push(apply_result);
                // response.apply_state = delegate.apply_state.clone();
                // response
            }));
        }

        for fut in futs {
            let response = fut.await.unwrap();
            self.multi_groups_apply_state
                .insert(response.group_id, response.apply_state.clone());
            self.tx.send(response).unwrap();
        }
    }
    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorInner::start",
    //     fields(node_id=self.node_id)
    //     skip_all
    // )]
    async fn main_loop(mut self, mut stopper: Stopper) {
        info!("node {}: start apply main_loop", self.node_id);

        let request_limit = 100; // FIXME: as cfg
        loop {
            tokio::select! {
                _ = &mut stopper => {
                    break
                },
                Some((span, incoming_request)) = self.rx.recv() =>  {
                    let mut requests = vec![];
                    requests.push(incoming_request);
                    // try to receive more requests until channel buffer
                    // empty or reach limit.
                    loop {
                        // TODO: try stop
                        // TODO: better limit by compute  entries size
                        if requests.len() >= request_limit {
                            break;
                        }

                        let request = match self.rx.try_recv() {
                            Ok((_, request)) => request,
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => {
                                self.do_stop();
                                return;
                            }
                        };

                        requests.push(request);
                    }
                    self.handle_requests(requests);
                    self.delegate_handle_applys().await;
                },
            }
        }

        // info!("node ({}) apply actor stop", self.node_id);
        self.do_stop();
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "ApplyActorRuntime::do_stop", 
        skip_all
    )]
    fn do_stop(mut self) {}
}

const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

struct PendingSender<AppResponse> {
    index: u64,
    term: u64,
    tx: Option<oneshot::Sender<Result<AppResponse, Error>>>,
}

impl<AppResponse> PendingSender<AppResponse> {
    fn new(index: u64, term: u64, tx: Option<oneshot::Sender<Result<AppResponse, Error>>>) -> Self {
        Self { index, term, tx }
    }
}

struct PendingSenderQueue<AppResponse> {
    normals: VecDeque<PendingSender<AppResponse>>,
    conf_change: Option<PendingSender<AppResponse>>,
}

impl<AppResponse> PendingSenderQueue<AppResponse> {
    pub fn new() -> Self {
        Self {
            normals: VecDeque::new(),
            conf_change: None,
        }
    }

    fn try_gc(&mut self) {
        if self.normals.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP
            && self.normals.len() < SHRINK_PENDING_CMD_QUEUE_CAP
        {
            self.normals.shrink_to_fit();
        }
    }

    #[inline]
    pub fn push_normal(&mut self, normal: PendingSender<AppResponse>) {
        self.normals.push_back(normal)
    }

    pub fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingSender<AppResponse>> {
        self.normals.pop_front().and_then(|cmd| {
            self.try_gc();
            if (cmd.term, cmd.index) > (term, index) {
                self.normals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    #[inline]
    pub fn set_conf_change(&mut self, conf_change: PendingSender<AppResponse>) {
        self.conf_change = Some(conf_change)
    }

    #[inline]
    pub fn take_conf_change(&mut self) -> Option<PendingSender<AppResponse>> {
        self.conf_change.take()
    }
}

pub struct ApplyDelegate {
    node_id: u64,
    group_id: u64,
    event_tx: Sender<Vec<Event>>,
    callback_event_tx: Sender<CallbackEvent>,
    // pending_proposals: VecDeque<Proposal>,
    staging_events: Vec<Event>,

    apply_state: RaftGroupApplyState,
    pending_senders: PendingSenderQueue<()>, // TODO: add generic
}

impl ApplyDelegate {
    fn new(
        node_id: u64,
        group_id: u64,
        event_tx: &Sender<Vec<Event>>,
        callback_event_tx: &Sender<CallbackEvent>,
        apply_state: RaftGroupApplyState,
    ) -> Self {
        Self {
            node_id,
            group_id,
            event_tx: event_tx.clone(),
            callback_event_tx: callback_event_tx.clone(),
            staging_events: Vec::new(),
            apply_state,
            pending_senders: PendingSenderQueue::new(),
        }
    }

    fn set_pending_conf_change(
        &mut self,
        sender: PendingSender<()>, /*FIXME: add generic type */
    ) {
        if let Some(sender) = self.pending_senders.take_conf_change() {
            // From tikv:
            // if it loses leadership before conf change is replicated, there may be
            // a stale pending conf change before next conf change is applied. If it
            // becomes leader again with the stale pending conf change, will enter
            // this block, so we notify leadership may have been changed.
            // TODO: notify stale command
            unimplemented!()
        }

        self.pending_senders.set_conf_change(sender);
    }

    fn push_pending_proposals(&mut self, proposals: Vec<Proposal>) {
        for mut p in proposals {
            let sender = PendingSender::new(p.index, p.term, p.tx.take());
            if p.is_conf_change {
                self.set_pending_conf_change(sender);
            } else {
                self.pending_senders.push_normal(sender);
            }
        }
    }

    fn find_pending_conf_change(&mut self, term: u64, index: u64) -> Option<PendingSender<()>> /* FIXME: add generic type */
    {
        if let Some(p) = self.pending_senders.take_conf_change() {
            if p.term == term && p.index == index {
                return Some(p);
            }
            // TODO: notify stale sender
            return None;
        }

        return None;
    }

    fn find_pending(
        &mut self,
        term: u64,
        index: u64,
        is_conf_change: bool,
    ) -> Option<PendingSender<()>> {
        if is_conf_change {
            return self.find_pending_conf_change(term, index);
        }

        while let Some(p) = self.pending_senders.pop_normal(index, term) {
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
                p.tx.map(|tx| {
                    tx.send(Err(Error::Proposal(ProposalError::Stale(
                        p.term, 0, /*FIXME: with term */
                    ))))
                });
            }
        }
        return None;
    }

    async fn handle_applys(
        &mut self,
        group_id: u64,
        applys: Vec<Apply>,
    ) -> Result<Response, Error> {
        let mut results = vec![];
        for mut apply in applys {
            let (prev_applied_index, prev_applied_term) = (
                self.apply_state.applied_index,
                self.apply_state.applied_term,
            );
            let (curr_commit_index, curr_commit_term) = (apply.commit_index, apply.commit_term);
            // check if the state machine is backword
            if prev_applied_index > curr_commit_index || prev_applied_term > curr_commit_term {
                panic!(
                    "commit state jump backward {:?} -> {:?}",
                    (prev_applied_index, prev_applied_term),
                    (curr_commit_index, curr_commit_term)
                );
            }

            self.push_pending_proposals(std::mem::take(&mut apply.proposals));
            let res = self
                .handle_entries(std::mem::take(&mut apply.entries))
                .await;
            results.push(res.unwrap());
        }

        Self::notify_apply(&self.event_tx, std::mem::take(&mut self.staging_events)).await;
        Ok(Response {
            group_id,
            apply_state: self.apply_state.clone(),
            apply_results: results,
        })
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_committed_entries",
    //     skip_all
    // )]
    async fn handle_entries(&mut self, ents: Vec<Entry>) -> Result<ApplyResult, Error> {
        for entry in ents.into_iter() {
            // TODO: applied index need load from storage.
            // let next_apply_index = self.apply_state.applied_index + 1;
            // if entry.index != next_apply_index {
            //     panic!(
            //         "apply entry index does not match, expect {}, but got {}",
            //         next_apply_index, entry.index
            //     );
            // }

            // TODO: add result
            match entry.entry_type() {
                EntryType::EntryNormal => self.handle_committed_normal(entry),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_committed_conf_change(entry)
                }
            }
        }

        Ok(ApplyResult {})
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
    fn handle_committed_normal(&mut self, entry: Entry) {
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
            self.response_stale_proposals(entry_index, entry_term);
        } else {
            trace!(
                "staging pending apply entry log ({}, {})",
                entry_index,
                entry_term
            );
            let tx = self
                .find_pending(entry.term, entry.index, false)
                .map_or(None, |p| p.tx);

            let apply_command = Event::ApplyNormal(ApplyNormalEvent {
                group_id: self.group_id,
                is_conf_change: false,
                entry,
                tx,
            });
            self.staging_events.push(apply_command);
        }

        self.apply_state.applied_index = entry_index;
        self.apply_state.applied_term = entry_term;
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::handle_committed_conf_change",
    //     skip_all,
    //     fields(node_id = self.node_id),
    // )]
    fn handle_committed_conf_change(&mut self, entry: Entry) {
        // TODO: empty adta?
        let entry_index = entry.index;
        let entry_term = entry.term;

        let tx = self
            .find_pending(entry.term, entry.index, false)
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

        self.apply_state.applied_index = entry_index;
        self.apply_state.applied_term = entry_term;
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "ApplyActorRuntime::response_stale_proposals",
    //     skip_all
    // )]
    fn response_stale_proposals(&mut self, index: u64, term: u64) {
        while let Some(p) = self.pending_senders.pop_normal(index, term) {
            p.tx.map(|tx| {
                tx.send(Err(Error::Proposal(ProposalError::Stale(
                    p.term, 0, /*FIXME: with term */
                ))))
            });
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

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use tokio::sync::mpsc::channel;
    use tokio::sync::mpsc::unbounded_channel;

    use crate::multiraft::util::compute_entry_size;
    use crate::multiraft::Config;
    use crate::prelude::Entry;
    use crate::prelude::EntryType;

    use super::Apply;
    use super::ApplyActorRuntime;
    use super::Request;
    use super::State;

    // TODO: as common method
    fn new_entries(start: u64, end: u64, term: u64, entry_size: usize) -> Vec<Entry> {
        let mut ents = vec![];
        for i in start..end {
            let mut ent = Entry::default();
            ent.set_entry_type(EntryType::EntryNormal);
            ent.index = i;
            ent.term = term;
            ent.sync_log = false;
            ent.context = vec![];
            ent.data = vec![0_u8; entry_size];
            ents.push(ent);
        }
        ents
    }

    fn new_apply(
        group_id: u64,
        replica_id: u64,
        term: u64,
        ent_start: u64,
        ent_end: u64,
        entry_size: usize,
    ) -> Apply {
        let entries = new_entries(ent_start, ent_end, term, entry_size);
        Apply {
            group_id,
            replica_id,
            term,
            commit_index: entries[entries.len() - 1].index,
            commit_term: entries[entries.len() - 1].term,
            entries_size: entries.iter().map(|ent| compute_entry_size(ent)).sum(),
            proposals: Vec::default(),
            entries,
        }
    }

    fn new_worker(batch_apply: bool, batch_size: usize) -> ApplyActorRuntime {
        let (_request_tx, request_rx) = unbounded_channel();
        let (response_tx, _response_rx) = unbounded_channel();
        let (callback_tx, _callback_rx) = channel(1);
        let (event_tx, _event_rx) = channel(1);
        let state = Arc::new(State::default());
        let cfg = Config {
            batch_apply,
            batch_size,
            ..Default::default()
        };

        ApplyActorRuntime::new(
            &cfg,
            &state,
            request_rx,
            response_tx,
            callback_tx,
            &event_tx,
        )
    }
    #[test]
    fn test_batch_pendings() {
        struct Expect {
            group_id: u64,
            pending_apply_len: usize,
            ent_index_ranges: Vec<(u64, u64)>,
        }
        let cases = vec![(
            vec![
                // group_id, batchs
                //  1, [[1,1,1]]
                //  2, [[2,2,2]]
                //  3, [[3,3,3]]
                //  4, [4, [4,4]]
                //  5, [5, [5,5]]
                Request::Apply {
                    applys: HashMap::from([
                        (1, new_apply(1, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (2, new_apply(2, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (3, new_apply(3, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (4, new_apply(4, 1, 1, 1, 2, 400)),
                        (5, new_apply(5, 1, 1, 1, 2, 400)),
                    ]),
                },
                Request::Apply {
                    applys: HashMap::from([
                        (1, new_apply(1, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (2, new_apply(2, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (3, new_apply(3, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (4, new_apply(4, 1, 1, 2, 4, 100)),
                        (5, new_apply(5, 1, 1, 2, 4, 100)),
                    ]),
                },
                Request::Apply {
                    applys: HashMap::from([
                        (1, new_apply(1, 1, 1, 5, 8, 50)), // [5 * 50, 6 * 50, 7 * 50]
                        (2, new_apply(2, 1, 1, 5, 8, 50)), // [5 * 50, 6 * 50, 7 * 50]
                        (3, new_apply(3, 1, 1, 5, 8, 50)), // [5 * 50, 6 * 50, 7 * 50]
                        (4, new_apply(4, 1, 1, 4, 5, 100)),
                        (5, new_apply(5, 1, 1, 4, 5, 100)),
                    ]),
                },
            ],
            vec![
                Expect {
                    group_id: 1,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 2,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 3,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 4,
                    pending_apply_len: 2,
                    ent_index_ranges: vec![(1, 1), (2, 4)],
                },
                Expect {
                    group_id: 5,
                    pending_apply_len: 2,
                    ent_index_ranges: vec![(1, 1), (2, 4)],
                },
            ],
        )];

        for case in cases {
            let mut worker = new_worker(true, 400);
            worker.handle_requests(case.0);
            for expect in case.1 {
                let pending_applys = worker.pending_applys.get(&expect.group_id).unwrap();
                assert_eq!(pending_applys.len(), expect.pending_apply_len);

                for (i, pending_apply) in pending_applys.iter().enumerate() {
                    let got_ents_index: Vec<u64> =
                        pending_apply.entries.iter().map(|ent| ent.index).collect();
                    let expect_ents_index: Vec<u64> =
                        (expect.ent_index_ranges[i].0..expect.ent_index_ranges[i].1 + 1).collect();
                    assert_eq!(got_ents_index, expect_ents_index);
                }
            }
        }
    }
}

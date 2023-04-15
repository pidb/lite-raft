use std::collections::HashMap;
use std::collections::VecDeque;
use std::marker::PhantomData;

use prost::Message;
use raft::prelude::ConfState;
use raft::prelude::Entry;
use raft_proto::ConfChangeI;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::Span;

use crate::Apply;
use crate::ApplyMembership;
use crate::ApplyNoOp;
use crate::ApplyNormal;
use crate::Config;
use crate::Error;
use crate::GroupState;
use crate::GroupStates;
use crate::ProposeData;
use crate::ProposeError;
use crate::ProposeResponse;
use crate::StateMachine;

use crate::msg::MembershipRequestContext;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeV2;
use crate::prelude::EntryType;
use crate::protos::MembershipChangeData;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::task_group::Stopper;
use crate::task_group::TaskGroup;
use crate::utils::flexbuffer_deserialize;

use super::error::ChannelError;
use super::error::DeserializationError;
use super::msg::ApplyCommitMessage;
use super::msg::ApplyData;
use super::msg::ApplyMessage;
use super::msg::ApplyResultMessage;
use super::msg::CommitMembership;
use super::proposal::Proposal;

#[derive(Debug)]
struct LocalApplyState {
    applied_term: u64,
    applied_index: u64,
}

pub struct ApplyActor;

impl ApplyActor {
    pub(crate) fn spawn<W, R, RSM, S, MS>(
        cfg: &Config,
        rsm: RSM,
        storage: MS,
        shared_states: GroupStates,
        request_rx: UnboundedReceiver<(Span, ApplyMessage<R>)>,
        response_tx: UnboundedSender<ApplyResultMessage>,
        commit_tx: UnboundedSender<ApplyCommitMessage>,
        task_group: &TaskGroup,
    ) -> Self
    where
        W: ProposeData,
        R: ProposeResponse,
        RSM: StateMachine<W, R>,
        S: RaftStorage,
        MS: MultiRaftStorage<S>,
    {
        let worker = ApplyWorker::new(
            cfg,
            rsm,
            storage,
            shared_states,
            request_rx,
            response_tx,
            commit_tx,
        );
        let stopper = task_group.stopper();
        task_group.spawn(async move {
            worker.main_loop(stopper).await;
        });

        Self {}
    }
}

pub struct ApplyWorker<W, R, RSM, S, MS>
where
    W: ProposeData,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
{
    node_id: u64,
    cfg: Config,
    rx: UnboundedReceiver<(tracing::span::Span, ApplyMessage<R>)>,
    tx: UnboundedSender<ApplyResultMessage>,
    delegate: ApplyDelegate<W, R, RSM>,
    local_apply_states: HashMap<u64, LocalApplyState>,
    shared_states: GroupStates,
    storage: MS,
    _m: PhantomData<S>,
}

impl<W, R, RSM, S, MS> ApplyWorker<W, R, RSM, S, MS>
where
    W: ProposeData,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
{
    #[inline]
    fn insert_pending_apply(
        applys: &mut HashMap<(u64, u64), Vec<ApplyData<R>>>,
        group_id: u64,
        replica_id: u64,
        apply: ApplyData<R>,
    ) {
        match applys.get_mut(&(group_id, replica_id)) {
            Some(applys) => applys.push(apply),
            None => {
                applys.insert((group_id, replica_id), vec![apply]);
            }
        };
    }

    // This method performs a batch of apply from the same group in multiple requests,
    // if batch is successful, multiple requests from the same group are batched into one apply,
    // otherwise pending in FIFO order.
    //
    // Note: This method provides scalability for us to make more flexible apply decisions in the future.
    fn batch_msgs(
        &mut self,
        msgs: std::vec::Drain<'_, ApplyMessage<R>>,
    ) -> HashMap<(u64, u64), Vec<ApplyData<R>>> {
        let mut pending_applys = HashMap::new();
        let mut batch_applys: HashMap<u64, Option<ApplyData<R>>> = HashMap::new();

        for msg in msgs {
            match msg {
                ApplyMessage::Apply { applys } => {
                    for (group_id, mut apply) in applys.into_iter() {
                        if !self.cfg.batch_apply {
                            Self::insert_pending_apply(
                                &mut pending_applys,
                                group_id,
                                apply.replica_id,
                                apply,
                            );
                        } else {
                            match batch_applys.get_mut(&group_id) {
                                Some(batch_apply) => {
                                    if let Some(batch) = batch_apply.as_mut() {
                                        if batch.try_batch(&mut apply, self.cfg.batch_size) {
                                            continue;
                                        } else {
                                            Self::insert_pending_apply(
                                                &mut pending_applys,
                                                group_id,
                                                apply.replica_id,
                                                batch_apply.take().expect("unreachable"),
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

        for (group_id, batch_apply) in batch_applys {
            if let Some(batch_apply) = batch_apply {
                Self::insert_pending_apply(
                    &mut pending_applys,
                    group_id,
                    batch_apply.replica_id,
                    batch_apply,
                );
            }
        }

        pending_applys
    }

    async fn handle_msgs(&mut self, msgs: std::vec::Drain<'_, ApplyMessage<R>>) {
        let pending_applys = self.batch_msgs(msgs);
        for ((group_id, replica_id), applys) in pending_applys {
            let gs = self
                .storage
                .group_storage(group_id, replica_id)
                .await
                .unwrap();

            if !self.local_apply_states.contains_key(&group_id) {
                let (index, term) = gs.get_applied().unwrap();
                self.local_apply_states.insert(
                    group_id,
                    LocalApplyState {
                        applied_index: index,
                        applied_term: term,
                    },
                );
            }

            let local_apply_state = self.local_apply_states.get_mut(&group_id).unwrap();

            let _ = self
                .delegate
                .handle_applys(group_id, replica_id, applys, local_apply_state, &gs)
                .await;

            let res = ApplyResultMessage {
                group_id,
                applied_index: local_apply_state.applied_index,
                applied_term: local_apply_state.applied_term,
            };

            if let Err(_) = self.tx.send(res) {
                error!(
                    "node {}: send response failed, the node actor dropped",
                    self.node_id
                );
            }
        }
    }

    async fn main_loop(mut self, mut stopper: Stopper) {
        info!("node {}: start apply main_loop", self.node_id);
        let mut pending_msgs = Vec::with_capacity(self.cfg.max_batch_apply_msgs);

        loop {
            tokio::select! {
                _ = &mut stopper => {
                    // self.do_stop();
                    break
                },
                // TODO: handle if the node actor stopped
                Some((_span, msg)) = self.rx.recv() =>  {
                    if pending_msgs.len() < self.cfg.max_batch_apply_msgs {
                        pending_msgs.push(msg);
                    }
                },
                else => {}
            }

            if pending_msgs.len() == self.cfg.max_batch_apply_msgs {
                self.handle_msgs(pending_msgs.drain(..)).await;
            }
        }
    }

    fn new(
        cfg: &Config,
        rsm: RSM,
        storage: MS,
        shared_states: GroupStates,
        request_rx: UnboundedReceiver<(Span, ApplyMessage<R>)>,
        response_tx: UnboundedSender<ApplyResultMessage>,
        commit_tx: UnboundedSender<ApplyCommitMessage>,
    ) -> Self {
        Self {
            local_apply_states: HashMap::default(),
            node_id: cfg.node_id,
            cfg: cfg.clone(),
            rx: request_rx,
            tx: response_tx,
            shared_states,
            storage,
            delegate: ApplyDelegate::new(cfg.node_id, rsm, commit_tx),
            _m: PhantomData,
        }
    }
}

/// Shrink queue if queue capacity more than and len less than
/// this value.
const SHRINK_PENDING_CMD_QUEUE_CAP: usize = 64;

struct PendingSender<RES>
where
    RES: ProposeResponse,
{
    index: u64,
    term: u64,
    tx: Option<oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>>,
}

impl<RES> PendingSender<RES>
where
    RES: ProposeResponse,
{
    fn new(
        index: u64,
        term: u64,
        tx: Option<oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>>,
    ) -> Self {
        Self { index, term, tx }
    }
}

struct PendingSenderQueue<RES>
where
    RES: ProposeResponse,
{
    normals: VecDeque<PendingSender<RES>>,
    conf_change: Option<PendingSender<RES>>,
}

impl<RES> PendingSenderQueue<RES>
where
    RES: ProposeResponse,
{
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
    pub fn push_normal(&mut self, normal: PendingSender<RES>) {
        self.normals.push_back(normal)
    }

    pub fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingSender<RES>> {
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
    pub fn set_conf_change(&mut self, conf_change: PendingSender<RES>) {
        self.conf_change = Some(conf_change)
    }

    #[inline]
    pub fn take_conf_change(&mut self) -> Option<PendingSender<RES>> {
        self.conf_change.take()
    }

    pub fn remove_stales(&mut self, index: u64, term: u64) {
        while let Some(p) = self.pop_normal(index, term) {
            p.tx.map(|tx| {
                tx.send(Err(Error::Propose(ProposeError::Stale(
                    p.term, 0, /*FIXME: with term */
                ))))
            });
        }
    }
}

pub struct ApplyDelegate<W, R, RSM>
where
    W: ProposeData,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
{
    node_id: u64,
    pending_senders: PendingSenderQueue<R>,
    rsm: RSM,
    commit_tx: UnboundedSender<ApplyCommitMessage>,
    _m1: PhantomData<W>,
    _m2: PhantomData<R>,
}

impl<W, R, RSM> ApplyDelegate<W, R, RSM>
where
    W: ProposeData,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
{
    fn new(node_id: u64, rsm: RSM, commit_tx: UnboundedSender<ApplyCommitMessage>) -> Self {
        Self {
            node_id,
            pending_senders: PendingSenderQueue::new(),
            rsm,
            commit_tx,
            _m1: PhantomData,
            _m2: PhantomData,
        }
    }

    fn set_pending_conf_change(&mut self, sender: PendingSender<R>) {
        if let Some(sender) = self.pending_senders.take_conf_change() {
            // From tikv:
            // if it loses leadership before conf change is replicated, there may be
            // a stale pending conf change before next conf change is applied. If it
            // becomes leader again with the stale pending conf change, will enter
            // this block, so we notify leadership may have been changed.
            sender.tx.map(|tx| {
                tx.send(Err(Error::Propose(ProposeError::Stale(
                    sender.term,
                    0, /*FIXME: with term */
                ))))
            });
        }

        self.pending_senders.set_conf_change(sender);
    }

    fn push_pending_proposals(&mut self, proposals: Vec<Proposal<R>>) {
        for mut p in proposals {
            let sender = PendingSender::new(p.index, p.term, p.tx.take());
            if p.is_conf_change {
                self.set_pending_conf_change(sender);
            } else {
                self.pending_senders.push_normal(sender);
            }
        }
    }

    fn find_pending_conf_change(&mut self, term: u64, index: u64) -> Option<PendingSender<R>> {
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
    ) -> Option<PendingSender<R>> {
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
                    tx.send(Err(Error::Propose(ProposeError::Stale(
                        p.term, 0, /*FIXME: with term */
                    ))))
                });
            }
        }
        return None;
    }

    /// Commit memberhsip change to specific raft group.
    async fn commit_membership_change(&self, commit: CommitMembership) -> Result<ConfState, Error> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self
            .commit_tx
            .send(ApplyCommitMessage::Membership((commit, tx)))
        {
            return Err(Error::Channel(ChannelError::ReceiverClosed(
                "node actor dropped".to_owned(),
            )));
        }

        // TODO: got conf state from commit to raft and save to self.
        let conf_state = rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed("node actor dropped".to_owned()))
        })??;

        Ok(conf_state)
    }

    async fn handle_conf_change(&mut self, group_id: u64, ent: Entry) -> Option<Apply<W, R>> {
        let index = ent.index;
        let term = ent.term;
        if ent.data.is_empty() && ent.entry_type() != EntryType::EntryConfChangeV2 {
            return Some(Apply::NoOp(ApplyNoOp {
                group_id,
                index,
                term,
            }));
        }

        let tx = self.find_pending(term, index, true).map_or(None, |p| p.tx);
        let (conf_change, request_ctx) = match parse_conf_change(&ent) {
            Err(err) => {
                tx.map(|tx| {
                    if let Err(backed) = tx.send(Err(err)) {
                        error!(
                            "response {:?} error to client failed, receiver dropped",
                            backed
                        )
                    }
                });

                return None;
            }
            Ok(val) => val,
        };

        // apply the membership changes of apply to oceanraft and raft group first.
        // It doesn't matter if the user state machine then fails to apply,
        // because we set the applied index based on the index successfully
        // applied by the user and then promote the raft based on that applied index,
        // so the user can apply the log later. For oceanraft and raft,
        // we make the commit an idempotent operation (TODO).
        let conf_state = match self
            .commit_membership_change(CommitMembership {
                group_id,
                index,
                term,
                conf_change,
                change_request: request_ctx.data.clone(),
            })
            .await
        {
            Err(err) => {
                tx.map(|tx| {
                    if let Err(backed) = tx.send(Err(err)) {
                        error!(
                            "response {:?} error to client failed, receiver dropped",
                            backed
                        )
                    }
                });
                return None;
            }
            Ok(conf_state) => conf_state,
        };

        Some(Apply::Membership(ApplyMembership {
            group_id,
            index,
            term,
            // conf_change,
            conf_state,
            change_data: request_ctx.data,
            ctx: request_ctx.user_ctx,
            tx,
        }))
    }

    fn handle_normal(&mut self, group_id: u64, ent: Entry) -> Option<Apply<W, R>> {
        let index = ent.index;
        let term = ent.term;
        if ent.data.is_empty() {
            // When the new leader online, a no-op log will be send and commit.
            // we will skip this log for the application and set index and term after
            // apply.
            info!(
                "node {}: group = {} skip no-op entry index = {}, term = {}",
                self.node_id, group_id, index, term
            );
            self.pending_senders.remove_stales(index, term);
            return Some(Apply::NoOp(ApplyNoOp {
                group_id,
                index,
                term,
            }));
        }

        trace!(
            "staging pending apply entry log ({}, {})",
            ent.index,
            ent.term
        );

        let tx = self
            .find_pending(ent.term, ent.index, false)
            .map_or(None, |p| p.tx);

        // TODO: handle this error
        let write_data = flexbuffer_deserialize(&ent.data).unwrap();

        Some(Apply::Normal(ApplyNormal {
            group_id,
            is_conf_change: false,
            // entry,
            index,
            term,
            data: write_data,
            context: if ent.context.is_empty() {
                None
            } else {
                Some(ent.context)
            },
            tx,
        }))
    }

    async fn handle_apply<S: RaftStorage>(
        &mut self,
        mut apply: ApplyData<R>,
        local_apply_state: &mut LocalApplyState,
        gs: &S,
    ) {
        let group_id = apply.group_id;
        let (prev_applied_index, prev_applied_term) =
            (local_apply_state.applied_index, local_apply_state.applied_term);
        let (curr_commit_index, curr_commit_term) = (apply.commit_index, apply.commit_term);
        // check if the state machine is backword
        if prev_applied_index > curr_commit_index || prev_applied_term > curr_commit_term {
            panic!(
                "commit state jump backward {:?} -> {:?}",
                (prev_applied_index, prev_applied_term),
                (curr_commit_index, curr_commit_term)
            );
        }

        if apply.entries.is_empty() {
            return;
        }

        // Helps applications establish monotonically increasing apply constraints for each batch.
        //
        // Notes:
        // If the `LocalApplyState` applied_index is equal to 0, it means the `Storage` **is not**
        // created with a configuration, and its last index and term should be equal to 0. This
        // case can happen when a consensus group is started with a membership change.
        // In this case, we give up continue check and then catch up leader state.
        if prev_applied_index != 0 && apply.entries[0].index != prev_applied_index + 1 {
            panic!(
                "node {}: group {} apply entries index does not match, expect {}, but got {}",
                self.node_id,
                group_id,
                prev_applied_index + 1,
                apply.entries[0].index
            );
        }

        self.push_pending_proposals(std::mem::take(&mut apply.proposals));
        let last_index = apply.entries.last().expect("unreachable").index;
        let last_term = apply.entries.last().expect("unreachable").term;
        let mut applys = vec![];
        for ent in apply.entries.into_iter() {
            let apply = match ent.entry_type() {
                EntryType::EntryNormal => self.handle_normal(group_id, ent),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_conf_change(group_id, ent).await
                }
            };

            if let Some(apply) = apply {
                applys.push(apply)
            }
        }

        // Since we feed the state machine probably a batch of entry logs, represented by IntoIter,
        //processing applied can be divided into the following scenarios:
        // 1. If maybe_failed_iter returns None, all entries have been applied successfully, and
        //    applied is set to commit_index
        // 2. If maybe_failed_iter returns Some, but next of maybe_failed_iter is None, this equals case 1
        // 3. Otherwise, maybe_failed_iter.next() -1 fails. We set applied as the index of the successful application log
        //
        // Edge case: If index is 1, no logging has been applied, and applied is set to 0

        // TODO: handle apply error: setting applied to error before
        self.rsm.apply(group_id, &GroupState::default(), applys).await;
        gs.set_applied(last_index, last_term).unwrap();
        local_apply_state.applied_index = last_index;
        local_apply_state.applied_term = last_term;
        // shared_state.set_applied_index(last_index);
        // shared_state.set_applied_index(last_term);
    }

    async fn handle_applys<S: RaftStorage>(
        &mut self,
        group_id: u64,
        replica_id: u64,
        applys: Vec<ApplyData<R>>,
        local_apply_state: &mut LocalApplyState,
        gs: &S,
    ) {
        for apply in applys {
            self.handle_apply(apply, local_apply_state, gs).await;
        }
    }
}

/// Parse out ConfChangeV2 and MembershipChangeData from entry.
/// Return Error if serialization error.
fn parse_conf_change(ent: &Entry) -> Result<(ConfChangeV2, MembershipRequestContext), Error> {
    match ent.entry_type() {
        EntryType::EntryNormal => unreachable!(),
        EntryType::EntryConfChange => {
            let conf_change = ConfChange::decode(ent.data.as_ref())
                .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?;

            let ctx = flexbuffer_deserialize(&ent.context)?;
            // let change_data = MembershipChangeData::decode(ent.context.as_ref())
            //     .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?;

            Ok((conf_change.into_v2(), ctx))
        }
        EntryType::EntryConfChangeV2 => {
            Ok((
                ConfChangeV2::decode(ent.data.as_ref())
                    .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?,
                // TODO: use flexbuffer
                // MembershipChangeData::decode(ent.context.as_ref())
                //     .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?,
                flexbuffer_deserialize(&ent.context)?,
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use futures::Future;
    use std::collections::HashMap;
    use tokio::sync::mpsc::unbounded_channel;

    use crate::state::GroupState;
    use crate::state::GroupStates;
    use crate::storage::MemStorage;
    use crate::storage::MultiRaftMemoryStorage;
    use crate::utils::compute_entry_size;
    use crate::Config;
    // use crate::multiraft::MultiStateMachine;
    use crate::prelude::Entry;
    use crate::prelude::EntryType;
    use crate::Apply;
    use crate::StateMachine;

    use super::ApplyData;
    use super::ApplyMessage;
    use super::ApplyWorker;

    struct NoOpStateMachine {}
    impl StateMachine<(), ()> for NoOpStateMachine {
        type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn apply(&self, _: u64, _: &GroupState, _: Vec<Apply<(), ()>>) -> Self::ApplyFuture<'_> {
            async move {}
        }
    }

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
    ) -> ApplyData<()> {
        let entries = new_entries(ent_start, ent_end, term, entry_size);
        ApplyData {
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

    fn new_worker(
        batch_apply: bool,
        batch_size: usize,
    ) -> ApplyWorker<(), (), NoOpStateMachine, MemStorage, MultiRaftMemoryStorage> {
        let (_request_tx, request_rx) = unbounded_channel();
        let (response_tx, _response_rx) = unbounded_channel();
        let (callback_tx, _callback_rx) = unbounded_channel();
        let cfg = Config {
            batch_apply,
            batch_size,
            ..Default::default()
        };

        let storage = MultiRaftMemoryStorage::new(1);
        let rsm = NoOpStateMachine {};
        let shared_states = GroupStates::new();
        ApplyWorker::new(
            &cfg,
            rsm,
            storage,
            shared_states,
            request_rx,
            response_tx,
            callback_tx,
        )
    }
    #[test]
    fn test_batch_pendings() {
        struct Expect {
            group_id: u64,
            replica_id: u64,
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
                ApplyMessage::Apply {
                    applys: HashMap::from([
                        (1, new_apply(1, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (2, new_apply(2, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (3, new_apply(3, 1, 1, 1, 3, 50)), // [1 * 50, 2 * 50]
                        (4, new_apply(4, 1, 1, 1, 2, 400)),
                        (5, new_apply(5, 1, 1, 1, 2, 400)),
                    ]),
                },
                ApplyMessage::Apply {
                    applys: HashMap::from([
                        (1, new_apply(1, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (2, new_apply(2, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (3, new_apply(3, 1, 1, 3, 5, 50)), // [3 * 50, 4 * 50]
                        (4, new_apply(4, 1, 1, 2, 4, 100)),
                        (5, new_apply(5, 1, 1, 2, 4, 100)),
                    ]),
                },
                ApplyMessage::Apply {
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
                    replica_id: 1,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 2,
                    replica_id: 1,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 3,
                    replica_id: 1,
                    pending_apply_len: 1,
                    ent_index_ranges: vec![(1, 7)],
                },
                Expect {
                    group_id: 4,
                    replica_id: 1,
                    pending_apply_len: 2,
                    ent_index_ranges: vec![(1, 1), (2, 4)],
                },
                Expect {
                    group_id: 5,
                    replica_id: 1,
                    pending_apply_len: 2,
                    ent_index_ranges: vec![(1, 1), (2, 4)],
                },
            ],
        )];

        for mut case in cases {
            let mut worker = new_worker(true, 400);
            let pending_applys = worker.batch_msgs(case.0.drain(..));
            for expect in case.1 {
                let pending_applys = pending_applys.get(&(expect.group_id, expect.replica_id)).unwrap();
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

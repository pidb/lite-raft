use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use raft::prelude::ConfChangeTransition;
use raft::prelude::ConfState;
use raft::prelude::Entry;
use raft_proto::ConfChangeI;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;
use tracing::trace;

use crate::apply::actor::LocalApplyState;
use crate::error::ChannelError;
use crate::error::DeserializationError;
use crate::msg::ApplyCommitRequest;
use crate::msg::ApplyData;
use crate::msg::CommitMembership;
use crate::msg::MembershipRequestContext;
use crate::msg::NodeMessage;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeV2;
use crate::prelude::EntryType;
use crate::proposal::Proposal;
use crate::rsm::StateMachine;
use crate::rsm_event;
use crate::utils::flexbuffer_deserialize;
use crate::utils::mpsc;
use crate::Error;
use crate::ProposeError;
use crate::ProposeRequest;
use crate::ProposeResponse;

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
    fn new() -> Self {
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
    fn push_normal(&mut self, normal: PendingSender<RES>) {
        self.normals.push_back(normal)
    }

    fn pop_normal(&mut self, index: u64, term: u64) -> Option<PendingSender<RES>> {
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
    fn set_conf_change(&mut self, conf_change: PendingSender<RES>) {
        self.conf_change = Some(conf_change)
    }

    #[inline]
    fn take_conf_change(&mut self) -> Option<PendingSender<RES>> {
        self.conf_change.take()
    }

    fn remove_stales(&mut self, index: u64, term: u64) {
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
    W: ProposeRequest,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
{
    node_id: u64,
    pending_senders: PendingSenderQueue<R>,
    rsm: Arc<RSM>,
    node_msg_tx: mpsc::WrapSender<NodeMessage<W, R>>,
    _m1: PhantomData<W>,
    _m2: PhantomData<R>,
}

impl<W, R, RSM> ApplyDelegate<W, R, RSM>
where
    W: ProposeRequest,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
{
    pub(super) fn new(
        node_id: u64,
        rsm: &Arc<RSM>,
        node_msg_tx: mpsc::WrapSender<NodeMessage<W, R>>,
    ) -> Self {
        Self {
            node_id,
            pending_senders: PendingSenderQueue::new(),
            rsm: rsm.clone(),
            node_msg_tx,
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
                p.tx.map(|tx| {
                    tx.send(Err(Error::Propose(ProposeError::Stale(
                        p.term, 0, /*FIXME: with term */
                    ))))
                });
            }
        }
        return None;
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

    /// Commit memberhsip change to specific raft group.
    async fn commit_membership_change(&self, commit: CommitMembership) -> Result<ConfState, Error> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self
            .node_msg_tx
            .send(NodeMessage::ApplyCommit(ApplyCommitRequest::Membership((
                commit, tx,
            ))))
            .await
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

    async fn handle_conf_change(
        &mut self,
        group_id: u64,
        ent: Entry,
    ) -> Option<rsm_event::Apply<W, R>> {
        let index = ent.index;
        let term = ent.term;

        // 当 ConfChangeV2 的转换类型为 Explicit/Auto 时，会发送一个空的 v2 变更来让联合共识离开
        // 联合点, 所以需要处理这种情况.
        if ent.data.is_empty() && ent.entry_type() != EntryType::EntryConfChangeV2 {
            // if ent.data.is_empty()  {
            return Some(rsm_event::Apply::NoOp(rsm_event::ApplyNoOp {
                group_id,
                index,
                term,
            }));
        }

        let tx = self.find_pending(term, index, true).map_or(None, |p| p.tx);
        let (conf_change, mut request_ctx) = match parse_conf_change(&ent) {
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

        let change_request = request_ctx
            .as_ref()
            .map_or(None, |request_ctx| Some(request_ctx.data.clone()));

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
                change_request,
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

        let change_request = request_ctx
            .take()
            .map_or(None, |request_ctx| Some(request_ctx.data));
        let user_ctx = request_ctx.map_or(None, |ctx| ctx.user_ctx);

        Some(rsm_event::Apply::Membership(rsm_event::ApplyMembership {
            group_id,
            index,
            term,
            conf_state,
            change_data: change_request,
            ctx: user_ctx,
            tx,
        }))
    }

    fn handle_normal(&mut self, group_id: u64, ent: Entry) -> Option<rsm_event::Apply<W, R>> {
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
            return Some(rsm_event::Apply::NoOp(rsm_event::ApplyNoOp {
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

        Some(rsm_event::Apply::Normal(rsm_event::ApplyNormal {
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

    async fn handle_apply(&mut self, mut apply: ApplyData<R>, state: &mut LocalApplyState) {
        let group_id = apply.group_id;
        let (prev_applied_index, prev_applied_term) = (state.applied_index, state.applied_term);
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
        // if prev_applied_index != 0 && apply.entries[0].index != prev_applied_index + 1 {
        //     panic!(
        //         "node {}: group {} apply entries index does not match, expect {}, but got {}",
        //         self.node_id,
        //         group_id,
        //         prev_applied_index + 1,
        //         apply.entries[0].index
        //     );
        // }

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
        let apply_event = rsm_event::ApplyEvent {
            node_id: self.node_id,
            group_id,
            replica_id: apply.replica_id,
            commit_index: curr_commit_index,
            commit_term: curr_commit_term,
            leader_id: 0, // TODO: get leader id from raft group
            applys,
        };
        self.rsm.apply(apply_event).await;
        // gs.set_applied(last_index, last_term).unwrap();
        state.applied_index = last_index;
        state.applied_term = last_term;
    }

    pub(super) async fn handle_applys(
        &mut self,
        group_id: u64,
        replica_id: u64,
        applys: Vec<ApplyData<R>>,
        apply_state: &mut LocalApplyState,
    ) {
        for apply in applys {
            self.handle_apply(apply, apply_state).await;
        }
    }
}

/// Parse out ConfChangeV2 and MembershipChangeData from entry.
/// Return Error if serialization error.
fn parse_conf_change(
    ent: &Entry,
) -> Result<(ConfChangeV2, Option<MembershipRequestContext>), Error> {
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
            let ccv2 = ConfChangeV2::decode(ent.data.as_ref())
                .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?;

            tracing::info!("v2 is leaved {:?}", ccv2);
            // 这种情况下, 如果 transition 是 auto leave 并且是一个空的变更, 说明正在进行 raft joint consensus
            // 该 entry 是由 raft-rs 构造的, 当该 entry 提交到 raft-rs 之后，变更会 auto leave 联合共识，即整个
            // 变更结束。
            if ccv2.get_transition() == ConfChangeTransition::Auto && ccv2.changes.is_empty() {
                return Ok((ccv2, None));
            }

            Ok((
                ccv2,
                // TODO: use flexbuffer
                // MembershipChangeData::decode(ent.context.as_ref())
                //     .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?,
                Some(flexbuffer_deserialize(&ent.context)?),
            ))
        }
    }
}

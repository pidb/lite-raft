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
use crate::msg::ApplyConfChange;
use crate::msg::ApplyData;
use crate::msg::ApplyDataMeta;
use crate::msg::ApplyResultMessage;
use crate::msg::ConfChangeContext;
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
use crate::GroupStates;
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

    fn pop_stales(&mut self, index: u64, term: u64) -> Vec<PendingSender<RES>> {
        let mut stales = vec![];
        while let Some(p) = self.pop_normal(index, term) {
            stales.push(p);
        }
        stales
    }
}

pub struct Delegate<W, R, RSM>
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

impl<W, R, RSM> Delegate<W, R, RSM>
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

    pub(super) async fn handle_applys(
        &mut self,
        applys: Vec<ApplyData<R>>,
        apply_state: &mut LocalApplyState,
    ) {
        for apply in applys {
            self.handle_apply(apply, apply_state).await;
        }
    }

    async fn handle_apply(&mut self, mut apply: ApplyData<R>, state: &mut LocalApplyState) {
        let group_id = apply.meta.group_id;
        let (prev_applied_index, prev_applied_term) = (state.applied_index, state.applied_term);
        let (curr_commit_index, curr_commit_term) =
            (apply.meta.commit_index, apply.meta.commit_term);

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
                EntryType::EntryNormal => self.handle_normal(&apply.meta, ent),
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    self.handle_conf_change(&apply.meta, ent).await
                }
            };

            if let Some(apply) = apply {
                applys.push(apply)
            }
        }

        // Since we feed the state machine probably a batch of entry logs, represented by IntoIter,
        // processing applied can be divided into the following scenarios:
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
            replica_id: apply.meta.replica_id,
            commit_index: curr_commit_index,
            commit_term: curr_commit_term,
            leader_id: apply.meta.leader_id,
            applys,
        };
        self.rsm.apply(apply_event).await;
        // gs.set_applied(last_index, last_term).unwrap();
        state.applied_index = last_index;
        state.applied_term = last_term;
    }

    fn handle_normal(
        &mut self,
        meta: &ApplyDataMeta,
        ent: Entry,
    ) -> Option<rsm_event::Apply<W, R>> {
        let index = ent.index;
        let term = ent.term;
        if ent.data.is_empty() {
            // When the new leader online, a no-op log will be send and commit.
            // we will skip this log for the application and set index and term after
            // apply.
            info!(
                "node {}: group = {} skip no-op entry index = {}, term = {}",
                self.node_id, meta.group_id, index, term
            );

            // If there are still pending proposals, these should be removed because a
            // new leader elected.
            let stales = self.pending_senders.pop_stales(index, term);
            for p in stales {
                p.tx.map(|tx| tx.send(Err(Error::Propose(ProposeError::Stale(p.term, meta.term)))));
            }
            return Some(rsm_event::Apply::NoOp(rsm_event::ApplyNoOp {
                group_id: meta.group_id,
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
        let data = flexbuffer_deserialize(&ent.data).unwrap();

        Some(rsm_event::Apply::Normal(rsm_event::ApplyNormal {
            group_id: meta.group_id,
            is_conf_change: false,
            index,
            term,
            data,
            context: if ent.context.is_empty() {
                None
            } else {
                Some(ent.context)
            },
            tx,
        }))
    }

    #[tracing::instrument(
        name = "ApplyActor::handle_conf_change",
        skip_all,
        fields(
            group_id = meta.group_id,
            index = ent.index,
            term = ent.term,
            entry_type = ?ent.entry_type(),
        )
    )]
    async fn handle_conf_change(
        &mut self,
        meta: &ApplyDataMeta,
        ent: Entry,
    ) -> Option<rsm_event::Apply<W, R>> {
        let index = ent.index;
        let term = ent.term;
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

        // apply the conf changes of apply to oceanraft and raft group first.
        // It doesn't matter if the user state machine then fails to apply,
        // because we set the applied index based on the index successfully
        // applied by the user and then promote the raft based on that applied index,
        // so the user can apply the log later. For oceanraft and raft,
        // we make the commit an idempotent operation (TODO).
        let conf_state = match self
            .apply_conf_change_to_group(ApplyConfChange {
                group_id: meta.group_id,
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
            group_id: meta.group_id,
            index,
            term,
            conf_state,
            change_data: change_request,
            ctx: user_ctx,
            tx,
        }))
    }

    /// apply conf change to specific raft group.
    #[tracing::instrument(name = "ApplyActor::apply_conf_change_to_group", skip_all)]
    async fn apply_conf_change_to_group(
        &self,
        commit: ApplyConfChange,
    ) -> Result<ConfState, Error> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self
            .node_msg_tx
            .send(NodeMessage::ApplyResult(
                ApplyResultMessage::ApplyConfChange((commit, tx)),
            ))
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
}

/// Parse out ConfChangeV2 and MembershipChangeData from entry.
/// Return Error if serialization error.
fn parse_conf_change(ent: &Entry) -> Result<(ConfChangeV2, Option<ConfChangeContext>), Error> {
    match ent.entry_type() {
        EntryType::EntryNormal => unreachable!(),
        EntryType::EntryConfChange => {
            let conf_change = ConfChange::decode(ent.data.as_ref())
                .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?;

            let ctx = flexbuffer_deserialize(&ent.context)?;
            Ok((conf_change.into_v2(), ctx))
        }
        EntryType::EntryConfChangeV2 => {
            let ccv2 = ConfChangeV2::decode(ent.data.as_ref())
                .map_err(|err| Error::Deserialization(DeserializationError::Prost(err)))?;

            // In this case, if the transition is auto leave and an empty change, there are two cases
            // 1. auto is used in the previous joint consensus, and this entry constructed by raft-rs
            // is used to leave joint.
            // 2. The previous joint consensus uses explicit, and then manually constructs an entry for
            // the leave joint.
            if ccv2.leave_joint() {
                return Ok((ccv2, None));
            }

            Ok((ccv2, Some(flexbuffer_deserialize(&ent.context)?)))
        }
    }
}
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::error;
use tracing::info;
use tracing::Span;

use crate::msg::NodeMessage;
use crate::utils::mpsc;
// use crate::Apply;
// use crate::ApplyMembership;
// use crate::ApplyNoOp;
// use crate::ApplyNormal;
use crate::rsm::StateMachine;
use crate::Config;
use crate::GroupStates;
use crate::ProposeRequest;
use crate::ProposeResponse;

use crate::apply::delegate::Delegate;
use crate::msg::ApplyData;
use crate::msg::ApplyMessage;
use crate::msg::ApplyResultRequest;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

#[derive(Debug, Default)]
pub(super) struct LocalApplyState {
    pub applied_term: u64,
    pub applied_index: u64,
}

pub struct ApplyActor;

impl ApplyActor {
    pub(crate) fn spawn<W, R, RSM, S, MS>(
        cfg: &Config,
        rsm: &Arc<RSM>,
        storage: MS,
        shared_states: GroupStates,
        node_msg_tx: mpsc::WrapSender<NodeMessage<W, R>>,
        request_rx: UnboundedReceiver<(Span, ApplyMessage<R>)>,
        // response_tx: UnboundedSender<ApplyResultRequest>,
        // commit_tx: UnboundedSender<ApplyCommitRequest>,
        stopped: Arc<AtomicBool>,
    ) -> Self
    where
        W: ProposeRequest,
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
            node_msg_tx,
            // response_tx,
            // commit_tx,
        );
        tokio::spawn(async move {
            worker.main_loop(stopped).await;
        });

        Self {}
    }
}

pub struct ApplyWorker<W, R, RSM, S, MS>
where
    W: ProposeRequest,
    R: ProposeResponse,
    RSM: StateMachine<W, R>,
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
{
    node_id: u64,
    cfg: Config,
    rx: UnboundedReceiver<(tracing::span::Span, ApplyMessage<R>)>,
    // tx: UnboundedSender<ApplyResultRequest>,
    node_msg_tx: mpsc::WrapSender<NodeMessage<W, R>>,

    delegate: Delegate<W, R, RSM>,
    local_apply_states: HashMap<u64, LocalApplyState>,
    shared_states: GroupStates,
    storage: MS,
    _m: PhantomData<S>,
}

impl<W, R, RSM, S, MS> ApplyWorker<W, R, RSM, S, MS>
where
    W: ProposeRequest,
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

    async fn main_loop(mut self, stopped: Arc<AtomicBool>) {
        info!("node {}: start apply main_loop", self.node_id);
        let mut pending_msgs = Vec::with_capacity(self.cfg.max_batch_apply_msgs);

        loop {
            if stopped.load(Ordering::SeqCst) {
                break;
            }
            tokio::select! {
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

    async fn handle_msgs(&mut self, msgs: std::vec::Drain<'_, ApplyMessage<R>>) {
        let pending_applys = self.batch_msgs(msgs);
        for ((group_id, replica_id), applys) in pending_applys {
            let apply_state = self
                .local_apply_states
                .entry(group_id)
                .or_insert(LocalApplyState::default());

            let _ = self.delegate.handle_applys(applys, apply_state).await;

            let res = ApplyResultRequest {
                group_id,
                applied_index: apply_state.applied_index,
                applied_term: apply_state.applied_term,
            };

            if let Err(_) = self.node_msg_tx.send(NodeMessage::ApplyResult(res)).await {
                error!(
                    "node {}: send response failed, the node actor dropped",
                    self.node_id
                );
            }
        }
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
                                apply.meta.replica_id,
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
                                                apply.meta.replica_id,
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
                    batch_apply.meta.replica_id,
                    batch_apply,
                );
            }
        }

        pending_applys
    }

    fn new(
        cfg: &Config,
        rsm: &Arc<RSM>,
        storage: MS,
        shared_states: GroupStates,
        request_rx: UnboundedReceiver<(Span, ApplyMessage<R>)>,
        node_msg_tx: mpsc::WrapSender<NodeMessage<W, R>>,
        // response_tx: UnboundedSender<ApplyResultRequest>,
        // commit_tx: UnboundedSender<ApplyCommitRequest>,
    ) -> Self {
        Self {
            local_apply_states: HashMap::default(),
            node_id: cfg.node_id,
            cfg: cfg.clone(),
            rx: request_rx,
            node_msg_tx: node_msg_tx.clone(),
            // tx: response_tx,
            shared_states: shared_states.clone(),
            storage,
            delegate: Delegate::new(cfg.node_id, rsm, node_msg_tx),
            _m: PhantomData,
        }
    }
}

#[cfg(test)]
mod test {
    use futures::Future;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::mpsc::unbounded_channel;

    use crate::msg::ApplyDataMeta;
    use crate::multiraft::NO_LEADER;
    use crate::rsm_event;
    use crate::rsm_event::GroupCreateEvent;
    use crate::rsm_event::LeaderElectionEvent;
    use crate::state::GroupState;
    use crate::state::GroupStates;
    use crate::storage::MemStorage;
    use crate::storage::MultiRaftMemoryStorage;
    use crate::utils::compute_entry_size;
    use crate::Config;
    // use crate::multiraft::MultiStateMachine;
    use crate::prelude::Entry;
    use crate::prelude::EntryType;
    use crate::utils::mpsc;
    use crate::StateMachine;

    use super::ApplyData;
    use super::ApplyMessage;
    use super::ApplyWorker;

    struct NoOpStateMachine {}
    impl StateMachine<(), ()> for NoOpStateMachine {
        type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn apply(&self, _: rsm_event::ApplyEvent<(), ()>) -> Self::ApplyFuture<'_> {
            async move {}
        }

        type OnLeaderElectionFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn on_leader_election(&self, _: LeaderElectionEvent) -> Self::OnLeaderElectionFuture<'_> {
            async move {}
        }

        type OnGroupCreateFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn on_group_create(&self, _: GroupCreateEvent) -> Self::OnGroupCreateFuture<'_> {
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
            meta: ApplyDataMeta {
                group_id,
                replica_id,
                leader_id: NO_LEADER,
                term,
                commit_index: entries[entries.len() - 1].index,
                commit_term: entries[entries.len() - 1].term,
                entries_size: entries.iter().map(|ent| compute_entry_size(ent)).sum(),
            },
            proposals: Vec::default(),
            entries,
        }
    }

    fn new_worker(
        batch_apply: bool,
        batch_size: usize,
    ) -> ApplyWorker<(), (), NoOpStateMachine, MemStorage, MultiRaftMemoryStorage> {
        let (_request_tx, request_rx) = unbounded_channel();
        let (response_tx, _response_rx) = mpsc::channel_wrap(-1);
        // let (callback_tx, _callback_rx) = unbounded_channel();
        let cfg = Config {
            batch_apply,
            batch_size,
            ..Default::default()
        };

        let storage = MultiRaftMemoryStorage::new(1);
        let rsm = Arc::new(NoOpStateMachine {});
        let shared_states = GroupStates::new();
        ApplyWorker::new(
            &cfg,
            &rsm,
            storage,
            shared_states,
            request_rx,
            response_tx,
            // callback_tx,
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
                let pending_applys = pending_applys
                    .get(&(expect.group_id, expect.replica_id))
                    .unwrap();
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

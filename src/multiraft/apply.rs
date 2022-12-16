use std::collections::VecDeque;
use std::{collections::HashMap};

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::proto::Entry;

use super::apply_command::ApplyCommand;
use super::proposal::Proposal;

const MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

pub struct ApplyCommandQueue<R> {
    tx: Sender<Vec<ApplyCommand<R>>>,
    rx: Receiver<Vec<ApplyCommand<R>>>,
}

pub struct Apply<R> {
    pub peer_id: u64,
    pub group_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entires: Vec<Entry>,
    pub entries_size: usize,
    pub proposals: VecDeque<Proposal<R>>,
}

impl<R> Apply<R> {
    fn try_batch(&mut self, that: &mut Apply<R>) -> bool {
        assert_eq!(self.peer_id, that.peer_id);
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
        self.entires.append(&mut that.entires);
        self.entries_size += that.entries_size;
        self.proposals.append(&mut that.proposals);
        return true;
    }
}

// #[derive(Default)]
// pub struct GroupApplyRequest<R> {
//     pub term: u64,
//     pub commit_index: u64,
//     pub entries: Vec<raft::prelude::Entry>,
//     pub apply_commands: Vec<ApplyCommand<R>>,
// }

pub enum ApplyTask<R> {
    Apply(Apply<R>),
}

#[derive(Default)]
pub struct ApplyTaskRequest<R> {
    pub groups: HashMap<u64, ApplyTask<R>>,
}

pub struct ApplyTaskResponse {}

pub struct ApplyActorAddress<R> {
    pub tx: Sender<ApplyTaskRequest<R>>,
    pub rx: Receiver<ApplyTaskResponse>,
}

pub struct ApplyActor<R> {
    rx: Receiver<ApplyTaskRequest<R>>,
    tx: Sender<ApplyTaskResponse>,
    group_pending_apply: HashMap<u64, Apply<R>>,
}

impl<R> ApplyActor<R> {
    fn handle_committed_entries(&mut self) {}
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

    fn handle_request(&mut self, task: ApplyTaskRequest<R>) {
        for (group_id, task) in task.groups.into_iter() {
            match task {
                ApplyTask::Apply(apply) => {
                    match self.group_pending_apply.get_mut(&group_id) {
                        Some(batch) => {
                            if batch.try_batch(&mut apply) {
                                continue;
                            }
                            
                            let take_batch = self.group_pending_apply.remove(&group_id).unwrap();  
                            self.handle_apply(take_batch);
                        },
                        None => {
                            self.group_pending_apply.insert(group_id, apply);
                        },
                    };
                },
            }
        }
    }

    fn handle_apply(&mut self, apply: Apply<R>) {
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


pub struct ApplyDelegate<R> {
    ctx: Arc<RaftContext>,
    apply_state: RaftApplyState,
    apply_to_rsm: Option<RSM>,
    pending_proposals: VecDeque<Proposal<RSM>>,
    pending_conf_changes: Option<VecDeque<Proposal<RSM>>>,
    applied_conf_changes: Option<Vec<(Option<ConfChange>, Option<ConfChangeV2>)>>,
}

impl<R> ApplyDelegate<R> {
    fn push_proposals(&mut self, proposal_drainner: Drain<'_, Proposal<RSM>>) {
        for proposal in proposal_drainner {
            // TODO: batch
            if proposal.is_conf_change {
                if let Some(queue) = self.pending_conf_changes.as_mut() {
                    queue.push_back(proposal);
                } else {
                    let mut queue = VecDeque::new();
                    queue.push_back(proposal);
                    self.pending_conf_changes = Some(queue);
                }
            } else {
                self.pending_proposals.push_back(proposal);
            }
        }
    }

    fn pop_normal(&mut self, index: u64, term: u64) -> Option<Proposal<RSM>> {
        self.pending_proposals.pop_front().and_then(|cmd| {
            if self.pending_proposals.capacity() > SHRINK_PENDING_CMD_QUEUE_CAP
                && self.pending_proposals.len() < SHRINK_PENDING_CMD_QUEUE_CAP
            {
                self.pending_proposals.shrink_to_fit();
            }
            if (cmd.term, cmd.index) > (term, index) {
                self.pending_proposals.push_front(cmd);
                return None;
            }
            Some(cmd)
        })
    }

    fn pop_conf_change(&mut self, term: u64, index: u64) -> Option<Proposal<RSM>> {
        let queue = self.pending_conf_changes.as_mut()?;
        let p = queue.pop_front()?;
        if (p.term, p.index) > (term, index) {
            queue.push_front(p);
            return None;
        }
        Some(p)
    }

    fn find_pending(
        &mut self,
        term: u64,
        index: u64,
        is_conf_change: bool,
    ) -> Option<Proposal<RSM>> {
        if is_conf_change {
            while let Some(mut p) = self.pop_conf_change(term, index) {
                if (p.term, p.index) == (term, index) {
                    return Some(p);
                }

                response_stale_proposal(p, term);
                return None;
            }
        }
        while let Some(mut p) = self.pop_normal(index, term) {
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
                response_stale_proposal(p, term);
            }
        }
        return None;
    }

    // fn set_apply_state<'life0>(&'life0 mut self, apply_state: &ApplyState) {
    // }

    fn handle_committed_entries<'life0>(
        &'life0 mut self,
        mut entries: Drain<'life0, Entry>,
    ) -> impl Future<Output = ()> + 'life0 {
        async move {
            while let Some(entry) = entries.next() {
                // applied_index mustbe ensure strict increments.
                // let next_applied_index = self.apply_state.applied_index + 1;
                // if next_applied_index != entry.index {
                //     panic!(
                //         "expect applied {}, but got {}",
                //         next_applied_index, entry.index
                //     );
                // }
                match entry.get_entry_type() {
                    EntryType::EntryNormal => self.handle_committed_normal(entry).await,
                    EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                        self.handle_committed_conf_change(entry).await
                    }
                }
            }
        }
    }

    fn handle_committed_normal<'life0>(
        &'life0 mut self,
        entry: Entry,
    ) -> impl Future<Output = ()> + 'life0 {
        async {
            let entry_index = entry.get_index();
            let entry_term = entry.get_term();

            if entry.data.is_empty() {
                // TODO: detect user propose empty data and if there any pending proposal, we don't
                // apply it and notify client.
                info!(
                    self.ctx.root_logger,
                    "{} skip no-op log index = {}, term = {}", self.ctx.peer_id, entry_index, entry_term
                );
                self.apply_state.applied_term = entry_term;
                self.apply_state.applied_index = entry_index;
                self.response_stale_proposals(entry_index, entry_term);
                return;
            }

            let mut response = None;
            if let Some(rsm) = self.apply_to_rsm.as_mut() {
                let state = self.ctx.shard_state.rl().clone();
                response = Some(rsm.apply(entry.into(), state).await);
            }

            if let Some(p) = self.find_pending(entry_term, entry_index, false) {
                p.tx.map(|tx| match tx {
                    InnerSender::Write { tx } => {
                        if let Err(_error) = tx.send(Ok(response)) {
                            todo!()
                        }
                    }
                    _ => {}
                });
            }

            self.apply_state.applied_index = entry_index;
            self.apply_state.applied_term = entry_term;
        }
    }

    fn handle_committed_conf_change<'life0>(
        &'life0 mut self,
        entry: Entry,
    ) -> impl Future<Output = ()> + 'life0 {
        async move {
            // TODO: empty adta?

            let proposal = self.find_pending(entry.term, entry.index, true);
            match entry.get_entry_type() {
                EntryType::EntryNormal => unreachable!(),
                EntryType::EntryConfChange => {
                    let mut cc = ConfChange::default();
                    cc.merge_from_bytes(&entry.data).unwrap();
                    match cc.get_change_type() {
                        ConfChangeType::AddNode => {
                            // let group_id = self.ctx.group_id;
                            // let peer_id = cc.get_node_id();
                            // if GLOBAL_RAFT_GROUP_MANAGER.has_peer(group_id, peer_id) {
                            //     return;
                            // }
                            // let state = PeerState {
                            //     is_voter: true,
                            //     is_leaner: false,
                            // };
                            // println!("join_peer {}", peer_id);
                            // GLOBAL_RAFT_GROUP_MANAGER.join_peer(self.ctx.group_id, peer_id , state);
                        }
                        ConfChangeType::RemoveNode => {
                            // println!("handle remove nove");
                            // let group_id = self.ctx.group_id;
                            // let peer_id = cc.get_node_id();
                            // if !GLOBAL_RAFT_GROUP_MANAGER.has_peer(group_id, peer_id) {
                            //     return
                            // }

                            // println!("leave_peer {}", peer_id);
                            // GLOBAL_RAFT_GROUP_MANAGER.leave_peer(group_id, peer_id);
                        }
                        ConfChangeType::AddLearnerNode => {
                            unimplemented!()
                        }
                    }

                    if let Some(applied_cc) = self.applied_conf_changes.as_mut() {
                        applied_cc.push((Some(cc), None));
                    } else {
                        let new_queue = vec![(Some(cc), None)];
                        self.applied_conf_changes = Some(new_queue);
                    }

                    // response proposal

                    if let Some(p) = proposal {
                        p.tx.map(|tx| match tx {
                            InnerSender::Membership { tx } => {
                                // println!("response confchange proposal");
                                if let Err(_error) = tx.send(Ok(MembershipChangeResponse {})) {
                                    todo!()
                                }
                            }
                            _ => {}
                        });
                    }
                }
                EntryType::EntryConfChangeV2 => {
                    unimplemented!();
                    let mut cc_v2 = ConfChangeV2::default();
                    cc_v2.merge_from_bytes(&entry.data).unwrap();
                    let changes = cc_v2.get_changes();
                    for change in changes.iter() {}

                    if let Some(applied_cc) = self.applied_conf_changes.as_mut() {
                        applied_cc.push((None, Some(cc_v2)));
                    } else {
                        let new_queue = vec![(None, Some(cc_v2))];
                        self.applied_conf_changes = Some(new_queue);
                    }
                }
            }

            self.apply_state.applied_index = entry.index;
            self.apply_state.applied_term = entry.term;
        }
    }

    fn response_stale_proposals(&mut self, index: u64, term: u64) {
        while let Some(p) = self.pop_normal(index, term) {
            p.tx.map(|tx| tx.response_error(Error::StaleRequest(term)));
        }
    }
}
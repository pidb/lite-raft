use std::collections::HashMap;
use std::collections::VecDeque;
use std::vec::IntoIter;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use prost::Message as ProstMessage;

use crate::proto::Entry;
use crate::proto::EntryType;
use crate::proto::ConfChange;
use crate::proto::ConfChangeType;
use crate::proto::MembershipChangeRequest;

use super::apply_command::ApplyCommand;
use super::error::Error;
use super::error::ProposalError;
use super::proposal::Proposal;

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

/// Apply membership change results. 
/// 
/// If proposed change is ConfChange, the ConfChangeV2 is converted 
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple 
/// requests, otherwise changes contains only one request.
pub struct MembershipChangeResult {
    pub index: u64,
    pub conf_change: raft::prelude::ConfChangeV2,
    pub changes: Vec<MembershipChangeRequest>,
}

pub enum ApplyResult {
    MembershipChange(MembershipChangeResult),
}

#[derive(Default)]
pub struct ApplyTaskRequest {
    pub groups: HashMap<u64, ApplyTask>,
}

pub struct ApplyTaskResponse {
    pub groups: HashMap<u64, Vec<ApplyResult>>,
}

pub struct ApplyActorAddress {
    pub tx: Sender<ApplyTaskRequest>,
    pub rx: Receiver<ApplyTaskResponse>,
}

pub struct ApplyActor {
    rx: Receiver<ApplyTaskRequest>,
    tx: Sender<ApplyTaskResponse>,
    apply_to_tx: Sender<Vec<ApplyCommand>>,
    group_pending_apply: HashMap<u64, Apply>,
}

impl ApplyActor {
    fn handle_committed_entries(&mut self) {}
}

impl ApplyActor {
    pub fn spawn(apply_to_tx: Sender<Vec<ApplyCommand>>, mut stop_rx: watch::Receiver<bool>) -> (JoinHandle<()>, ApplyActorAddress) {
        let (request_tx, request_rx) = channel(1);
        let (response_tx, response_rx) = channel(1);

        let address = ApplyActorAddress {
            tx: request_tx,
            rx: response_rx,
        };

        let actor = ApplyActor {
            apply_to_tx,
            rx: request_rx,
            tx: response_tx,
            group_pending_apply: HashMap::new(),
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

    async fn handle_request(&mut self, task: ApplyTaskRequest) {
        for (group_id, task) in task.groups.into_iter() {
            match task {
                ApplyTask::Apply(mut apply) => {
                    match self.group_pending_apply.get_mut(&group_id) {
                        Some(batch) => {
                            if batch.try_batch(&mut apply) {
                                continue;
                            }

                            let take_batch = self.group_pending_apply.remove(&group_id).unwrap();
                            self.handle_apply(take_batch).await;
                        }
                        None => {
                            self.group_pending_apply.insert(group_id, apply);
                        }
                    };
                }
            }
        }
    }

    async fn handle_apply(&mut self, apply: Apply) {
        let mut proposals = VecDeque::new();
        for proposal in apply.proposals {
            proposals.push_back(proposal)
        }

        let mut delegate = ApplyDelegate {
            group_id: apply.group_id,
            pending_proposals: proposals,
            staging_applys: Vec::new(),
        };

        delegate.handle_committed_entries(apply.entries);
        self.apply_to_tx.send(delegate.staging_applys).await;
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
    pending_proposals: VecDeque<Proposal>,
    staging_applys: Vec<ApplyCommand>,
}

impl ApplyDelegate {
    fn push_proposals(&mut self, proposals: IntoIter<Proposal>) {
        for proposal in proposals {
            self.pending_proposals.push_back(proposal);
        }
    }

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

        // async move {
        //     while let Some(entry) = entries.next() {
        //         // applied_index mustbe ensure strict increments.
        //         // let next_applied_index = self.apply_state.applied_index + 1;
        //         // if next_applied_index != entry.index {
        //         //     panic!(
        //         //         "expect applied {}, but got {}",
        //         //         next_applied_index, entry.index
        //         //     );
        //         // }
        //         match entry.get_entry_type() {
        //             EntryType::EntryNormal => self.handle_committed_normal(entry).await,
        //             EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
        //                 self.handle_committed_conf_change(entry).await
        //             }
        //         }
        //     }
        // }
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
        let tx = self.find_pending(entry.term, entry.index).map_or(None, |p| p.tx);

        let apply_command = ApplyCommand {
            group_id: self.group_id,
            is_conf_change: false,
            entry,
            tx,
        };
        self.staging_applys.push(apply_command);
    }

    fn handle_committed_conf_change(&mut self, entry: Entry) {
        // TODO: empty adta?

        let proposal = self.find_pending(entry.term, entry.index);
        match entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut cc = ConfChange::default();
                cc.merge(entry.data.as_ref()).unwrap();
                match cc.change_type() {
                    ConfChangeType::AddNode => {},
                    ConfChangeType::RemoveNode => {},
                    ConfChangeType::AddLearnerNode => {},
                }
            }
            EntryType::EntryConfChangeV2 => {
                unimplemented!();
            }
        }

        let tx = if let Some(proposal) = proposal {proposal.tx} else { None};

        let apply_command = ApplyCommand {
            group_id: self.group_id,
            is_conf_change: true,
            entry,
            tx
        };
        self.staging_applys.push(apply_command);
        // self.staging_entries.push(entry);

        // self.apply_state.applied_index = entry.index;
        // self.apply_state.applied_term = entry.term;
    }

    fn response_stale_proposals(&mut self, index: u64, term: u64) {
        while let Some(p) = self.pop_normal(index, term) {
            p.tx.map(|tx| tx.send(Err(Error::Proposal(ProposalError::Stale(p.term)))));
        }
    }
}

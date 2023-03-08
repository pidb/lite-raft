// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// We use `default` method a lot to be support prost and rust-protobuf at the
// same time. And reassignment can be optimized by compiler.
#![allow(clippy::field_reassign_with_default)]

mod confchange;
mod confstate;

pub use crate::confchange::{
    new_conf_change_single, parse_conf_change, stringify_conf_change, ConfChangeI,
};
pub use crate::confstate::conf_state_eq;
pub use crate::protos::eraftpb;
pub use crate::protos::multiraft;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
    include!(concat!(env!("OUT_DIR"), "/protos/multiraft.rs"));

    use self::eraftpb::Snapshot;

    impl Snapshot {
        /// For a given snapshot, determine if it's empty or not.
        pub fn is_empty(&self) -> bool {
            self.get_metadata().index == 0
        }
    }
}

pub mod prelude {
    pub use crate::eraftpb::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, EntryType, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };

    pub use crate::multiraft::{
        AdminMessage, AdminMessageType,
        MembershipChangeData, RaftGroupDesc, RaftGroupManagement,
        RaftGroupManagementType, MultiRaftMessage, MultiRaftMessageResponse, ReplicaDesc,
        SingleMembershipChange,
    };
}

pub mod util {
    use crate::eraftpb::ConfState;

    impl<Iter1, Iter2> From<(Iter1, Iter2)> for ConfState
    where
        Iter1: IntoIterator<Item = u64>,
        Iter2: IntoIterator<Item = u64>,
    {
        fn from((voters, learners): (Iter1, Iter2)) -> Self {
            let mut conf_state = ConfState::default();
            conf_state.mut_voters().extend(voters.into_iter());
            conf_state.mut_learners().extend(learners.into_iter());
            conf_state
        }
    }
}

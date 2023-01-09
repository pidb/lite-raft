#![feature(type_alias_impl_trait)]

pub mod memstore;
pub mod multiraft;


// pub use multiraft::MultiRaft;
// pub use multiraft::MultiRaftConfig;
// pub use multiraft::MultiRaftMessageSender;

pub mod prelude {
    pub use raft_proto::prelude::*;
}
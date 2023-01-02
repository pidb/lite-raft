#![feature(type_alias_impl_trait)]

pub mod proto;

pub mod util;
pub mod rsm;
pub mod storage;
pub mod multiraft;

pub use multiraft::MultiRaft;
pub use multiraft::MultiRaftConfig;
pub use multiraft::MultiRaftMessageSender;
pub use multiraft::LocalTransport;
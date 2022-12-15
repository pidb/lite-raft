use std::fmt::Debug;

use tokio::sync::oneshot;

use crate::proto::Entry;

pub struct ApplyCommand<R> {
    pub group_id: u64,
    pub entry: Entry,
    pub tx: oneshot::Sender<R>,
}

use std::fmt::Debug;

use tokio::sync::oneshot;

use crate::proto::Entry;

use super::error::Error;

pub struct ApplyCommand {
    pub group_id: u64,
    pub entry: Entry,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

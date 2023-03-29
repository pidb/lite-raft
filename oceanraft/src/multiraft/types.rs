use std::fmt::Debug;

use serde::Deserialize;
use serde::Serialize;

pub trait WriteData:
    Default + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
    // type EncodeError: Error + Send + Sync;
    // fn encode(&self) -> Result<Vec<u8>, Self::EncodeError>;
}

impl<T> WriteData for T where
    T: Default + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
}

/// Only use for tests
#[derive(thiserror::Error, Debug)]
#[allow(unused)]
pub(crate) enum EmptyWriteDataError {
    #[error("encode")]
    Encode,
}

pub trait WriteResponse: Debug + Clone + Default + Send + Sync + 'static {}

impl<R> WriteResponse for R where R: Debug  + Clone + Default + Send + Sync + 'static {}

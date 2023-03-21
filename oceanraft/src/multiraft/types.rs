use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait WriteData:
    Default + Clone + Send + Sync + Serialize + DeserializeOwned + 'static
{
    // type EncodeError: Error + Send + Sync;
    // fn encode(&self) -> Result<Vec<u8>, Self::EncodeError>;
}

impl<T> WriteData for T where
    T: Default + Clone + Send + Sync + Serialize + DeserializeOwned + 'static
{
}

/// Only use for tests
#[derive(thiserror::Error, Debug)]
#[allow(unused)]
pub(crate) enum EmptyWriteDataError {
    #[error("encode")]
    Encode,
}

pub trait WriteResponse: Debug + Clone + Send + Sync + 'static {}

impl<R> WriteResponse for R where R: Debug + Clone + Send + Sync + 'static {}

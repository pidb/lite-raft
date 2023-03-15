use std::error::Error;
use std::fmt::Debug;

pub trait WriteData: Clone + Send + Sync + 'static {
    type EncodeError: Error + Send + Sync;
    fn encode(&self) -> Result<Vec<u8>, Self::EncodeError>;
}

/// Only use for tests
#[derive(thiserror::Error, Debug)]
#[allow(unused)]
pub(crate) enum EmptyWriteDataError {
    #[error("encode")]
    Encode,
}

/// Only use for tests
#[derive(Clone)]
pub(crate) struct EmptyWriteData;

impl WriteData for EmptyWriteData {
    type EncodeError = EmptyWriteDataError;
    fn encode(&self) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(vec![])
    }
}

pub trait WriteResponse: Debug + Clone + Send + Sync + 'static {}

impl<R> WriteResponse for R where R: Debug + Clone + Send + Sync + 'static {}

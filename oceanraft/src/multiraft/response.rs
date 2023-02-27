use std::fmt::Debug;

pub trait AppWriteResponse: Debug + Clone + Send + Sync + 'static {}

impl<R> AppWriteResponse for R where
    R: Debug + Clone + Send + Sync + 'static
{
}
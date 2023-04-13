use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::prelude::multi_raft_service_server::MultiRaftService;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::MultiRaftMessageSender;
use crate::MultiRaftMessageSenderImpl;

pub use crate::prelude::multi_raft_service_client::MultiRaftServiceClient;
pub use crate::prelude::multi_raft_service_server::MultiRaftServiceServer;

/// Implementing `MultiRaftService` defined in protobuf,
/// users can add it to the service of their gRPC server.
pub struct MultiRaftServiceImpl {
    forward: MultiRaftMessageSenderImpl,
}

impl MultiRaftServiceImpl {
    /// Create a new implementation of `MultiRaftService` that
    /// takes a `MultiRaftSenderImpl` parameter to forward requests
    /// received by the server to the main thread of the Node.
    #[allow(unused)]
    pub fn new(forward: MultiRaftMessageSenderImpl) -> Self {
        Self { forward }
    }
}

#[tonic::async_trait]
impl MultiRaftService for MultiRaftServiceImpl {
    async fn send(
        &self,
        request: Request<MultiRaftMessage>,
    ) -> Result<Response<MultiRaftMessageResponse>, Status> {
        let msg = request.into_inner();
        // FIXME: handle error
        let message = self.forward.send(msg).await.unwrap();
        Ok(Response::new(message))
    }
}

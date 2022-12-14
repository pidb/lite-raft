use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Weak;

use futures::Future;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use super::error::Error;

use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum TransportError {
    #[error("the node {0} of server not found")]
    ServerNodeFound(u64),

    #[error("server error: {0}")]
    Server(String),
}

pub trait MessageInterface: Send + Sync + 'static {
    type RaftMessageFuture<'life0>: Future<Output = Result<RaftMessageResponse, Error>> + Send
    where
        Self: 'life0;

    fn raft_message<'life0>(&'life0 self, msg: RaftMessage) -> Self::RaftMessageFuture<'life0>;
}

pub trait Transport<M>: Send + Sync + 'static
where
    M: MessageInterface,
{
    type ListenFuture<'life0>: Future<Output = Result<(), Error>>
    where
        Self: 'life0;

    fn listen<'life0>(
        &'life0 self,
        store_id: u64,
        addr: &'life0 str,
        msg_impl: M,
    ) -> Self::ListenFuture<'life0>;

    fn send(&self, msg: RaftMessage) -> Result<(), Error>;

    // fn stop(store_id: u64);

    // fn close();
}

struct LocalServer<M: MessageInterface> {
    addr: String,
    // rx: Receiver<RaftMessage>,
    // msg_impl: M,
    tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    _m1: PhantomData<M>,
}

impl<M: MessageInterface> LocalServer<M> {
    /// Spawn a server to accepct request.
    fn spawn(
        msg_impl: M,
        mut rx: Receiver<(
            RaftMessage,
            oneshot::Sender<Result<RaftMessageResponse, Error>>,
        )>,
        mut stop: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let main_loop = async move {
            loop {
                tokio::select! {
                    Some((msg, tx)) = rx.recv() => {
                        let res = msg_impl.raft_message(msg).await;
                        tx.send(res);
                    },
                    Ok(_) = stop.changed() => {
                        if *stop.borrow() {
                            break
                        }
                    },
                }
            }
        };

        tokio::spawn(main_loop)
    }

    /// Connect self from client.
    async fn connect(&self, _from: u64) -> Result<LocalClient, Error> {
        Ok(LocalClient {
            tx: self.tx.clone(),
        })
    }
}

struct LocalClient {
    tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
}

pub struct LocalTransport<M: MessageInterface> {
    stop: watch::Sender<bool>,
    servers: RwLock<HashMap<u64, LocalServer<M>>>,
}

impl<M: MessageInterface> LocalTransport<M> {
    /// Connect server by givn id
    async fn connect(&self, client_id: u64, node_id: u64) -> Result<LocalClient, Error> {
        let rl = self.servers.read().await;
        if !rl.contains_key(&node_id) {
            return Err(Error::Transport(TransportError::ServerNodeFound(node_id)));
        }

        let local_server = rl.get(&node_id).unwrap();
        local_server.connect(client_id).await
    }
}

impl<M: MessageInterface> Transport<M> for LocalTransport<M> {
    type ListenFuture<'life0> = impl Future<Output = Result<(), Error>>
    where
        Self: 'life0;

    fn listen<'life0>(
        &'life0 self,
        id: u64,
        addr: &'life0 str,
        msg_impl: M,
    ) -> Self::ListenFuture<'life0> {
        async move {
            // check exists
            {
                let rl = self.servers.write().await;
                if rl.contains_key(&id) {
                    panic!("the server ({}) alread listen at {}", id, addr);
                }
            }

            // create server
            let (tx, rx) = channel(128);
            let local_server = LocalServer {
                addr: addr.clone().to_string(),
                tx,
                _m1: PhantomData,
            };

            // spawn server to accepct request
            let _ = LocalServer::spawn(msg_impl, rx, self.stop.subscribe());

            let mut wl = self.servers.write().await;
            wl.insert(id, local_server);
            Ok(())
        }
    }
    
    fn send(&self, msg: RaftMessage) -> Result<(), Error> {
        // get from and to node id
        let from_replica = match msg.from_replica.as_ref() {
            None => return Err(Error::BadParameter(format!("to from replica is none"))),
            Some(rm) => rm.node_id,
        };
        let to_replica = match msg.to_replica.as_ref() {
            None => return Err(Error::BadParameter(format!("to to replica is none"))),
            Some(rm) => rm.node_id,
        };

        let send_fn = async move {
            // get client
            let local_client = self.connect(from_replica, to_replica).await?;

            // send reqeust
            let (tx, rx) = oneshot::channel();
            if let Err(_) = local_client.tx.send((msg, tx)).await {}

            // and receive response
            if let Ok(res) = rx.await {
                res
            } else {
                Err(Error::Transport(TransportError::Server(format!(
                    "server ({}) stopped",
                    to_replica
                ))))
            }
        };
        tokio::spawn(send_fn);
        Ok(())
    }
}

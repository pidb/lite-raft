use std::collections::hash_map::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use tracing::info;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use futures::Future;

use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;

use super::error::Error;
use super::error::TransportError;
use super::transport::MessageInterface;
use super::transport::Transport;

struct LocalServer<M: MessageInterface> {
    tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    stop_tx: watch::Sender<bool>,
    _m1: PhantomData<M>,
}

impl<M: MessageInterface> LocalServer<M> {
    /// Spawn a server to accepct request.
    #[tracing::instrument(name = "LocalServer::spawn", skip(rx, msg_impl, stop))]
    fn spawn(
        node_id: u64,
        addr: &str,
        msg_impl: M,
        mut rx: Receiver<(
            RaftMessage,
            oneshot::Sender<Result<RaftMessageResponse, Error>>,
        )>,
        mut stop: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let addr = addr.to_string().clone();
        let main_loop = async move {
            info!("the node ({}) of server listen at {}", node_id, addr);
            loop {
                tokio::select! {
                    Some((msg, tx)) = rx.recv() => {
                        let res = msg_impl.raft_message(msg).await;
                        tx.send(res).unwrap();
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
}

pub struct LocalTransport<M: MessageInterface> {
    servers: Arc<RwLock<HashMap<u64, LocalServer<M>>>>,
}

impl<M: MessageInterface> LocalTransport<M> {
    pub fn new() -> Self {
        Self {
            servers: Default::default(),
        }
    }
}

impl<M: MessageInterface> Transport<M> for LocalTransport<M> {
    type ListenFuture<'life0> = impl Future<Output = Result<(), Error>> + 'life0
    where
        Self: 'life0;

    #[tracing::instrument(name = "LocalTransport::listen", skip(self, msg_impl))]
    fn listen<'life0>(
        &'life0 self,
        node_id: u64,
        addr: &'life0 str,
        msg_impl: M,
    ) -> Self::ListenFuture<'life0> {
        async move {
            // check exists
            {
                let rl = self.servers.write().await;
                if rl.contains_key(&node_id) {
                    return Err(Error::Transport(TransportError::ServerAlreadyExists(
                        node_id,
                    )));
                }
            }

            // create server
            let (stop_tx, stop_rx) = watch::channel(false);
            let (tx, rx) = channel(1);
            let local_server = LocalServer {
                tx,
                stop_tx,
                _m1: PhantomData,
            };

            let mut wl = self.servers.write().await;
            wl.insert(node_id, local_server);

            // spawn server to accepct request
            let _ = LocalServer::spawn(node_id, addr, msg_impl, rx, stop_rx);

            Ok(())
        }
    }

    #[tracing::instrument(name = "LocalTransport::send", skip(self, msg))]
    fn send(&self, msg: RaftMessage) -> Result<(), Error> {
        let (_from_node, to_node) = (msg.from_node, msg.to_node);

        let servers = self.servers.clone();

        // get client
        let send_fn = async move {
            // get server by to
            let rl = servers.read().await;
            if !rl.contains_key(&to_node) {
                return Err(Error::Transport(TransportError::ServerNodeFound(to_node)));
            }

            let (tx, rx) = oneshot::channel();
            // send reqeust
            let local_server = rl.get(&to_node).unwrap();
            local_server.tx.send((msg, tx)).await.unwrap();

            // and receive response
            if let Ok(res) = rx.await {
                res
            } else {
                Err(Error::Transport(TransportError::Server(format!(
                    "server ({}) stopped",
                    to_node
                ))))
            }
        };
        tokio::spawn(send_fn);
        Ok(())
    }

    type StopFuture<'life0> = impl Future<Output = Result<(), Error>> + 'life0
    where
        Self: 'life0;
    #[tracing::instrument(name = "LocalTransport::stop", skip(self))]
    fn stop(&self, node_id: u64) -> Self::StopFuture<'_> {
        async move {
            let mut wl = self.servers.write().await;
            if let Some(node) = wl.remove(&node_id) {
                let _ = node.stop_tx.send(true);
            }

            Ok(())
        }
    }
}

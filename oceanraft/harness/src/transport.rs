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

use oceanraft::prelude::RaftMessage;
use oceanraft::prelude::RaftMessageResponse;
use oceanraft::multiraft::transport::RaftMessageDispatcher;
use oceanraft::multiraft::transport::Transport;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::TransportError;

struct LocalServer<M: RaftMessageDispatcher> {
    tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    stop_tx: watch::Sender<bool>,
    _m1: PhantomData<M>,
}

impl<RD: RaftMessageDispatcher> LocalServer<RD> {
    /// Spawn a server to accepct request.
    #[tracing::instrument(name = "LocalServer::spawn", skip(rx, dispatcher, stop))]
    fn spawn(
        node_id: u64,
        addr: &str,
        dispatcher: RD,
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
                        let res = dispatcher.dispatch(msg).await;
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

pub struct LocalTransport<M: RaftMessageDispatcher> {
    servers: Arc<RwLock<HashMap<u64, LocalServer<M>>>>,
}

impl<M: RaftMessageDispatcher> LocalTransport<M> {
    pub fn new() -> Self {
        Self {
            servers: Default::default(),
        }
    }
}

impl<RD: RaftMessageDispatcher> LocalTransport<RD> {
    #[tracing::instrument(name = "LocalTransport::listen", skip(self, dispatcher))]
    pub async fn listen<'life0>(
        &'life0 self,
        node_id: u64,
        addr: &'life0 str,
        dispatcher: RD,
    ) -> Result<(), Error> {
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
        let _ = LocalServer::spawn(node_id, addr, dispatcher, rx, stop_rx);

        Ok(())
    }

    #[tracing::instrument(name = "LocalTransport::stop", skip(self))]
    pub async fn stop(&self, node_id: u64) -> Result<(), Error> {
        let mut wl = self.servers.write().await;
        if let Some(node) = wl.remove(&node_id) {
            let _ = node.stop_tx.send(true);
        }

        Ok(())
    }
}

impl<RD> Transport for LocalTransport<RD>
where
    RD: RaftMessageDispatcher,
{
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
}

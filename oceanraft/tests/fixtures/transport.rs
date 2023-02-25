use std::collections::hash_map::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use oceanraft::multiraft::transport::Transport;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::MultiRaftMessageSender;
use oceanraft::multiraft::TransportError;
use oceanraft::prelude::MultiRaftMessage;
use oceanraft::prelude::MultiRaftMessageResponse;
use oceanraft::util::TaskGroup;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

struct LocalServer<M: MultiRaftMessageSender> {
    tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    stopped: bool,
    _m1: PhantomData<M>,
}

impl<RD: MultiRaftMessageSender> LocalServer<RD> {
    /// Spawn a server to accepct request.
    #[tracing::instrument(name = "LocalServer::spawn", skip(rx, dispatcher, task_group))]
    fn spawn(
        node_id: u64,
        addr: &str,
        dispatcher: RD,
        mut rx: Receiver<(
            MultiRaftMessage,
            oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
        )>,
        task_group: &TaskGroup,
    ) -> JoinHandle<()> {
        let addr = addr.to_string().clone();
        let mut stopper = task_group.stopper();
        let main_loop = async move {
            info!("node {}: start server listen at {}", node_id, addr);
            loop {
                tokio::select! {
                    Some((msg, tx)) = rx.recv() => {
                        let res = dispatcher.send(msg).await;
                        tx.send(res).unwrap(); // FIXME: handle error
                    },
                    _ = &mut stopper => {

                        break
                    },
                }
            }
        };

        task_group.spawn(main_loop)
    }
}

#[derive(Clone)]
pub struct LocalTransport<M: MultiRaftMessageSender> {
    task_group: TaskGroup,
    servers: Arc<RwLock<HashMap<u64, LocalServer<M>>>>,
    disconnected: Arc<RwLock<HashMap<u64, Vec<u64>>>>,
}

impl<M: MultiRaftMessageSender> LocalTransport<M> {
    pub fn new() -> Self {
        Self {
            task_group: TaskGroup::new(),
            servers: Default::default(),
            disconnected: Default::default(),
        }
    }
}

impl<RD: MultiRaftMessageSender> LocalTransport<RD> {
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

        let (tx, rx) = channel(1);
        let local_server = LocalServer {
            tx,
            stopped: false,
            _m1: PhantomData,
        };

        let mut wl = self.servers.write().await;
        wl.insert(node_id, local_server);

        // spawn server to accepct request
        let _ = LocalServer::spawn(node_id, addr, dispatcher, rx, &self.task_group);

        Ok(())
    }

    pub async fn is_disconnected(
        disconnected: &Arc<RwLock<HashMap<u64, Vec<u64>>>>,
        from: u64,
        to: u64,
    ) -> bool {
        match disconnected.read().await.get(&from) {
            Some(dis) => dis.contains(&to),
            None => false,
        }
    }

    pub async fn disconnect(&self, from: u64, to: u64) {
        self.disconnect_inner(from, to).await;
        self.disconnect_inner(to, from).await;
    }

    async fn disconnect_inner(&self, from: u64, to: u64) {
        let mut wl = self.disconnected.write().await;
        match wl.get_mut(&from) {
            Some(dis) => {
                if !dis.contains(&to) {
                    debug!("server {} -> {} disconnected", from, to);
                    dis.push(to);
                }
            }
            None => {
                debug!("server {} -> {} disconnected", from, to);
                wl.insert(from, vec![to]);
            }
        };
    }

    pub async fn reconnect(&self, from: u64, to: u64) {
        self.reconnect_inner(from, to).await;
        self.reconnect_inner(to, from).await;
    }

    async fn reconnect_inner(&self, from: u64, to: u64) {
        let mut wl = self.disconnected.write().await;
        match wl.get_mut(&from) {
            Some(dis) => {
                if let Some(pos) = dis.iter().position(|node| *node == to) {
                    debug!("server {} -> {} reconnected", from, to);
                    dis.remove(pos);
                }
            }
            None => {}
        };
    }

    #[tracing::instrument(name = "LocalTransport::stop_all", skip(self))]
    pub async fn stop_all(&self) -> Result<(), Error> {
        let mut wl = self.servers.write().await;
        for (_, server) in wl.iter_mut() {
            server.stopped = true
        }
        self.task_group.stop();
        self.task_group.joinner().await;
        Ok(())
    }
}

impl<RD> Transport for LocalTransport<RD>
where
    RD: MultiRaftMessageSender,
{
    fn send(&self, msg: MultiRaftMessage) -> Result<(), Error> {
        let (from_node, to_node) = (msg.from_node, msg.to_node);
        let (from_rep, to_rep) = (msg.msg.as_ref().unwrap().from, msg.msg.as_ref().unwrap().to);
        debug!(
            "node {}: group = {}, send {:?} to {} and forward replica {} -> {}",
            from_node,
            msg.group_id,
            msg.get_msg().msg_type(),
            to_node,
            from_rep,
            to_rep,
        );
        let servers = self.servers.clone();
        let disconnected = self.disconnected.clone();
        // get client
        let send_fn = async move {
            if LocalTransport::<RD>::is_disconnected(&disconnected, from_node, to_node).await {
                error!(
                    "discard {} -> {} {:?}, because  disconnected",
                    from_node,
                    to_node,
                    msg.get_msg().msg_type(),
                );
                return Err(Error::Transport(TransportError::Server(format!(
                    "server {} disconnected",
                    to_node
                ))));
            }

            // get server by to
            let rl = servers.read().await;
            if !rl.contains_key(&to_node) {
                return Err(Error::Transport(TransportError::ServerNodeFound(to_node)));
            }

            // send reqeust
            let to_server = rl.get(&to_node).unwrap();
            if to_server.stopped {
                // FIXME: should return some error
                return Ok(MultiRaftMessageResponse {});
            }

            let (tx, rx) = oneshot::channel();
            if let Err(_) = to_server.tx.send((msg, tx)).await {
                warn!(
                    "node {}: send msg failed, the {} node server stopped",
                    from_node, to_node
                );
                return Ok(MultiRaftMessageResponse {}); // FIXME: should return error
            }

            // and receive response
            if let Ok(res) = rx.await {
                // info!("recv response ok()");
                res
            } else {
                warn!("node {}: receive response failed, the {} node server stopped or discard the request", from_node, to_node);
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

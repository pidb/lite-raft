use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::Future;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::info;

use super::error::Error;
use super::multiraft::Node;
use super::multiraft::RaftGroup;
use super::node::NodeManager;

use crate::proto::Message;
use crate::proto::MessageType;
use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

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

pub async fn send_messages<MI, TR, RS, MRS>(
    from_node_id: u64,
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msgs: Vec<Message>,
) where
    MI: MessageInterface,
    TR: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    for msg in msgs {
        match msg.msg_type() {
            MessageType::MsgHeartbeat => {
                info!(
                    "node {} drop indvidual heartbeat message to replica {}",
                    from_node_id, msg.to
                );
            }
            MessageType::MsgHeartbeatResponse => {
                info!(
                    "node {} drop indvidual heartbeat response message to replica {}",
                    from_node_id, msg.to
                );
            }
            _ => send_message(storage, transport, node_mgr, group_id, msg).await,
        }
    }
}

pub async fn send_message<MI, TR, RS, MRS>(
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msg: Message,
) where
    MI: MessageInterface,
    TR: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{

    let to_replica = storage.replica_metadata(group_id, msg.to).await.unwrap().unwrap();
    assert_ne!(to_replica.node_id, 0);

    let from_replica = storage.replica_metadata(group_id, msg.from).await.unwrap().unwrap();
    assert_ne!(from_replica.node_id, 0);

    if !node_mgr.contains_node(&to_replica.node_id) {
        node_mgr.add_node(to_replica.node_id, group_id);
    }

    let msg = RaftMessage {
        group_id,
        from_node: from_replica.node_id,
        to_node: to_replica.node_id,
        msg: Some(msg),
    };
    transport.send(msg).unwrap();
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
    fn connect(&self, _from: u64) -> Result<LocalClient, Error> {
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

impl LocalClient {
    fn connect<M: MessageInterface>(
        id: u64,
        server: &LocalServer<M>,
    ) -> Result<LocalClient, Error> {
        server.connect(id)
    }
}

pub struct LocalTransport<M: MessageInterface> {
    stop: watch::Sender<bool>,
    servers: Arc<RwLock<HashMap<u64, LocalServer<M>>>>,
}

impl<M: MessageInterface> LocalTransport<M> {}

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
        // let from_replica = match msg.from_replica.as_ref() {
        //     None => return Err(Error::BadParameter(format!("to from replica is none"))),
        //     Some(rm) => rm.node_id,
        // };
        // let to_replica = match msg.to_replica.as_ref() {
        //     None => return Err(Error::BadParameter(format!("to to replica is none"))),
        //     Some(rm) => rm.node_id,
        // };

        let (from_node, to_node) = (msg.from_node, msg.to_node);

        let servers = self.servers.clone();

        // get client
        let send_fn = async move {
            // get server by to
            let rl = servers.read().await;
            if !rl.contains_key(&to_node) {
                return Err(Error::Transport(TransportError::ServerNodeFound(
                    to_node,
                )));
            }
            let local_server = rl.get(&to_node).unwrap();

            // create from client by server
            let local_client = LocalClient::connect(from_node, local_server)?;

            // send reqeust
            let (tx, rx) = oneshot::channel();
            if let Err(_) = local_client.tx.send((msg, tx)).await {}

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

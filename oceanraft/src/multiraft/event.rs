use super::error::Error;

/// A LeaderElectionEvent is send when leader changed.
#[derive(Debug, Clone)]
pub struct LeaderElectionEvent {
    /// The id of the group where the leader belongs.
    pub group_id: u64,
    /// Current replica id. If current replica is the
    /// leader, then `replica_id` equal to `leader_id`.
    pub replica_id: u64,
    /// Current leader id.
    pub leader_id: u64,
}

#[derive(Debug, Clone)]
pub enum Event {
    LederElection(LeaderElectionEvent),

    /// Sent when consensus group is created.
    GroupCreate {
        group_id: u64,
        replica_id: u64,
        // commit_index: u64,
        // commit_term: u64,
        // applied_index: u64,
        // applied_term: u64,
    },
}

/// Shrink queue if queue capacity more than and len less than
/// this value.
const SHRINK_CACHE_CAPACITY: usize = 64;

#[derive(Clone)]
pub struct EventReceiver {
    rx: flume::Receiver<Event>,
}

impl EventReceiver {
    /// Wait for an incoming value from the channel associated with this receiver, returning an
    /// error if all senders have been dropped or the deadline has passed.
    #[inline]
    pub async fn recv(&self) -> Result<Event, Error> {
        self.rx.recv_async().await.map_err(|_| {
            Error::Channel(super::error::ChannelError::SenderClosed(
                "channel of event sender is closed".to_owned(),
            ))
        })
    }
}

pub struct EventChannel {
    tx: flume::Sender<Event>,
    rx: flume::Receiver<Event>,
    cap: usize,
    cache: Vec<Event>,
}

impl Clone for EventChannel {
    fn clone(&self) -> Self {
        Self {
            cap: self.cap,
            cache: Vec::with_capacity(self.cap),
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}

impl EventChannel {
    pub fn new(cap: usize) -> Self {
        let (tx, rx) = flume::bounded(cap);
        Self {
            cap,
            tx,
            rx,
            cache: Vec::with_capacity(cap),
        }
    }

    #[inline]
    pub fn push(&mut self, event: Event) {
        self.cache.push(event);
    }

    #[inline]
    pub fn subscribe(&self) -> EventReceiver {
        EventReceiver {
            rx: self.rx.clone(),
        }
    }

    fn try_gc(&mut self) {
        // TODO: think move the shrink_to_fit operation  to background task?
        if self.cache.capacity() > SHRINK_CACHE_CAPACITY && self.cache.len() < SHRINK_CACHE_CAPACITY
        {
            self.cache.shrink_to_fit();
        }
    }

    pub fn flush(&mut self) {
        if self.cache.is_empty() {
            return;
        }

        let events = self.cache.drain(..).collect::<Vec<_>>();
        let tx = self.tx.clone();
        let _ = tokio::spawn(async move {
            for event in events {
                match tx.send_async(event).await {
                    Ok(_) => {}
                    Err(_) => {}
                }
            }
        });
    }
}

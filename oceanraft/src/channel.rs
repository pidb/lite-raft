use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug, thiserror::Error)]
pub enum TrySendError<T> {
    #[error("no available capacity")]
    Full(T),

    #[error("channel closed")]
    Closed(T),
}

#[derive(Debug, thiserror::Error)]
pub enum SendError<T> {
    #[error("channel closed")]
    Closed(T),

    #[error("timed out waiting on send operation")]
    Timeout(T),
}

#[derive(Debug, thiserror::Error)]
pub enum TryRecvError {
    #[error("receiving on an empty channel")]
    Empty,

    #[error("receiving on a closed channel")]
    Disconnected,
}

pub enum Capacity {
    Bounded(usize),
    Unbounded,
}

pub struct WrapSender<T> {
    bounded_tx: Option<mpsc::Sender<T>>,
    unbounded_tx: Option<mpsc::UnboundedSender<T>>,
}

impl<T> WrapSender<T> {
    pub fn blocking_send(&self, value: T) -> Result<(), SendError<T>> {
        if let Some(tx) = &self.bounded_tx {
            tx.blocking_send(value).map_err(|e| SendError::Closed(e.0))
        } else if let Some(tx) = &self.unbounded_tx {
            tx.send(value).map_err(|e| SendError::Closed(e.0))
        } else {
            panic!("Sender is not initialized")
        }
    }

    /// Returns the number of messages that can be buffered by this channel.
    pub fn capacity(&self) -> Capacity {
        if let Some(tx) = &self.bounded_tx {
            Capacity::Bounded(tx.capacity())
        } else if let Some(tx) = &self.unbounded_tx {
            Capacity::Unbounded
        } else {
            panic!("Sender is not initialized")
        }
    }

    pub async fn closed(&self) {
        if let Some(tx) = &self.bounded_tx {
            tx.closed().await
        } else if let Some(tx) = &self.unbounded_tx {
            tx.closed().await
        } else {
            panic!("Sender is not initialized")
        }
    }

    pub fn is_closed(&self) -> bool {
        if let Some(tx) = &self.bounded_tx {
            tx.is_closed()
        } else if let Some(tx) = &self.unbounded_tx {
            tx.is_closed()
        } else {
            panic!("Sender is not initialized")
        }
    }

    /// Returns the maximum buffer capacity of the channel.
    /// If the channel is unbounded then returns usize::MAX
    pub fn max_capacity(&self) -> Capacity {
        if let Some(tx) = &self.bounded_tx {
            Capacity::Bounded(tx.max_capacity())
        } else if let Some(tx) = &self.unbounded_tx {
            Capacity::Unbounded
        } else {
            panic!("Sender is not initialized")
        }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        if let Some(tx) = &self.bounded_tx {
            tx.send(value).await.map_err(|e| SendError::Closed(e.0))
        } else if let Some(tx) = &self.unbounded_tx {
            tx.send(value).map_err(|e| SendError::Closed(e.0))
        } else {
            panic!("Sender is not initialized")
        }
    }

    /// Attempts to send a value on this `Sender` without blocking.
    /// If the channel is unbounded then immediately sends the value and return Ok(()).
    pub async fn send_timeout(
        &self,
        value: T,
        timeout: Duration,
    ) -> Result<(), SendError<T>> {
        if let Some(tx) = &self.bounded_tx {
            match tx.send_timeout(value, timeout).await {
                Ok(()) => Ok(()),
                Err(e) => match e {
                    mpsc::error::SendTimeoutError::Timeout(v) => Err(SendError::Timeout(v)),
                    mpsc::error::SendTimeoutError::Closed(v) => Err(SendError::Closed(v)),
                },
            }
        } else if let Some(tx) = &self.unbounded_tx {
            tx.send(value).map_err(|e| SendError::Closed(e.0))
        } else {
            panic!("Sender is not initialized")
        }
    }

    pub fn try_send(&self, message: T) -> Result<(), TrySendError<T>> {
        if let Some(tx) = &self.bounded_tx {
            match tx.try_send(message) {
                Ok(()) => Ok(()),
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(v) => Err(TrySendError::Full(v)),
                    mpsc::error::TrySendError::Closed(v) => Err(TrySendError::Closed(v)),
                },
            }
        } else if let Some(tx) = &self.unbounded_tx {
            tx.send(message).map_err(|e| TrySendError::Closed(e.0))
        } else {
            panic!("Sender is not initialized")
        }
    }
}


pub struct WrapReceiver<T> {
    bounded_rx: Option<mpsc::Receiver<T>>,
    unbounded_rx: Option<mpsc::UnboundedReceiver<T>>,
}

impl<T> WrapReceiver<T> {
    pub fn blocking_recv(&mut self) -> Option<T> {
        if let Some(rx) = &mut self.bounded_rx {
            rx.blocking_recv()
        } else if let Some(rx) = &mut self.unbounded_rx {
            rx.blocking_recv()
        } else {
            panic!("Receiver is not initialized")
        }
    }

    pub async fn recv(&mut self) -> Option<T> {
        if let Some(rx) = &mut self.bounded_rx {
            rx.recv().await
        } else if let Some(rx) = &mut self.unbounded_rx {
            rx.recv().await
        } else {
            panic!("Receiver is not initialized")
        }
    }

    pub fn close(&mut self) {
        if let Some(rx) = &mut self.bounded_rx {
            rx.close()
        } else if let Some(rx) = &mut self.unbounded_rx {
            rx.close()
        } else {
            panic!("Receiver is not initialized")
        }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(rx) = &mut self.bounded_rx {
            match rx.try_recv() {
                Ok(v) => Ok(v),
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => Err(TryRecvError::Empty),
                    mpsc::error::TryRecvError::Disconnected => Err(TryRecvError::Disconnected),
                },
            }
        } else if let Some(rx) = &mut self.unbounded_rx {
            match rx.try_recv() {
                Ok(v) => Ok(v),
                Err(e) => match e {
                    mpsc::error::TryRecvError::Empty => Err(TryRecvError::Empty),
                    mpsc::error::TryRecvError::Disconnected => Err(TryRecvError::Disconnected),
                }
            }
        } else {
            panic!("Receiver is not initialized")
        }
    }

    pub fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<T>> {
        if let Some(rx) = &mut self.bounded_rx {
            rx.poll_recv(cx)
        } else if let Some(rx) = &mut self.unbounded_rx {
            rx.poll_recv(cx)
        } else {
            panic!("Receiver is not initialized")
        }
    }
}

/// Create a mpsc channel for communicating between asynchronous tasks with backpressure.
/// If buffer is -1 then creates a unbounded channel otherwise creates a bounded channel.
pub fn wrap_channel<T>(buffer: isize) -> (WrapSender<T>, WrapReceiver<T>) {
    if buffer == -1 {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            WrapSender {
                bounded_tx: None,
                unbounded_tx: Some(tx),
            },
            WrapReceiver {
                bounded_rx: None,
                unbounded_rx: Some(rx),
            },
        )
    } else {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer as usize);
        (
            WrapSender {
                bounded_tx: Some(tx),
                unbounded_tx: None,
            },
            WrapReceiver {
                bounded_rx: Some(rx),
                unbounded_rx: None,
            },
        )
    }
}
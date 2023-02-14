use std::sync::Arc;
#[allow(unused)]
use std::time::Duration;

use futures::future::BoxFuture;
use prost::Message;
use raft_proto::prelude::Entry;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
#[allow(unused)]
use tokio::time::Instant;
use tokio::time::Interval;

#[inline]
pub fn compute_entry_size(ent: &Entry) -> usize {
    Message::encoded_len(ent)
}

pub trait Ticker: Send + 'static {
    fn recv(&mut self) -> BoxFuture<'_, std::time::Instant>;
}

impl Ticker for Interval {
    fn recv(&mut self) -> BoxFuture<'_, std::time::Instant> {
        Box::pin(async {
            let ins = self.tick().await;
            ins.into_std()
        })
    }
}

#[derive(Clone)]
pub struct ManualTick {
    tx: UnboundedSender<oneshot::Sender<()>>,
    rx: Arc<Mutex<UnboundedReceiver<oneshot::Sender<()>>>>,
}

impl ManualTick {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    pub async fn tick(&mut self) {
        let (tx, rx) = oneshot::channel();
        self.tx.send(tx).unwrap();
        rx.await.unwrap();
    }

    pub fn non_blocking_tick(&mut self) {
        let tx = self.tx.clone();
        let _ = tokio::spawn(async move {
            let (res_tx, res_rx) = oneshot::channel();
            tx.send(res_tx).unwrap();
            res_rx.await.unwrap();
        });
    }
}

impl Ticker for ManualTick {
    fn recv(&mut self) -> BoxFuture<'_, std::time::Instant> {
        Box::pin(async {
            let mut rx = { self.rx.lock().await };
            let res_tx = rx.recv().await.unwrap();
            res_tx.send(()).unwrap();
            std::time::Instant::now()
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tokio_ticker() {
    let start = tokio::time::Instant::now();
    let mut interval =
        tokio::time::interval_at(start + Duration::from_millis(10), Duration::from_millis(10));
    interval.recv().await; // approximately 10ms have elapsed
    assert!(start.elapsed() >= Duration::from_millis(10));

    tokio::time::sleep(Duration::from_millis(20)).await; // approximately 30ms have elapsed
    interval.reset();
    interval.recv().await; // approximately 40ms have elapsed
    assert!(start.elapsed() >= Duration::from_millis(40));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_manual_ticker() {
    let start = Instant::now();
    let mut ticker = ManualTick::new();
    for _ in 0..10 {
        ticker.non_blocking_tick();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut ticks = 0;
    for _ in 0..10 {
        ticker.recv().await;
        ticks += 1;
    }

    assert_eq!(ticks, 10);
    assert!(start.elapsed() >= Duration::from_millis(100)); // approximately 100ms have elapsed
}

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
use tokio::time::interval_at;
#[allow(unused)]
use tokio::time::Instant;
use tokio::time::Interval;

#[inline]
pub fn compute_entry_size(ent: &Entry) -> usize {
    Message::encoded_len(ent)
}


/// Ticker periodically sends tick and provides recv future. 
/// Ticker doesn't care how the tick is sent.
/// 
/// Note: Abstract this trait because need to manually send the 
/// tick for testing, and in most cases you should use `tokio::time::Interval`.
/// the lib providers its implementation.
pub trait Ticker: Send + 'static {
    /// New an implementer of a new `Ticker` trait with a defined sized requirement.
    /// 
    /// `start` specifies the time after which the periodic tick is allowed to start. 
    /// `pediod` presentation periodic tick.
    fn new(start: std::time::Instant, period: Duration) -> Self
    where
        Self: Sized;

    /// Recv tick, returns a boxed future.
    fn recv(&mut self) -> BoxFuture<'_, std::time::Instant>;
}

impl Ticker for Interval {
    fn new(start: std::time::Instant, period: Duration) -> Self {
        interval_at(Instant::from_std(start), period)
    }

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
    fn new(_start: std::time::Instant, _period: Duration) -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }

    fn recv(&mut self) -> BoxFuture<'_, std::time::Instant> {
        Box::pin(async {
            let mut rx = { self.rx.lock().await };
            rx.recv().await.map(|res_tx| {
                if let Err(_) = res_tx.send(()) {
                    // the receiver waiting for the tick response is dropped.
                }
            });
            std::time::Instant::now()
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tokio_ticker() {
    let start = tokio::time::Instant::now();

    let mut default_ticker = Interval::new(
        std::time::Instant::now() + Duration::from_millis(10),
        Duration::from_millis(10),
    );

    default_ticker.recv().await; // approximately 10ms have elapsed
    assert!(start.elapsed() >= Duration::from_millis(10));

    tokio::time::sleep(Duration::from_millis(20)).await; // approximately 30ms have elapsed
    default_ticker.reset();
    default_ticker.recv().await; // approximately 40ms have elapsed
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

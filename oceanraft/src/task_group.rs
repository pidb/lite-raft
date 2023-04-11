use std::fmt;
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures_util::future::FusedFuture;

use tokio::sync::futures::Notified;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::utils::defer;

struct TaskSharedState {
    num_tasks: AtomicU32,
    waker: Mutex<Option<Waker>>,
    stopped: AtomicBool,
    stop_notify: Notify,
}

impl Default for TaskSharedState {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskSharedState {
    fn new() -> Self {
        Self {
            num_tasks: AtomicU32::new(0),
            waker: Mutex::new(None),
            stopped: AtomicBool::new(false),
            stop_notify: Notify::new(),
        }
    }

    /// Notify runtime to repoll Joinner, if any.
    fn wake(&self) {
        _ = self
            .waker
            .lock()
            .map(|mut waker| waker.take().map(Waker::wake));
    }

    /// Save runtime waker to notify runtime when state changed.
    /// `false` returned if save error.
    fn set_waker(&self, cx: &Context<'_>) -> bool {
        self.waker.lock().map_or(false, |mut waker| {
            *waker = Some(cx.waker().clone());
            true
        })
    }

    /// Returns `true` if this call signalled stopping or `false`
    /// if the [`Tasker`] was already stopped.
    fn stop(&self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }

        self.stop_notify.notify_waiters();
    }

    fn ptr(&self) -> *const Self {
        self as _
    }
}

// stop guard in async has trap..
// pub struct StopGuard(Pin<Arc<TaskSharedState>>);

// impl Drop for StopGuard {
//     fn drop(&mut self) {
//         let prev_num_tasks = self.0.num_tasks.fetch_sub(1, Ordering::AcqRel);
//         self.0.wake();
//         // prev_num_tasks using load is relax
//         if prev_num_tasks == 1 {
//             self.0.stopped.store(true, Ordering::Release);
//         }
//     }
// }

/// An error type for the "stopped by [`Stopper`]" situation.
///
/// May be convenient to bubble task stopping up error chains.
#[derive(Debug)]
pub struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The task was stopped")
    }
}

impl std::error::Error for Stopped {
    fn description(&self) -> &str {
        "The task was stopped"
    }
}

pin_project_lite::pin_project! {
    pub struct Stopper {
        // SAFETY: Drop order matters! `notified` must come before `shared`.
        #[pin] notified: Option<Pin<Box<Notified<'static>>>>,
        shared: Pin<Arc<TaskSharedState>>,
    }
}

impl Stopper {
    fn new(shared: &Pin<Arc<TaskSharedState>>) -> Self {
        // all tasks stopped.
        let notified = if shared.stopped.load(Ordering::Acquire) {
            None
        } else {
            // get notifyed poller from notify
            let notified = shared.stop_notify.notified();
            // SAFETY: We're keeping a Pin<Arc> to the referenced value until Self is dropped.
            let notified: Notified<'static> = unsafe { mem::transmute(notified) };
            Some(Box::pin(notified))
        };

        Self {
            shared: shared.clone(),
            notified,
        }
    }

    /// `true` if stopping was already signalled.
    pub fn stopped(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }
}

impl Future for Stopper {
    type Output = Stopped;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // 1. all task stopped
        if self.shared.stopped.load(Ordering::Acquire) {
            return Poll::Ready(Stopped);
        }

        // 2. or this projection associated task stopped
        let this = self.project();
        match this.notified.as_pin_mut() {
            Some(notified) => notified.poll(cx).map(|_| Stopped),
            None => Poll::Ready(Stopped),
        }
    }
}

impl FusedFuture for Stopper {
    fn is_terminated(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }
}

impl fmt::Debug for Stopper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stopper")
            .field("tasker", &self.shared.ptr())
            .field("stopped", &self.stopped())
            .finish()
    }
}

pub struct Joinner {
    shared: Pin<Arc<TaskSharedState>>,
}

impl Future for Joinner {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.shared.stopped.load(Ordering::Acquire) {
            true => Poll::Ready(()),
            false => {
                if self.shared.set_waker(cx) {
                    Poll::Pending
                } else {
                    // save error, we ready it
                    Poll::Ready(())
                }
            }
        }
    }
}

impl FusedFuture for Joinner {
    fn is_terminated(&self) -> bool {
        self.shared.stopped.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct TaskGroup {
    shared: Pin<Arc<TaskSharedState>>,
}

impl TaskGroup {
    pub fn new() -> Self {
        Self {
            shared: Arc::pin(TaskSharedState::new()),
        }
    }

    pub fn spawn<T>(&self, future: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.add();
        let this = self.clone();
        tokio::spawn(async move {
            defer! {
               this.done();
            };
            future.await
        })
    }

    #[inline]
    pub fn add(&self) {
        self.shared.num_tasks.fetch_add(1, Ordering::Release);
    }

    pub fn done(&self) {
        if self.shared.stopped.load(Ordering::Acquire) {
            return;
        }

        let prev_num_tasks = self.shared.num_tasks.fetch_sub(1, Ordering::AcqRel);
        self.shared.wake();
        if prev_num_tasks == 1 {
            self.shared.stopped.store(true, Ordering::Release);
        }
    }

    #[inline]
    pub fn stop(&self) {
        self.shared.stop()
    }

    #[inline]
    pub fn stopper(&self) -> Stopper {
        Stopper::new(&self.shared)
    }

    #[inline]
    pub fn joinner(&self) -> Joinner {
        Joinner {
            shared: self.shared.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;

    use tokio::sync::mpsc::channel;
    use tokio::time::sleep;
    use tokio::time::Duration;

    use crate::task_group::TaskGroup;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_group_stopper() {
        let (waiting_tx, mut waiting_rx) = channel(1);
        let (running_tx, mut running_rx) = channel(1);

        let tg = TaskGroup::new();
        let tg_clone = tg.clone();
        // running task
        tg.add();
        tokio::spawn(async move {
            running_rx.recv().await.unwrap();
            tg_clone.done();
        });

        let stopper = tg.stopper();
        tokio::spawn(async move {
            stopper.await;
            tokio::select! {
                Some(()) = waiting_rx.recv() => {
                    panic!("expected stopper to have blocked");
                },

                _ = sleep(Duration::from_millis(1)) => {
                }
            }
            // notify running task to release counter.
            running_tx.send(()).await.unwrap();
            tokio::select! {
                Some(()) = waiting_rx.recv() => {
                },
                _ = sleep(Duration::from_millis(100)) => {
                    panic!("stopper should have finished waiting")
                }
            }
        });

        tg.stop();
        tg.joinner().await;
        waiting_tx.send(()).await.unwrap();
    }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn test_multiple_async_stoppes() {
    //     // emit tasks
    //     let task_nums = 3;
    //     let tg = TaskGroup::new();
    //     for _ in 0..task_nums {
    //         tg.add();
    //         let tg2 = tg.clone();
    //         let stopper = tg.stopper();
    //         tokio::spawn(async move {
    //             stopper.await;
    //             tg2.done();
    //         });
    //     }

    //     let (tx, mut rx) = channel(1);

    //     // stop all tasks
    //     tokio::spawn(async move {
    //         tg.stop();
    //         tg.joinner().await;
    //         tx.send(()).await.unwrap();
    //     });

    //     tokio::select! {
    //         _ = rx.recv() => {},
    //         _ = sleep(Duration::from_millis(10)) => {
    //             panic!("timed out waiting for stop")
    //         }
    //     }
    // }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_num_tasks() {
        let task_nums = 3;
        let tg = TaskGroup::new();
        for i in 0..task_nums  {
            tg.add();
        }
    }


    // #[tokio::test(flavor = "multi_thread")]
    // #[should_panic]
    // async fn test_panic_safe() {
    //     let tg = TaskGroup::new();
    //     tg.spawn(async {
    //         _ = catch_unwind(|| panic!("opps"));
    //     });

    //     tg.stop();
    //     tokio::select! {
    //         _ = tg.joinner() => {},
    //         _ = sleep(Duration::from_millis(10)) => {
    //             panic!("timed out waiting for all task join")
    //         }
    //     }
    // }
}

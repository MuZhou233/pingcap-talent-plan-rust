use std::{sync::Arc, thread};

use crossbeam_channel::{unbounded, Sender, Receiver};

use crate::error::Result;
use super::ThreadPool;

/// A thread pool using a shared queue inside.
///
/// If a spawned task panics, the old thread will be destroyed and a new one will be
/// created. It fails silently when any failure to create the thread at the OS level
/// is captured after the thread pool is created. So, the thread number in the pool
/// can decrease to zero, then spawning a task to the thread pool will panic.
pub struct SharedQueueThreadPool {
    queue: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

struct ThreadPoolSharedData {
    job: Receiver<Box<dyn FnOnce() + Send + 'static>>
}

struct Sentinel {
    data: Arc<ThreadPoolSharedData>,
    active: bool
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        let (tx, rx) = unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        
        let shared = Arc::new(ThreadPoolSharedData{
            job: rx
        });

        for _ in 0..threads {
            Self::create_worker(shared.clone())?;
        }
        
        Ok(SharedQueueThreadPool{
            queue: tx,
        })
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.queue.send(Box::new(job)).expect(
            "Send job error"
        );
    }
}

impl SharedQueueThreadPool {
    fn create_worker(shared: Arc<ThreadPoolSharedData>) -> Result<()> {
        let thread_builder = thread::Builder::new();
        thread_builder.spawn(move|| {
            let shared = Sentinel::new(shared);

            loop {
                let job = match {
                    let receiver = &shared.data.job;
                    receiver.recv()
                } {
                    Ok(job) => job,
                    Err(_) => break
                };

                job();
            }

            shared.cancel();
        })?;
        Ok(())
    }
}

impl Sentinel {
    fn new(data: Arc<ThreadPoolSharedData>) -> Self {
        Sentinel {
            data,
            active: true
        }
    }
    fn cancel(mut self) {
        self.active = false;
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        if self.active {
            SharedQueueThreadPool::create_worker(self.data.clone()).expect(
                "Sentinel recovery failed"
            );
        }
    }
}
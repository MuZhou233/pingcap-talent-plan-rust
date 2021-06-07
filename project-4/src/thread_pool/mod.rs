use crate::error::Result;

mod naive;
mod rayon;
mod shared_queue;

pub use self::naive::NaiveThreadPool;
pub use self::rayon::RayonThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;

/// todo
pub trait ThreadPool {
    /// todo
    fn new(threads: u32) -> Result<Self> where Self:Sized;
    /// todo
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}
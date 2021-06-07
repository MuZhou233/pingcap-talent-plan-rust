use std::convert::TryInto;

use crate::error::Result;
use super::ThreadPool;

/// todo
pub struct RayonThreadPool {
    pool: rayon::ThreadPool
}
impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        Ok(RayonThreadPool{
            pool: rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap()
        })
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.pool.spawn(job)
    }
}
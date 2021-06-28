use crate::error::Result;
use super::ThreadPool;

/// Wrapper of rayon::ThreadPool
pub struct RayonThreadPool {
    pool: rayon::ThreadPool
}
impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        Ok(RayonThreadPool{
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize).build()?
        })
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.pool.spawn(job)
    }
}
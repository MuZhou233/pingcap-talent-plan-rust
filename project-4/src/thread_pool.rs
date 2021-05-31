use super::error::Result;

/// todo
pub trait ThreadPool {
    /// todo
    fn new(threads: u32) -> Result<Self> where Self:Sized;
    /// todo
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

/// todo
pub struct NaiveThreadPool {}
impl ThreadPool for NaiveThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        Ok(NaiveThreadPool{})
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {

    }
}

/// todo
pub struct SharedQueueThreadPool {}
impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        Ok(SharedQueueThreadPool{})
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {

    }
}

/// todo
pub struct RayonThreadPool {}
impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self> where Self:Sized {
        Ok(RayonThreadPool{})
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {

    }
}
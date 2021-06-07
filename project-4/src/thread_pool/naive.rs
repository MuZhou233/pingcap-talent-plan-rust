use crate::error::Result;
use super::ThreadPool;
use std::thread;

/// This implementation is not going to reuse threads between jobs
pub struct NaiveThreadPool {}
impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> where Self:Sized {
        Ok(NaiveThreadPool{})
    }
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let thread_builder = thread::Builder::new();
        thread_builder.spawn(job).unwrap();
    }
}
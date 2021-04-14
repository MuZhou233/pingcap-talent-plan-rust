#![deny(missing_docs)]

//! todo

use std::{fmt::{self, Display}, option::NoneError};
use failure::{Backtrace, Context, Fail};

/// todo
pub type Result<T> = std::result::Result<T, MyError>;
/// todo
#[derive(Debug)]
pub struct MyError {
    inner: Context<MyErrorKind>,
}
/// todo
#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum MyErrorKind {
    /// todo
    #[fail(display = "None")]
    None,
    /// todo
    #[fail(display = "Unimplement!")]
    Unimplement,
    // ...
}
impl Fail for MyError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl MyError {
    /// todo
    pub fn kind(&self) -> MyErrorKind {
        *self.inner.get_context()
    }
}

impl From<MyErrorKind> for MyError {
    fn from(kind: MyErrorKind) -> MyError {
        MyError { inner: Context::new(kind) }
    }
}

impl From<Context<MyErrorKind>> for MyError {
    fn from(inner: Context<MyErrorKind>) -> MyError {
        MyError { inner: inner }
    }
}

impl From<NoneError> for MyError {
    fn from(_kind: NoneError) -> MyError {
        MyError { inner: Context::new(MyErrorKind::None) }
    }
}
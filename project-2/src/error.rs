#![deny(missing_docs)]

//! todo

use std::{fmt::{self, Display}, option::NoneError};
use failure::{Backtrace, Context, Fail};

/// todo
pub type Result<T> = std::result::Result<T, Error>;
/// todo
#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>,
}
/// todo
#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum ErrorKind {
    /// todo
    #[fail(display = "ronError")]
    RonError,
    /// todo
    #[fail(display = "ioError")]
    IoError,
    /// todo
    #[fail(display = "None")]
    None,
    /// todo
    #[fail(display = "Unimplement!")]
    Unimplement,
    // ...
}
impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl Error {
    /// todo
    pub fn kind(&self) -> ErrorKind {
        *self.inner.get_context()
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner: inner }
    }
}

impl From<NoneError> for Error {
    fn from(_kind: NoneError) -> Error {
        Error { inner: Context::new(ErrorKind::None) }
    }
}

impl From<std::io::Error> for Error {
    fn from(_kind: std::io::Error) -> Error {
        Error { inner: Context::new(ErrorKind::IoError) }
    }
}

impl From<ron::Error> for Error {
    fn from(_kind: ron::Error) -> Error {
        Error { inner: Context::new(ErrorKind::RonError) }
    }
}
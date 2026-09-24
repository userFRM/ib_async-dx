//! The error every fallible call returns: ib_async's exceptions as one enum.

use std::fmt;
use std::sync::Arc;

/// What a call fails with. Each variant stands for the exception ib_async
/// raises in the same place.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Error {
    /// No session is open: ib_async's `ConnectionError("Not connected")`.
    NotConnected,
    /// The session could not be opened or was lost: ib_async's
    /// `ConnectionError`. A failed logon, a socket disconnect, a failed
    /// startup sync, an internal close, or an engine shutdown that was not
    /// confirmed.
    Connection(String),
    /// The API reported an error tied to one request: ib_async's
    /// `RequestError`.
    Request {
        /// The request's id, ib_async's `reqId`.
        req_id: i64,
        /// The error code.
        code: i64,
        /// The error message.
        message: String,
    },
    /// A deadline passed: ib_async's `asyncio.TimeoutError`.
    Timeout,
    /// A value the call cannot take or produce: ib_async's `ValueError`,
    /// `AssertionError`, `KeyError` or `RuntimeError`.
    Value(String),
    /// A Flex report could not be downloaded or read: ib_async's `FlexError`.
    #[cfg(feature = "flex")]
    Flex(String),
    /// An I/O failure: a thread that could not be spawned, or a Flex report's
    /// file or network I/O.
    Io(Arc<std::io::Error>),
}

/// A result whose error is [`Error`] unless another is named.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotConnected => f.write_str("Not connected"),
            Error::Connection(m) | Error::Value(m) => f.write_str(m),
            #[cfg(feature = "flex")]
            Error::Flex(m) => f.write_str(m),
            Error::Request {
                req_id,
                code,
                message,
            } => write!(f, "[reqId {req_id}] API error: {code}: {message}"),
            Error::Timeout => f.write_str("timed out"),
            Error::Io(e) => fmt::Display::fmt(e, f),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(Arc::new(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_ib_asyncs_text() {
        let request = Error::Request {
            req_id: 7,
            code: 200,
            message: "No security definition has been found".into(),
        };
        assert_eq!(
            request.to_string(),
            "[reqId 7] API error: 200: No security definition has been found"
        );
        assert_eq!(Error::NotConnected.to_string(), "Not connected");
        assert_eq!(Error::Timeout.to_string(), "timed out");
        assert_eq!(Error::Value("bad pair".into()).to_string(), "bad pair");
        let io = Error::from(std::io::Error::other("no thread"));
        assert_eq!(io.to_string(), "no thread");
    }
}

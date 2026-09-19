//! Optional OS adapters. Unsafe calls are confined to these platform boundaries.
use std::io;

/// Independent blocking halves; reads never hold a lock needed by writes.
pub trait Duplex: Sized {
    /// Receive half owned by the downstream pump.
    type Reader: io::Read + Send + 'static;
    /// Send half with an explicit half-close operation.
    type Writer: crate::transport::WriteHalf;
    /// Split ownership without serializing reads and writes.
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)>;
}

/// Connection establishment failure, before application traffic is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectError {
    /// No connection was established within the configured setup budget.
    ConnectTimeout,
    /// The OS denied access to the endpoint.
    PermissionDenied,
}
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ConnectTimeout => "daemon connection timed out",
            Self::PermissionDenied => "daemon endpoint permission denied",
        })
    }
}
impl std::error::Error for ConnectError {}

#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub mod unix_socket;
#[cfg(windows)]
pub mod windows;

#[cfg(test)]
fn test_handshake(write: &mut impl io::Write, read: &mut impl io::Read, _: &str) -> io::Result<()> {
    write.write_all(b"hello\n")?;
    write.flush()?;
    let mut reply = [0; 6];
    read.read_exact(&mut reply)?;
    if reply != *b"ready\n" {
        return Err(io::Error::other("invalid test reply"));
    }
    Ok(())
}

#[cfg(all(windows, feature = "local-async"))]
pub mod windows_socket;

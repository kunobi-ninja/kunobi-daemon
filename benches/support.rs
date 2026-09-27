//! In-memory transports and fixtures shared by the benchmarks.
//!
//! Nothing here opens a descriptor, starts a thread or sleeps, so a benchmark
//! executes the same instructions on every run.

use kunobi_daemon::ServiceIdentity;
use kunobi_daemon::transport::WriteHalf;
use kunobi_daemon::wire::{Hello, capability};
use std::io::{self, Read, Write};

/// Accepts every byte and only counts it. A benchmark then measures the
/// crate's per-call work, not a copy into a growing buffer.
#[derive(Default)]
pub struct Discard {
    /// Bytes accepted.
    pub bytes: usize,
    /// Calls to `flush`.
    pub flushes: usize,
}

impl Write for Discard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushes += 1;
        Ok(())
    }
}

impl WriteHalf for Discard {
    fn shutdown_write(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Returns its data in reads of at most `chunk` bytes, like a socket that
/// delivers one segment per call, then end of stream.
pub struct Chunked {
    data: Vec<u8>,
    position: usize,
    chunk: usize,
}

impl Chunked {
    /// `total` bytes of [`payload`], delivered `chunk` bytes at a time.
    pub fn new(total: usize, chunk: usize) -> Self {
        Self {
            data: payload(total),
            position: 0,
            chunk,
        }
    }
}

impl Read for Chunked {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let n = self
            .chunk
            .min(buffer.len())
            .min(self.data.len() - self.position);
        buffer[..n].copy_from_slice(&self.data[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}

/// `len` bytes of 64-byte newline-terminated records, the shape of
/// line-delimited relay traffic. Any length that is a multiple of 64 ends on
/// a record boundary.
pub fn payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| {
            if i % 64 == 63 {
                b'\n'
            } else {
                b'a' + (i % 26) as u8
            }
        })
        .collect()
}

/// Every shared capability, as a daemon that implements all of them offers.
pub const ALL_CAPABILITIES: u64 = capability::HEALTH
    | capability::DRAIN
    | capability::HANDOFF
    | capability::APPLICATION
    | capability::HEALTH_DETAILS;

/// A valid identity with realistic component lengths.
pub fn identity() -> ServiceIdentity {
    ServiceIdentity::new([0x5a; 16], "org.example.cache", "stable", "default")
        .expect("valid fixture identity")
}

/// The offer both peers make in these benchmarks.
pub fn offer() -> Hello {
    Hello::new(&identity(), ALL_CAPABILITIES, capability::HEALTH)
}

/// One frame as it crosses the socket: little-endian body length, then the
/// Protobuf body.
pub fn frame<M: buffa::Message>(message: &M) -> Vec<u8> {
    let body = message.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    frame
}

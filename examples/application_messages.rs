//! Application-owned messages and capability negotiation over shared framing.
//! Loopback is only for this example; consumers must authenticate their peers.
use buffa::Message;
use kunobi_daemon::wire::{Control, Hello, MessageKind, Session, capability};
use std::{io, net::TcpListener, net::TcpStream, thread, time::Duration};

// These IDs belong to the example.cache namespace, not the shared crate.
const LOOKUP: u32 = 1;
const CACHE_LOOKUP: u64 = 1 << 16;

#[rustfmt::skip]
#[allow(clippy::all, dead_code)]
mod payload { include!("support/cache.rs"); }
use payload::{CacheLookup, CacheResult};

fn offer() -> Hello {
    // Requiring our specific capability rejects peers that only understand
    // the shared APPLICATION envelope. It does not imply support for LOOKUP.
    let required = capability::APPLICATION | CACHE_LOOKUP;
    Hello::new(
        &kunobi_daemon::ServiceIdentity::new(
            [
                0x71, 0xf0, 0x09, 0x40, 0xbb, 0xdb, 0x40, 0xf2, 0x9a, 0x75, 0x11, 0x8c, 0xb1, 0x67,
                0xd1, 0xdc,
            ],
            "example.cache",
            "stable",
            "default",
        )
        .expect("valid package identity"),
        required,
        required,
    )
}

fn set_deadlines(stream: &TcpStream) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let server = thread::spawn(move || -> io::Result<()> {
        let (stream, _) = listener.accept()?;
        set_deadlines(&stream)?;
        let mut session = Session::accept(stream, &offer())?;
        let request = session.receive()?;
        if request.kind != MessageKind::Application || request.operation != LOOKUP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown operation",
            ));
        }
        let lookup =
            CacheLookup::decode_from_slice(request.payload.as_slice()).map_err(io::Error::other)?;
        session.send(&Control {
            kind: MessageKind::Application.into(),
            operation: LOOKUP,
            request_id: request.request_id,
            payload: CacheResult {
                hit: lookup.key == b"example-key",
                ..Default::default()
            }
            .encode_to_vec(),
            ..Default::default()
        })
    });
    let stream = TcpStream::connect(address)?;
    set_deadlines(&stream)?;
    let mut session = Session::connect(stream, &offer())?;
    session.send(&Control {
        kind: MessageKind::Application.into(),
        operation: LOOKUP,
        request_id: 1,
        payload: CacheLookup {
            key: b"example-key".to_vec(),
            ..Default::default()
        }
        .encode_to_vec(),
        ..Default::default()
    })?;
    let response = session.receive()?;
    assert_eq!(response.kind, MessageKind::Application);
    assert_eq!(response.operation, LOOKUP);
    assert_eq!(response.request_id, 1);
    assert!(
        CacheResult::decode_from_slice(response.payload.as_slice())
            .map_err(io::Error::other)?
            .hit
    );
    server.join().expect("server thread panicked")?;
    println!("Application-owned Protobuf lookup succeeded");
    Ok(())
}

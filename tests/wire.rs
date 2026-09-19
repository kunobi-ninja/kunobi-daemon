//! Binary framing, negotiation, and schema compatibility.
#![cfg(feature = "wire")]

use buffa::Message;
use kunobi_daemon::wire::{
    self, Control, Endpoint, Framed, Hello, Session, capability as c, operation as o,
};
use std::{
    io::{self, Cursor, Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
};

fn offer() -> Hello {
    Hello::new(
        &kunobi_daemon::ServiceIdentity::new([1; 16], "fixture", "stable", "default").unwrap(),
        c::HEALTH | c::DRAIN,
        c::HEALTH,
    )
}
fn health() -> Control {
    Control {
        operation: o::HEALTH,
        request_id: 42,
        ..Default::default()
    }
}

fn sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    for socket in [&client, &server] {
        socket.set_nodelay(true).unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }
    (client, server)
}

#[test]
fn a_live_negotiated_session_reuses_the_connection_and_correlates_replies() {
    let (client, server) = sockets();
    let server = std::thread::spawn(move || {
        let mut session = Session::accept(server, &offer()).unwrap();
        assert_eq!(session.agreement().capabilities, c::HEALTH);
        assert_eq!(session.agreement().max_frame, 512);
        for id in [42, 43] {
            let request = session.receive().unwrap();
            assert_eq!(request.request_id, id);
            session
                .send(&Control {
                    payload: b"ready".to_vec(),
                    ..request
                })
                .unwrap();
        }
    });
    let mut hello = offer();
    hello.supported = c::HEALTH | (1 << 50);
    hello.max_frame = 512;
    let mut session = Session::connect(client, &hello).unwrap();
    for id in [42, 43] {
        session
            .send(&Control {
                request_id: id,
                ..health()
            })
            .unwrap();
        let response = session.receive().unwrap();
        assert_eq!(response.request_id, id);
        assert_eq!(response.payload, b"ready");
    }
    server.join().unwrap();
}

#[test]
fn negotiation_rejects_incompatible_peers_before_dispatch() {
    let local = offer();
    let cases = [
        Hello {
            minimum: 3,
            maximum: 3,
            ..offer()
        },
        Hello {
            minimum: 1,
            maximum: 1,
            ..offer()
        },
        Hello {
            minimum: 3,
            maximum: 2,
            ..offer()
        },
        Hello {
            supported: c::HEALTH | (1 << 40),
            required: 1 << 40,
            ..offer()
        },
        Hello {
            supported: 0,
            ..offer()
        },
        Hello {
            max_frame: 0,
            ..offer()
        },
        Hello {
            max_frame: wire::MAX_FRAME + 1,
            ..offer()
        },
        Hello {
            application: "different-app".into(),
            ..offer()
        },
    ];
    for remote in cases {
        assert!(wire::negotiate(&local, &remote).is_err(), "{remote:?}");
        assert!(wire::negotiate(&remote, &local).is_err(), "{remote:?}");
    }
    let (client, server) = sockets();
    let server = std::thread::spawn(move || {
        assert!(Session::accept(server, &offer()).is_err());
    });
    assert!(
        Session::connect(
            client,
            &Hello::new(
                &kunobi_daemon::ServiceIdentity::new([1; 16], "wrong-app", "stable", "default")
                    .unwrap(),
                c::HEALTH,
                0
            )
        )
        .is_err()
    );
    server.join().unwrap();
}

#[test]
fn operation_support_is_not_inferred_from_decodability() {
    let (client, server) = sockets();
    let server = std::thread::spawn(move || {
        let mut session = Session::accept(server, &offer()).unwrap();
        assert_eq!(session.receive().unwrap(), health());
    });
    let hello = Hello::new(
        &kunobi_daemon::ServiceIdentity::new([1; 16], "fixture", "stable", "default").unwrap(),
        c::HEALTH,
        c::HEALTH,
    );
    let mut session = Session::connect(client, &hello).unwrap();
    assert!(
        session
            .send(&Control {
                operation: o::DRAIN,
                ..health()
            })
            .is_err()
    );
    assert!(
        session
            .send(&Control {
                operation: 99,
                ..health()
            })
            .is_err()
    );
    session.send(&health()).unwrap();
    server.join().unwrap();
}

#[rustfmt::skip]
#[allow(clippy::all, dead_code)]
mod future { include!("fixtures/future.rs"); }
use future::FutureControl;

#[test]
fn future_optional_fields_and_old_messages_decode_in_both_directions() {
    let future = FutureControl {
        operation: o::HEALTH,
        request_id: 42,
        optional_trace: "new-field".into(),
        ..Default::default()
    };
    let old_reader = Control::decode_from_slice(&future.encode_to_vec()).unwrap();
    assert_eq!(old_reader.operation, o::HEALTH);
    assert_eq!(old_reader.request_id, 42);
    assert_eq!(
        FutureControl::decode_from_slice(&old_reader.encode_to_vec()).unwrap(),
        future
    );
    let old = FutureControl::decode_from_slice(health().encode_to_vec().as_slice()).unwrap();
    assert_eq!(old.optional_trace, "");
    assert_eq!(old.request_id, 42);
}

#[derive(Default)]
struct Memory {
    input: Cursor<Vec<u8>>,
    output: Vec<u8>,
    fragment: bool,
    reads: usize,
}
impl Read for Memory {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reads += 1;
        let n = if self.fragment {
            buf.len().min(1)
        } else {
            buf.len()
        };
        self.input.read(&mut buf[..n])
    }
}
impl Write for Memory {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let n = if self.fragment {
            bytes.len().min(1)
        } else {
            bytes.len()
        };
        self.output.extend_from_slice(&bytes[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn fragmented_and_coalesced_frames_preserve_exact_message_boundaries() {
    let mut io = Memory {
        fragment: true,
        ..Default::default()
    };
    {
        let mut writer = Framed::new(&mut io, 256).unwrap();
        writer.send(&health()).unwrap();
        writer
            .send(&Control {
                request_id: 43,
                ..health()
            })
            .unwrap();
    }
    io.input = Cursor::new(std::mem::take(&mut io.output));
    let mut reader = Framed::new(&mut io, 256).unwrap();
    assert_eq!(reader.receive::<Control>().unwrap().request_id, 42);
    assert_eq!(reader.receive::<Control>().unwrap().request_id, 43);
}

#[test]
fn oversized_and_truncated_frames_poison_the_connection() {
    for bytes in [
        u32::MAX.to_le_bytes().to_vec(),
        vec![0, 0, 0, 0],
        vec![5, 0, 0, 0, 1],
        vec![1, 0],
    ] {
        let mut io = Memory {
            input: Cursor::new(bytes),
            ..Default::default()
        };
        {
            let mut reader = Framed::new(&mut io, 256).unwrap();
            assert!(reader.receive::<Control>().is_err());
            assert!(reader.receive::<Control>().is_err());
            assert!(reader.send(&health()).is_err());
        }
        assert!(io.output.is_empty());
        assert!(io.reads <= 3, "must stop reading on invalid framing");
    }
}

#[test]
fn every_truncation_of_a_valid_frame_is_rejected() {
    let mut io = Memory::default();
    Framed::new(&mut io, 256).unwrap().send(&health()).unwrap();
    for end in 0..io.output.len() {
        let bytes = Cursor::new(io.output[..end].to_vec());
        assert!(
            Framed::new(bytes, 256)
                .unwrap()
                .receive::<Control>()
                .is_err()
        );
    }
}

#[test]
fn local_oversize_is_rejected_before_writing_any_bytes() {
    let mut io = Memory::default();
    let mut writer = Framed::new(&mut io, 256).unwrap();
    assert!(
        writer
            .send(&Control {
                payload: vec![0; 257],
                ..health()
            })
            .is_err()
    );
    assert!(io.output.is_empty());
}

#[test]
fn legacy_discovery_never_selects_binary_and_advertisements_cannot_alias() {
    assert_eq!(
        wire::select_endpoint("old.sock", None).unwrap(),
        Endpoint::Legacy("old.sock")
    );
    assert_eq!(
        wire::select_endpoint("old.sock", Some("new.sock")).unwrap(),
        Endpoint::Binary("new.sock")
    );
    assert!(wire::select_endpoint("old.sock", Some("old.sock")).is_err());
    assert!(wire::select_endpoint("old.sock", Some("")).is_err());
}

#[test]
fn a_write_failure_never_replays_a_partially_sent_request() {
    struct PartialWriter {
        written: usize,
    }
    impl Read for PartialWriter {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            panic!("no reads")
        }
    }
    impl Write for PartialWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.written != 0 {
                return Err(io::ErrorKind::BrokenPipe.into());
            }
            self.written += bytes.len().min(2);
            Ok(self.written)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut io = PartialWriter { written: 0 };
    let mut writer = Framed::new(&mut io, 256).unwrap();
    assert!(writer.send(&health()).is_err());
    assert!(writer.send(&health()).is_err());
    assert_eq!(io.written, 2);
}

#[cfg(feature = "wire-async")]
#[tokio::test]
async fn async_and_blocking_peers_speak_the_same_wire() {
    use wire::AsyncSession;
    let (client, server) = sockets();
    server.set_nonblocking(true).unwrap();
    let server = tokio::net::TcpStream::from_std(server).unwrap();
    let client = tokio::task::spawn_blocking(move || {
        let mut session = Session::connect(client, &offer()).unwrap();
        session.send(&health()).unwrap();
        assert_eq!(session.receive().unwrap(), health());
    });
    let mut session = AsyncSession::accept(server, &offer()).await.unwrap();
    let request = session.receive().await.unwrap();
    session.send(&request).await.unwrap();
    client.await.unwrap();
}

#[cfg(feature = "wire-async")]
#[tokio::test]
async fn cancelling_a_partial_async_read_prevents_stream_reuse() {
    use wire::AsyncSession;
    let (client, server) = tokio::io::duplex(128);
    let hello = offer();
    let (client, server) = tokio::join!(
        AsyncSession::connect(client, &hello),
        AsyncSession::accept(server, &hello),
    );
    let _client = client.unwrap();
    let mut server = server.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), server.receive())
            .await
            .is_err()
    );
    assert!(server.receive().await.is_err());
    assert!(server.send(&health()).await.is_err());
}

#[test]
fn first_binary_version_keeps_its_golden_health_frame() {
    let golden = vec![4, 0, 0, 0, 8, 1, 16, 42];
    let mut io = Memory::default();
    Framed::new(&mut io, 256).unwrap().send(&health()).unwrap();
    assert_eq!(io.output, golden);
    assert_eq!(
        Framed::new(Cursor::new(golden), 256)
            .unwrap()
            .receive::<Control>()
            .unwrap(),
        health()
    );
}

#[test]
fn protoc_golden_bodies_preserve_all_fields_and_explicit_zero() {
    let hello = Hello {
        supported: (1 << 63) | c::HEALTH | c::HANDOFF,
        required: c::HANDOFF,
        ..offer()
    };
    let hello_bytes = include_bytes!("fixtures/wire/hello.bin");
    assert_eq!(Hello::decode_from_slice(hello_bytes).unwrap(), hello);
    assert_eq!(hello.encode_to_vec(), hello_bytes);
    let control = Control {
        operation: o::READY,
        request_id: u64::MAX,
        generation: 1 << 32,
        token: b"\0\xfftoken".to_vec(),
        offset: Some(0),
        payload: b"\xff\0payload".to_vec(),
        ..Default::default()
    };
    let control_bytes = include_bytes!("fixtures/wire/control.bin");
    assert_eq!(Control::decode_from_slice(control_bytes).unwrap(), control);
    assert_eq!(control.encode_to_vec(), control_bytes);
    let absent = Control {
        offset: None,
        ..control
    };
    assert_ne!(absent.encode_to_vec(), control_bytes);
    assert_eq!(
        Control::decode_from_slice(&absent.encode_to_vec())
            .unwrap()
            .offset,
        None
    );
}

#[test]
fn wrong_service_identity_gets_no_server_reply() {
    let mut mismatches = Vec::new();
    let mut remote = offer();
    remote.service_id = vec![2; 16];
    mismatches.push(remote);
    let mut remote = offer();
    remote.application = "different.service".into();
    mismatches.push(remote);
    let mut remote = offer();
    remote.profile = "dev".into();
    mismatches.push(remote);
    let mut remote = offer();
    remote.instance = "other".into();
    mismatches.push(remote);
    let mut remote = offer();
    remote.service_id.clear();
    mismatches.push(remote);
    let mut remote = offer();
    remote.service_id = vec![0; 16];
    mismatches.push(remote);
    let mut remote = offer();
    remote.service_id = vec![1; 15];
    mismatches.push(remote);
    let mut remote = offer();
    remote.service_id = vec![1; 17];
    mismatches.push(remote);
    for remote in mismatches {
        assert!(wire::negotiate(&offer(), &remote).is_err());
        assert!(wire::negotiate(&remote, &offer()).is_err());
        let (mut client, server) = sockets();
        let server =
            std::thread::spawn(move || assert!(Session::accept(server, &offer()).is_err()));
        client.write_all(&wire::MAGIC).unwrap();
        Framed::new(&mut client, 1024)
            .unwrap()
            .send(&remote)
            .unwrap();
        let mut reply = [0; 1];
        assert_eq!(
            client.read(&mut reply).unwrap(),
            0,
            "wrong identity received a reply"
        );
        server.join().unwrap();
    }
}

#[test]
fn application_operation_one_is_separate_from_lifecycle_operation_one() {
    use wire::MessageKind;
    let (client, server) = sockets();
    let mut hello = offer();
    hello.supported = c::HEALTH | c::APPLICATION | (1 << 16);
    hello.required = hello.supported;
    let server_hello = hello.clone();
    let server = std::thread::spawn(move || {
        let mut session = Session::accept(server, &server_hello).unwrap();
        let lifecycle = session.receive().unwrap();
        let application = session.receive().unwrap();
        assert_eq!(lifecycle.operation, 1);
        assert_eq!(lifecycle.kind, MessageKind::Lifecycle);
        assert_eq!(application.operation, 1);
        assert_eq!(application.kind, MessageKind::Application);
    });
    let mut session = Session::connect(client, &hello).unwrap();
    for (kind, operation) in [
        (99.into(), 1),
        (MessageKind::Application.into(), 0),
        (MessageKind::Lifecycle.into(), 1024),
    ] {
        assert!(
            session
                .send(&Control {
                    kind,
                    operation,
                    ..Default::default()
                })
                .is_err()
        );
    }
    session.send(&health()).unwrap();
    session
        .send(&Control {
            kind: MessageKind::Application.into(),
            ..health()
        })
        .unwrap();
    server.join().unwrap();
    let mut unsupported = hello.clone();
    unsupported.supported = c::HEALTH | c::APPLICATION;
    unsupported.required = c::HEALTH;
    assert!(
        wire::negotiate(&hello, &unsupported).is_err(),
        "generic envelope support must not imply support for application operation 1"
    );
}

#[test]
fn malformed_protobuf_and_excess_unknown_fields_poison_the_session() {
    for body in [
        vec![0],          // Invalid field number.
        vec![8, 0x80],    // Truncated varint.
        vec![0x22, 5, 1], // Truncated bytes field.
        [vec![8, 1], [0xa0, 6, 1].repeat(129)].concat(),
        [vec![8, 1], [0xa3, 6].repeat(33), [0xa4, 6].repeat(33)].concat(),
    ] {
        let mut frame = (body.len() as u32).to_le_bytes().to_vec();
        frame.extend(body);
        let mut reader = Framed::new(Cursor::new(frame), 1024).unwrap();
        assert!(reader.receive::<Control>().is_err());
        assert!(reader.send(&health()).is_err());
    }
}

#[test]
fn the_unreleased_non_protobuf_preamble_is_rejected() {
    let (mut client, server) = sockets();
    let server = std::thread::spawn(move || assert!(Session::accept(server, &offer()).is_err()));
    client.write_all(b"KNDAEM02").unwrap();
    assert_eq!(client.read(&mut [0; 1]).unwrap(), 0);
    server.join().unwrap();
}

//! The binary control protocol: session establishment, which every client of
//! a binary endpoint pays on connect, and framed `Control` messages, which
//! every lifecycle or application operation pays in each direction.

use gungraun::prelude::*;
use kunobi_daemon::Lifecycle;
use kunobi_daemon::control::ControlService;
use kunobi_daemon::transport::SplitIo;
use kunobi_daemon::wire::{Control, Health, MAGIC, MessageKind, Session, operation};
use kunobi_daemon_benches::{Discard, frame, offer};
use std::hint::black_box;
use std::io::Cursor;
use std::sync::Arc;

type Io = SplitIo<Cursor<Vec<u8>>, Discard>;

/// Messages sent or received per benchmark, so one frame's fixed cost is
/// measured many times over.
const MESSAGES: usize = 64;

/// A client session whose peer has already answered the handshake, followed
/// by `inbound` in its receive buffer.
fn client_session(inbound: &[u8]) -> Session<Io> {
    let mut script = frame(&offer());
    script.push(1);
    script.extend_from_slice(inbound);
    Session::connect(
        SplitIo {
            read: Cursor::new(script),
            write: Discard::default(),
        },
        &offer(),
    )
    .expect("scripted handshake")
}

fn health_request() -> Control {
    Control {
        operation: operation::HEALTH,
        request_id: 42,
        ..Default::default()
    }
}

/// One message of each shape the protocol carries.
fn message(kind: &str) -> Control {
    let token = b"0123456789abcdef0123456789abcdef".to_vec();
    match kind {
        "health_request" => health_request(),
        "health_reply" => Health {
            process_id: 4242,
            generation: 7,
            build: "1.4.2+0123456789ab".into(),
            revision: 3,
            ready: true,
            draining: false,
            active: 12,
            ..Default::default()
        }
        .response(&health_request())
        .expect("health reply"),
        "handoff_prepare" => Control {
            operation: operation::PREPARE,
            request_id: 43,
            generation: 8,
            token,
            ..Default::default()
        },
        "handoff_ready" => Control {
            operation: operation::READY,
            request_id: 44,
            generation: 8,
            token,
            offset: Some(1_048_576),
            ..Default::default()
        },
        "application_64b" => application(64),
        "application_4k" => application(4096),
        "application_60k" => application(60 * 1024),
        _ => unreachable!("unknown message fixture {kind}"),
    }
}

fn application(size: usize) -> Control {
    Control {
        kind: MessageKind::Application.into(),
        operation: 1,
        request_id: 45,
        payload: vec![42; size],
        ..Default::default()
    }
}

// Handshake: magic, one Hello each way, validation of both offers,
// negotiation and the acceptance bytes.

struct Handshake {
    io: Io,
    offer: kunobi_daemon::wire::Hello,
}

fn connect_script() -> Handshake {
    let mut script = frame(&offer());
    script.push(1);
    Handshake {
        io: SplitIo {
            read: Cursor::new(script),
            write: Discard::default(),
        },
        offer: offer(),
    }
}

fn accept_script() -> Handshake {
    let mut script = MAGIC.to_vec();
    script.extend_from_slice(&frame(&offer()));
    script.push(1);
    Handshake {
        io: SplitIo {
            read: Cursor::new(script),
            write: Discard::default(),
        },
        offer: offer(),
    }
}

#[library_benchmark(setup = connect_script)]
fn session_connect(input: Handshake) -> Session<Io> {
    Session::connect(input.io, black_box(&input.offer)).expect("scripted handshake")
}

#[library_benchmark(setup = accept_script)]
fn session_accept(input: Handshake) -> Session<Io> {
    Session::accept(input.io, black_box(&input.offer)).expect("scripted handshake")
}

// Framed messages. Send encodes into the session's reusable buffer and writes
// one frame; receive reads the length and body, decodes and checks the
// negotiated capability.

struct Outbound {
    session: Session<Io>,
    message: Control,
}

fn outbound(kind: &str) -> Outbound {
    Outbound {
        session: client_session(&[]),
        message: message(kind),
    }
}

fn inbound(kind: &str) -> Session<Io> {
    let one = frame(&message(kind));
    client_session(&one.repeat(MESSAGES))
}

#[library_benchmark]
#[bench::health_request(args = ("health_request"), setup = outbound)]
#[bench::health_reply(args = ("health_reply"), setup = outbound)]
#[bench::handoff_prepare(args = ("handoff_prepare"), setup = outbound)]
#[bench::handoff_ready(args = ("handoff_ready"), setup = outbound)]
#[bench::application_64b(args = ("application_64b"), setup = outbound)]
#[bench::application_4k(args = ("application_4k"), setup = outbound)]
#[bench::application_60k(args = ("application_60k"), setup = outbound)]
fn send(mut input: Outbound) -> Outbound {
    for _ in 0..MESSAGES {
        input
            .session
            .send(black_box(&input.message))
            .expect("in-memory send");
    }
    input
}

#[library_benchmark]
#[bench::health_request(args = ("health_request"), setup = inbound)]
#[bench::health_reply(args = ("health_reply"), setup = inbound)]
#[bench::handoff_prepare(args = ("handoff_prepare"), setup = inbound)]
#[bench::handoff_ready(args = ("handoff_ready"), setup = inbound)]
#[bench::application_64b(args = ("application_64b"), setup = inbound)]
#[bench::application_4k(args = ("application_4k"), setup = inbound)]
#[bench::application_60k(args = ("application_60k"), setup = inbound)]
fn receive(mut session: Session<Io>) -> Session<Io> {
    for _ in 0..MESSAGES {
        black_box(session.receive().expect("in-memory receive"));
    }
    session
}

// A daemon answering health probes: snapshot the lifecycle, build the typed
// Health payload and wrap it in the reply envelope.

struct Probes {
    service: ControlService,
    request: Control,
}

fn probes() -> Probes {
    let service = ControlService::new(
        Arc::new(Lifecycle::default()),
        7,
        "1.4.2+0123456789ab".into(),
        3,
    );
    service.mark_ready();
    Probes {
        service,
        request: health_request(),
    }
}

#[library_benchmark(setup = probes)]
fn control_service_reply(input: Probes) -> Probes {
    for _ in 0..MESSAGES {
        black_box(
            input
                .service
                .handle(black_box(&input.request))
                .expect("health reply"),
        );
    }
    input
}

library_benchmark_group!(
    name = handshake,
    benchmarks = [session_connect, session_accept]
);

library_benchmark_group!(name = frames, benchmarks = [send, receive]);

library_benchmark_group!(name = control_service, benchmarks = [control_service_reply]);

main!(library_benchmark_groups = [handshake, frames, control_service]);

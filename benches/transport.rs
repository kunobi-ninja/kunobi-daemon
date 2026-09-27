//! The byte path between a client and its daemon, and the per-request
//! bookkeeping around it.
//!
//! Each benchmark repeats its operation enough times that the per-call cost
//! dominates; setup (allocating inputs, building the slot) is not measured.

use gungraun::prelude::*;
use kunobi_daemon::Lifecycle;
use kunobi_daemon::admission::{Admission, Limits, Pool};
use kunobi_daemon::observation::{Observations, ObservedIo};
use kunobi_daemon::transport::{self, Outstanding, PumpExit, WriterSlot};
use kunobi_daemon_benches::{Chunked, Discard, payload};
use std::hint::black_box;
use std::io::{Read, Write};
use std::sync::Arc;

// WriterSlot::write_observed runs once per client read on the request path:
// it takes the slot lock, runs the observer and writes one window.

struct Windows {
    slot: WriterSlot<Discard>,
    window: Vec<u8>,
    count: usize,
}

fn windows(size: usize, count: usize) -> Windows {
    Windows {
        slot: WriterSlot::new(Discard::default()),
        window: payload(size),
        count,
    }
}

#[library_benchmark]
#[bench::line_64b_x1000(args = (64, 1000), setup = windows)]
#[bench::window_4k_x256(args = (4096, 256), setup = windows)]
#[bench::window_64k_x16(args = (65_536, 16), setup = windows)]
fn write_observed(input: Windows) -> Windows {
    let mut observed = 0usize;
    for _ in 0..input.count {
        input
            .slot
            .write_observed(black_box(&input.window), || observed += 1);
    }
    black_box(observed);
    input
}

// The pumps copy every byte of a session, one read and one flushed write per
// chunk the transport returns. Small chunks expose per-call overhead; large
// ones expose per-byte work.

struct Stream {
    from: Chunked,
    to: Discard,
    total: usize,
}

fn stream(total: usize, chunk: usize) -> Stream {
    Stream {
        from: Chunked::new(total, chunk),
        to: Discard::default(),
        total,
    }
}

#[library_benchmark]
#[bench::lines_64k_in_64b(args = (65_536, 64), setup = stream)]
#[bench::mib_in_4k(args = (1 << 20, 4096), setup = stream)]
#[bench::mib_in_64k(args = (1 << 20, 65_536), setup = stream)]
fn pump_downstream(mut input: Stream) -> Stream {
    let exit = transport::pump_downstream(&mut input.from, &mut input.to);
    assert_eq!(exit, PumpExit::PeerClosed);
    assert_eq!(input.to.bytes, input.total);
    input
}

#[library_benchmark]
#[bench::lines_64k_in_64b(args = (65_536, 64), setup = stream)]
#[bench::mib_in_4k(args = (1 << 20, 4096), setup = stream)]
#[bench::mib_in_64k(args = (1 << 20, 65_536), setup = stream)]
fn pump_upstream(mut input: Stream) -> Stream {
    transport::pump_upstream(&mut input.from, &mut input.to).expect("in-memory pump");
    assert_eq!(input.to.bytes, input.total);
    input
}

// Outstanding is updated twice per request: begin when it is forwarded,
// settle when its reply reaches the client.

struct Requests<K> {
    outstanding: Outstanding<K>,
    keys: Vec<K>,
    lookup: Vec<K>,
    in_flight: usize,
}

fn numeric_requests(count: u64, in_flight: usize) -> Requests<u64> {
    Requests {
        outstanding: Outstanding::new(1024),
        keys: (0..count).collect(),
        lookup: (0..count).collect(),
        in_flight,
    }
}

fn string_requests(count: u64, in_flight: usize) -> Requests<String> {
    // JSON-RPC ids are often opaque strings of this size.
    let keys: Vec<String> = (0..count).map(|id| format!("req-{id:08x}")).collect();
    Requests {
        outstanding: Outstanding::new(1024),
        lookup: keys.clone(),
        keys,
        in_flight,
    }
}

fn begin_settle<K: Ord>(input: Requests<K>) -> Outstanding<K> {
    let epoch = input.outstanding.epoch();
    let mut keys = input.keys.into_iter();
    for batch in input.lookup.chunks(input.in_flight) {
        for key in keys.by_ref().take(batch.len()) {
            input.outstanding.begin(key);
        }
        for key in batch {
            input.outstanding.settle(epoch, key);
        }
    }
    input.outstanding
}

#[library_benchmark]
#[bench::one_at_a_time_x1000(args = (1000, 1), setup = numeric_requests)]
#[bench::pipelined_64_x1024(args = (1024, 64), setup = numeric_requests)]
fn outstanding_u64(input: Requests<u64>) -> Outstanding<u64> {
    begin_settle(input)
}

#[library_benchmark]
#[bench::pipelined_64_x1024(args = (1024, 64), setup = string_requests)]
fn outstanding_string(input: Requests<String>) -> Outstanding<String> {
    begin_settle(input)
}

// ObservedIo wraps a transport when a consumer attaches observations; every
// read, write and flush then updates shared counters.

struct ObservedWrites {
    io: ObservedIo<Discard>,
    line: Vec<u8>,
    count: usize,
}

fn observed_writes(size: usize, count: usize) -> ObservedWrites {
    ObservedWrites {
        io: ObservedIo::new(Discard::default(), Arc::new(Observations::default())),
        line: payload(size),
        count,
    }
}

#[library_benchmark]
#[bench::line_64b_x1000(args = (64, 1000), setup = observed_writes)]
fn observed_write_flush(mut input: ObservedWrites) -> ObservedWrites {
    for _ in 0..input.count {
        input.io.write_all(black_box(&input.line)).expect("discard");
        input.io.flush().expect("discard");
    }
    input
}

struct ObservedReads {
    io: ObservedIo<Chunked>,
    buffer: Vec<u8>,
}

fn observed_reads(total: usize, chunk: usize) -> ObservedReads {
    ObservedReads {
        io: ObservedIo::new(
            Chunked::new(total, chunk),
            Arc::new(Observations::default()),
        ),
        buffer: vec![0; transport::BUFFER_SIZE],
    }
}

#[library_benchmark]
#[bench::lines_64k_in_64b(args = (65_536, 64), setup = observed_reads)]
fn observed_read(mut input: ObservedReads) -> ObservedReads {
    while input.io.read(&mut input.buffer).expect("in-memory read") != 0 {}
    input
}

// Admission gates: Lifecycle::begin for every request a daemon serves,
// Admission::try_acquire for every connection it accepts.

#[library_benchmark]
#[bench::x1000(args = (1000), setup = lifecycle)]
fn lifecycle_begin_release(input: (Arc<Lifecycle>, usize)) -> Arc<Lifecycle> {
    let (lifecycle, count) = input;
    for _ in 0..count {
        let guard = lifecycle.begin().expect("admission is open");
        drop(black_box(guard));
    }
    lifecycle
}

fn lifecycle(count: usize) -> (Arc<Lifecycle>, usize) {
    (Arc::new(Lifecycle::default()), count)
}

#[library_benchmark]
#[bench::x1000(args = (1000), setup = admission)]
fn admission_acquire_release(input: (Arc<Admission>, usize)) -> Arc<Admission> {
    let (admission, count) = input;
    for _ in 0..count {
        let permit = admission
            .try_acquire(Pool::Application)
            .expect("capacity is free");
        drop(black_box(permit));
    }
    admission
}

fn admission(count: usize) -> (Arc<Admission>, usize) {
    (Arc::new(Admission::new(Limits::default())), count)
}

library_benchmark_group!(name = writer_slot, benchmarks = [write_observed]);

library_benchmark_group!(name = pumps, benchmarks = [pump_downstream, pump_upstream]);

library_benchmark_group!(
    name = outstanding,
    benchmarks = [outstanding_u64, outstanding_string]
);

library_benchmark_group!(
    name = observation,
    benchmarks = [observed_write_flush, observed_read]
);

library_benchmark_group!(
    name = admission_gates,
    benchmarks = [lifecycle_begin_release, admission_acquire_release]
);

main!(
    library_benchmark_groups = [
        writer_slot,
        pumps,
        outstanding,
        observation,
        admission_gates
    ]
);

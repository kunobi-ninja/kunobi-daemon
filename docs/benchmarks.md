# Benchmarks

The benchmarks count the instructions the crate executes on its hot paths:
the byte path between a client and its daemon, framed protocol messages, and
the work a client does before it connects. They use
[Gungraun](https://github.com/gungraun/gungraun), which runs each benchmark
once under Valgrind's Callgrind. An instruction count does not depend on the
machine's load, so two runs of the same code agree.

## Run them locally

You need Linux, Valgrind, and the `gungraun-runner` binary at the same version
as the `gungraun` library in `benches/Cargo.lock`:

```sh
sudo apt-get install valgrind
cargo install --locked gungraun-runner --version 0.20.0
cargo bench --manifest-path benches/Cargo.toml
```

A run compares each benchmark with the previous run and prints the change. To
compare two revisions, save a named baseline on the first and compare the
second with it:

```sh
git switch main
cargo bench --manifest-path benches/Cargo.toml -- --save-baseline=main
git switch my-change
cargo bench --manifest-path benches/Cargo.toml -- --baseline=main
```

Pass a filter such as `-- 'transport::pumps::*'` to run one group. Callgrind
profiles are written under `benches/target/gungraun/`; open them with
`callgrind_annotate` or KCachegrind to see where the instructions go.

## What they measure

| File | Group | What runs |
| --- | --- | --- |
| `transport.rs` | `writer_slot` | `WriterSlot::write_observed` for request lines and larger windows |
| | `pumps` | `pump_downstream` and `pump_upstream` copying 64 KiB to 1 MiB in reads of 64 B to 64 KiB |
| | `outstanding` | `Outstanding::begin` and `settle`, one at a time and 64 in flight, with numeric and string keys |
| | `observation` | `ObservedIo` reads, writes and flushes |
| | `admission_gates` | `Lifecycle::begin` and `Admission::try_acquire`, each released at once |
| `wire.rs` | `handshake` | `Session::connect` and `Session::accept` |
| | `frames` | `Session::send` and `Session::receive` for each kind of `Control` message |
| | `control_service` | `ControlService::handle` answering a health probe |
| `startup.rs` | `paths` | `ServiceIdentity::paths` and `SocketDir` checks |
| | `windows_launch` | The Windows command line and environment block, compiled from the same source on Linux |

Every benchmark runs over in-memory readers and writers, without descriptors,
threads or sleeps, and repeats its operation enough times that the per-call
cost dominates. Setup is not counted. The numbers are instruction counts, not
wall-clock latency, and they do not include the kernel's side of real I/O.

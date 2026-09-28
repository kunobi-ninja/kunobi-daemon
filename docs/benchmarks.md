# Benchmarks

The benchmarks count the instructions the crate executes on its hot paths:
the byte path between a client and its daemon, framed protocol messages, and
the work a client does before it connects. They use
[Gungraun](https://github.com/gungraun/gungraun), which runs each benchmark
once under Valgrind's Callgrind. An instruction count does not depend on the
machine's load, so CI can gate on it.

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

Every benchmark runs over in-memory readers and writers, without descriptors,
threads or sleeps, and repeats its operation enough times that the per-call
cost dominates. Setup is not counted. The numbers are instruction counts, not
wall-clock latency, and they do not include the kernel's side of real I/O.

## The CI gate

The `Benchmarks` job in `.github/workflows/ci.yml` measures two commits in one
job with the same toolchain and runner:

1. On a pull request, the base branch tip and the merge result. On a push to
   `main`, the previous and the new `main`.
2. The first run saves a baseline; the second compares with it.
3. A pull request fails when any benchmark's instruction count grows by more
   than 5%. `BENCH_LIMIT` in the workflow sets the threshold.
4. The job summary lists every benchmark with its base count, head count and
   change, whether or not the gate failed.

A benchmark that the base does not have is reported as new and never fails the
gate. The comparison is also skipped, with a notice, when the base commit uses
a different Gungraun version, because the runner only runs its own version.

The threshold applies to each benchmark on its own, so an improvement in one
cannot hide a regression in another. When the growth is expected, for example a
correctness fix that must do more work, explain it in the pull request; the
table shows reviewers which paths grew and by how much.

## Real subprocess and socket measurements

On Linux or macOS, run:

```sh
cargo run --release --features wire-async,launch --example ipc_bench -- 200 8
```

The arguments are samples per client and concurrent clients. Output is JSONL:

- Process start through a readiness notification and a verified health response.
- Connect, negotiate, and health round-trip latency at one client and the chosen concurrency.
- Daemon CPU time and voluntary context switches during one idle second.

The server is a separate process using Unix sockets and the production session,
health, and launch primitives. Every health sample establishes a connection;
these numbers do not describe an already connected application stream. Startup
uses five samples and includes ordinary OS executable-cache effects; it does not
force a cold disk cache. Idle counters include the bounding counter requests.
Voluntary context switches are not a hardware wakeup count. Windows named-pipe
measurements are not implemented by this harness.

Record OS, architecture, build profile, sample count, and system load alongside
results. Run the same harness on both revisions and repeat measurements before
claiming an improvement. Wall-clock results are diagnostic and have no fixed CI
threshold; the instruction-count gate above remains the regression gate.

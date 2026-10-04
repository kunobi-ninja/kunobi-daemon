//! Wall-clock IPC benchmark. Run with --release; see docs/benchmarks.md.
#[cfg(unix)]
mod unix {
    use kunobi_daemon::{
        Lifecycle, ServiceIdentity, client,
        control::ControlService,
        launch::{DaemonChild, DaemonCommand},
        local::{Duplex, peer::PeerCredentials, unix::UnixDuplex},
        peer::{Policy, SameUser},
        transport::SplitIo,
        wire::{self, Control, Hello, capability, operation},
    };
    use std::{
        io,
        path::{Path, PathBuf},
        sync::{Arc, Barrier},
        time::{Duration, Instant},
    };

    const BUDGET: Duration = Duration::from_secs(10);
    const STATS: u32 = 1;
    fn offer() -> Hello {
        Hello::new(
            &ServiceIdentity::new([0x62; 16], "ipc-bench", "bench", "one").unwrap(),
            capability::HEALTH
                | capability::HEALTH_DETAILS
                | capability::DRAIN
                | capability::APPLICATION,
            capability::HEALTH | capability::HEALTH_DETAILS,
        )
    }
    fn request(path: &Path, operation: u32, application: bool) -> io::Result<Control> {
        if !application {
            let request = if operation == operation::DRAIN {
                client::drain
            } else {
                client::health
            };
            let health =
                request(path, &offer(), None, Instant::now() + BUDGET).map_err(io::Error::other)?;
            if operation == operation::HEALTH && !health.ready {
                return Err(io::Error::other("peer is not ready"));
            }
            return health.response(&Control {
                operation,
                request_id: 1,
                ..Default::default()
            });
        }
        let stream = UnixDuplex::connect_once_until(path, Instant::now() + BUDGET)
            .map_err(io::Error::other)?;
        SameUser
            .grant(&stream.credentials()?)
            .map_err(io::Error::other)?;
        stream.set_read_deadline(Some(BUDGET))?;
        let (read, write) = stream.split()?;
        let mut session = wire::Session::connect(SplitIo { read, write }, &offer())?;
        let request = Control {
            operation,
            request_id: 1,
            kind: if application {
                wire::MessageKind::Application
            } else {
                wire::MessageKind::Lifecycle
            }
            .into(),
            ..Default::default()
        };
        session.send(&request)?;
        let reply = session.receive()?;
        Ok(reply)
    }

    // Voluntary context switches are reported explicitly: they are a useful
    // idle signal, but are not a count of hardware or scheduler wakeups.
    #[allow(unsafe_code)]
    fn usage() -> io::Result<[u64; 3]> {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes a valid writable rusage on success.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: getrusage succeeded above.
        let usage = unsafe { usage.assume_init() };
        let micros = |value: libc::timeval| value.tv_sec as u64 * 1_000_000 + value.tv_usec as u64;
        Ok([
            micros(usage.ru_utime),
            micros(usage.ru_stime),
            usage.ru_nvcsw as u64,
        ])
    }
    fn stats(path: &Path) -> io::Result<[u64; 3]> {
        let reply = request(path, STATS, true)?;
        if reply.payload.len() != 24 {
            return Err(io::Error::other("invalid benchmark counters"));
        }
        Ok(std::array::from_fn(|i| {
            u64::from_le_bytes(reply.payload[i * 8..i * 8 + 8].try_into().unwrap())
        }))
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "One absolute watchdog bounds the lifetime of a benchmark subprocess if its client dies."
    )]
    async fn serve(
        path: PathBuf,
        readiness: Option<kunobi_daemon::readiness::channel::Notifier>,
    ) -> io::Result<()> {
        let listener = tokio::net::UnixListener::bind(path)?;
        let lifecycle = Arc::new(Lifecycle::default());
        let service = Arc::new(ControlService::new(
            Arc::clone(&lifecycle),
            1,
            "bench".into(),
            1,
        ));
        service.mark_ready();
        if let Some(readiness) = readiness {
            readiness.ready()?;
        }
        let mut tasks = tokio::task::JoinSet::new();
        // Reclaim the subprocess even if a benchmark client crashes.
        let expires = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(expires) => break,
                _ = lifecycle.draining() => break,
                Some(result) = tasks.join_next(), if !tasks.is_empty() => { result??; },
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let service = Arc::clone(&service);
                    tasks.spawn(async move {
                        tokio::time::timeout(BUDGET, async {
                            let mut session = wire::AsyncSession::accept(stream, &offer()).await?;
                            let request = session.receive().await?;
                            let reply = if request.kind == wire::MessageKind::Application && request.operation == STATS {
                                Control { payload: usage()?.into_iter().flat_map(u64::to_le_bytes).collect(), ..request }
                            } else { service.handle(&request)? };
                            session.send(&reply).await
                        }).await.map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
                    });
                }
            }
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        Ok(())
    }

    struct Server {
        child: DaemonChild,
        path: PathBuf,
    }
    impl Server {
        fn start(path: PathBuf) -> io::Result<Self> {
            let mut command = DaemonCommand::new(std::env::current_exe()?);
            command.arg("--server").arg(&path).readiness_channel();
            let child = command.spawn()?;
            let mut server = Self { child, path };
            server
                .child
                .take_readiness()
                .unwrap()
                .wait(BUDGET)
                .map_err(io::Error::other)?;
            request(&server.path, operation::HEALTH, false)?;
            Ok(server)
        }
    }
    impl Drop for Server {
        fn drop(&mut self) {
            let _ = request(&self.path, operation::DRAIN, false);
            let _ = self.child.wait_until(Instant::now() + BUDGET);
        }
    }
    fn report(name: &str, mut samples: Vec<Duration>, elapsed: Duration) {
        samples.sort_unstable();
        let percentile = |p: usize| {
            samples[(samples.len() * p).div_ceil(100).saturating_sub(1)].as_secs_f64() * 1e6
        };
        println!(
            "{{\"scenario\":\"{name}\",\"samples\":{},\"p50_us\":{:.3},\"p95_us\":{:.3},\"p99_us\":{:.3},\"operations_per_second\":{:.3}}}",
            samples.len(),
            percentile(50),
            percentile(95),
            percentile(99),
            samples.len() as f64 / elapsed.as_secs_f64()
        );
    }
    fn load(path: &Path, clients: usize, samples: usize) -> io::Result<()> {
        let barrier = Arc::new(Barrier::new(clients + 1));
        let threads: Vec<_> = (0..clients)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                let path = path.to_owned();
                std::thread::spawn(move || -> io::Result<Vec<Duration>> {
                    barrier.wait();
                    (0..samples)
                        .map(|_| {
                            let start = Instant::now();
                            request(&path, operation::HEALTH, false)?;
                            Ok(start.elapsed())
                        })
                        .collect()
                })
            })
            .collect();
        let start = Instant::now();
        barrier.wait();
        let mut times = Vec::new();
        for thread in threads {
            times.extend(
                thread
                    .join()
                    .map_err(|_| io::Error::other("benchmark thread panicked"))??,
            );
        }
        report(&format!("health_{clients}_clients"), times, start.elapsed());
        Ok(())
    }
    pub fn main() -> Result<(), Box<dyn std::error::Error>> {
        // SAFETY: first operation, before starting any thread or runtime. Only
        // the harness's DaemonCommand passes an owned readiness descriptor.
        #[allow(unsafe_code)]
        let readiness = unsafe { kunobi_daemon::readiness::channel::Notifier::from_env() }?;
        let args: Vec<_> = std::env::args().skip(1).collect();
        if args.first().map(String::as_str) == Some("--server") {
            let path = args.get(1).ok_or("missing socket path")?;
            return Ok(tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?
                .block_on(serve(path.into(), readiness))?);
        }
        let samples: usize = args.first().map(|s| s.parse()).transpose()?.unwrap_or(200);
        let clients: usize = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(8);
        if !(1..=10_000).contains(&samples) || !(1..=64).contains(&clients) {
            return Err("usage: ipc_bench [samples-per-client:1..10000] [clients:1..64]".into());
        }
        // Keep the path below macOS's Unix socket limit even with a long TMPDIR.
        let root = tempfile::Builder::new()
            .prefix("daemon-ipc-")
            .tempdir_in("/tmp")?;
        let mut startup = Vec::new();
        for index in 0..5 {
            let start = Instant::now();
            let server = Server::start(root.path().join(format!("start-{index}.sock")))?;
            startup.push(start.elapsed());
            drop(server);
        }
        let total = startup.iter().sum();
        report("process_start_to_verified_ready", startup, total);
        let server = Server::start(root.path().join("load.sock"))?;
        load(&server.path, 1, samples)?;
        load(&server.path, clients, samples)?;
        let before = stats(&server.path)?;
        let start = Instant::now();
        #[expect(
            clippy::disallowed_methods,
            reason = "The benchmark measures one second with an idle daemon."
        )]
        std::thread::sleep(Duration::from_secs(1));
        let after = stats(&server.path)?;
        println!(
            "{{\"scenario\":\"idle\",\"seconds\":{:.6},\"user_cpu_us\":{},\"system_cpu_us\":{},\"voluntary_context_switches\":{}}}",
            start.elapsed().as_secs_f64(),
            after[0].saturating_sub(before[0]),
            after[1].saturating_sub(before[1]),
            after[2].saturating_sub(before[2])
        );
        Ok(())
    }
}
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    unix::main()
}
#[cfg(not(unix))]
fn main() {
    eprintln!(
        "ipc_bench currently measures Unix sockets; Windows named-pipe measurements are not implemented."
    );
    std::process::exit(1);
}

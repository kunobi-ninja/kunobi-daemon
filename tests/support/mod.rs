//! Real child processes; a small test protocol supplies the application adapters.

use kunobi_daemon::transport::{PumpExit, WriteHalf, WriterSlot, pump_downstream};
use kunobi_daemon::{DrainOutcome, Lifecycle, ProcessLock, publish_record};
use std::{
    collections::BTreeSet,
    fs::OpenOptions,
    io::{self, BufRead, Write},
    net::{Shutdown, TcpStream as StdStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};

pub const BUDGET: Duration = Duration::from_secs(20);
pub type Peer = BufReader<TcpStream>;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Identity {
    pub pid: u32,
    pub build: u64,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid fixture frame")
}

fn identity(line: &str) -> io::Result<Identity> {
    let mut words = line.split_whitespace();
    if words.next() != Some("IDENTITY") {
        return Err(invalid());
    }
    Ok(Identity {
        pid: words
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?,
        build: words
            .next()
            .ok_or_else(invalid)?
            .parse()
            .map_err(|_| invalid())?,
    })
}

pub async fn read_line(peer: &mut Peer) -> io::Result<String> {
    let mut line = String::new();
    let n = timeout(BUDGET, peer.read_line(&mut line)).await??;
    if n == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(line.trim_end().to_owned())
}

pub async fn send(peer: &mut Peer, line: &str) -> io::Result<()> {
    timeout(BUDGET, async {
        peer.get_mut().write_all(line.as_bytes()).await?;
        peer.get_mut().write_all(b"\n").await?;
        peer.get_mut().flush().await
    })
    .await?
}

fn disconnected(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
    )
}

pub async fn connect(root: &Path) -> io::Result<Option<(Identity, Peer)>> {
    let address = match std::fs::read_to_string(root.join("daemon.addr")) {
        Ok(address) => address,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let socket = match timeout(BUDGET, TcpStream::connect(address.trim())).await? {
        Ok(socket) => socket,
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(e),
    };
    let mut peer = BufReader::new(socket);
    if let Err(error) = send(&mut peer, "HEALTH").await {
        return if disconnected(&error) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    match read_line(&mut peer).await {
        Ok(line) => Ok(Some((identity(&line)?, peer))),
        Err(e) if disconnected(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

pub async fn wait_file(path: &Path) {
    timeout(BUDGET, async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("missing fixture signal: {}", path.display()));
}

struct ChildGuard {
    child: Child,
    log: tempfile::NamedTempFile,
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let status = self.child.wait();
        if std::thread::panicking() || matches!(status, Ok(s) if !s.success()) {
            eprintln!(
                "fixture pid {}: {:?}\n{}",
                self.child.id(),
                status,
                std::fs::read_to_string(self.log.path()).unwrap_or_default()
            );
        }
    }
}

pub struct Fixture {
    // Reap children before removing their directory (also required on Windows).
    children: Mutex<Vec<ChildGuard>>,
    root: tempfile::TempDir,
}

impl Fixture {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            root: tempfile::tempdir().unwrap(),
            children: Mutex::new(Vec::new()),
        })
    }
    pub fn root(&self) -> &Path {
        self.root.path()
    }
    pub fn spawn(&self, role: &str, build: u64) -> u32 {
        let log = tempfile::NamedTempFile::new_in(self.root()).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fixture_process", "--ignored", "--nocapture"])
            .env("KUNOBI_DAEMON_E2E_ROOT", self.root())
            .env("KUNOBI_DAEMON_E2E_ROLE", role)
            .env("KUNOBI_DAEMON_E2E_BUILD", build.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.as_file().try_clone().unwrap()))
            .stderr(Stdio::from(log.as_file().try_clone().unwrap()))
            .spawn()
            .unwrap();
        let pid = child.id();
        self.children
            .lock()
            .unwrap()
            .push(ChildGuard { child, log });
        pid
    }
    pub async fn start(&self, build: u64) -> Identity {
        let pid = self.spawn("daemon", build);
        self.wait_identity(pid).await
    }
    pub async fn wait_identity(&self, pid: u32) -> Identity {
        timeout(BUDGET, async {
            loop {
                if let Some((id, _)) = connect(self.root()).await.unwrap()
                    && id.pid == pid
                {
                    return id;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("daemon did not publish live identity")
    }
    pub async fn relay(&self) -> (u32, Peer) {
        let pid = self.spawn("relay", 1);
        let ready = self.root().join(format!("relay-{pid}.addr"));
        wait_file(&ready).await;
        let address = std::fs::read_to_string(ready).unwrap();
        let stream = timeout(BUDGET, TcpStream::connect(address.trim()))
            .await
            .unwrap()
            .unwrap();
        (pid, BufReader::new(stream))
    }
    pub fn kill(&self, pid: u32) {
        let mut children = self.children.lock().unwrap();
        let child = &mut children
            .iter_mut()
            .find(|c| c.child.id() == pid)
            .unwrap()
            .child;
        child.kill().unwrap();
        child.wait().unwrap();
    }
    pub fn alive(&self, pid: u32) -> bool {
        self.children
            .lock()
            .unwrap()
            .iter_mut()
            .find(|c| c.child.id() == pid)
            .unwrap()
            .child
            .try_wait()
            .unwrap()
            .is_none()
    }
    pub async fn exited_cleanly(&self, pid: u32) {
        timeout(BUDGET, async {
            loop {
                let status = self
                    .children
                    .lock()
                    .unwrap()
                    .iter_mut()
                    .find(|c| c.child.id() == pid)
                    .unwrap()
                    .child
                    .try_wait()
                    .unwrap();
                if let Some(status) = status {
                    assert!(status.success(), "daemon failed: {status}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("daemon did not finish draining");
    }
    pub async fn request_drain(&self) -> io::Result<()> {
        if let Some((_, mut peer)) = connect(self.root()).await? {
            send(&mut peer, "DRAIN").await?;
            assert_eq!(read_line(&mut peer).await?, "DRAINING");
        }
        Ok(())
    }
}

pub fn child_main() {
    let root = PathBuf::from(std::env::var_os("KUNOBI_DAEMON_E2E_ROOT").unwrap());
    let build: u64 = std::env::var("KUNOBI_DAEMON_E2E_BUILD")
        .unwrap()
        .parse()
        .unwrap();
    match std::env::var("KUNOBI_DAEMON_E2E_ROLE").unwrap().as_str() {
        "daemon" => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(daemon(root, build))
            .unwrap(),
        "relay" => relay(root, build).unwrap(),
        _ => panic!("unknown fixture role"),
    }
}

async fn daemon(root: PathBuf, build: u64) -> io::Result<()> {
    let pid = std::process::id();
    std::fs::write(root.join(format!("attempted-lock-{pid}")), b"")?;
    let _owner = timeout(BUDGET, async {
        loop {
            if let Some(owner) = ProcessLock::try_acquire(root.join("run.lock"))? {
                return Ok::<_, io::Error>(owner);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let record = root.join("daemon.addr");
    publish_record(&record, listener.local_addr()?.to_string().as_bytes())?;
    let lifecycle = Arc::new(Lifecycle::default());
    let mut handlers = tokio::task::JoinSet::new();
    #[cfg(feature = "wire-async")]
    {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        publish_record(
            &root.join("daemon.v2.addr"),
            listener.local_addr()?.to_string().as_bytes(),
        )?;
        let lifecycle = Arc::clone(&lifecycle);
        let root = root.clone();
        handlers.spawn(async move { binary_listener(listener, lifecycle, root, build).await });
    }
    loop {
        tokio::select! {
            biased;
            _ = lifecycle.draining() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let lifecycle = Arc::clone(&lifecycle);
                let root = root.clone();
                handlers.spawn(async move { handle(stream, lifecycle, root, build).await });
            }
        }
    }
    drop(listener);
    assert_eq!(
        lifecycle.drain_until(Instant::now() + BUDGET).await,
        DrainOutcome::Complete
    );
    timeout(BUDGET, async {
        while let Some(result) = handlers.join_next().await {
            result.unwrap().unwrap();
        }
    })
    .await?;
    // The run guard remains held through discovery cleanup.
    std::fs::remove_file(record)?;
    #[cfg(feature = "wire-async")]
    std::fs::remove_file(root.join("daemon.v2.addr"))?;
    Ok(())
}

async fn handle(
    stream: TcpStream,
    lifecycle: Arc<Lifecycle>,
    root: PathBuf,
    build: u64,
) -> io::Result<()> {
    let mut peer = BufReader::new(stream);
    let pid = std::process::id();
    loop {
        let mut line = String::new();
        let n = tokio::select! {
            biased;
            _ = lifecycle.draining() => return Ok(()),
            n = peer.read_line(&mut line) => n?,
        };
        if n == 0 {
            return Ok(());
        }
        if line.trim() == "HEALTH" {
            send(&mut peer, &format!("IDENTITY {pid} {build}")).await?;
        } else if line.trim() == "DRAIN" {
            // Count the acknowledgement so shutdown cannot truncate it.
            let _ack = lifecycle.begin().ok_or_else(invalid)?;
            lifecycle.start_drain();
            std::fs::write(root.join(format!("draining-{pid}")), b"")?;
            send(&mut peer, "DRAINING").await?;
            return Ok(());
        } else {
            let mut words = line.split_whitespace();
            if words.next() != Some("CALL") {
                return Err(invalid());
            }
            let id: u64 = words
                .next()
                .ok_or_else(invalid)?
                .parse()
                .map_err(|_| invalid())?;
            let Some((_request, reply)) =
                call(&root, &lifecycle, id, words.next() == Some("hold"), build).await?
            else {
                return Ok(());
            };
            send(&mut peer, &reply).await?;
        }
    }
}

// Both wire adapters hold this same request guard through the response flush.
async fn call(
    root: &Path,
    lifecycle: &Arc<Lifecycle>,
    id: u64,
    hold: bool,
    build: u64,
) -> io::Result<Option<(kunobi_daemon::RequestGuard, String)>> {
    let Some(request) = lifecycle.begin() else {
        return Ok(None);
    };
    let pid = std::process::id();
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join(format!("accepted-{id}")))?;
    writeln!(log, "{pid}")?;
    log.sync_all()?;
    publish_record(&root.join(format!("accepted-ready-{id}")), b"ready")?;
    if hold {
        wait_file(&root.join(format!("release-{id}"))).await;
    }
    Ok(Some((request, format!("OK {id} {pid} {build}"))))
}

#[cfg(feature = "wire-async")]
async fn binary_listener(
    listener: TcpListener,
    lifecycle: Arc<Lifecycle>,
    root: PathBuf,
    build: u64,
) -> io::Result<()> {
    let mut handlers = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = lifecycle.draining() => break,
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let lifecycle = Arc::clone(&lifecycle);
                let root = root.clone();
                handlers.spawn(async move { binary_handle(stream, lifecycle, root, build).await });
            }
        }
    }
    drop(listener);
    while let Some(result) = handlers.join_next().await {
        result.unwrap()?;
    }
    Ok(())
}

#[cfg(feature = "wire-async")]
async fn binary_handle(
    stream: TcpStream,
    lifecycle: Arc<Lifecycle>,
    root: PathBuf,
    build: u64,
) -> io::Result<()> {
    use kunobi_daemon::wire::{AsyncSession, Hello, capability, operation};
    stream.set_nodelay(true)?;
    let offer = Hello::new("fixture", capability::APPLICATION | capability::HEALTH, 0);
    let mut session = timeout(BUDGET, AsyncSession::accept(stream, &offer)).await??;
    loop {
        let message = tokio::select! {
            biased;
            _ = lifecycle.draining() => return Ok(()),
            message = session.receive() => message?,
        };
        if message.operation == operation::HEALTH {
            session
                .send(&kunobi_daemon::wire::Control {
                    payload: format!("IDENTITY {} {build}", std::process::id()).into_bytes(),
                    ..message
                })
                .await?;
        } else {
            if message.operation != operation::APPLICATION_START {
                return Err(invalid());
            }
            let hold = match message.payload.as_slice() {
                b"hold" => true,
                b"ok" => false,
                _ => return Err(invalid()),
            };
            let Some((_request, reply)) =
                call(&root, &lifecycle, message.request_id, hold, build).await?
            else {
                return Ok(());
            };
            session
                .send(&kunobi_daemon::wire::Control {
                    payload: reply.into_bytes(),
                    ..message
                })
                .await?;
        }
    }
}

struct SocketWriter(StdStream);
impl Write for SocketWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl WriteHalf for SocketWriter {
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.0.shutdown(Shutdown::Write)
    }
}

fn connect_sync(
    root: &Path,
    minimum: u64,
    previous: Option<u32>,
) -> io::Result<(Identity, StdStream)> {
    let deadline = std::time::Instant::now() + BUDGET;
    loop {
        if let Ok(address) = std::fs::read_to_string(root.join("daemon.addr")) {
            let address = address.trim().parse().map_err(|_| invalid())?;
            if let Ok(mut socket) = StdStream::connect_timeout(&address, Duration::from_millis(100))
            {
                socket.set_read_timeout(Some(BUDGET))?;
                socket.set_write_timeout(Some(BUDGET))?;
                socket.write_all(b"HEALTH\n")?;
                let mut reader = std::io::BufReader::new(socket);
                let mut line = String::new();
                if reader.read_line(&mut line).is_ok()
                    && let Ok(id) = identity(&line)
                    && id.build >= minimum
                    && previous != Some(id.pid)
                {
                    let socket = reader.into_inner();
                    socket.set_read_timeout(None)?;
                    return Ok((id, socket));
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(io::ErrorKind::TimedOut.into());
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

// The fixture protocol owns IDs; the shared pump remains byte-oriented.
struct Responses {
    client: StdStream,
    pending: Arc<Mutex<BTreeSet<u64>>>,
    frame: Vec<u8>,
}
impl Write for Responses {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for &byte in bytes {
            if self.frame.len() >= 4096 {
                return Err(invalid());
            }
            self.frame.push(byte);
            if byte == b'\n' {
                let line = std::str::from_utf8(&self.frame).map_err(|_| invalid())?;
                let id = line
                    .split_whitespace()
                    .nth(1)
                    .ok_or_else(invalid)?
                    .parse()
                    .map_err(|_| invalid())?;
                self.pending.lock().unwrap().remove(&id);
                self.frame.clear();
            }
        }
        self.client.write_all(bytes)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.client.flush()
    }
}

fn relay(root: PathBuf, minimum: u64) -> io::Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    publish_record(
        &root.join(format!("relay-{}.addr", std::process::id())),
        listener.local_addr()?.to_string().as_bytes(),
    )?;
    let (client, _) = listener.accept()?;
    client.set_write_timeout(Some(BUDGET))?;
    let (mut current, mut peer) = connect_sync(&root, minimum, None)?;
    let writer = Arc::new(WriterSlot::new(SocketWriter(peer.try_clone()?)));
    let pending = Arc::new(Mutex::new(BTreeSet::new()));
    let upstream = {
        let writer = Arc::clone(&writer);
        let pending = Arc::clone(&pending);
        let client = client.try_clone()?;
        std::thread::spawn(move || {
            let mut client = std::io::BufReader::new(client);
            loop {
                let mut line = String::new();
                if client.read_line(&mut line).unwrap() == 0 {
                    writer.shutdown();
                    return;
                }
                let id: u64 = line.split_whitespace().nth(1).unwrap().parse().unwrap();
                writer.write_observed(line.as_bytes(), || {
                    pending.lock().unwrap().insert(id);
                });
            }
        })
    };
    let mut out = Responses {
        client,
        pending,
        frame: Vec::new(),
    };
    loop {
        match pump_downstream(&mut peer, &mut out) {
            PumpExit::ClientGone => {
                writer.close();
                return Ok(());
            }
            PumpExit::PeerClosed => {}
        }
        out.frame.clear();
        let ids = std::mem::take(&mut *out.pending.lock().unwrap());
        for id in ids {
            writeln!(out.client, "UNCERTAIN {id}")?;
        }
        out.client.flush()?;
        if upstream.is_finished() {
            writer.close();
            upstream.join().unwrap();
            return Ok(());
        }
        let (next, socket) = connect_sync(&root, minimum, Some(current.pid))?;
        writer.replace(SocketWriter(socket.try_clone()?));
        std::fs::write(
            root.join(format!(
                "relay-{}-connected-{}",
                std::process::id(),
                next.pid
            )),
            b"",
        )?;
        current = next;
        peer = socket;
    }
}

//! macOS and the BSDs: a kqueue watching `EVFILT_PROC` for `NOTE_EXIT`.
//!
//! The filter attaches to the process itself when it is registered, so a PID
//! reused later does not reach it. Registering for a process that no longer
//! runs, a zombie included, fails with `ESRCH` on macOS; a BSD may instead
//! report a zombie's exit at once. Either reads as exited. A kqueue is not
//! inherited across `fork`, so a child never holds one of these.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::time::{Duration, Instant};

use super::Opened;

#[derive(Debug)]
pub(super) struct Event {
    kqueue: OwnedFd,
}

pub(super) fn open(pid: u32) -> io::Result<Opened> {
    let pid =
        libc::pid_t::try_from(pid).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: kqueue(2) takes no arguments and returns a new descriptor or -1.
    let kqueue = unsafe { libc::kqueue() };
    if kqueue == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the kernel just returned this descriptor, and nothing else owns it.
    let kqueue = unsafe { OwnedFd::from_raw_fd(kqueue) };

    // SAFETY: `kevent` is plain data; all zeroes is a valid value with a null
    // `udata`.
    let mut change: libc::kevent = unsafe { std::mem::zeroed() };
    change.ident = pid as libc::uintptr_t;
    change.filter = libc::EVFILT_PROC;
    change.flags = libc::EV_ADD | libc::EV_ONESHOT;
    change.fflags = libc::NOTE_EXIT;
    // SAFETY: one initialized change for a kqueue this function owns. The
    // event list is empty, so the kernel writes nothing back, and a
    // registration error is returned through errno.
    let registered = unsafe {
        libc::kevent(
            kqueue.as_raw_fd(),
            &change,
            1,
            ptr::null_mut(),
            0,
            ptr::null(),
        )
    };
    if registered == -1 {
        let error = io::Error::last_os_error();
        // EINTR still applies every change in the list.
        if error.kind() != io::ErrorKind::Interrupted {
            return match error.raw_os_error() {
                Some(libc::ESRCH) => Ok(Opened::Gone),
                // FreeBSD refuses another user's process; a sandbox may
                // refuse the filter.
                Some(libc::EPERM | libc::EACCES) => Ok(Opened::Unsupported),
                _ => Err(error),
            };
        }
    }
    Ok(Opened::Watching(Event { kqueue }))
}

impl Event {
    pub(super) fn wait(&mut self, deadline: Option<Instant>) -> io::Result<bool> {
        next_exit(self.kqueue.as_raw_fd(), deadline)
    }

    #[cfg(feature = "async")]
    pub(super) async fn exited(&mut self) -> io::Result<()> {
        use std::os::fd::AsFd;
        use tokio::io::{Interest, unix::AsyncFd};
        if next_exit(self.kqueue.as_raw_fd(), Some(Instant::now()))? {
            return Ok(());
        }
        // A kqueue is itself readable while it holds an event, so the
        // runtime's own kqueue can watch this one.
        let registered = AsyncFd::with_interest(self.kqueue.as_fd(), Interest::READABLE)?;
        loop {
            let mut ready = registered.readable().await?;
            if next_exit(self.kqueue.as_raw_fd(), Some(Instant::now()))? {
                return Ok(());
            }
            ready.clear_ready();
        }
    }
}

/// Take the exit event from `kqueue`, waiting until `deadline` or without
/// one. True once the process exited. The event is delivered once; the
/// caller remembers it.
fn next_exit(kqueue: RawFd, deadline: Option<Instant>) -> io::Result<bool> {
    loop {
        let timeout =
            deadline.map(|deadline| timespec(deadline.saturating_duration_since(Instant::now())));
        // SAFETY: `kevent` is plain data; all zeroes is a valid output slot.
        let mut event: libc::kevent = unsafe { std::mem::zeroed() };
        // SAFETY: no changes; one writable event slot that outlives the call;
        // the timeout, when given, is a local that outlives it too.
        let count = unsafe {
            libc::kevent(
                kqueue,
                ptr::null(),
                0,
                &mut event,
                1,
                timeout.as_ref().map_or(ptr::null(), ptr::from_ref),
            )
        };
        if count == -1 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if count == 0 {
            if deadline.is_none_or(|deadline| Instant::now() >= deadline) {
                return Ok(false);
            }
            continue;
        }
        if event.flags & libc::EV_ERROR != 0 {
            return Err(io::Error::from_raw_os_error(event.data as i32));
        }
        if event.fflags & libc::NOTE_EXIT != 0 {
            return Ok(true);
        }
    }
}

fn timespec(left: Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: libc::time_t::try_from(left.as_secs()).unwrap_or(libc::time_t::MAX),
        tv_nsec: left.subsec_nanos() as libc::c_long,
    }
}

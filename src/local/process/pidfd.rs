//! Linux and Android: a pidfd, which becomes readable once its process exits.
//!
//! `pidfd_open` arrived in Linux 5.3. Older kernels return `ENOSYS`, and a
//! seccomp policy that predates the call (older container runtimes) returns
//! `EPERM`, which `pidfd_open` never does on its own. Both leave the handle
//! following the PID instead.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::Instant;

use super::{Opened, millis_until};


#[derive(Debug)]
pub(super) struct Event {
    pidfd: OwnedFd,
}

pub(super) fn open(pid: u32) -> io::Result<Opened> {
    let pid =
        libc::pid_t::try_from(pid).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    // SAFETY: pidfd_open(2) takes a PID and a flags word by value and reads no
    // memory of ours. It returns a new descriptor, always close-on-exec, or -1
    // with errno set.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0 as libc::c_uint) };
    if fd == -1 {
        return opened_after(io::Error::last_os_error());
    }
    // SAFETY: the kernel just returned this descriptor, and nothing else owns it.
    let pidfd = unsafe { OwnedFd::from_raw_fd(fd as RawFd) };
    Ok(Opened::Watching(Event { pidfd }))
}

/// What a failed `pidfd_open` says about the process.
fn opened_after(error: io::Error) -> io::Result<Opened> {
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(Opened::Gone),
        Some(libc::ENOSYS | libc::EPERM) => Ok(Opened::Unsupported),
        _ => Err(error),
    }
}

impl Event {
    pub(super) fn wait(&mut self, deadline: Option<Instant>) -> io::Result<bool> {
        wait_readable(self.pidfd.as_raw_fd(), deadline)
    }

    #[cfg(feature = "async")]
    pub(super) async fn exited(&mut self) -> io::Result<()> {
        use std::os::fd::AsFd;
        use tokio::io::{Interest, unix::AsyncFd};
        if wait_readable(self.pidfd.as_raw_fd(), Some(Instant::now()))? {
            return Ok(());
        }
        let registered = AsyncFd::with_interest(self.pidfd.as_fd(), Interest::READABLE)?;
        loop {
            let mut ready = registered.readable().await?;
            if wait_readable(self.pidfd.as_raw_fd(), Some(Instant::now()))? {
                return Ok(());
            }
            ready.clear_ready();
        }
    }
}

/// Poll `fd` for readability until `deadline`, or without one. True once the
/// process exited. A poll timeout rounded up to whole milliseconds never ends
/// before the deadline, so a poll that reports nothing means it has passed.
fn wait_readable(fd: RawFd, deadline: Option<Instant>) -> io::Result<bool> {
    let revents = restarting(|| {
        let mut entry = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll receives one initialized entry for a descriptor the
        // caller keeps open for the duration of the call.
        cvt(unsafe { libc::poll(&mut entry, 1, poll_timeout(deadline)) })?;
        Ok(entry.revents)
    })?;
    exited_from(revents)
}

/// `poll`'s timeout argument: -1 waits without a deadline.
fn poll_timeout(deadline: Option<Instant>) -> libc::c_int {
    deadline.map_or(-1, |deadline| {
        libc::c_int::try_from(millis_until(deadline, Instant::now())).unwrap_or(libc::c_int::MAX)
    })
}

/// Whether the returned events report an exit. None at all is a timeout.
///
/// POLLIN means the process exited; newer kernels add POLLHUP once it has
/// also been reaped. POLLERR or POLLNVAL means the descriptor itself failed.
/// Each bit is tested on its own: OR-ing disjoint flags into one mask gives
/// the same value as XOR, a change no test could tell apart.
fn exited_from(revents: libc::c_short) -> io::Result<bool> {
    if revents & libc::POLLERR != 0 || revents & libc::POLLNVAL != 0 {
        return Err(io::Error::other(format!(
            "polling the pidfd failed with events {revents:#x}"
        )));
    }
    Ok(revents & libc::POLLIN != 0 || revents & libc::POLLHUP != 0)
}

fn cvt(result: libc::c_int) -> io::Result<libc::c_int> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

/// Run `call` again for as long as a signal interrupts it.
fn restarting<T>(mut call: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match call() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_missing_process_is_gone_and_a_missing_call_falls_back() {
        let from = |errno| opened_after(io::Error::from_raw_os_error(errno));
        assert!(matches!(from(libc::ESRCH), Ok(Opened::Gone)));
        assert!(matches!(from(libc::ENOSYS), Ok(Opened::Unsupported)));
        assert!(matches!(from(libc::EPERM), Ok(Opened::Unsupported)));
        let error = from(libc::EMFILE).err().unwrap();
        assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
    }

    #[test]
    fn only_readable_or_hung_up_reports_an_exit() {
        assert!(!exited_from(0).unwrap());
        assert!(exited_from(libc::POLLIN).unwrap());
        assert!(exited_from(libc::POLLHUP).unwrap());
        assert!(exited_from(libc::POLLIN | libc::POLLHUP).unwrap());
        assert!(exited_from(libc::POLLNVAL).is_err());
        assert!(exited_from(libc::POLLIN | libc::POLLERR).is_err());
    }

    #[test]
    fn the_poll_timeout_covers_the_whole_deadline() {
        assert_eq!(poll_timeout(None), -1);
        assert_eq!(poll_timeout(Some(Instant::now())), 0);
        let timeout = poll_timeout(Some(Instant::now() + Duration::from_secs(5)));
        assert!((4_900..=5_000).contains(&timeout), "{timeout}");
        let far = Instant::now() + Duration::from_secs(u64::from(u32::MAX));
        assert_eq!(poll_timeout(Some(far)), libc::c_int::MAX);
    }

    #[test]
    fn only_minus_one_is_a_failed_call() {
        assert_eq!(cvt(0).unwrap(), 0);
        assert_eq!(cvt(1).unwrap(), 1);
        assert!(cvt(-1).is_err());
    }

    #[test]
    fn an_interrupted_call_is_restarted_and_other_errors_are_not() {
        let mut calls = 0;
        let result = restarting(|| {
            calls += 1;
            if calls == 1 {
                Err(io::ErrorKind::Interrupted.into())
            } else {
                Ok(calls)
            }
        });
        assert_eq!(result.unwrap(), 2);

        let mut calls = 0;
        let result = restarting(|| {
            calls += 1;
            if calls == 1 {
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            } else {
                Ok(calls)
            }
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(calls, 1);
    }
}

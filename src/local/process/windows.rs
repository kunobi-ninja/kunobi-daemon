//! Windows: a process handle, which is signaled once its process exits.
//!
//! An open handle keeps the process object, and so its PID, from being reused.

use std::ffi::c_void;
use std::io;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::time::Instant;

use super::{Opened, millis_until};
use crate::local::ProcessState;

type Handle = *mut c_void;

#[link(name = "kernel32")]
unsafe extern "system" {
    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> Handle;
    fn WaitForSingleObject(handle: Handle, milliseconds: u32) -> u32;
    #[cfg(feature = "async")]
    fn RegisterWaitForSingleObject(
        wait: *mut Handle,
        object: Handle,
        callback: Option<unsafe extern "system" fn(*mut c_void, u8)>,
        context: *const c_void,
        milliseconds: u32,
        flags: u32,
    ) -> i32;
    #[cfg(feature = "async")]
    fn UnregisterWaitEx(wait: Handle, completion: Handle) -> i32;
}

const SYNCHRONIZE: u32 = 0x0010_0000;
const ERROR_INVALID_PARAMETER: i32 = 87;
const INFINITE: u32 = 0xFFFF_FFFF;
const WAIT_OBJECT_0: u32 = 0;
const WAIT_TIMEOUT: u32 = 258;

#[derive(Debug)]
pub(super) struct Event {
    process: OwnedHandle,
}

pub(super) fn open(pid: u32) -> io::Result<Opened> {
    // SAFETY: asks only for the right to wait on the process with this PID;
    // no memory of ours is passed.
    let process = unsafe { OpenProcess(SYNCHRONIZE, 0, pid) };
    if process.is_null() {
        return opened_after(io::Error::last_os_error());
    }
    // SAFETY: OpenProcess succeeded, so this is an open handle we now own.
    let process = unsafe { OwnedHandle::from_raw_handle(process) };
    Ok(Opened::Watching(Event { process }))
}

/// A PID no process has makes `OpenProcess` fail with
/// `ERROR_INVALID_PARAMETER`, which is proof it exited. Access denial is not:
/// the process may run under another user, or be an exited process object
/// someone still holds a handle to.
fn opened_after(error: io::Error) -> io::Result<Opened> {
    if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER) {
        Ok(Opened::Gone)
    } else {
        Err(error)
    }
}

/// Where no handle could be opened, Windows has nothing else to ask.
pub(super) fn unknown_state(_pid: u32) -> ProcessState {
    ProcessState::Unknown
}

impl Event {
    pub(super) fn wait(&mut self, deadline: Option<Instant>) -> io::Result<bool> {
        loop {
            let milliseconds = match deadline {
                None => INFINITE,
                Some(deadline) => u32::try_from(millis_until(deadline, Instant::now()))
                    .unwrap_or(INFINITE)
                    .min(INFINITE - 1),
            };
            // SAFETY: the handle stays open for the duration of the call, and
            // the wait only reads it.
            match unsafe { WaitForSingleObject(self.process.as_raw_handle(), milliseconds) } {
                WAIT_OBJECT_0 => return Ok(true),
                // A wait can time out up to a clock tick early; only a
                // deadline that has passed ends it.
                WAIT_TIMEOUT if deadline.is_none_or(|deadline| Instant::now() >= deadline) => {
                    return Ok(false);
                }
                WAIT_TIMEOUT => {}
                _ => return Err(io::Error::last_os_error()),
            }
        }
    }

    #[cfg(feature = "async")]
    pub(super) async fn exited(&mut self) -> io::Result<()> {
        use std::sync::Arc;
        if self.wait(Some(Instant::now()))? {
            return Ok(());
        }
        let exited = Arc::new(tokio::sync::Notify::new());
        let _registration = Registration::new(&self.process, &exited)?;
        exited.notified().await;
        Ok(())
    }
}

/// A thread-pool wait for one process handle, which wakes a `Notify` once.
#[cfg(feature = "async")]
struct Registration {
    wait: Handle,
    context: *const tokio::sync::Notify,
}

#[cfg(feature = "async")]
// SAFETY: both pointers are used only by `Drop`, and UnregisterWaitEx and
// releasing an `Arc` have no thread affinity, so the registration may be
// dropped on any thread a Tokio runtime moves its future to.
unsafe impl Send for Registration {}

#[cfg(feature = "async")]
impl Registration {
    fn new(
        process: &OwnedHandle,
        exited: &std::sync::Arc<tokio::sync::Notify>,
    ) -> io::Result<Self> {
        const WT_EXECUTEONLYONCE: u32 = 0x0000_0008;
        let context = std::sync::Arc::into_raw(std::sync::Arc::clone(exited));
        let mut wait: Handle = std::ptr::null_mut();
        // SAFETY: `wait` is a valid output slot. The caller borrows the
        // process handle for as long as this registration exists, and
        // `context` is a reference this registration owns until `Drop` has
        // waited out any running callback.
        let registered = unsafe {
            RegisterWaitForSingleObject(
                &mut wait,
                process.as_raw_handle(),
                Some(signal),
                context.cast(),
                INFINITE,
                WT_EXECUTEONLYONCE,
            )
        };
        if registered == 0 {
            let error = io::Error::last_os_error();
            // SAFETY: registration failed, so no callback holds `context`;
            // this releases the reference taken above.
            drop(unsafe { std::sync::Arc::from_raw(context) });
            return Err(error);
        }
        Ok(Self { wait, context })
    }
}

#[cfg(feature = "async")]
impl Drop for Registration {
    fn drop(&mut self) {
        const INVALID_HANDLE_VALUE: Handle = -1isize as Handle;
        // SAFETY: `wait` came from a successful registration and is
        // unregistered only here. INVALID_HANDLE_VALUE makes the call return
        // only after a callback already running has finished, so none can
        // use `context` afterwards.
        let _ = unsafe { UnregisterWaitEx(self.wait, INVALID_HANDLE_VALUE) };
        // SAFETY: releases the reference taken in `new`; no callback can run
        // any more.
        drop(unsafe { std::sync::Arc::from_raw(self.context) });
    }
}

#[cfg(feature = "async")]
unsafe extern "system" fn signal(context: *mut c_void, _timed_out: u8) {
    // SAFETY: `context` is the `Notify` that the registration keeps alive
    // until this callback can no longer run.
    let exited = unsafe { &*context.cast::<tokio::sync::Notify>() };
    // A stored permit wakes the waiter even if it is not polling yet.
    exited.notify_one();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_missing_pid_is_gone() {
        let from = |code| opened_after(io::Error::from_raw_os_error(code));
        assert!(matches!(from(87), Ok(Opened::Gone)));
        let error = from(5).err().unwrap();
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(unknown_state(1), ProcessState::Unknown);
    }
}

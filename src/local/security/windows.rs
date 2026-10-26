//! Windows primary-token comparison.

use super::{SecurityContextComparison, compare_token_generations};
use std::io;
use std::mem::{offset_of, size_of};
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Security::*;
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

/// Compare a process's primary token with this process's primary token.
///
/// The borrowed process handle must permit `PROCESS_QUERY_LIMITED_INFORMATION`.
/// Obtain and retain it from an authenticated, pinned peer; a PID from an
/// application message alone does not establish peer identity. This function
/// performs Windows queries and must run outside `peer::Policy::grant`.
///
/// Compares user, logon session, owner, primary group, groups, restricted SIDs,
/// device groups, privileges, integrity, elevation, session, UI access, mandatory
/// policy, virtualization, default DACL, trust level and token type. AppContainer,
/// claim-bearing and capability-bearing tokens return `Incompatible`.
///
/// Different opaque-attribute generations return `ProcessIdentityDiffers` only
/// for matching empty lists or the known UINT64 `TSA://ProcUnique` shape (flags
/// `0x41`, two values). Unknown shapes return `Incompatible`. Query or decoding
/// failures return an error; callers must not turn that into an access grant.
/// Thread impersonation tokens and process mitigations are not compared.
/// Recheck before delegating work because tokens can change after this snapshot.
pub fn compare_with_current_process(
    process: BorrowedHandle<'_>,
) -> io::Result<SecurityContextComparison> {
    let peer = token(process.as_raw_handle(), TOKEN_QUERY)?;
    let current = token(current_process(), TOKEN_QUERY)?;
    compare_tokens(peer.as_raw_handle(), current.as_raw_handle())
}

fn current_process() -> HANDLE {
    // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle and has no preconditions.
    unsafe { GetCurrentProcess() }
}

fn token(process: HANDLE, access: u32) -> io::Result<OwnedHandle> {
    let mut handle = std::ptr::null_mut();
    // SAFETY: The caller retains this live process handle. Win32 writes one token handle.
    if unsafe { OpenProcessToken(process, access, &mut handle) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OpenProcessToken succeeded and transferred ownership of this token handle.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

struct Information {
    storage: Vec<usize>,
    length: usize,
}

impl Information {
    fn read(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> io::Result<Self> {
        let mut length = 0;
        // SAFETY: The token is open for TOKEN_QUERY; a zero-sized buffer queries its size.
        unsafe {
            GetTokenInformation(token, class, std::ptr::null_mut(), 0, &mut length);
        }
        // The required size is useful even when a class reports BAD_LENGTH
        // rather than INSUFFICIENT_BUFFER. The second call must still succeed.
        if length == 0 || length > (1 << 20) {
            return Err(io::Error::other("cannot size process security context"));
        }
        let mut storage = vec![0usize; (length as usize).div_ceil(size_of::<usize>())];
        // SAFETY: The aligned allocation contains at least length writable bytes;
        // the token and out-length pointer remain valid throughout the query.
        if unsafe {
            GetTokenInformation(
                token,
                class,
                storage.as_mut_ptr().cast(),
                length,
                &mut length,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if length as usize > storage.len() * size_of::<usize>() {
            return Err(io::Error::other(
                "process security context grew while reading",
            ));
        }
        Ok(Self {
            storage,
            length: length as usize,
        })
    }

    fn pointer(&self) -> *const u8 {
        self.storage.as_ptr().cast()
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: A successful query initialized length bytes within this retained allocation.
        unsafe { std::slice::from_raw_parts(self.pointer(), self.length) }
    }

    fn value<T: Copy>(&self) -> io::Result<T> {
        if self.length < size_of::<T>() {
            return Err(io::Error::other("truncated process security context"));
        }
        // SAFETY: The bound above covers T. Callers use only Win32 integer/pointer records.
        Ok(unsafe { self.pointer().cast::<T>().read_unaligned() })
    }

    fn array<T: Copy>(&self, offset: usize, count: usize) -> io::Result<Vec<T>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if count > self.length.saturating_sub(offset) / size_of::<T>() || offset > self.length {
            return Err(io::Error::other("invalid process security context array"));
        }
        Ok((0..count)
            .map(|index| {
                // SAFETY: The bound above covers every element, and callers select
                // Win32 integer/pointer records whose bit patterns are valid.
                unsafe {
                    self.pointer()
                        .add(offset + index * size_of::<T>())
                        .cast::<T>()
                        .read_unaligned()
                }
            })
            .collect())
    }

    fn pointed_bytes(&self, pointer: *const u8, length: usize) -> io::Result<Vec<u8>> {
        let offset = (pointer as usize)
            .checked_sub(self.pointer() as usize)
            .ok_or_else(|| io::Error::other("invalid process security context pointer"))?;
        if offset > self.length || length > self.length - offset {
            return Err(io::Error::other(
                "process security context pointer is out of bounds",
            ));
        }
        Ok(self.bytes()[offset..offset + length].to_owned())
    }

    fn sid(&self, pointer: PSID) -> io::Result<Vec<u8>> {
        // Validate the SID header in the returned buffer before asking Win32
        // to read its variable-length subauthorities.
        self.pointed_bytes(pointer.cast(), 8)?;
        // SAFETY: The SID header was checked in the retained Win32 query buffer.
        // GetLengthSid derives its length from that header; pointed_bytes checks the rest.
        let length = unsafe { GetLengthSid(pointer) } as usize;
        self.pointed_bytes(pointer.cast(), length)
    }
}

fn compare_tokens(peer: HANDLE, current: HANDLE) -> io::Result<SecurityContextComparison> {
    for token in [peer, current] {
        if Information::read(token, TokenIsAppContainer)?.value::<u32>()? != 0 {
            return Ok(SecurityContextComparison::Incompatible);
        }
    }
    for token in [peer, current] {
        // Claim/capability access rules need more than user/group equality.
        // Report them as incompatible until they can be compared fully.
        for class in [TokenUserClaimAttributes, TokenDeviceClaimAttributes] {
            if Information::read(token, class)?.array::<u32>(4, 1)?[0] != 0 {
                return Ok(SecurityContextComparison::Incompatible);
            }
        }
        if Information::read(token, TokenCapabilities)?.value::<u32>()? != 0 {
            return Ok(SecurityContextComparison::Incompatible);
        }
    }
    let peer_statistics = Information::read(peer, TokenStatistics)?.value::<TOKEN_STATISTICS>()?;
    let current_statistics =
        Information::read(current, TokenStatistics)?.value::<TOKEN_STATISTICS>()?;
    let authentication = |info: TOKEN_STATISTICS| {
        (
            info.AuthenticationId.HighPart,
            info.AuthenticationId.LowPart,
        )
    };
    if authentication(peer_statistics) != authentication(current_statistics) {
        return Ok(SecurityContextComparison::Incompatible);
    }
    let same_generation = peer_statistics.ModifiedId.HighPart
        == current_statistics.ModifiedId.HighPart
        && peer_statistics.ModifiedId.LowPart == current_statistics.ModifiedId.LowPart;
    // Windows exposes opaque security attributes even on ordinary tokens.
    // Unchanged token copies can retain opaque attributes. Independently
    // created processes may have different TSA://ProcUnique values. Report
    // that difference separately because token attributes can affect access rules.
    // https://learn.microsoft.com/windows/win32/api/winnt/ns-winnt-token_statistics
    let peer_access = Information::read(peer, TokenAccessInformation)?;
    let current_access = Information::read(current, TokenAccessInformation)?;
    let a = peer_access.value::<TOKEN_ACCESS_INFORMATION>()?;
    let b = current_access.value::<TOKEN_ACCESS_INFORMATION>()?;
    let attributes = compare_token_generations(
        same_generation,
        !a.SecurityAttributes.is_null() || !b.SecurityAttributes.is_null(),
        || ordinary_attributes_match(peer, current),
    )?;
    if attributes == SecurityContextComparison::Incompatible {
        return Ok(SecurityContextComparison::Incompatible);
    }

    if a.TokenType != TokenPrimary
        || b.TokenType != TokenPrimary
        || a.Flags != b.Flags
        || optional_sid(&peer_access, a.TrustLevelSid)?
            != optional_sid(&current_access, b.TrustLevelSid)?
    {
        return Ok(SecurityContextComparison::Incompatible);
    }
    for class in [
        TokenElevation,
        TokenElevationType,
        TokenSessionId,
        TokenUIAccess,
        TokenHasRestrictions,
        TokenVirtualizationAllowed,
        TokenVirtualizationEnabled,
        TokenMandatoryPolicy,
    ] {
        if Information::read(peer, class)?.bytes() != Information::read(current, class)?.bytes() {
            return Ok(SecurityContextComparison::Incompatible);
        }
    }
    for class in [
        TokenUser,
        TokenOwner,
        TokenPrimaryGroup,
        TokenIntegrityLevel,
    ] {
        let a = Information::read(peer, class)?;
        let b = Information::read(current, class)?;
        // All four records start with their SID pointer.
        if a.sid(a.value::<PSID>()?)? != b.sid(b.value::<PSID>()?)? {
            return Ok(SecurityContextComparison::Incompatible);
        }
    }
    if groups(peer, TokenGroups)? != groups(current, TokenGroups)?
        || groups(peer, TokenRestrictedSids)? != groups(current, TokenRestrictedSids)?
        || groups(peer, TokenDeviceGroups)? != groups(current, TokenDeviceGroups)?
        || privileges(peer)? != privileges(current)?
    {
        return Ok(SecurityContextComparison::Incompatible);
    }
    if default_dacl(peer)? == default_dacl(current)? {
        Ok(attributes)
    } else {
        Ok(SecurityContextComparison::Incompatible)
    }
}

fn ordinary_attributes_match(peer: HANDLE, current: HANDLE) -> io::Result<bool> {
    use super::attributes::matching_ordinary_attributes;
    let peer = Information::read(peer, TokenSecurityAttributes)?;
    let current = Information::read(current, TokenSecurityAttributes)?;
    matching_ordinary_attributes(peer.bytes(), current.bytes())
}

fn optional_sid(info: &Information, pointer: PSID) -> io::Result<Option<Vec<u8>>> {
    if pointer.is_null() {
        Ok(None)
    } else {
        info.sid(pointer).map(Some)
    }
}

fn groups(token: HANDLE, class: TOKEN_INFORMATION_CLASS) -> io::Result<Vec<(Vec<u8>, u32)>> {
    let info = Information::read(token, class)?;
    let group_count = info.value::<u32>()? as usize;
    let mut groups = info
        .array::<SID_AND_ATTRIBUTES>(offset_of!(TOKEN_GROUPS, Groups), group_count)?
        .into_iter()
        .map(|group| Ok((info.sid(group.Sid)?, group.Attributes)))
        .collect::<io::Result<Vec<_>>>()?;
    groups.sort();
    Ok(groups)
}

fn privileges(token: HANDLE) -> io::Result<Vec<(i32, u32, u32)>> {
    let info = Information::read(token, TokenPrivileges)?;
    let count = info.value::<u32>()? as usize;
    let mut privileges = info
        .array::<LUID_AND_ATTRIBUTES>(offset_of!(TOKEN_PRIVILEGES, Privileges), count)?
        .into_iter()
        .map(|value| (value.Luid.HighPart, value.Luid.LowPart, value.Attributes))
        .collect::<Vec<_>>();
    privileges.sort();
    Ok(privileges)
}

fn default_dacl(token: HANDLE) -> io::Result<Option<Vec<u8>>> {
    let info = Information::read(token, TokenDefaultDacl)?;
    let pointer = info.value::<TOKEN_DEFAULT_DACL>()?.DefaultDacl;
    if pointer.is_null() {
        return Ok(None);
    }
    info.pointed_bytes(pointer.cast(), size_of::<ACL>())?;
    // SAFETY: The preceding bounds check covers the ACL header in the retained buffer.
    let length = unsafe { pointer.read_unaligned() }.AclSize as usize;
    info.pointed_bytes(pointer.cast(), length).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "subprocess fixture for a separately created process token"]
    fn token_child_waits_for_input() {
        use std::io::{Read, Write};
        if std::env::var_os("KUNOBI_DAEMON_TOKEN_TEST_CHILD").is_none() {
            return;
        }
        println!("token child ready");
        io::stdout().flush().unwrap();
        let mut finish = [0];
        io::stdin().read_exact(&mut finish).unwrap();
    }

    #[test]
    fn separately_created_processes_report_their_process_identity_difference() {
        use std::io::{BufRead, Write};
        use std::os::windows::io::AsHandle;
        use std::process::{Command, Stdio};

        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }

        let mut child = Child(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "local::security::windows::tests::token_child_waits_for_input",
                    "--nocapture",
                ])
                .env("KUNOBI_DAEMON_TOKEN_TEST_CHILD", "1")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let mut output = io::BufReader::new(child.0.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                output.read_line(&mut line).unwrap(),
                0,
                "token child exited early"
            );
            if line.contains("token child ready") {
                break;
            }
        }
        let peer = token(child.0.as_handle().as_raw_handle(), TOKEN_QUERY).unwrap();
        let current = token(current_process(), TOKEN_QUERY).unwrap();
        let comparison = compare_with_current_process(child.0.as_handle()).unwrap();
        assert!(matches!(
            comparison,
            SecurityContextComparison::Equivalent
                | SecurityContextComparison::ProcessIdentityDiffers
        ));

        let statistics = |token: HANDLE| {
            Information::read(token, TokenStatistics)
                .unwrap()
                .value::<TOKEN_STATISTICS>()
                .unwrap()
        };
        let a = statistics(peer.as_raw_handle()).ModifiedId;
        let b = statistics(current.as_raw_handle()).ModifiedId;
        let opaque = |token: HANDLE| {
            !Information::read(token, TokenAccessInformation)
                .unwrap()
                .value::<TOKEN_ACCESS_INFORMATION>()
                .unwrap()
                .SecurityAttributes
                .is_null()
        };
        // Some runners copy ModifiedId unchanged. On affected Windows hosts,
        // the comparison must distinguish the process identity from equivalence.
        if (a.HighPart, a.LowPart) != (b.HighPart, b.LowPart)
            && (opaque(peer.as_raw_handle()) || opaque(current.as_raw_handle()))
        {
            assert_eq!(
                comparison,
                SecurityContextComparison::ProcessIdentityDiffers
            );
        }
        child.0.stdin.take().unwrap().write_all(&[1]).unwrap();
        assert!(child.0.wait().unwrap().success());
    }

    #[test]
    fn the_same_primary_token_has_the_same_context() {
        let token = token(current_process(), TOKEN_QUERY).unwrap();
        if Information::read(token.as_raw_handle(), TokenIsAppContainer)
            .unwrap()
            .value::<u32>()
            .unwrap()
            == 0
        {
            assert_eq!(
                compare_tokens(token.as_raw_handle(), token.as_raw_handle()).unwrap(),
                SecurityContextComparison::Equivalent
            );
        }
    }

    #[test]
    fn an_unmodified_primary_token_copy_has_the_same_context() {
        let token = token(current_process(), TOKEN_QUERY | TOKEN_DUPLICATE).unwrap();
        let mut duplicate = std::ptr::null_mut();
        assert_ne!(
            // SAFETY: The source token permits duplication; Win32 initializes duplicate.
            unsafe {
                DuplicateTokenEx(
                    token.as_raw_handle(),
                    TOKEN_QUERY,
                    std::ptr::null(),
                    SecurityImpersonation,
                    TokenPrimary,
                    &mut duplicate,
                )
            },
            0,
            "{}",
            io::Error::last_os_error()
        );
        // SAFETY: DuplicateTokenEx succeeded and transferred ownership of this handle.
        let duplicate = unsafe { OwnedHandle::from_raw_handle(duplicate) };
        let original = Information::read(token.as_raw_handle(), TokenStatistics)
            .unwrap()
            .value::<TOKEN_STATISTICS>()
            .unwrap();
        let copied = Information::read(duplicate.as_raw_handle(), TokenStatistics)
            .unwrap()
            .value::<TOKEN_STATISTICS>()
            .unwrap();
        assert_ne!(
            (original.TokenId.HighPart, original.TokenId.LowPart),
            (copied.TokenId.HighPart, copied.TokenId.LowPart)
        );
        assert_eq!(
            compare_tokens(token.as_raw_handle(), duplicate.as_raw_handle()).unwrap(),
            SecurityContextComparison::Equivalent
        );
    }

    #[test]
    fn deleting_privileges_makes_contexts_incompatible() {
        let token = token(current_process(), TOKEN_QUERY | TOKEN_DUPLICATE).unwrap();
        let mut restricted = std::ptr::null_mut();
        assert_ne!(
            // SAFETY: The source token permits duplication; all empty arrays use null pointers.
            unsafe {
                CreateRestrictedToken(
                    token.as_raw_handle(),
                    DISABLE_MAX_PRIVILEGE,
                    0,
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    0,
                    std::ptr::null(),
                    &mut restricted,
                )
            },
            0
        );
        // SAFETY: CreateRestrictedToken succeeded and transferred ownership of this handle.
        let restricted = unsafe { OwnedHandle::from_raw_handle(restricted) };
        assert_eq!(
            compare_tokens(restricted.as_raw_handle(), token.as_raw_handle()).unwrap(),
            SecurityContextComparison::Incompatible
        );
        assert_eq!(
            compare_tokens(token.as_raw_handle(), restricted.as_raw_handle()).unwrap(),
            SecurityContextComparison::Incompatible
        );
    }
}

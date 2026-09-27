//! Unix socket paths that fit, checked where names and directories are chosen.
//!
//! A Unix socket address has a fixed-size path field, and the OS reports an
//! overflow only as an invalid argument, at bind or connect time, naming
//! neither the length nor the limit. A service usually derives several sockets
//! from one directory, so the one that overflows is often not the one anybody
//! checked. Here a name is checked when it is written, as a `const`, and a
//! directory once, against every socket it will hold.
//!
//! ```
//! use kunobi_daemon::socket_path::{SocketDir, SocketName};
//!
//! const DAEMON: SocketName = SocketName::new("daemon.sock");
//! const CONTROL: SocketName = SocketName::new("daemon.ctl");
//!
//! let dir = SocketDir::new("/tmp/kache-1f2e", &[DAEMON, CONTROL])?;
//! let control = dir.path(CONTROL)?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Windows names a pipe after the path, which has no such limit, so every check
//! passes there.
use std::{
    io,
    path::{Path, PathBuf},
};

/// Longest Unix socket path, in bytes, on this platform: `sockaddr_un.sun_path`
/// less the NUL that terminates it. `None` where local endpoints are not Unix
/// sockets.
pub const MAX_SOCKET_PATH_BYTES: Option<usize> = if cfg!(unix) {
    Some(if cfg!(any(target_os = "linux", target_os = "android")) {
        107
    } else {
        103
    })
} else {
    None
};

/// Longest socket name [`SocketName::new`] accepts. A name is a file name, not a
/// place to spend the directory's budget.
pub const MAX_NAME_BYTES: usize = 48;

/// A socket file name, checked when it is constructed.
///
/// [`SocketName::new`] is a `const fn` that panics on an empty name, `.` or
/// `..`, a path separator or NUL, or a name longer than [`MAX_NAME_BYTES`].
/// Declared as a `const`, a bad name fails the build:
///
/// ```compile_fail
/// use kunobi_daemon::socket_path::SocketName;
/// const NESTED: SocketName = SocketName::new("run/daemon.sock");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SocketName(&'static str);

impl SocketName {
    /// Check `name` and wrap it. Use it in a `const` so a bad name is a
    /// compile error rather than a runtime one.
    pub const fn new(name: &'static str) -> Self {
        let bytes = name.as_bytes();
        assert!(!bytes.is_empty(), "socket name is empty");
        assert!(
            bytes.len() <= MAX_NAME_BYTES,
            "socket name is longer than MAX_NAME_BYTES"
        );
        assert!(!matches!(bytes, b"." | b".."), "socket name is `.` or `..`");
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            assert!(
                byte != b'/' && byte != b'\\' && byte != 0,
                "socket name contains a path separator or NUL"
            );
            index += 1;
        }
        Self(name)
    }

    /// The file name.
    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

/// Fails when `socket` is too long to bind or connect to as a Unix socket.
///
/// The error names the length and the limit, which the OS does not. Passes on
/// platforms without the limit.
pub fn check_socket_path(socket: &Path) -> io::Result<()> {
    let Some(limit) = MAX_SOCKET_PATH_BYTES else {
        return Ok(());
    };
    let bytes = path_bytes(socket);
    if bytes > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "socket path is {bytes} bytes, but a Unix socket path can be at most \
                 {limit} on this platform: {}",
                socket.display()
            ),
        ));
    }
    Ok(())
}

/// Longest directory, in bytes, that still fits every socket in `names`, or
/// `None` on a platform without the limit.
pub fn max_dir_bytes(names: &[SocketName]) -> Option<usize> {
    let limit = MAX_SOCKET_PATH_BYTES?;
    let longest = names.iter().map(|name| name.0.len()).max().unwrap_or(0);
    // One separator between the directory and the name.
    Some(limit.saturating_sub(longest + 1))
}

/// A directory checked once against every socket it will hold.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SocketDir {
    dir: PathBuf,
}

impl SocketDir {
    /// Fails when `dir` joined with the longest of `names` would not fit.
    ///
    /// Declare every socket the directory will hold, including names derived
    /// at runtime (declare the longest a derivation can produce, e.g. with its
    /// widest generation suffix). The error says how long the directory may be.
    pub fn new(dir: impl Into<PathBuf>, names: &[SocketName]) -> io::Result<Self> {
        let dir = dir.into();
        if let (Some(limit), Some(longest)) = (
            MAX_SOCKET_PATH_BYTES,
            names.iter().max_by_key(|name| name.0.len()),
        ) {
            let bytes = path_bytes(&dir.join(longest.0));
            if bytes > limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "socket `{}` in this directory is {bytes} bytes, but a Unix socket path \
                         can be at most {limit} on this platform, so the directory can be at \
                         most {} bytes: {}",
                        longest.0,
                        limit.saturating_sub(longest.0.len() + 1),
                        dir.display()
                    ),
                ));
            }
        }
        Ok(Self { dir })
    }

    /// The directory itself.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The socket `name` in this directory. Checked again, so a name that was
    /// not declared to [`SocketDir::new`] cannot slip through.
    pub fn path(&self, name: SocketName) -> io::Result<PathBuf> {
        let path = self.dir.join(name.0);
        check_socket_path(&path)?;
        Ok(path)
    }
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> usize {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len()
}

#[cfg(not(unix))]
fn path_bytes(path: &Path) -> usize {
    path.as_os_str().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAEMON: SocketName = SocketName::new("daemon.sock");
    // Only the Unix tests reach the limit it overflows.
    #[cfg(unix)]
    const CONTROL: SocketName = SocketName::new("daemon.control.v2.sock");

    #[test]
    fn a_name_is_checked_where_it_is_declared() {
        assert_eq!(DAEMON.as_str(), "daemon.sock");
        for bad in ["", ".", "..", "a/b", "a\\b", "a\0b"] {
            let name = bad.to_string().leak();
            assert!(
                std::panic::catch_unwind(|| SocketName::new(name)).is_err(),
                "{bad:?} was accepted"
            );
        }
        let long = "a".repeat(MAX_NAME_BYTES + 1).leak();
        assert!(std::panic::catch_unwind(|| SocketName::new(long)).is_err());
        let limit = "a".repeat(MAX_NAME_BYTES).leak();
        assert_eq!(SocketName::new(limit).as_str().len(), MAX_NAME_BYTES);
    }

    #[cfg(unix)]
    #[test]
    fn a_directory_is_checked_against_its_longest_socket() {
        // The kache runner that failed: daemon.sock fit, the control socket did not.
        let dir = "/Users/zondax-ci/actions-runner/runner-3/_work/_temp/kache-runtime-36081599948-1-build-sign";
        let daemon_only = SocketDir::new(dir, &[DAEMON]);
        let both = SocketDir::new(dir, &[DAEMON, CONTROL]);
        if MAX_SOCKET_PATH_BYTES == Some(103) {
            assert!(daemon_only.is_ok());
            let error = both.unwrap_err().to_string();
            assert!(error.contains("daemon.control.v2.sock"), "{error}");
            assert!(error.contains("at most 80 bytes"), "{error}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_directory_at_the_budget_fits_and_one_byte_more_does_not() {
        let limit = MAX_SOCKET_PATH_BYTES.unwrap();
        let budget = max_dir_bytes(&[DAEMON, CONTROL]).unwrap();
        let dir = format!("/{}", "d".repeat(budget - 1));
        assert_eq!(dir.len(), budget);
        let fits = SocketDir::new(&dir, &[DAEMON, CONTROL]).unwrap();
        assert_eq!(path_bytes(&fits.path(CONTROL).unwrap()), limit);
        assert!(SocketDir::new(format!("{dir}d"), &[DAEMON, CONTROL]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn an_undeclared_name_is_still_checked() {
        let budget = max_dir_bytes(&[DAEMON]).unwrap();
        let dir = SocketDir::new(format!("/{}", "d".repeat(budget - 1)), &[DAEMON]).unwrap();
        assert!(dir.path(DAEMON).is_ok());
        assert!(dir.path(CONTROL).is_err());
    }

    #[test]
    fn a_path_names_its_length_and_the_limit() {
        if let Some(limit) = MAX_SOCKET_PATH_BYTES {
            let path = format!("/{}", "a".repeat(limit));
            let error = check_socket_path(Path::new(&path)).unwrap_err().to_string();
            assert!(error.contains(&format!("{} bytes", limit + 1)), "{error}");
            assert!(error.contains(&format!("at most {limit}")), "{error}");
        } else {
            assert!(check_socket_path(Path::new(&"a".repeat(4096))).is_ok());
            assert_eq!(max_dir_bytes(&[DAEMON]), None);
        }
    }
}

/// Name and directory rules over generated names and directories.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::ffi::OsStr;

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    /// Names that follow the documented rules, including `:`, spaces and
    /// dots, which are ordinary file name characters.
    fn valid_name() -> impl Strategy<Value = String> {
        prop_oneof![
            4 => "[a-zA-Z0-9._ :-]{1,48}",
            // A drive letter and colon, which Windows paths read as a prefix.
            1 => "[a-zA-Z]:[a-z.]{0,6}",
        ]
        .prop_filter("`.` and `..` are not names", |name| {
            name != "." && name != ".."
        })
    }

    fn any_name() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => valid_name(),
            // Separators, NUL and dots.
            1 => "[a-z./\\\\\\x00]{0,4}",
            1 => "\\PC{0,64}",
            1 => "[a-z]{40,60}",
        ]
    }

    /// Leaked, because a name is `&'static str`; a few hundred short strings.
    fn declare(name: String) -> SocketName {
        SocketName::new(name.leak())
    }

    proptest! {
        #![proptest_config(config())]

        #[test]
        fn a_name_is_accepted_exactly_when_the_rules_allow_it(name in any_name()) {
            let allowed = !name.is_empty()
                && name.len() <= MAX_NAME_BYTES
                && name != "."
                && name != ".."
                && !name.contains(['/', '\\', '\0']);
            let name: &'static str = name.leak();
            prop_assert_eq!(std::panic::catch_unwind(|| SocketName::new(name)).is_ok(), allowed);
        }

        #[test]
        fn a_socket_path_is_its_name_directly_inside_the_directory(
            dir in "(/[a-z0-9-]{1,8}){1,3}",
            name in valid_name(),
        ) {
            let name = declare(name);
            // Short enough for every platform's limit.
            let socket_dir = SocketDir::new(&dir, &[name]).unwrap();
            let path = socket_dir.path(name).unwrap();
            prop_assert_eq!(path.parent(), Some(Path::new(&dir)), "{:?}", path);
            prop_assert_eq!(path.file_name(), Some(OsStr::new(name.as_str())), "{:?}", path);
        }
    }

    #[cfg(unix)]
    proptest! {
        #![proptest_config(config())]

        #[test]
        fn a_directory_fits_exactly_when_it_is_within_the_budget(
            names in proptest::collection::vec(valid_name(), 1..4),
            undeclared in valid_name(),
            dir in "(/[a-z0-9-]{1,16}){1,10}",
        ) {
            let limit = MAX_SOCKET_PATH_BYTES.unwrap();
            let names: Vec<SocketName> = names.into_iter().map(declare).collect();
            let budget = max_dir_bytes(&names).unwrap();
            let checked = SocketDir::new(&dir, &names);
            prop_assert_eq!(checked.is_ok(), dir.len() <= budget);
            if let Ok(checked) = checked {
                for &name in &names {
                    prop_assert!(checked.path(name).is_ok(), "declared {:?} does not fit", name);
                }
                // A name that was not declared is checked on its own.
                let undeclared = declare(undeclared);
                prop_assert_eq!(
                    checked.path(undeclared).is_ok(),
                    dir.len() + 1 + undeclared.as_str().len() <= limit
                );
            }
        }

        #[test]
        fn a_directory_at_the_budget_fits_and_one_byte_more_does_not(
            names in proptest::collection::vec(valid_name(), 1..4),
        ) {
            let names: Vec<SocketName> = names.into_iter().map(declare).collect();
            let budget = max_dir_bytes(&names).unwrap();
            let dir = format!("/{}", "d".repeat(budget - 1));
            let longest = names.iter().map(|name| name.as_str().len()).max().unwrap();
            let fits = SocketDir::new(&dir, &names).unwrap();
            for &name in &names {
                let path = fits.path(name).unwrap();
                prop_assert!(path_bytes(&path) <= MAX_SOCKET_PATH_BYTES.unwrap());
                if name.as_str().len() == longest {
                    prop_assert_eq!(path_bytes(&path), MAX_SOCKET_PATH_BYTES.unwrap());
                }
            }
            prop_assert!(SocketDir::new(format!("{dir}d"), &names).is_err());
        }
    }
}

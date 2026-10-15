//! Named-pipe names scoped to one service installation and one OS user.
//!
//! Named pipes share one namespace across the machine. A name built only from
//! the service would let another user's process create the pipe first and wait
//! for this user's clients. [`for_current_user`] builds the name from this
//! process's user SID, read from its token, and the service's UUID, profile,
//! instance and endpoint [`SocketName`]:
//!
//! ```text
//! S-1-5-21-…-1001_service-<uuid>_profile-stable_instance-default_daemon.sock
//! ```
//!
//! The name only keeps users apart; bind it with [`super::socket::acquire`],
//! which also restricts the pipe to its owner, SYSTEM and Administrators.
//!
//! Only Windows has this module. A Unix socket is a path below a per-user
//! directory, which [`crate::ServiceIdentity::paths`] and [`crate::socket_path`]
//! already scope and check. Services that already have a pipe name keep it: a
//! new name is a new rendezvous, which older clients would not find.

use crate::{ServiceIdentity, socket_path::SocketName};
use std::io;

/// Longest pipe name, in UTF-16 code units, after the `\\.\pipe\` prefix the
/// crate's named-pipe adapters add. A pipe path can be at most 256 characters.
pub const MAX_PIPE_NAME_UNITS: usize = 256 - r"\\.\pipe\".len();

/// This process's pipe name for the endpoint `name` of `identity`, scoped by
/// the user SID in this process's token, never by an environment variable.
///
/// Fails when the token cannot be read, or when the name is longer than
/// [`MAX_PIPE_NAME_UNITS`]; the error names both lengths. Pipe names are not
/// case-sensitive, so endpoint names that differ only in case name one pipe.
/// Pass the result as the endpoint to [`super::socket::acquire`] and
/// [`super::Duplex::connect`].
#[cfg(windows)]
pub fn for_current_user(identity: &ServiceIdentity, name: SocketName) -> io::Result<String> {
    user_pipe_name(&super::windows::current_user_sid()?, identity, name)
}

/// The pipe name for the endpoint `name` of `identity`, for the user whose SID
/// string is `sid`.
fn user_pipe_name(sid: &str, identity: &ServiceIdentity, name: SocketName) -> io::Result<String> {
    // `_` appears in no SID, UUID or identity component, so components cannot
    // alias. The endpoint name may contain one, so it comes last.
    let pipe = format!(
        "{sid}_service-{:032x}_profile-{}_instance-{}_{}",
        u128::from_be_bytes(*identity.id()),
        identity.profile(),
        identity.instance(),
        name.as_str()
    );
    let units = pipe.encode_utf16().count();
    if units > MAX_PIPE_NAME_UNITS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "pipe name is {units} characters, but a named pipe name can be at most \
                 {MAX_PIPE_NAME_UNITS}: {pipe}"
            ),
        ));
    }
    Ok(pipe)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAEMON: SocketName = SocketName::new("daemon.sock");
    const SID: &str = "S-1-5-21-3623811015-3361044348-30300820-1013";

    fn identity(id: u8, profile: &str, instance: &str) -> ServiceIdentity {
        ServiceIdentity::new([id; 16], "example.service", profile, instance).unwrap()
    }

    #[test]
    fn a_name_carries_the_user_the_service_and_the_endpoint() {
        assert_eq!(
            user_pipe_name(SID, &identity(0xab, "stable", "default"), DAEMON).unwrap(),
            format!(
                "{SID}_service-{}_profile-stable_instance-default_daemon.sock",
                "ab".repeat(16)
            )
        );
    }

    #[test]
    fn users_services_profiles_instances_and_endpoints_get_disjoint_names() {
        let other = SocketName::new("daemon.ctl");
        let names = [
            user_pipe_name(SID, &identity(1, "stable", "default"), DAEMON),
            user_pipe_name(
                "S-1-5-21-1-2-3-1001",
                &identity(1, "stable", "default"),
                DAEMON,
            ),
            user_pipe_name(SID, &identity(2, "stable", "default"), DAEMON),
            user_pipe_name(SID, &identity(1, "dev", "default"), DAEMON),
            user_pipe_name(SID, &identity(1, "stable", "second"), DAEMON),
            user_pipe_name(SID, &identity(1, "stable", "default"), other),
            // Components may contain `-`, so a separator made of it would let
            // these two alias.
            user_pipe_name(SID, &identity(1, "a-instance-b", "c"), DAEMON),
            user_pipe_name(SID, &identity(1, "a", "b-instance-c"), DAEMON),
        ]
        .map(Result::unwrap);
        let distinct: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(distinct.len(), names.len(), "{names:#?}");
    }

    #[test]
    fn a_name_at_the_limit_fits_and_one_unit_more_does_not() {
        let identity = identity(1, "stable", "default");
        let rest = user_pipe_name("", &identity, DAEMON).unwrap().len();
        let at_limit = "S".repeat(MAX_PIPE_NAME_UNITS - rest);
        let fits = user_pipe_name(&at_limit, &identity, DAEMON).unwrap();
        assert_eq!(fits.encode_utf16().count(), MAX_PIPE_NAME_UNITS);

        let error = user_pipe_name(&format!("{at_limit}S"), &identity, DAEMON).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let text = error.to_string();
        assert!(
            text.contains(&format!("{} characters", MAX_PIPE_NAME_UNITS + 1)),
            "{text}"
        );
        assert!(
            text.contains(&format!("at most {MAX_PIPE_NAME_UNITS}")),
            "{text}"
        );
    }

    #[test]
    fn the_limit_counts_utf16_units_not_bytes() {
        let identity = identity(1, "stable", "default");
        // Two bytes in UTF-8, one unit in UTF-16.
        let wide = SocketName::new("é");
        let rest = user_pipe_name("", &identity, wide).unwrap();
        let sid = "S".repeat(MAX_PIPE_NAME_UNITS - rest.encode_utf16().count());
        let name = user_pipe_name(&sid, &identity, wide).unwrap();
        assert!(name.len() > MAX_PIPE_NAME_UNITS);
    }

    /// The SID `whoami` reports for this process's user.
    #[cfg(windows)]
    fn whoami_sid() -> String {
        let output = std::process::Command::new("whoami")
            .args(["/user", "/fo", "csv", "/nh"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        // "DOMAIN\user","S-1-5-..."
        let text = String::from_utf8(output.stdout).unwrap();
        let sid = text.trim().rsplit(',').next().unwrap().trim_matches('"');
        assert!(sid.starts_with("S-1-"), "{text}");
        sid.to_owned()
    }

    #[cfg(windows)]
    #[test]
    fn the_current_users_name_starts_with_the_sid_of_this_process() {
        let identity = identity(1, "stable", "default");
        let name = for_current_user(&identity, DAEMON).unwrap();
        assert!(name.starts_with(&format!("{}_", whoami_sid())), "{name}");
        assert_eq!(
            name,
            user_pipe_name(&whoami_sid(), &identity, DAEMON).unwrap()
        );
    }

    #[cfg(windows)]
    #[test]
    fn a_service_binds_and_reaches_its_user_scoped_pipe() {
        use crate::local::{Bound, Duplex, PlatformDuplex, peer::PeerCredentials, socket::acquire};
        let unique = format!("test{}", std::process::id());
        let name = for_current_user(&identity(7, "stable", &unique), DAEMON).unwrap();
        let Bound::Won(_listener) = acquire(std::path::Path::new(&name)).unwrap() else {
            panic!("first owner of {name}")
        };
        let client = PlatformDuplex::connect(&name).unwrap();
        assert!(client.credentials().unwrap().same_user);
    }
}

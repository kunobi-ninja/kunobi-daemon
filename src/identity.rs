//! Stable identity and resource paths for independent daemon installations.
use std::{
    io,
    path::{Path, PathBuf},
};

/// A service installation, independent of its binary version or process ID.
///
/// Generate a UUID once per service and store its bytes in the consumer package.
/// Never generate a new UUID at startup or per release. Also use a reverse-DNS name, for example `ninja.kunobi.kache`.
/// Profiles separate installations such as `stable` and `dev`; instances allow
/// multiple services within one profile. Each component is canonical lowercase
/// ASCII to avoid aliases on case-insensitive filesystems.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceIdentity {
    id: [u8; 16],
    application: String,
    profile: String,
    instance: String,
}

impl ServiceIdentity {
    /// Validate an identity without creating directories or changing permissions.
    pub fn new(
        id: [u8; 16],
        application: impl Into<String>,
        profile: impl Into<String>,
        instance: impl Into<String>,
    ) -> io::Result<Self> {
        if id == [0; 16] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "service UUID must not be nil",
            ));
        }
        let identity = Self {
            id,
            application: application.into(),
            profile: profile.into(),
            instance: instance.into(),
        };
        for component in [&identity.application, &identity.profile, &identity.instance] {
            if component.is_empty()
                || component.len() > 64
                || !component
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
                || component.split('.').any(|part| {
                    part.is_empty()
                        || !part.as_bytes()[0].is_ascii_alphanumeric()
                        || !part.as_bytes()[part.len() - 1].is_ascii_alphanumeric()
                })
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid service identity component",
                ));
            }
        }
        Ok(identity)
    }

    /// Package-owned UUID bytes, fixed across all versions of this service.
    pub fn id(&self) -> &[u8; 16] {
        &self.id
    }

    /// Application-owned name. It must remain stable across binary upgrades.
    pub fn application(&self) -> &str {
        &self.application
    }
    /// Installation profile, such as stable or dev.
    pub fn profile(&self) -> &str {
        &self.profile
    }
    /// Named service instance.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// Derive disjoint paths below a private per-user runtime directory.
    ///
    /// The caller secures the root and creates these directories. Prefixes avoid
    /// Windows device names; distinct components cannot alias through separators.
    /// No version is included, so an upgrader finds its predecessor's resources.
    /// Keep the root short enough for the OS socket-path limit.
    pub fn paths(&self, user_root: impl AsRef<Path>) -> ServicePaths {
        ServicePaths {
            directory: user_root
                .as_ref()
                .join(format!("service-{:032x}", u128::from_be_bytes(self.id)))
                .join(format!("profile-{}", self.profile))
                .join(format!("instance-{}", self.instance)),
        }
    }
}

/// Conventional resource paths for one service identity under a per-user root.
///
/// This is a naming helper, not peer authentication or a directory-security API.
/// Windows named-pipe users must scope their pipe name by both this identity and
/// the user's SID, and enforce a same-user ACL; filesystem paths do not do that.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServicePaths {
    directory: PathBuf,
}

impl ServicePaths {
    /// Parent directory for the service's filesystem resources.
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    /// Persistent daemon ownership lock; never unlink it during replacement.
    pub fn ownership_lock(&self) -> PathBuf {
        self.directory.join("owner.lock")
    }
    /// Persistent upgrade lock, distinct from daemon ownership.
    pub fn upgrade_lock(&self) -> PathBuf {
        self.directory.join("upgrade.lock")
    }
    /// Application-defined discovery record.
    pub fn discovery(&self) -> PathBuf {
        self.directory.join("discovery")
    }
    /// Protobuf control endpoint for Unix-domain socket consumers.
    pub fn control_socket(&self) -> PathBuf {
        self.directory.join("control.sock")
    }
}

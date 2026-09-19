//! Service installations cannot share resources accidentally.
use kunobi_daemon::{ProcessLock, ServiceIdentity, publish_record};

fn identity(id: u8, profile: &str, instance: &str) -> ServiceIdentity {
    ServiceIdentity::new([id; 16], "example.service", profile, instance).unwrap()
}

#[test]
fn service_uuids_profiles_instances_and_users_have_disjoint_resources() {
    let root = tempfile::tempdir().unwrap();
    let identities = [
        identity(1, "stable", "default"),
        identity(2, "stable", "default"),
        identity(1, "dev", "default"),
        identity(1, "stable", "second"),
    ];
    let mut guards = Vec::new();
    let mut all_paths = std::collections::HashSet::new();
    for (index, identity) in identities.iter().enumerate() {
        let paths = identity.paths(root.path());
        std::fs::create_dir_all(paths.directory()).unwrap();
        for path in [
            paths.ownership_lock(),
            paths.upgrade_lock(),
            paths.discovery(),
            paths.control_socket(),
        ] {
            assert!(all_paths.insert(path), "resource path collision");
        }
        guards.push(
            ProcessLock::try_acquire(paths.ownership_lock())
                .unwrap()
                .unwrap(),
        );
        guards.push(
            ProcessLock::try_acquire(paths.upgrade_lock())
                .unwrap()
                .unwrap(),
        );
        publish_record(&paths.discovery(), &[index as u8]).unwrap();
        assert_ne!(paths, identity.paths(root.path().join("other-user")));
    }
    for (index, identity) in identities.iter().enumerate() {
        let paths = identity.paths(root.path());
        assert_eq!(std::fs::read(paths.discovery()).unwrap(), [index as u8]);
        assert!(
            ProcessLock::try_acquire(paths.ownership_lock())
                .unwrap()
                .is_none()
        );
        assert!(
            ProcessLock::try_acquire(paths.upgrade_lock())
                .unwrap()
                .is_none()
        );
    }
    drop(guards);
    for identity in &identities {
        assert!(
            ProcessLock::try_acquire(identity.paths(root.path()).ownership_lock())
                .unwrap()
                .is_some()
        );
    }
}

#[test]
fn invalid_or_ambiguous_identity_components_cannot_escape_the_user_root() {
    for invalid in [
        "",
        ".",
        "..",
        "../other",
        "a/b",
        "a\\b",
        "/absolute",
        "Name",
        "a..b",
        "a.",
        "-a",
        "a-",
        "a b",
        "c:",
        "é",
    ] {
        assert!(
            ServiceIdentity::new([1; 16], invalid, "stable", "default").is_err(),
            "{invalid}"
        );
        assert!(
            ServiceIdentity::new([1; 16], "app", invalid, "default").is_err(),
            "{invalid}"
        );
        assert!(
            ServiceIdentity::new([1; 16], "app", "stable", invalid).is_err(),
            "{invalid}"
        );
    }
    assert!(ServiceIdentity::new([0; 16], "app", "stable", "default").is_err());
    assert!(ServiceIdentity::new([1; 16], "a".repeat(65), "stable", "default").is_err());
    assert!(ServiceIdentity::new([1; 16], "a".repeat(64), "stable", "default").is_ok());
    let windows_names = identity(1, "con", "nul").paths("root");
    assert_eq!(
        windows_names.directory().file_name().unwrap(),
        "instance-nul"
    );
}

#[test]
fn upgrades_reuse_the_same_identity_and_lock_paths() {
    let old_build = identity(1, "stable", "default");
    let new_build = identity(1, "stable", "default");
    assert_eq!(old_build, new_build);
    assert_eq!(old_build.paths("root"), new_build.paths("root"));
    assert_eq!(old_build.id(), &[1; 16]);
    assert_eq!(old_build.application(), "example.service");
    assert_eq!(old_build.profile(), "stable");
    assert_eq!(old_build.instance(), "default");
}

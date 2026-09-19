//! Successor-safe cleanup on real Unix sockets.
#![cfg(all(unix, feature = "local"))]
use kunobi_daemon::local::unix_socket::{self, Bound, SocketGuard};
use std::os::unix::{fs::MetadataExt, net::UnixStream};

#[test]
fn late_cleanup_cannot_unlink_the_successor() {
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("service.sock");
    let Bound::Won(old) = unix_socket::acquire(&socket).unwrap() else {
        panic!("first owner")
    };
    let cleanup = SocketGuard::capture(&socket).unwrap();
    let old_inode = std::fs::metadata(&socket).unwrap().ino();
    std::fs::remove_file(&socket).unwrap();
    let Bound::Won(new) = unix_socket::acquire(&socket).unwrap() else {
        panic!("successor")
    };
    assert_ne!(std::fs::metadata(&socket).unwrap().ino(), old_inode);
    drop(cleanup);
    assert!(UnixStream::connect(&socket).is_ok());
    let cleanup = SocketGuard::capture(&socket).unwrap();
    drop(new);
    drop(cleanup);
    assert!(!socket.exists());
    drop(old);
}

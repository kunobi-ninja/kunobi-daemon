//! The named-pipe listener. Built and tested by the Windows jobs.

use std::io;

impl super::Listener for interprocess::local_socket::tokio::Listener {
    type Connection = interprocess::local_socket::tokio::Stream;
    async fn accept(&self) -> io::Result<interprocess::local_socket::tokio::Stream> {
        interprocess::local_socket::traits::tokio::Listener::accept(self).await
    }
}

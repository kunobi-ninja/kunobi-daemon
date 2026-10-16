//! The named-pipe listener. Built and tested by the Windows jobs.

use std::io;

impl super::Listener for interprocess::local_socket::tokio::Listener {
    type Connection = interprocess::local_socket::tokio::Stream;
    async fn accept(&self) -> io::Result<interprocess::local_socket::tokio::Stream> {
        interprocess::local_socket::traits::tokio::Listener::accept(self).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Lifecycle,
        admission::{Admission, Limits, Pool},
        local::{Bound, socket::acquire_tokio},
        peer::{Authenticated, SameUser},
        serve::{Served, serve},
    };
    use interprocess::local_socket::traits::Stream as _;
    use interprocess::local_socket::{GenericNamespaced, Stream, ToNsName};
    use std::{
        io::{Read, Write},
        path::Path,
        sync::Arc,
        time::Duration,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const BUDGET: Duration = Duration::from_secs(20);

    /// Serve one client on a fresh pipe with `handler`, and report what the
    /// accept loop did. `client` runs on a blocking thread with a connected
    /// stream; the loop drains once it returns.
    async fn serve_one<F>(
        tag: &str,
        handler: impl Fn(Authenticated<interprocess::local_socket::tokio::Stream, ()>) -> F
        + Send
        + 'static,
        client: impl FnOnce(Stream) + Send + 'static,
    ) -> Served
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let name = format!("daemon-serve-{tag}-{}", std::process::id());
        let Bound::Won(listener) = acquire_tokio(Path::new(&name)).await.unwrap() else {
            panic!("first owner")
        };
        let lifecycle = Arc::new(Lifecycle::default());
        let server = tokio::spawn(
            serve(
                listener,
                Arc::clone(&lifecycle),
                Arc::new(Admission::new(Limits::default())),
                SameUser,
            )
            .run(
                |()| Pool::Application,
                move |connection, permit| {
                    let served = handler(connection);
                    async move {
                        let _permit = permit;
                        served.await;
                    }
                },
            ),
        );
        let client = tokio::task::spawn_blocking(move || {
            client(
                Stream::connect(name.as_str().to_ns_name::<GenericNamespaced>().unwrap()).unwrap(),
            )
        });
        tokio::time::timeout(BUDGET, client)
            .await
            .expect("the client did not finish")
            .unwrap();
        lifecycle.start_drain();
        tokio::time::timeout(BUDGET, server)
            .await
            .expect("the accept loop did not drain")
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn the_handler_receives_the_clients_bytes_unchanged() {
        let served = serve_one(
            "echo",
            |mut connection| async move {
                let mut request = [0u8; 13];
                connection.read_exact(&mut request).await.unwrap();
                connection.write_all(&request).await.unwrap();
            },
            |mut pipe| {
                pipe.write_all(b"hello, server").unwrap();
                let mut echo = [0u8; 13];
                pipe.read_exact(&mut echo).unwrap();
                assert_eq!(&echo, b"hello, server");
            },
        )
        .await;
        assert_eq!(served.admitted, 1, "{served:?}");
    }

    #[tokio::test]
    async fn a_server_that_speaks_first_serves_a_client_that_has_not_written() {
        let served = serve_one(
            "greeting",
            |mut connection| async move {
                connection.write_all(b"welcome").await.unwrap();
                let mut reply = [0u8; 3];
                connection.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"ack");
                connection.write_all(b"done").await.unwrap();
            },
            |mut pipe| {
                let mut greeting = [0u8; 7];
                pipe.read_exact(&mut greeting).unwrap();
                assert_eq!(&greeting, b"welcome");
                pipe.write_all(b"ack").unwrap();
                let mut done = [0u8; 4];
                pipe.read_exact(&mut done).unwrap();
                assert_eq!(&done, b"done");
            },
        )
        .await;
        assert_eq!(
            (served.admitted, served.unauthenticated),
            (1, 0),
            "{served:?}"
        );
    }
}

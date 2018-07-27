use future_utils::FutureExt;
use futures::future::{self, Future};
use futures::Stream;
use std::time::Duration;
use tokio;
use tokio::runtime::Runtime;
use tokio::io::AsyncRead;
use UtpSocket;

#[test]
fn it_receives_data_after_write_shutdown() {
    let mut evloop = unwrap!(Runtime::new());

    let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
    let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
    let listener_addr = unwrap!(listener.local_addr());

    let accept_connections = listener
        .incoming()
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(stream, _incoming)| {
            let stream = unwrap!(stream);
            // delay data sending and allow remote peer to tick and actually shutdown stream
            future::empty::<(), _>()
                .with_timeout(Duration::from_secs(2))
                .and_then(move |_| tokio::io::write_all(stream, vec![1, 2, 3, 4]))
        })
        .then(|_| Ok(()));
    tokio::spawn(accept_connections);

    let res = evloop.block_on(future::lazy(move || {
        sock.connect(&listener_addr).and_then(|stream| {
            unwrap!(stream.shutdown_write());
            tokio::io::read_exact(stream, vec![0; 4]).map(|(_stream, buff)| buff)
        })
    }));

    let received = unwrap!(res);
    assert_eq!(received, [1, 2, 3, 4]);
}

#[test]
fn when_fin_is_received_read_returns_0_bytes_read() {
    let mut evloop = unwrap!(Runtime::new());

    let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
    let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
    let listener_addr = unwrap!(listener.local_addr());

    let accept_connections = listener
        .incoming()
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(stream, _incoming)| {
            let stream = unwrap!(stream);
            tokio::io::shutdown(stream)
        })
        .then(|_| Ok(()));
    tokio::spawn(accept_connections);

    let mut data = vec![0; 64];
    let res = evloop.block_on(future::lazy(move || {
        sock.connect(&listener_addr).and_then(|mut stream| {
            // read until smth arrives or read is terminated by Fin packet
            future::poll_fn(move || stream.poll_read(&mut data))
        })
    }));

    let received = unwrap!(res);
    assert_eq!(received, 0);
}

mod finalize {
    use super::*;

    #[test]
    fn it_waits_for_both_peers_to_shutdown() {
        let mut evloop = unwrap!(Runtime::new());

        let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
        let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
        let listener_addr = unwrap!(listener.local_addr());

        let accept_connections = listener
            .incoming()
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(move |(stream, _incoming)| {
                let stream = unwrap!(stream);
                tokio::io::shutdown(stream).and_then(|stream| stream.finalize().infallible())
            })
            .then(|_| Ok(()));
        tokio::spawn(accept_connections);

        let res = evloop.block_on(future::lazy(move || {
            sock.connect(&listener_addr).and_then(|stream| {
                tokio::io::shutdown(stream).and_then(|stream| stream.finalize().infallible())
            })
        }));

        unwrap!(res);
    }

    #[test]
    fn it_times_out_when_not_both_peers_shutdown() {
        let mut evloop = unwrap!(Runtime::new());

        let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
        let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0")));
        let listener_addr = unwrap!(listener.local_addr());

        let accept_connections = listener
            .incoming()
            .into_future()
            .map_err(|(e, _)| e)
            .and_then(move |(stream, _incoming)| {
                let stream = unwrap!(stream);
                tokio::io::shutdown(stream).and_then(|stream| stream.finalize().infallible())
            })
            .then(|_| Ok(()));
        tokio::spawn(accept_connections);

        let res = evloop.block_on(future::lazy(move || {
            sock.connect(&listener_addr)
                .and_then(|stream| stream.finalize().infallible())
                .with_timeout(Duration::from_secs(2))
        }));

        let finalize_timedout = unwrap!(res).is_none();
        assert!(finalize_timedout);
    }
}

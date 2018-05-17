use UtpSocket;
use tokio_core::reactor::Core;
use tokio_io;
use futures::future::{self, Future};
use futures::Stream;
use future_utils::FutureExt;
use std::time::Duration;

#[test]
fn it_receives_data_after_write_shutdown() {
    let mut evloop = unwrap!(Core::new());
    let handle = evloop.handle();
    let handle2 = evloop.handle();

    let (sock, _) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0"), &handle));
    let (_, listener) = unwrap!(UtpSocket::bind(&addr!("127.0.0.1:0"), &handle));
    let listener_addr = unwrap!(listener.local_addr());

    let accept_connections = listener
        .incoming()
        .into_future()
        .map_err(|(e, _)| e)
        .and_then(move |(stream, _incoming)| {
            let stream = unwrap!(stream);
            // delay data sending and allow remote peer to tick and actually shutdown stream
            future::empty::<(), _>()
                .with_timeout(Duration::from_secs(2), &handle2)
                .and_then(move |_| tokio_io::io::write_all(stream, vec![1, 2, 3, 4]))
        })
        .then(|_| Ok(()));
    handle.spawn(accept_connections);

    let res = evloop.run(future::lazy(|| {
        sock.connect(&listener_addr).and_then(|stream| {
            unwrap!(stream.shutdown_write());
            tokio_io::io::read_exact(stream, vec![0; 4]).map(|(_stream, buff)| buff)
        })
    }));

    let received = unwrap!(res);
    assert_eq!(received, [1, 2, 3, 4]);
}

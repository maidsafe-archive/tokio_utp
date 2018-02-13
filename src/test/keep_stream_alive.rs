use UtpSocket;
use tokio_core::reactor::Core;
use tokio_io;
use futures::future;
use futures::{Future, Stream};
use future_utils;
use std::time::Duration;
use future_utils::Timeout;
use void::ResultVoidExt;
use env_logger;
use netsim::{self, SubnetV4};
use netsim::node::Ipv4Node;
use std;
use std::net::{SocketAddr, SocketAddrV4};
use rand;

const KEEP_ALIVE_DURATION_SECS: u64 = 50;

#[test]
fn keep_stream_alive() {
    let _ = env_logger::init();

    let mut core = unwrap!(Core::new());
    let handle = core.handle();

    let (addr_tx, addr_rx) = std::sync::mpsc::channel();
    let msg: [u8; 8] = rand::random();

    let res = core.run(future::lazy(|| {
        let node_server = netsim::node::endpoint_v4(move |ip| {
            let mut core = unwrap!(Core::new());
            let handle = core.handle();

            let res = core.run(future::lazy(move || {
                let (_, listener) = unwrap!(UtpSocket::bind(&addr!("0.0.0.0:0"), &handle));
                let our_port = unwrap!(listener.local_addr()).port();
                let our_addr = SocketAddr::V4(SocketAddrV4::new(ip, our_port));
                unwrap!(addr_tx.send(our_addr));

                listener
                .incoming()
                .into_future()
                .map_err(|(e, _listener)| panic!("listener errored: {}", e))
                .and_then(move |(stream_opt, _listener)| {
                    let stream = unwrap!(stream_opt);

                    trace!("reading from stream");
                    tokio_io::io::read_to_end(stream, Vec::new())
                    .map_err(|e| panic!("reading socket errored: {}", e))
                    .map(move |(_stream, data)| {
                        trace!("finished reading from stream");
                        assert_eq!(&data[..], &msg[..]);
                    })
                })
            }));
            res.void_unwrap()
        });

        let node_client = netsim::node::endpoint_v4(move |_ip| {
            let mut core = unwrap!(Core::new());
            let handle = core.handle();

            let res = core.run(future::lazy(move || {
                let their_addr = unwrap!(addr_rx.recv());

                let (socket, _) = unwrap!(UtpSocket::bind(&addr!("0.0.0.0:0"), &handle));

                socket
                .connect(&their_addr)
                .map_err(|e| panic!("connecting errored: {}", e))
                .and_then(move |stream| {
                    Timeout::new(Duration::from_secs(KEEP_ALIVE_DURATION_SECS), &handle)
                    .and_then(move |()| {
                        tokio_io::io::write_all(stream, msg)
                        .map_err(|e| panic!("writing errored: {}", e))
                        .and_then(|(_stream, _msg)| {
                            socket
                            .finalize()
                            .map(|()| {
                                trace!("finished writing to stream");
                            })
                        })
                    })
                })
            }));
            res.void_unwrap()
        });

        let min_latency = Duration::from_millis(200);
        let mean_add_latency = Duration::from_millis(20);
        //let loss_rate = 0.1;
        //let loss_duration = Duration::from_millis(10);
        let (join_handle, _plug) = netsim::spawn::network_v4(&handle, SubnetV4::global(), netsim::node::router_v4((
            node_client
            .hops(4)
            .latency(min_latency, mean_add_latency),
            //.packet_loss(loss_rate, loss_duration),

            node_server
            .hops(4)
            .latency(min_latency, mean_add_latency),
            //.packet_loss(loss_rate, loss_duration),
        )));

        future_utils::thread_future(|| unwrap!(join_handle.join()))
        .map(|((), ())| ())
    }));
    res.void_unwrap()
}


use env_logger;
use future_utils::{FutureExt, Timeout};
use futures::future;
use futures::future::Loop;
use futures::{Future, Stream};
use netsim::node::Ipv4Node;
use netsim::{self, Ipv4Range};
use std;
use std::fs::File;
use std::io::Write;
use std::net::{SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};
use tokio_core::reactor::Core;
use tokio_io;
use util;
use void::ResultVoidExt;
use UtpSocket;

const KEEP_ALIVE_PERIOD_SECS: u64 = 20;
const DATA_LEN: usize = 1024;

#[test]
fn keep_stream_alive() {
    let _ = env_logger::init();

    let mut core = unwrap!(Core::new());
    let handle = core.handle();
    let network = netsim::Network::new(&handle);
    let network_handle = network.handle();

    let (addr_tx, addr_rx) = std::sync::mpsc::channel();

    let res = core.run(future::lazy(|| {
        let node_a = netsim::node::ipv4::machine(move |ip| {
            let mut core = unwrap!(Core::new());
            let handle = core.handle();
            let addr = SocketAddr::V4(SocketAddrV4::new(ip, 0));
            let (socket_a, _) = unwrap!(UtpSocket::bind(&addr, &handle));
            let their_addr = unwrap!(addr_rx.recv());
            let res = core.run(future::lazy(move || {
                let all_send_data = Vec::new();

                trace!("connecting to {}", their_addr);
                socket_a
                    .connect(&their_addr)
                    .map_err(|e| {
                        panic!("connect error: {}", e);
                    })
                    .and_then(move |stream_a| {
                        trace!("connected");
                        let start_time = Instant::now();
                        let keep_alive_period = Duration::from_secs(KEEP_ALIVE_PERIOD_SECS);
                        let mut increment = keep_alive_period;
                        while increment > Duration::from_secs(1) {
                            increment /= 2;
                        }
                        future::loop_fn(
                            (all_send_data, stream_a, start_time, increment),
                            move |(mut all_send_data, stream_a, prev_instant, increment)| {
                                trace!("sleeping for {:?}", increment);
                                let send_data = util::random_vec(DATA_LEN);
                                let next_instant = prev_instant + increment;
                                Timeout::new_at(next_instant, &handle).and_then(move |()| {
                                    trace!("awake, sending data");
                                    tokio_io::io::write_all(stream_a, send_data)
                                        .map_err(|e| {
                                            panic!("write error: {}", e);
                                        })
                                        .map(move |(stream_a, send_data)| {
                                            trace!("data sent");
                                            all_send_data.extend(send_data);
                                            let next_increment = increment * 2;
                                            if next_increment > keep_alive_period {
                                                trace!("finished! breaking...");
                                                unwrap!(stream_a.shutdown_write());
                                                Loop::Break(all_send_data)
                                            } else {
                                                Loop::Continue((
                                                    all_send_data,
                                                    stream_a,
                                                    next_instant,
                                                    next_increment,
                                                ))
                                            }
                                        })
                                })
                            },
                        )
                    })
            }));
            res.void_unwrap()
        });

        let node_b = netsim::node::ipv4::machine(move |ip| {
            let mut core = unwrap!(Core::new());
            let handle = core.handle();
            let addr = SocketAddr::V4(SocketAddrV4::new(ip, 1234));
            unwrap!(addr_tx.send(addr));
            let (_, listener_b) = unwrap!(UtpSocket::bind(&addr, &handle));
            let res = core.run(future::lazy(move || {
                listener_b
                    .incoming()
                    .into_future()
                    .map_err(|(e, _listener_b)| {
                        panic!("accept error: {}", e);
                    })
                    .and_then(|(stream_b_opt, listener_b)| {
                        let stream_b = unwrap!(stream_b_opt);
                        trace!("accepted connection");
                        tokio_io::io::read_to_end(stream_b, Vec::new())
                            .map_err(|e| {
                                panic!("read error: {}", e);
                            })
                            .map(|(_stream_b, recv_data)| {
                                drop(listener_b);
                                recv_data
                            })
                    })
            }));
            res.void_unwrap()
        });

        let node_a = {
            node_a
                .latency(Duration::from_millis(150), Duration::from_millis(10))
                .packet_loss(0.2, Duration::from_millis(10))
        };

        let node_b = {
            node_b
                .latency(Duration::from_millis(150), Duration::from_millis(10))
                .packet_loss(0.2, Duration::from_millis(10))
        };

        let (spawn_complete, _plug) = netsim::spawn::ipv4_tree(
            &network_handle,
            Ipv4Range::global(),
            netsim::node::ipv4::router((node_a, node_b)),
        );

        spawn_complete
            .resume_unwind()
            .map(|(send_data, recv_data)| {
                if send_data != recv_data {
                    let mut send = unwrap!(File::create("round-trip-failed-send.dat"));
                    let mut recv = unwrap!(File::create("round-trip-failed-recv.dat"));
                    unwrap!(send.write_all(&send_data));
                    unwrap!(recv.write_all(&recv_data));
                    panic!("Data corrupted during round-trip! Sent/received data saved to keep-stream-alive-failed-send.dat and keep-stream-alive-failed-recv.dat");
                }
            })
    }));
    res.void_unwrap()
}

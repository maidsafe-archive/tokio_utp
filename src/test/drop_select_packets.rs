use bytes::BytesMut;
use env_logger;
use future_utils::FutureExt;
use futures::future;
use futures::{Future, Stream};
use netsim;
use netsim::wire::{IntoIpPlug, IpPacket, IpPlug, Ipv4Payload};
use netsim::{Ipv4AddrExt, Ipv4Range, Ipv4Route};
use packet::Packet;
use rand;
use std;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use tokio_core::reactor::Core;
use tokio_io;
use void::ResultVoidExt;
use UtpSocket;

#[test]
fn drop_select_packets() {
    for i in 0..6 {
        drop_selected_packet(i, true);
        drop_selected_packet(i, false);
    }
}

fn drop_selected_packet(n: u32, sender: bool) {
    trace!("");
    trace!("");
    trace!("drop_selected_packet({}, {})", n, sender);
    let _ = env_logger::init();

    let mut core = unwrap!(Core::new());
    let handle = core.handle();
    let network = netsim::Network::new(&handle);
    let network_handle = network.handle();

    let (addr_tx, addr_rx) = std::sync::mpsc::channel();

    let res = core.run(future::lazy(move || {
        let (mut plug_client, mut plug_server) = IpPlug::new_pair();

        let mut i = 0;
        let filter = move |packet| {
            if let IpPacket::V4(ref ipv4_packet) = packet {
                if let Ipv4Payload::Udp(udp_packet) = ipv4_packet.payload() {
                    let data = BytesMut::from(udp_packet.payload());
                    if let Ok(p) = Packet::parse(data) {
                        let prev_i = i;
                        i += 1;
                        if prev_i == n {
                            trace!("dropping packet: {:?}", p);
                            return None;
                        }
                    }
                }
            }
            Some(packet)
        };

        if sender {
            plug_server = plug_server.filter_map(filter).into_ip_plug(&network_handle);
        } else {
            plug_client = plug_client.filter_map(filter).into_ip_plug(&network_handle);
        }

        let ip_client = Ipv4Addr::random_global();
        let spawn_client = netsim::device::MachineBuilder::new()
            .add_ip_iface(
                netsim::iface::IpIfaceBuilder::new()
                    .ipv4_addr(ip_client, 0)
                    .ipv4_route(Ipv4Route::new(Ipv4Range::global(), None)),
                plug_client,
            ).spawn(&network_handle, move || {
                let mut core = unwrap!(Core::new());
                let handle = core.handle();
                let (socket_client, _) = unwrap!(UtpSocket::bind(&addr!("0.0.0.0:0"), &handle));
                let their_addr = unwrap!(addr_rx.recv());
                let res = core.run(future::lazy(move || {
                    socket_client
                        .connect(&their_addr)
                        .map_err(|e| {
                            panic!("connect error: {}", e);
                        }).and_then(|stream_client| {
                            tokio_io::io::write_all(stream_client, b"ping")
                                .map_err(|e| panic!("write error: {}", e))
                        }).and_then(|(stream_client, _bytes)| {
                            tokio_io::io::read_to_end(stream_client, Vec::new())
                                .map_err(|e| panic!("read error: {}", e))
                        }).and_then(|(stream_client, bytes)| {
                            assert_eq!(&bytes[..], b"pong");
                            tokio_io::io::shutdown(stream_client)
                                .map_err(|e| panic!("error shuting down: {}", e))
                        }).and_then(|stream_client| stream_client.finalize().infallible())
                }));
                res.void_unwrap()
            });

        let ip_server = Ipv4Addr::random_global();
        let spawn_server = netsim::device::MachineBuilder::new()
            .add_ip_iface(
                netsim::iface::IpIfaceBuilder::new()
                    .ipv4_addr(ip_server, 0)
                    .ipv4_route(Ipv4Route::new(Ipv4Range::global(), None)),
                plug_server,
            ).spawn(&network_handle, move || {
                let mut core = unwrap!(Core::new());
                let handle = core.handle();

                let port = rand::random::<u16>() / 2 + 1000;
                let addr = SocketAddr::V4(SocketAddrV4::new(ip_server, port));
                let (_socket_server, listener_server) = unwrap!(UtpSocket::bind(&addr, &handle));

                unwrap!(addr_tx.send(addr));
                let res = core.run(future::lazy(move || {
                    listener_server
                        .incoming()
                        .into_future()
                        .map_err(|(e, _listener_server)| {
                            panic!("accept error: {}", e);
                        }).and_then(|(stream_b_opt, _listener_server)| {
                            let stream_server = unwrap!(stream_b_opt);
                            tokio_io::io::read_exact(stream_server, [0u8; 4])
                                .map_err(|e| panic!("read error: {}", e))
                        }).and_then(|(stream_server, bytes)| {
                            assert_eq!(&bytes, b"ping");
                            tokio_io::io::write_all(stream_server, b"pong")
                                .map_err(|e| panic!("write error: {}", e))
                        }).and_then(|(stream_server, _bytes)| {
                            tokio_io::io::shutdown(stream_server)
                                .map_err(|e| panic!("error shuting down: {}", e))
                        }).and_then(|stream_server| stream_server.finalize().infallible())
                }));
                res.void_unwrap()
            });

        let spawn_client = spawn_client.resume_unwind();
        let spawn_server = spawn_server.resume_unwind();

        spawn_client.join(spawn_server).map(|((), ())| ())
    }));
    res.void_unwrap()
}

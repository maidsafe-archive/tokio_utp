extern crate env_logger;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_utp;
#[macro_use]
extern crate unwrap;
extern crate void;

use tokio_utp::*;

use futures::{future, Future};
use tokio_core::reactor::Core;
use void::Void;

use std::net::SocketAddr;

pub fn main() {
    unwrap!(::env_logger::init());

    let local_addr: SocketAddr = unwrap!("127.0.0.1:0".parse());
    let remote_addr: SocketAddr = unwrap!("127.0.0.1:4561".parse());

    let mut core = unwrap!(Core::new());
    let handle = core.handle();

    let _: Result<(), Void> = core.run(future::lazy(|| {
        let (socket, _) = unwrap!(UtpSocket::bind(&local_addr, &handle));

        // connect to the server
        socket
            .connect(&remote_addr)
            .and_then(|stream| {
                // send it some data
                println!("sending \"hello world\" to server");
                tokio_io::io::write_all(stream, "hello world").and_then(|(stream, _)| {
                    // shutdown our the write side of the connection.
                    tokio_io::io::shutdown(stream).and_then(|stream| {
                        // read the stream to completion.
                        tokio_io::io::read_to_end(stream, Vec::new()).and_then(|(_, data)| {
                            println!("received {:?} from server", String::from_utf8(data));
                            Ok(())
                        })
                    })
                })
            })
            .then(|res| {
                unwrap!(res);
                Ok(())
            })
    }));
}

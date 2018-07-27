extern crate env_logger;
extern crate futures;
extern crate tokio;
extern crate tokio_utp;
#[macro_use]
extern crate unwrap;
extern crate void;

use futures::{future, Future, Stream};
use tokio::runtime::Runtime;
use tokio::io::AsyncRead;
use tokio_utp::*;

use std::net::SocketAddr;

pub fn main() {
    unwrap!(::env_logger::init());

    // Start a simple echo server

    let addr: SocketAddr = unwrap!("127.0.0.1:4561".parse());
    let mut runtime = unwrap!(Runtime::new());
    let result = runtime.block_on(future::lazy(move || {
        let (_, listener) = unwrap!(UtpSocket::bind(&addr));
        listener
            .incoming()
            .for_each(|stream| {
                let (reader, writer) = stream.split();
                tokio::io::copy(reader, writer).map(|_| ())
            })
    }));
    unwrap!(result)
}

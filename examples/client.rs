extern crate utp;
extern crate mio;
extern crate env_logger;

use mio::*;
use utp::*;

use std::net::SocketAddr;

pub fn main() {
    ::env_logger::init().unwrap();

    let local_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let remote_addr: SocketAddr = "127.0.0.1:4561".parse().unwrap();

    let (socket, _) = UtpSocket::bind(&local_addr).unwrap();

    // Connect to the remote
    let stream = socket.connect(&remote_addr).unwrap();

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    poll.register(&socket, Token(0), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    poll.register(&stream, Token(1), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();

    println!("bound...");

    let mut stream = Some(stream);

    loop {
        poll.poll(&mut events, None).unwrap();

        for event in &events {
            match event.token() {
                Token(0) => {
                    socket.ready(event.readiness()).unwrap();
                }
                Token(1) => {
                    if event.readiness().is_writable() {
                        // A bit hacky, we know right now that this will never
                        // return WouldBlock
                        stream.as_ref().unwrap().write("hello world".as_bytes()).unwrap();
                    }

                    stream = None;
                }
                _ => {}
            }
        }
    }
}

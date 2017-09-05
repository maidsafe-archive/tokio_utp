extern crate utp2;
extern crate mio;
extern crate env_logger;

use mio::*;
use utp2::*;

use std::{io, str};
use std::net::SocketAddr;
use std::collections::HashMap;

macro_rules! unwrap_nb {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                break;
            }
            Err(e) => {
                panic!("unexpected error; e={:?}", e);
            }
        }
    }
}

pub fn main() {
    ::env_logger::init().unwrap();

    let addr: SocketAddr = "127.0.0.1:4561".parse().unwrap();
    let (socket, listener) = UtpSocket::bind(&addr).unwrap();

    let poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(1024);

    let mut connections = HashMap::new();

    let mut next_token = 2;

    poll.register(&socket, Token(0), Ready::readable() | Ready::writable(), PollOpt::edge()).unwrap();
    poll.register(&listener, Token(1), Ready::readable(), PollOpt::edge()).unwrap();

    println!("listening...");

    let mut buf = [0u8; 4096];

    loop {
        poll.poll(&mut events, None).unwrap();

        for event in &events {
            match event.token() {
                Token(0) => {
                    socket.ready(event.readiness()).unwrap();
                }
                Token(1) => {
                    loop {
                        let sock = unwrap_nb!(listener.accept());
                        let token = Token(next_token);
                        next_token += 1;

                        poll.register(&sock,
                                      token,
                                      Ready::readable() | Ready::writable(),
                                      PollOpt::edge()).unwrap();

                        connections.insert(token, sock);
                    }
                }
                token => {
                    if event.readiness().is_readable() {
                        loop {
                            let n = unwrap_nb!(connections[&token].read(&mut buf));

                            if n == 0 {
                                connections.remove(&token);
                                break;
                            }

                            let msg: &str = str::from_utf8(&buf[..n]).unwrap();

                            println!("READ: `{}`", msg);
                        }
                    }
                }
            }
        }
    }
}

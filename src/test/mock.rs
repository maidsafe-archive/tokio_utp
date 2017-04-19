use packet::Packet;

use mio::{Ready, Events, Poll, PollOpt, Token};
use mio::net::UdpSocket;

use bytes::BytesMut;

use std::io;
use std::net::SocketAddr;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};

/// Represents a mock UTP socket
pub struct Mock {
    // The UDP socket
    socket: UdpSocket,

    // Poll handle
    poll: Poll,

    // Packets that have been received but not handled
    recv: HashMap<SocketAddr, VecDeque<Packet>>,
}

const DEFAULT_TIMEOUT_MS: u64 = 5_000;

impl Mock {
    /// Create a new mock socket
    pub fn new() -> Mock {
        // Bind to an available local socket
        let socket = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
        let poll = Poll::new().unwrap();

        // Initial register
        poll.register(&socket, Token(0),
                      Ready::readable() | Ready::writable(),
                      PollOpt::edge()).unwrap();

        Mock {
            socket: socket,
            poll: poll,
            recv: HashMap::new(),
        }
    }

    pub fn background<F>(mut self, f: F) -> JoinHandle<Mock>
        where F: FnOnce(&mut Mock) + Send + 'static,
    {
        thread::spawn(move || {
            f(&mut self);
            self
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }

    /// Receive a packet from the specified remote
    pub fn recv_from(&mut self, remote: &SocketAddr) -> Packet {
        self.recv_from_ms(remote, DEFAULT_TIMEOUT_MS).unwrap()
    }

    /// Receive a packet from the specified remote
    pub fn recv_from_ms(&mut self, remote: &SocketAddr, ms: u64) -> Option<Packet> {
        // First check for buffered packets
        if let Some(buf) = self.recv.get_mut(remote).and_then(|b| b.pop_front()) {
            return Some(buf);
        }

        self.recv(Duration::from_millis(ms));

        self.recv.get_mut(remote).and_then(|b| b.pop_front())
    }

    /// Receive packets into local buffer
    fn recv(&mut self, dur: Duration) {
        let mut buf = [0; 2048];
        let now = Instant::now();

        let mut received = false;

        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((n, remote)) => {
                    received = true;

                    let buf = BytesMut::from(&buf[..n]);
                    let packet = Packet::parse(buf).unwrap();

                    self.recv.entry(remote)
                        .or_insert(VecDeque::new())
                        .push_back(packet);
                }
                Err(e) => {
                    if e.kind() != io::ErrorKind::WouldBlock {
                        panic!("unexpected error; {:?}", e);
                    }

                    if received {
                        return;
                    }

                    // Wait until the socket is readable
                    self.poll.reregister(&self.socket, Token(0),
                                         Ready::readable(),
                                         PollOpt::edge()).unwrap();

                    let elapsed = now.elapsed();

                    if elapsed >= dur {
                        return;
                    }

                    let mut events = Events::with_capacity(8);

                    // wait...
                    self.poll.poll(&mut events, Some(dur - elapsed)).unwrap();
                }
            }
        }
    }

    pub fn send_to(&self, packet: Packet, target: &SocketAddr) {
        loop {
            match self.socket.send_to(packet.as_slice(), target) {
                Ok(_) => return,
                Err(e) => {
                    if e.kind() != io::ErrorKind::WouldBlock {
                        panic!("unexpected error; {:?}", e);
                    }

                    // Wait until the socket is readable
                    self.poll.reregister(&self.socket, Token(0),
                                         Ready::writable(),
                                         PollOpt::edge()).unwrap();

                    let mut events = Events::with_capacity(8);

                    // wait...
                    self.poll.poll(&mut events, None).unwrap();
                }
            }
        }
    }

    pub fn wait(&mut self, ms: u64) {
        let now = Instant::now();
        let dur = Duration::from_millis(ms);

        loop {
            let elapsed = now.elapsed();
            if elapsed >= dur {
                return;
            }

            self.recv(dur - elapsed);
        }
    }

    pub fn assert_quiescence(&mut self, ms: u64) {
        self.recv(Duration::from_millis(ms));

        for (remote, buf) in &self.recv {
            if let Some(packet) = buf.front() {
                panic!("socket not quiescent; remote={:?}; packet={:?}",
                       remote, packet);
            }
        }
    }
}

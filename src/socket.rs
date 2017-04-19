use {util, TIMESTAMP_MASK};
use delays::Delays;
use in_queue::InQueue;
use out_queue::OutQueue;
use packet::{self, Packet};

use mio::net::UdpSocket;
use mio::{Evented, Registration, SetReadiness, Ready, Poll, PollOpt, Token};

use rand;
use bytes::{BytesMut, BufMut};
use slab::Slab;

use std::{cmp, io, u32};
use std::cell::RefCell;
use std::rc::Rc;
use std::net::SocketAddr;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub struct UtpSocket {
    // Shared state
    inner: InnerCell,
}

/// Manages the state for a single UTP connection
pub struct UtpStream {
    // Shared state
    inner: InnerCell,

    // Connection identifier
    token: usize,

    // Mio registration
    registration: Registration,
}

pub struct UtpListener {
    // Shared state
    inner: InnerCell,

    // Used to register interest in accepting UTP sockets
    registration: Registration,
}

// Shared between the UtpSocket and each UtpStream
struct Inner {
    // State that needs to be passed to `Connection`. This is broken out to make
    // the borrow checker happy.
    shared: Shared,

    // Connection specific state
    connections: Slab<Connection>,

    // Lookup a connection by key
    connection_lookup: HashMap<Key, usize>,

    // Buffer used for in-bound data
    in_buf: BytesMut,

    accept_buf: VecDeque<UtpStream>,

    listener: SetReadiness,
}

struct Shared {
    // The UDP socket backing everything!
    socket: UdpSocket,

    // The current readiness of the socket, this is used when figuring out the
    // readiness of each connection.
    ready: Ready,

    // Buffer used for out-bound data, this does not need to be share-able
    out_buf: Vec<u8>,

    // where to write the out_buf to
    out_buf_dst: Option<SocketAddr>,
}

// Owned by UtpSocket
#[derive(Debug)]
struct Connection {
    // Current socket state
    state: State,

    // A combination of the send ID and the socket address
    key: Key,

    // Used to signal readiness on the `UtpStream`
    set_readiness: SetReadiness,

    // Queue of outbound packets. Packets will stay in the queue until the peer
    // has acked them.
    out_queue: OutQueue,

    // Queue of inbound packets. The queue orders packets according to their
    // sequence number.
    in_queue: InQueue,

    // Activity deadline
    deadline: Option<Instant>,

    // Tracks delays for the congestion control algorithm
    our_delays: Delays,

    their_delays: Delays,

    last_maxed_out_window: Instant,
    average_delay: u32,
    current_delay_sum: i64,
    current_delay_samples: i64,
    average_delay_base: u32,
    average_sample_time: Instant,
    clock_drift: i32,
    slow_start: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Key {
    receive_id: u16,
    addr: SocketAddr,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum State {
    // Establishing a new connection, waiting for the peer to respond with a
    // STATE.
    SynSent,
    // Received Syn, the state packet is sent immediately, but the connection is
    // not transitioned to `Connected` until it has been accepted.
    SynRecv,
    // Fully connected state
    Connected,
    // A SYN has been sent and we are currently waiting for an ACK before
    // closing the connection.
    FinSent,
}

type InnerCell = Rc<RefCell<Inner>>;

const MIN_BUFFER_SIZE: usize = 4 * 1_024;
const MAX_BUFFER_SIZE: usize = 64 * 1_024;
const DEFAULT_IN_BUFFER_SIZE: usize = 64 * 1024;
const DEFAULT_OUT_BUFFER_SIZE: usize = 4 * 1024;
const MAX_CONNECTIONS_PER_SOCKET: usize = 2 * 1024;
const DEFAULT_TIMEOUT_MS: u64 = 1_000;
const TARGET_DELAY: u32 = 100_000; // 100ms in micros

const SLOW_START_THRESHOLD: usize = DEFAULT_IN_BUFFER_SIZE;
const MAX_CWND_INCREASE_BYTES_PER_RTT: usize = 3000;
const MIN_WINDOW_SIZE: usize = 10;
const MAX_DATA_SIZE: usize = 1_400 - 20;

impl UtpSocket {
    /// Bind a new `UtpSocket` to the given socket address
    pub fn bind(addr: &SocketAddr) -> io::Result<(UtpSocket, UtpListener)> {
        UdpSocket::bind(addr).map(UtpSocket::from_socket)
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.borrow().shared.socket.local_addr()
    }

    /// Create a new `Utpsocket` backed by the provided `UdpSocket`.
    pub fn from_socket(socket: UdpSocket) -> (UtpSocket, UtpListener) {
        let (registration, set_readiness) = Registration::new2();

        let inner = Rc::new(RefCell::new(Inner {
            shared: Shared {
                socket: socket,
                ready: Ready::empty(),
                out_buf: Vec::with_capacity(DEFAULT_OUT_BUFFER_SIZE),
                out_buf_dst: None,
            },
            connections: Slab::new(),
            connection_lookup: HashMap::new(),
            in_buf: BytesMut::with_capacity(DEFAULT_IN_BUFFER_SIZE),
            accept_buf: VecDeque::new(),
            listener: set_readiness,
        }));

        let listener = UtpListener {
            inner: inner.clone(),
            registration: registration,
        };

        let socket = UtpSocket {
            inner: inner,
        };

        (socket, listener)
    }

    /// Connect a new `UtpSocket` to the given remote socket address
    pub fn connect(&self, addr: &SocketAddr) -> io::Result<UtpStream> {
        self.inner.borrow_mut().connect(addr, &self.inner)
    }

    /// Called whenever the socket readiness changes
    pub fn ready(&self, ready: Ready) -> io::Result<()> {
        self.inner.borrow_mut().ready(ready, &self.inner)
    }

    /// This function should be called every 500ms
    pub fn tick(&self) -> io::Result<()> {
        self.inner.borrow_mut().tick()
    }
}

impl Evented for UtpSocket {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.inner.borrow_mut().shared.socket.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.inner.borrow_mut().shared.socket.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        self.inner.borrow_mut().shared.socket.deregister(poll)
    }
}

impl UtpListener {
    /// Receive a new inbound connection.
    ///
    /// This function will also advance the state of all associated connections.
    pub fn accept(&self) -> io::Result<UtpStream> {
        self.inner.borrow_mut().accept()
    }
}

impl Evented for UtpListener {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        Evented::deregister(&self.registration, poll)
    }
}

impl UtpStream {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        let inner = self.inner.borrow();
        inner.shared.socket.local_addr()
    }

    pub fn read(&self, dst: &mut [u8]) -> io::Result<usize> {
        let mut inner = self.inner.borrow_mut();
        let connection = &mut inner.connections[self.token];

        match connection.in_queue.read(dst) {
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if connection.state == State::Connected {
                    try!(connection.update_readiness());
                    Err(io::ErrorKind::WouldBlock.into())
                } else if connection.state.is_closed() {
                    // Connection is closed
                    Ok(0)
                } else {
                    unreachable!();
                }
            }
            ret => {
                connection.update_local_window();
                ret
            }
        }
    }

    pub fn write(&self, src: &[u8]) -> io::Result<usize> {
        self.inner.borrow_mut().write(self.token, src)
    }
}

#[cfg(test)]
impl UtpStream {
    pub fn is_readable(&self) -> bool {
        let inner = self.inner.borrow();
        let connection = &inner.connections[self.token];
        connection.set_readiness.readiness().is_readable()
    }

    pub fn is_writable(&self) -> bool {
        let inner = self.inner.borrow();
        let connection = &inner.connections[self.token];
        connection.set_readiness.readiness().is_writable()
    }
}

impl Drop for UtpStream {
    fn drop(&mut self) {
        self.inner.borrow_mut().close(self.token);
    }
}

impl Evented for UtpStream {
    fn register(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.registration.register(poll, token, interest, opts)
    }

    fn reregister(&self, poll: &Poll, token: Token, interest: Ready, opts: PollOpt)
        -> io::Result<()>
    {
        self.registration.reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &Poll) -> io::Result<()> {
        Evented::deregister(&self.registration, poll)
    }
}

impl Inner {
    fn accept(&mut self) -> io::Result<UtpStream> {
        match self.accept_buf.pop_front() {
            Some(socket) => {
                let conn = &mut self.connections[socket.token];

                if conn.state == State::SynRecv {
                    conn.state = State::Connected;
                } else if conn.state.is_closed() {
                    // Connection is being closed, but there may be data in the
                    // buffer...
                } else {
                    unimplemented!();
                }

                try!(conn.update_readiness());

                Ok(socket)
            }
            None => {
                // Unset readiness
                try!(self.listener.set_readiness(Ready::empty()));

                Err(io::ErrorKind::WouldBlock.into())
            }
        }
    }

    fn write(&mut self, token: usize, src: &[u8]) -> io::Result<usize> {
        let conn = &mut self.connections[token];

        if conn.state != State::Connected {
            assert!(conn.state.is_closed(),
                    "expected closed state; actual={:?}", conn.state);

            // TODO: What should this return
            return Err(io::ErrorKind::BrokenPipe.into());
        }

        match conn.out_queue.write(src) {
            Ok(n) => {
                conn.flush(&mut self.shared);
                Ok(n)
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                conn.last_maxed_out_window = Instant::now();
                try!(conn.update_readiness());
                Err(io::ErrorKind::WouldBlock.into())
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    /// Connect a new `UtpSocket` to the given remote socket address
    fn connect(&mut self, addr: &SocketAddr, inner: &InnerCell) -> io::Result<UtpStream> {
        if self.connections.len() == MAX_CONNECTIONS_PER_SOCKET {
            return Err(io::Error::new(io::ErrorKind::Other, "socket has max connections"));
        }

        debug_assert!(self.connections.len() < MAX_CONNECTIONS_PER_SOCKET);

        // The peer establishing the connection picks the identifiers uses for
        // the stream.
        let (receive_id, mut send_id) = util::generate_sequential_identifiers();

        let mut key = Key {
            receive_id: receive_id,
            addr: addr.clone()
        };

        // Because the IDs are randomly generated, there could already be an
        // existing connection with the key, so sequentially scan until we hit a
        // free slot.
        while self.connection_lookup.contains_key(&key) {
            key.receive_id += 1;
            send_id += 1;
        }

        // SYN packet has seq_nr of 1
        let mut out_queue = OutQueue::new(send_id, 0, None);

        let mut packet = Packet::syn();
        packet.set_connection_id(key.receive_id);

        // Queue the syn packet
        out_queue.push(packet);

        let (registration, set_readiness) = Registration::new2();
        let now = Instant::now();

        let token = self.connections.insert(Connection {
            state: State::SynSent,
            key: key.clone(),
            set_readiness: set_readiness,
            out_queue: out_queue,
            in_queue: InQueue::new(None),
            our_delays: Delays::new(),
            their_delays: Delays::new(),
            deadline: Some(now + Duration::from_millis(DEFAULT_TIMEOUT_MS)),
            last_maxed_out_window: now,
            average_delay: 0,
            current_delay_sum: 0,
            current_delay_samples: 0,
            average_delay_base: 0,
            average_sample_time: now,
            clock_drift: 0,
            slow_start: true,
        });

        // Track the connection in the lookup
        self.connection_lookup.insert(key, token);

        self.flush();

        Ok(UtpStream {
            inner: inner.clone(),
            token: token,
            registration: registration,
        })
    }

    fn close(&mut self, token: usize) {
        let finalized = {
            let conn = &mut self.connections[token];
            conn.send_fin(false, &mut self.shared);
            conn.flush(&mut self.shared);
            conn.is_finalized()
        };

        if finalized {
            self.remove_connection(token);
        }
    }

    fn ready(&mut self, ready: Ready, inner: &InnerCell) -> io::Result<()> {
        trace!("ready; ready={:?}", ready);

        // Update readiness
        self.shared.update_ready(ready);

        // TODO: Ensure out_buf has enough capacity
        // assert!(self.ready.is_writable() == self.out_buf_dst.is_none());

        loop {
            // Try to receive a packet
            let (packet, addr) = match self.recv_from() {
                Ok(v) => v,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    trace!("ready -> would block");
                    break;
                }
                Err(e) => {
                    trace!("recv_from; error={:?}", e);
                    return Err(e);
                }
            };

            trace!("recv_from; addr={:?}; packet={:?}", addr, packet);

            match self.process(packet, addr, inner) {
                Ok(_) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    panic!("NOPE");
                }
                Err(e) => return Err(e),
            }
        }

        self.flush();
        Ok(())
    }

    fn tick(&mut self) -> io::Result<()> {
        trace!("Socket::tick");
        for &idx in self.connection_lookup.values() {
            try!(self.connections[idx].tick(&mut self.shared));
        }

        Ok(())
    }

    fn process(&mut self,
               packet: Packet,
               addr: SocketAddr,
               inner: &InnerCell) -> io::Result<()>
    {
        // Process the packet
        match packet.ty() {
            packet::Type::Syn => {
                // SYN packets are special
                self.process_syn(packet, addr, inner)
            }
            _ => {
                // All other packets are associated with a connection, and as
                // such they should be sequenced.
                let key = Key::new(packet.connection_id(), addr);

                match self.connection_lookup.get(&key) {
                    Some(&token) => {
                        let finalized = {
                            let conn = &mut self.connections[token];
                            try!(conn.process(packet, &mut self.shared))
                        };

                        if finalized {
                            self.remove_connection(token);
                        }

                        Ok(())
                    }
                    None => {
                        trace!("no connection associated with ID; dropping packet");
                        // Invalid packet... ignore it.
                        // TODO: send RST
                        return Ok(());
                    }
                }
            }
        }
    }

    fn process_syn(&mut self,
                   packet: Packet,
                   addr: SocketAddr,
                   inner: &InnerCell) -> io::Result<()>
    {
        let seq_nr = rand::random();
        let ack_nr = packet.seq_nr();
        let send_id = packet.connection_id();
        let receive_id = send_id + 1;

        // TODO: If accept buffer is full, reset the connection

        let (registration, set_readiness) = Registration::new2();

        let key = Key {
            receive_id: receive_id,
            addr: addr,
        };

        if self.connection_lookup.contains_key(&key) {
            // TODO: What should happen here?
            unimplemented!();
        }

        let now = Instant::now();

        let mut connection = Connection {
            state: State::SynRecv,
            key: key.clone(),
            set_readiness: set_readiness,
            out_queue: OutQueue::new(send_id, seq_nr, Some(ack_nr)),
            in_queue: InQueue::new(Some(ack_nr)),
            our_delays: Delays::new(),
            their_delays: Delays::new(),
            deadline: None,
            last_maxed_out_window: now,
            average_delay: 0,
            current_delay_sum: 0,
            current_delay_samples: 0,
            average_delay_base: 0,
            average_sample_time: now,
            clock_drift: 0,
            slow_start: true,
        };

        // This will handle the state packet being sent
        connection.flush(&mut self.shared);

        let token = self.connections.insert(connection);
        self.connection_lookup.insert(key, token);

        // Store the connection in the accept buffer
        self.accept_buf.push_back(UtpStream {
            inner: inner.clone(),
            token: token,
            registration: registration,
        });

        // Notify the listener
        try!(self.listener.set_readiness(Ready::readable()));

        return Ok(());
    }

    fn recv_from(&mut self) -> io::Result<(Packet, SocketAddr)> {
        // Ensure the buffer has at least 4kb of available space.
        self.in_buf.reserve(MIN_BUFFER_SIZE);

        // Read in the bytes
        let addr = unsafe {
            let (n, addr) = try!(self.shared.socket.recv_from(self.in_buf.bytes_mut()));
            self.in_buf.advance_mut(n);
            addr
        };

        // Try loading the header
        let packet = try!(Packet::parse(self.in_buf.take()));

        Ok((packet, addr))
    }

    fn flush(&mut self) {
        // TODO: Make this smarter!

        for &token in self.connection_lookup.values() {
            if !self.shared.is_writable() {
                return;
            }

            let conn = &mut self.connections[token];
            conn.flush(&mut self.shared);
        }
    }

    fn remove_connection(&mut self, token: usize) {
        let connection = self.connections.remove(token);
        self.connection_lookup.remove(&connection.key);
        trace!("removing connection state; token={:?}, addr={:?}; id={:?}",
               token, connection.key.addr, connection.key.receive_id);
    }
}

impl Shared {
    fn update_ready(&mut self, ready: Ready) {
        self.ready = self.ready | ready;
    }

    fn is_writable(&self) -> bool {
        self.ready.is_writable()
    }

    fn need_writable(&mut self) {
        self.ready.remove(Ready::writable());
    }
}

impl Connection {
    fn update_local_window(&mut self) {
        self.out_queue.set_local_window(self.in_queue.local_window());
    }

    /// Process an inbound packet for the connection
    fn process(&mut self, packet: Packet, shared: &mut Shared) -> io::Result<bool> {
        let now = Instant::now();

        // TODO: Invalid packets should be discarded here.

        self.update_delays(now, &packet);

        if packet.ty() == packet::Type::State {
            // State packets are special, they do not have an associated
            // sequence number, thus do not require ordering. They are only used
            // to ACK packets, which is handled above, and to transition a
            // connection into the connected state.
            if self.state == State::SynSent {
                self.in_queue.set_initial_ack_nr(packet.seq_nr());
                self.out_queue.set_local_ack(packet.seq_nr());
                self.out_queue.set_peer_window(packet.wnd_size());

                self.state = State::Connected;
            }
        } else {
            // TODO: validate the packet's ack_nr

            // Add the packet to the inbound queue. This handles ordering
            trace!("inqueue -- push packet");
            if !self.in_queue.push(packet) {
                // Invalid packet, avoid any further processing
                trace!("invalid packet");
                return Ok(false);
            }
        }

        // TODO: count duplicate ACK counter

        trace!("polling from in_queue");

        while let Some(packet) = self.in_queue.poll() {
            trace!("process; packet={:?}; state={:?}", packet, self.state);

            // Update the peer window size
            self.out_queue.set_peer_window(packet.wnd_size());

            // At this point, we only receive CTL frames. Data is held in the
            // queue
            match packet.ty() {
                packet::Type::Reset => {
                    panic!("unimplemented; recv `RESET`");
                }
                packet::Type::Fin => {
                    self.send_fin(true, shared);
                }
                packet::Type::Data |
                    packet::Type::Syn |
                    packet::Type::State => unreachable!(),
            }
        }

        trace!("updating local window, acks; window={:?}; ack={:?}",
               self.in_queue.local_window(),
               self.in_queue.ack_nr());

        self.update_local_window();
        self.out_queue.set_local_ack(self.in_queue.ack_nr());

        // Reset the timeout
        self.reset_timeout();

        // Flush out queue
        self.flush(shared);

        // Update readiness
        try!(self.update_readiness());

        Ok(self.is_finalized())
    }

    fn flush(&mut self, shared: &mut Shared) {
        let mut sent = false;

        while let Some(next) = self.out_queue.next() {
            if !shared.is_writable() {
                return;
            }

            trace!("send_to; addr={:?}; packet={:?}", self.key.addr, next.packet());

            match shared.socket.send_to(next.packet().as_slice(), &self.key.addr) {
                Ok(n) => {
                    assert_eq!(n, next.packet().as_slice().len());
                    next.sent();

                    // Reset the connection timeout
                    sent = true;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    shared.need_writable();
                    return;
                }
                Err(e) => {
                    panic!("TODO: implement error handling {:?}", e);
                }
            }
        }

        if sent {
            self.reset_timeout();
        }
    }

    fn tick(&mut self, shared: &mut Shared) -> io::Result<()> {
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                trace!("connection timed out; id={}", self.out_queue.connection_id());
                self.out_queue.timed_out();
                self.flush(shared);
            }
        }

        Ok(())
    }

    fn update_delays(&mut self, now: Instant, packet: &Packet) {
        let mut actual_delay = u32::MAX;

        if packet.timestamp() > 0 {
            // Use the packet to update the delay value
            let their_delay = self.out_queue.update_their_delay(packet.timestamp());
            let prev_base_delay = self.their_delays.base_delay();

            // Track the delay
            self.their_delays.add_sample(their_delay, now);

            if let Some(prev) = prev_base_delay {
                let new = self.their_delays.base_delay().unwrap();

                // If their new base delay is less than their previous one, we
                // should shift our delay base in the other direction in order
                // to take the clock skew into account.
                let lt = util::wrapping_lt(new, prev, TIMESTAMP_MASK);
                let diff = prev.wrapping_sub(new);

                if lt && diff <= 10_000 {
                    self.our_delays.shift(diff);
                }
            }

            actual_delay = packet.timestamp_diff();

            if actual_delay != u32::MAX {
                self.our_delays.add_sample(actual_delay, now);

                if self.average_delay_base == 0 {
                    self.average_delay_base = actual_delay;
                }

                let mut average_delay_sample = 0;
                let dist_down = self.average_delay_base.wrapping_sub(actual_delay);
                let dist_up = actual_delay.wrapping_sub(self.average_delay_base);

                if dist_down > dist_up {
                    average_delay_sample = dist_up as i64;
                } else {
                    average_delay_sample = -(dist_down as i64);
                }

                self.current_delay_sum = self.current_delay_sum.wrapping_add(average_delay_sample);
                self.current_delay_samples += 1;

                if now > self.average_sample_time {
                    let mut prev_average_delay = self.average_delay;
                    self.average_delay = (self.current_delay_sum / self.current_delay_samples) as u32;
                    self.average_sample_time = now + Duration::from_secs(5);

                    self.current_delay_sum = 0;
                    self.current_delay_samples = 0;

                    let min_sample = cmp::min(prev_average_delay, self.average_delay);
                    let max_sample = cmp::max(prev_average_delay, self.average_delay);

                    let mut adjust = 0;

                    if min_sample > 0 {
                        adjust = min_sample;
                    } else if max_sample < 0 {
                        adjust = max_sample;
                    }

                    if adjust != 0 {
                        self.average_delay_base += adjust;
                        self.average_delay -= adjust;
                        prev_average_delay -= adjust;
                    }

                    // Update the clock drive estimate
                    let drift = self.average_delay as i64 - prev_average_delay as i64;

                    self.clock_drift = ((self.clock_drift as i64 * 7 + drift) / 8) as i32;
                }
            }
        }

        // Ack all packets
        if let Some((acked_bytes, min_rtt)) = self.out_queue.set_their_ack(packet.ack_nr(), now) {
            let min_rtt = util::as_wrapping_micros(min_rtt);

            if let Some(delay) = self.our_delays.get() {
                if delay > min_rtt {
                    self.our_delays.shift(delay.wrapping_sub(min_rtt));
                }
            }

            if actual_delay != u32::MAX && acked_bytes >= 1 {
                self.apply_congestion_control(acked_bytes, actual_delay, min_rtt, now);
            }
        }
    }

    fn apply_congestion_control(&mut self,
                                bytes_acked: usize,
                                actual_delay: u32,
                                min_rtt: u32,
                                now: Instant)
    {
        trace!("applying congenstion control; bytes_acked={}; actual_delay={}; min_rtt={}",
               bytes_acked, actual_delay, min_rtt);

        let target = TARGET_DELAY;

        let mut our_delay = cmp::min(self.our_delays.get().unwrap(), min_rtt);
        let mut penalty = 0;
        let max_window = self.out_queue.max_window() as usize;

        if self.clock_drift < -200_000 {
            penalty = (-self.clock_drift - 200_000) / 7;

            if penalty > 0 {
                our_delay += penalty as u32;
            } else {
                our_delay -= (-penalty) as u32;
            }
        }

        let off_target = (target - our_delay) as f64;
        let window_factor =
            cmp::min(bytes_acked, max_window) as f64 /
            cmp::max(max_window, bytes_acked) as f64;

        let delay_factor = off_target / target as f64;
        let mut scaled_gain = MAX_CWND_INCREASE_BYTES_PER_RTT as f64 *
            window_factor * delay_factor;

        if scaled_gain > 0.0 && now - self.last_maxed_out_window > Duration::from_secs(1) {
            // if it was more than 1 second since we tried to send a packet and
            // stopped because we hit the max window, we're most likely rate
            // limited (which prevents us from ever hitting the window size) if
            // this is the case, we cannot let the max_window grow indefinitely
            scaled_gain = 0.0;
        }

        let ledbat_cwnd = if max_window + (scaled_gain as usize) < MIN_WINDOW_SIZE {
            MIN_WINDOW_SIZE
        } else {
            max_window + scaled_gain as usize
        };

        if self.slow_start {
            let ss_cwnd = max_window + window_factor as usize * MAX_DATA_SIZE;

            if ss_cwnd > SLOW_START_THRESHOLD {
                self.slow_start = false;
            } else if our_delay > (target as f64 * 0.9) as u32 {
                // Even if we're a little under the target delay, we
                // conservatively discontinue the slow start phase
                self.slow_start = false;
            } else {
                self.out_queue.set_max_window(cmp::max(ss_cwnd, ledbat_cwnd) as u32);
            }
        } else {
            self.out_queue.set_max_window(ledbat_cwnd as u32);
        }
    }

    fn reset_timeout(&mut self) {
        self.deadline = self.out_queue.socket_timeout()
            .map(|dur| {
                trace!("resetting timeout; duration={:?}", dur);
                Instant::now() + dur
            });
    }

    fn send_fin(&mut self, _: bool, shared: &mut Shared) {
        if self.state.is_closed() {
            return;
        }

        self.out_queue.push(Packet::fin());
        self.state = State::FinSent;
    }

    fn is_finalized(&self) -> bool {
        self.out_queue.is_empty() && self.state.is_closed()
    }

    /// Update the UtpStream's readiness
    fn update_readiness(&self) -> io::Result<()> {
        let mut ready = Ready::empty();

        if self.state == State::Connected {
            if self.is_readable() {
                ready.insert(Ready::readable());
            }

            if self.is_writable() {
                ready.insert(Ready::writable());
            }
        } else if self.state.is_closed() {
            ready = Ready::readable();
        }

        self.set_readiness.set_readiness(ready)
    }

    // =========

    fn is_readable(&self) -> bool {
        self.in_queue.is_readable()
    }

    fn is_writable(&self) -> bool {
        self.out_queue.is_writable()
    }
}

impl State {
    fn is_closed(&self) -> bool {
        match *self {
            State::FinSent => true,
            _ => false,
        }
    }
}

impl Key {
    fn new(receive_id: u16, addr: SocketAddr) -> Key {
        Key {
            receive_id: receive_id,
            addr: addr,
        }
    }
}

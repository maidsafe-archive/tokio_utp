//! Queue of outgoing packets.

use {util, MAX_WINDOW_SIZE};
use packet::{self, Packet, HEADER_LEN};

use std::{cmp, io};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

// TODO:
//
// * Nagle check, don't flush the last data packet if there is in-flight data
//   and it is too small.

#[derive(Debug)]
pub struct OutQueue {
    // queued packets
    packets: VecDeque<Entry>,

    state: State,

    // Round trip time in microseconds
    rtt: u64,
    rtt_variance: i64,

    // Max number of bytes that we can have in-flight to the peer w/o acking.
    // This number dynamically changes to handle control flow.
    max_window: u32,

    // Peer's window. This is the number of bytes that it has locally but not
    // acked
    peer_window: u32,
}

#[derive(Debug)]
struct State {
    connection_id: u16,

    // Sequence number for the next packet
    seq_nr: u16,

    // Sequence number of the last locally acked packet (aka, read)
    local_ack: Option<u16>,

    // Last outbound ack
    last_ack: Option<u16>,

    // This is the number of bytes available in our inbound receive queue.
    local_window: u32,

    // The instant at which the `OutQueue` was created, this is used as the
    // reference point when calculating the current timestamp in microseconds.
    created_at: Instant,

    // Difference between the `timestamp` specified by the last incoming packet
    // and the current time.
    their_delay: u32,
}

#[derive(Debug)]
struct Entry {
    packet: Packet,
    num_sends: u32,
    last_sent_at: Option<Instant>,
    acked: bool,
}

pub struct Next<'a> {
    item: Item<'a>,
    state: &'a mut State,
}

enum Item<'a> {
    Entry(&'a mut Entry),
    State(Packet),
}

// Max size of a UDP packet... ideally this will be dynamically discovered using
// MTU.
const MAX_PACKET_SIZE: usize = 1_400;
const MIN_PACKET_SIZE: usize = 150;

const MAX_DATA_SIZE: usize = MAX_PACKET_SIZE - HEADER_LEN;
const MIN_DATA_SIZE: usize = MIN_PACKET_SIZE - HEADER_LEN;

const MICROS_PER_SEC: u32 = 1_000_000;
const NANOS_PER_MS: u32 = 1_000_000;
const NANOS_PER_MICRO: u32 = 1_000;

impl OutQueue {
    /// Create a new `OutQueue` with the specified `seq_nr` and `ack_nr`
    pub fn new(connection_id: u16,
               seq_nr: u16,
               local_ack: Option<u16>) -> OutQueue
    {
        OutQueue {
            packets: VecDeque::new(),
            state: State {
                connection_id: connection_id,
                seq_nr: seq_nr,
                local_ack: local_ack,
                last_ack: None,
                local_window: 0,
                created_at: Instant::now(),
                their_delay: 0,
            },
            rtt: 0,
            rtt_variance: 0,
            // Start the max window at the packet size
            max_window: MAX_PACKET_SIZE as u32,
            peer_window: MAX_WINDOW_SIZE as u32,
        }
    }

    pub fn connection_id(&self) -> u16 {
        self.state.connection_id
    }

    /// Returns true if the out queue is fully flushed and all packets have been
    /// ACKed.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Whenever a packet is received, the included timestamp is passed in here.
    pub fn update_their_delay(&mut self, their_timestamp: u32) -> u32 {
        let our_timestamp = util::as_wrapping_micros(self.state.created_at.elapsed());
        self.state.their_delay = our_timestamp.wrapping_sub(their_timestamp);
        self.state.their_delay
    }

    pub fn set_peer_window(&mut self, val: u32) {
        self.peer_window = val;
    }

    pub fn set_their_ack(&mut self, ack_nr: u16, now: Instant) -> Option<(usize, Duration)> {
        let mut acked_bytes = 0;
        let mut min_rtt = None;

        loop {
            let pop = self.packets.front()
                .map(|entry| {
                    let seq_nr = entry.packet.seq_nr();

                    let lower = ack_nr.wrapping_sub(ack_nr);

                    if lower < ack_nr {
                        seq_nr > lower && seq_nr <= ack_nr
                    } else {
                        seq_nr > lower || seq_nr <= ack_nr
                    }
                })
                .unwrap_or(false);

            if !pop {
                return min_rtt.map(|rtt| (acked_bytes, rtt));
            }

            // The packet has been acked..
            let p = self.packets.pop_front().unwrap();

            // If the packet has a payload, track the number of bytes sent
            acked_bytes += p.packet.payload().len();

            // Calculate the RTT for the packet.
            let packet_rtt = now.duration_since(p.last_sent_at.unwrap());

            min_rtt = Some(min_rtt
                .map(|curr| cmp::min(curr, packet_rtt))
                .unwrap_or(packet_rtt));

            if p.num_sends == 1 {
                // Use the packet to update rtt & rtt_variance
                let packet_rtt = util::as_ms(now.duration_since(p.last_sent_at.unwrap()));
                let delta = (self.rtt as i64 - packet_rtt as i64).abs();

                self.rtt_variance += (delta - self.rtt_variance) / 4;

                if self.rtt >= packet_rtt {
                    self.rtt -= (self.rtt - packet_rtt) / 8;
                } else {
                    self.rtt += (packet_rtt - self.rtt) / 8;
                }
            }
        }
    }

    pub fn set_local_window(&mut self, val: usize) {
        assert!(val <= ::std::u32::MAX as usize);
        self.state.local_window = val as u32;
    }

    /// Update peer ack
    pub fn set_local_ack(&mut self, val: u16) {
        // TODO: Since STATE packets can be lost, if any packet is received from
        // the remote, *some* sort of state packet needs to be sent out in the
        // near term future.

        if self.state.local_ack.is_none() {
            // Also update the last_ack as this is the connection's first state
            // packet which does not need to be acked.
            self.state.last_ack = Some(val);
        }

        self.state.local_ack = Some(val);
    }

    /// Returns the socket timeout based on an aggregate of packet round trip
    /// times.
    pub fn socket_timeout(&self) -> Option<Duration> {
        if self.is_empty() {
            return None;
        }

        // Until a packet is received from the peer, the timeout is 1 second.
        if self.state.local_ack.is_none() {
            return Some(Duration::from_secs(1));
        }

        let timeout = self.rtt as i64 + self.rtt_variance;

        if timeout > 500 {
            Some(Duration::from_millis(timeout as u64))
        } else {
            Some(Duration::from_millis(500))
        }
    }

    /// Push an outbound packet into the queue
    pub fn push(&mut self, mut packet: Packet) {
        assert!(packet.ty() != packet::Type::State);

        // SYN packets are special and will have the connection ID already
        // correctly set
        if packet.ty() != packet::Type::Syn {
            packet.set_connection_id(self.state.connection_id);
        }

        // Increment the seq_nr
        self.state.seq_nr = self.state.seq_nr.wrapping_add(1);

        // Set the sequence number
        packet.set_seq_nr(self.state.seq_nr);

        self.packets.push_back(Entry {
            packet: packet,
            num_sends: 0,
            last_sent_at: None,
            acked: false,
        });
    }

    pub fn next(&mut self) -> Option<Next> {
        let ts = self.timestamp();
        let diff = self.state.their_delay;
        let ack = self.state.local_ack.unwrap_or(0);
        let wnd_size = self.state.local_window;

        // Number of bytes in-flight
        let in_flight = self.in_flight();

        for entry in &mut self.packets {
            // The packet has been sent
            if entry.last_sent_at.is_some() {
                continue;
            }

            if in_flight > 0 {
                let max = cmp::min(self.max_window, self.peer_window) as usize;

                // Don't send more data than the window allows
                if in_flight + entry.packet.len() > max {
                    return None;
                }
            } else {
                // Don't send more data than the window allows
                if in_flight + entry.packet.len() > self.peer_window as usize {
                    return None;
                }
            }

            // Update timestamp
            entry.packet.set_timestamp(ts);
            entry.packet.set_timestamp_diff(diff);
            entry.packet.set_ack_nr(ack);
            entry.packet.set_wnd_size(wnd_size);

            return Some(Next {
                item: Item::Entry(entry),
                state: &mut self.state,
            });
        }

        if self.state.local_ack != self.state.last_ack {
            trace!("ack_required; local={:?}; last={:?}; seq_nr={:?}",
                   self.state.local_ack,
                   self.state.last_ack,
                   self.state.seq_nr);

            let mut packet = Packet::state();

            packet.set_connection_id(self.state.connection_id);
            packet.set_seq_nr(self.state.seq_nr);
            packet.set_timestamp(ts);
            packet.set_timestamp_diff(diff);
            packet.set_ack_nr(ack);
            packet.set_wnd_size(wnd_size);

            return Some(Next {
                item: Item::State(packet),
                state: &mut self.state,
            });
        }

        None
    }

    pub fn max_window(&self) -> u32 {
        self.max_window
    }

    pub fn set_max_window(&mut self, val: u32) {
        self.max_window = val;
    }

    /// The peer timed out, consider all the packets lost
    pub fn timed_out(&mut self) {
        for entry in &mut self.packets {
            entry.last_sent_at = None;
        }

        self.max_window = MIN_PACKET_SIZE as u32;
    }

    /// Push data into the outbound queue
    pub fn write(&mut self, mut src: &[u8]) -> io::Result<usize> {
        if src.len() == 0 {
            return Ok(0);
        }

        let cur_window = self.in_flight();
        let max = cmp::min(self.max_window, self.peer_window) as usize;

        if cur_window >= max {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        let mut rem = max - cur_window;
        let mut len = 0;

        while rem > HEADER_LEN {
            let packet_len = cmp::min(
                MAX_PACKET_SIZE,
                cmp::min(src.len(), rem - HEADER_LEN));

            if packet_len == 0 {
                break;
            }

            let packet = Packet::data(&src[..packet_len]);
            self.push(packet);

            len += packet_len;
            rem -= packet_len + HEADER_LEN;

            src = &src[packet_len..];
        }

        Ok(len)
    }

    pub fn is_writable(&self) -> bool {
        self.buffered() < MAX_WINDOW_SIZE as usize
    }

    pub fn in_flight(&self) -> usize {
        // TODO: Don't iterate each time
        self.packets.iter()
            .filter(|p| p.last_sent_at.is_some() && !p.acked)
            .count()
    }

    pub fn buffered(&self) -> usize {
        // TODO: Don't iterate each time
        self.packets.iter()
            .map(|p| p.packet.payload().len())
            .sum()
    }

    fn timestamp(&self) -> u32 {
        util::as_wrapping_micros(self.state.created_at.elapsed())
    }
}

impl<'a> Next<'a> {
    pub fn packet(&self) -> &Packet {
        match self.item {
            Item::Entry(ref e) => &e.packet,
            Item::State(ref p) => p,
        }
    }

    pub fn sent(mut self) {
        if let Item::Entry(ref mut e) = self.item {
            // Increment the number of sends
            e.num_sends += 1;

            // Track the time
            e.last_sent_at = Some(Instant::now());
        }

        self.state.last_ack = self.state.local_ack;
    }
}

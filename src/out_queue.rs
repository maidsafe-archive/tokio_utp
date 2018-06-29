//! Queue of outgoing packets.

use packet::{self, Packet};
use {util, MAX_WINDOW_SIZE};

use std::collections::VecDeque;
use std::time::{Duration, Instant};
use std::{cmp, io};

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

    resend_last_ack: bool,
}

#[derive(Debug)]
struct State {
    connection_id: u16,

    // Sequence number for the next packet
    seq_nr: u16,

    // Sequence number of the last locally seen packet which is yet to be acked.
    last_in_ack: Option<u16>,

    // Bitfields indicating whether the next 32 packets have been received
    selective_acks: [u8; 4],

    // Last outbound ack
    last_out_ack: Option<u16>,

    // This is the number of bytes available in our inbound receive queue.
    local_window: u32,

    // The instant at which the `OutQueue` was created, this is used as the
    // reference point when calculating the current timestamp in microseconds.
    created_at: Instant,

    // Difference between the `timestamp` specified by the last incoming packet
    // and the current time.
    their_delay: u32,
}

impl State {
    fn new(connection_id: u16, seq_nr: u16, last_in_ack: Option<u16>) -> Self {
        State {
            connection_id,
            seq_nr,
            last_in_ack,
            selective_acks: [0; 4],
            last_out_ack: None,
            local_window: MAX_WINDOW_SIZE as u32,
            created_at: Instant::now(),
            their_delay: 0,
        }
    }
}

/// Outgoing packets queue entry.
#[derive(Debug)]
struct Entry {
    packet: Packet,
    num_sends: u32,
    last_sent_at: Option<Instant>,
    acked: bool,
}

impl Entry {
    fn new(packet: Packet) -> Self {
        Entry {
            packet,
            num_sends: 0,
            last_sent_at: None,
            acked: false,
        }
    }

    /// Checks this packet entry is acknowledged by given ack information.
    fn acked_by(&self, ack_nr: u16, selective_acks: &[u8]) -> bool {
        let seq_nr = self.packet.seq_nr();
        ack_nr.wrapping_sub(seq_nr) <= seq_nr.wrapping_sub(ack_nr) || {
            match (seq_nr.wrapping_sub(ack_nr) as usize).checked_sub(2) {
                Some(index) => {
                    selective_acks.get(index / 8).cloned().unwrap_or(0) & (1 << (index % 8)) != 0
                }
                None => false,
            }
        }
    }
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
const MAX_HEADER_SIZE: usize = 26;

impl OutQueue {
    /// Create a new `OutQueue` with the specified `seq_nr` and `ack_nr`
    pub fn new(connection_id: u16, seq_nr: u16, last_in_ack: Option<u16>) -> OutQueue {
        OutQueue {
            packets: VecDeque::new(),
            state: State::new(connection_id, seq_nr, last_in_ack),
            rtt: 0,
            rtt_variance: 0,
            // Start the max window at the packet size
            max_window: MAX_PACKET_SIZE as u32,
            peer_window: MAX_WINDOW_SIZE as u32,
            resend_last_ack: false,
        }
    }

    pub fn connection_id(&self) -> u16 {
        self.state.connection_id
    }

    /// Returns true if the out queue is fully flushed and all packets have been
    /// ACKed.
    pub fn is_empty(&self) -> bool {
        // Only empty if all acks have been sent
        self.packets.is_empty() && self.state.last_in_ack == self.state.last_out_ack
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

    /// Acknowledges packets in out queue - acked packets are removed from the queue.
    /// Returns acked bytes and minimum round trip time - time between when packet was sent and
    /// when ack was received for it.
    pub fn set_their_ack(
        &mut self,
        ack_nr: u16,
        selective_acks: &[u8],
        now: Instant,
    ) -> Option<(usize, Duration)> {
        let mut acked_bytes = 0;
        let mut min_rtt = None;

        let mut packet_index = 0;
        loop {
            let acked = {
                let entry = match self.packets.get(packet_index) {
                    Some(entry) => entry,
                    None => break,
                };
                entry.acked_by(ack_nr, selective_acks)
            };
            if !acked {
                packet_index += 1;
                continue;
            }

            // The packet has been acked..
            let p = self.packets.remove(packet_index).unwrap();

            // If the packet has a payload, track the number of bytes sent
            acked_bytes += p.packet.payload().len();

            let last_sent_at = match p.last_sent_at {
                Some(last_sent_at) => last_sent_at,
                None => {
                    // We timed out, but the ack arrived after the timeout... the
                    // packet is ACKed but don't use it for congestion control
                    continue;
                }
            };

            // Calculate the RTT for the packet.
            let packet_rtt = now.duration_since(last_sent_at);

            min_rtt = Some(
                min_rtt
                    .map(|curr| cmp::min(curr, packet_rtt))
                    .unwrap_or(packet_rtt),
            );

            // The `rtt` and `rtt_variance` are only updated for packets that were sent only once.
            // This avoids problems with figuring out which packet was acked, the first or the
            // second one.
            if p.num_sends == 1 {
                self.recalc_rtt(packet_rtt);
            }
        }
        min_rtt.map(|rtt| (acked_bytes, rtt))
    }

    /// Checks if incoming packet, was already acked before. In such case, force to resend `State`
    /// packet. That might happen when `State` packets are lost.
    pub fn maybe_resend_ack_for(&mut self, incoming_packet: &Packet) {
        let resend = self
            .state
            .last_out_ack
            .map(|last_ack| last_ack >= incoming_packet.seq_nr())
            .unwrap_or(false);
        if resend {
            self.resend_last_ack = true;
        }
    }

    fn recalc_rtt(&mut self, packet_rtt: Duration) {
        // Use the packet to update rtt & rtt_variance
        let packet_rtt = util::as_ms(packet_rtt);
        let delta = (self.rtt as i64 - packet_rtt as i64).abs();

        self.rtt_variance += (delta - self.rtt_variance) / 4;

        if self.rtt >= packet_rtt {
            self.rtt -= (self.rtt - packet_rtt) / 8;
        } else {
            self.rtt += (packet_rtt - self.rtt) / 8;
        }
    }

    /// Sets local window size in bytes that will be included in every outgoing packet.
    pub fn set_local_window(&mut self, val: usize) {
        assert!(val <= ::std::u32::MAX as usize);
        self.state.local_window = val as u32;
    }

    /// Update peer ack
    pub fn set_local_ack(&mut self, val: u16, selective_acks: [u8; 4]) {
        // TODO: Since STATE packets can be lost, if any packet is received from
        // the remote, *some* sort of state packet needs to be sent out in the
        // near term future.

        if self.state.last_in_ack.is_none() {
            // Also update the last_out_ack as this is the connection's first state
            // packet which does not need to be acked.
            self.state.last_out_ack = Some(val);
        }

        self.state.last_in_ack = Some(val);
        self.state.selective_acks = selective_acks;
    }

    /// Returns the socket timeout based on an aggregate of packet round trip
    /// times.
    pub fn socket_timeout(&self) -> Option<Duration> {
        if self.is_empty() {
            return None;
        }

        // Until a packet is received from the peer, the timeout is 1 second.
        if self.state.last_in_ack.is_none() {
            return Some(Duration::from_secs(1));
        }

        let timeout = self.rtt as i64 + 4 * self.rtt_variance;

        if timeout > 500 {
            Some(Duration::from_millis(timeout as u64))
        } else {
            Some(Duration::from_millis(500))
        }
    }

    /// Push an outbound packet into the queue.
    /// Assigns sequence number for given packet and returns it.
    pub fn push(&mut self, mut packet: Packet) -> u16 {
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
        self.packets.push_back(Entry::new(packet));

        self.state.seq_nr
    }

    /// Returns next packet to be sent out.
    /// If there are already more packets in flight that the peer can receive, returns `None`.
    pub fn next(&mut self) -> Option<Next> {
        let ts = self.timestamp();
        let diff = self.state.their_delay;
        let ack = self.state.last_in_ack.unwrap_or(0);
        let selective_acks = self.state.selective_acks;
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
            } else if entry.packet.len() > self.peer_window as usize {
                // Don't send more data than the window allows
                return None;
            }

            // Update timestamp
            entry.packet.set_timestamp(ts);
            entry.packet.set_timestamp_diff(diff);
            entry.packet.set_ack_nr(ack);
            entry.packet.set_wnd_size(wnd_size);
            entry.packet.set_selective_acks(selective_acks);

            return Some(Next::entry(entry, &mut self.state));
        }

        if self.state.last_in_ack != self.state.last_out_ack || self.resend_last_ack {
            trace!(
                "ack_required; local={:?}; last={:?}; seq_nr={:?}",
                self.state.last_in_ack,
                self.state.last_out_ack,
                self.state.seq_nr
            );
            self.resend_last_ack = false;

            let mut packet = Packet::state();
            packet.set_connection_id(self.state.connection_id);
            packet.set_seq_nr(self.state.seq_nr);
            packet.set_timestamp(ts);
            packet.set_timestamp_diff(diff);
            packet.set_ack_nr(ack);
            packet.set_wnd_size(wnd_size);
            packet.set_selective_acks(selective_acks);

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
        trace!("set_max_window; old={:?}; new={:?}", self.max_window, val);
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
        if src.is_empty() {
            return Ok(0);
        }

        let mut rem = self.remaining_capacity();
        let mut len = 0;

        if rem == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        trace!("write; remaining={:?}; src={:?}", rem, src.len());

        while rem > MAX_HEADER_SIZE {
            let packet_len = cmp::min(MAX_PACKET_SIZE, cmp::min(src.len(), rem - MAX_HEADER_SIZE));

            if packet_len == 0 {
                break;
            }

            let packet = Packet::data(&src[..packet_len]);
            self.push(packet);

            len += packet_len;
            rem -= packet_len + MAX_HEADER_SIZE;

            src = &src[packet_len..];
        }

        if len == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        Ok(len)
    }

    fn remaining_capacity(&self) -> usize {
        let cur_window = self.buffered();
        let max = cmp::min(self.max_window, self.peer_window) as usize;

        if cur_window >= max {
            return 0;
        }

        max - cur_window
    }

    pub fn is_writable(&self) -> bool {
        self.remaining_capacity() > MAX_HEADER_SIZE
    }

    /// Total bytes of packets that were sent, but not acked yet.
    fn in_flight(&self) -> usize {
        // TODO: Don't iterate each time
        self.packets
            .iter()
            .filter(|p| p.last_sent_at.is_some() && !p.acked)
            .map(|p| p.packet.len())
            .sum()
    }

    fn buffered(&self) -> usize {
        // TODO: Don't iterate each time
        self.packets.iter().map(|p| p.packet.len()).sum()
    }

    fn timestamp(&self) -> u32 {
        util::as_wrapping_micros(self.state.created_at.elapsed())
    }
}

impl<'a> Next<'a> {
    fn entry(entry: &'a mut Entry, state: &'a mut State) -> Self {
        Next {
            item: Item::Entry(entry),
            state,
        }
    }

    pub fn packet(&self) -> &Packet {
        match self.item {
            Item::Entry(ref e) => &e.packet,
            Item::State(ref p) => p,
        }
    }

    /// Updates last acked packet number.
    pub fn sent(mut self) {
        if let Item::Entry(ref mut e) = self.item {
            // Increment the number of sends
            e.num_sends += 1;

            // Track the time
            e.last_sent_at = Some(Instant::now());
        }

        self.state.last_out_ack = self.state.last_in_ack;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod entry {
        use super::*;

        mod acked_by {
            use super::*;

            #[test]
            fn when_ack_nr_matches_packet_seq_nr_it_returns_true() {
                let mut packet = Packet::default();
                packet.set_seq_nr(15);
                let entry = Entry::new(packet);

                let acks = entry.acked_by(15, &[]);

                assert!(acks);
            }

            #[test]
            fn when_ack_nr_is_greater_than_seq_nr_it_returns_true() {
                let mut packet = Packet::default();
                packet.set_seq_nr(15);
                let entry = Entry::new(packet);

                let acks = entry.acked_by(20, &[]);

                assert!(acks);
            }

            mod when_ack_nr_is_smaller_than_seq_nr {
                use super::*;

                #[test]
                fn when_selective_acks_are_empty_it_returns_false() {
                    let mut packet = Packet::default();
                    packet.set_seq_nr(15);
                    let entry = Entry::new(packet);

                    let acks = entry.acked_by(14, &[]);

                    assert!(!acks);
                }

                #[test]
                fn when_seq_and_ack_nr_difference_is_encoded_in_selective_acks_it_returns_true() {
                    let mut packet = Packet::default();
                    packet.set_seq_nr(100);
                    let entry = Entry::new(packet);

                    let acks = entry.acked_by(90, &[0, 1, 0, 0]);

                    assert!(acks);
                }
            }
        }
    }

    mod next {
        use super::*;

        mod sent {
            use super::*;

            #[test]
            fn it_sets_packet_sent_time() {
                let mut entry = Entry::new(Packet::fin());
                let mut state = State::new(123, 1, Some(5000));

                {
                    let next = Next::entry(&mut entry, &mut state);

                    next.sent();
                }

                assert!(entry.last_sent_at.is_some());
            }
        }
    }

    mod out_queue {
        use super::*;

        mod push {
            use super::*;

            #[test]
            fn it_returns_sequence_number_assigned_to_packet() {
                let mut out_packets = OutQueue::new(123, 1, None);

                let seq_nr = out_packets.push(Packet::fin());

                assert_eq!(seq_nr, 2);
            }
        }

        mod set_their_ack {
            use super::*;

            #[test]
            fn when_queue_is_empty_it_returns_none() {
                let mut out_packets = OutQueue::new(123, 1, None);

                let acked = out_packets.set_their_ack(1, &[], Instant::now());

                assert!(acked.is_none());
            }
        }

        mod in_flight {
            use super::*;

            #[test]
            fn when_queue_is_empty_it_returns_0() {
                let out_packets = OutQueue::new(123, 1, None);

                assert_eq!(out_packets.in_flight(), 0);
            }

            #[test]
            fn it_returns_sum_of_sent_packet_sizes_in_bytes() {
                let mut out_packets = OutQueue::new(123, 1, None);
                let _ = out_packets.push(Packet::fin());
                let _ = out_packets.push(Packet::data(b"12345"));
                while let Some(entry) = out_packets.next() {
                    entry.sent();
                }

                assert_eq!(out_packets.in_flight(), 45);
            }
        }

        mod next {
            use super::*;

            #[test]
            fn it_returns_first_packet_that_was_not_sent_yet() {
                let mut out_packets = OutQueue::new(123, 1, None);
                let _ = out_packets.push(Packet::data(b"12345"));
                let _ = out_packets.push(Packet::fin());

                let next_entry = unwrap!(out_packets.next());

                assert_eq!(next_entry.packet().ty(), packet::Type::Data);
            }

            #[test]
            fn when_no_packets_in_flight_if_next_packet_len_exceeds_peer_window_it_returns_none() {
                let mut out_packets = OutQueue::new(123, 1, None);
                let _ = out_packets.push(Packet::data(b"12345"));
                out_packets.set_peer_window(5);

                let next_entry = out_packets.next();

                assert!(next_entry.is_none());
            }

            #[test]
            fn next_packet_len_plus_packets_in_flight_exceeds_peer_window_it_returns_none() {
                let mut out_packets = OutQueue::new(123, 1, None);
                // simulate some packets in-flight
                let _ = out_packets.push(Packet::data(b"12345"));
                while let Some(entry) = out_packets.next() {
                    entry.sent();
                }

                let _ = out_packets.push(Packet::data(b"12345"));
                out_packets.set_peer_window(5);

                let next_entry = out_packets.next();

                assert!(next_entry.is_none());
            }
        }
    }
}

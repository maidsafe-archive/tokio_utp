//! Queue of outgoing packets.

use MAX_WINDOW_SIZE;
use packet::{self, Packet};

use std::io;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct OutQueue {
    // queued packets
    packets: VecDeque<Entry>,

    state: State,
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

    // Peer's window. This is the number of bytes that it has locally but not
    // acked
    peer_window: u32,

    // Number of bytes that we have received but have not acked.
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

// This should be tuneable
const MAX_PACKET_SIZE: usize = 1_024;

const MICROS_PER_SEC: u32 = 1_000_000;
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
                peer_window: MAX_WINDOW_SIZE,
                local_window: 0,
                created_at: Instant::now(),
                their_delay: 0,
            }
        }
    }

    /// Returns true if the out queue is fully flushed and all packets have been
    /// ACKed.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Whenever a packet is received, the included timestamp is passed in here.
    pub fn set_their_delay(&mut self, their_timestamp: u32) {
        let our_timestamp = as_micros(self.state.created_at.elapsed());
        self.state.their_delay = our_timestamp.wrapping_sub(their_timestamp);
    }

    pub fn set_their_ack(&mut self, ack_nr: u16) {
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
                return;
            }

            self.packets.pop_front();
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
        self.state.local_ack = Some(val);
    }

    /// Push an outbound packet into the queue
    pub fn push(&mut self, mut packet: Packet) {
        assert!(packet.ty() != packet::Type::State);

        // SYN packets are special and will have the connection ID already
        // correctly set
        if packet.ty() != packet::Type::Syn {
            packet.set_connection_id(self.state.connection_id);
        }

        // Set the sequence number
        packet.set_seq_nr(self.state.seq_nr);

        self.state.seq_nr.wrapping_add(1);

        self.packets.push_back(Entry {
            packet: packet,
            last_sent_at: None,
            acked: false,
        });
    }

    pub fn next(&mut self) -> Option<Next> {
        let ts = self.timestamp();
        let diff = self.state.their_delay;
        let ack = self.state.local_ack.unwrap_or(0);
        let wnd_size = self.state.local_window;

        for entry in &mut self.packets {
            // The packet has been sent
            if entry.last_sent_at.is_some() {
                continue;
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

    pub fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        if src.len() == 0 {
            return Ok(0);
        }

        // TODO: Only write until the window is full
        for chunk in src.chunks(MAX_PACKET_SIZE) {
            let packet = Packet::data(chunk);
            self.push(packet);
        }

        Ok(src.len())
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
        as_micros(self.state.created_at.elapsed())
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
            e.last_sent_at = Some(Instant::now());
        }

        self.state.last_ack = self.state.local_ack;
    }
}

fn as_micros(duration: Duration) -> u32 {
    // Wrapping is OK
    let mut ret = duration.as_secs().wrapping_mul(MICROS_PER_SEC as u64) as u32;
    ret += duration.subsec_nanos() / NANOS_PER_MICRO;
    ret
}

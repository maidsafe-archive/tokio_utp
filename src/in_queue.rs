use {MAX_WINDOW_SIZE, MAX_DELTA_SEQ};
use packet::{self, Packet};

use bytes::{BytesMut, Buf};

use std::{mem, u16};
use std::io::{self, Read, Cursor};
use std::collections::VecDeque;

#[derive(Debug)]
pub struct InQueue {
    // Used to order inbound packets
    packets: [Option<Packet>; MAX_DELTA_SEQ],
    // packets: VecDeque<Option<packet::Inbound>>,

    // Sequenced data packets for reading
    data: VecDeque<Cursor<BytesMut>>,

    // Ignore all packets lower than this seq_nr
    ack_nr: Option<u16>,
}

impl InQueue {
    /// Returns a new `InQueue` with an uninitialized ack_nr value.
    ///
    /// The peer will send an `ST_STATE` packet which contains the first
    /// sequence number for the connection. `InQueue` will order received
    /// packets and not yield one until the ST_STATE packet has been received.
    pub fn new(ack_nr: Option<u16>) -> InQueue {
        InQueue {
            packets: Default::default(),
            data: VecDeque::new(),
            ack_nr: ack_nr,
        }
    }

    /// Returns the seq number of the last remote packet to ack
    pub fn ack_nr(&self) -> u16 {
        self.ack_nr.unwrap_or(0)
    }

    /// Poll the next CTL packet for processing. Data packets are queued for
    /// read
    pub fn poll(&mut self) -> Option<Packet> {
        // Get the current position, if none then no packets can be read
        let pos = match self.ack_nr {
            Some(ack_nr) => ack_nr as usize,
            None => return None,
        };

        loop {
            // Take the next packet
            let p = mem::replace(&mut self.packets[pos % MAX_DELTA_SEQ], None);

            let p = match p {
                Some(p) => p,
                None => return None,
            };

            // Update ack_nr
            self.ack_nr = Some((pos % u16::MAX as usize) as u16);

            if p.ty() == packet::Type::Data {
                if !p.payload().is_empty() {
                    let buf = Cursor::new(p.into_payload());
                    self.data.push_back(buf);
                }
            } else {
                return Some(p);
            }
        }
    }

    pub fn push(&mut self, packet: Packet) {
        trace!("InQueue::push; packet={:?}", packet);

        // State packets are handled outside of this queue
        assert!(packet.ty() != packet::Type::State);

        // Just drop if our window is full
        if self.bytes_pending() >= MAX_WINDOW_SIZE as usize {
            trace!("    -> window full; dropping packet");
            return;
        }

        let seq_nr = packet.seq_nr();

        if let Some(ack_nr) = self.ack_nr {
            if !in_range(ack_nr, seq_nr) {
                trace!("    -> not in range -- dropping");
                // Drop the packet
                return;
            }
        }

        // Track the packet
        let slot = seq_nr as usize % MAX_DELTA_SEQ;

        if self.packets[slot].is_some() {
            trace!("    -> slot occupied -- dropping");
            // Slot already occupied, ignore the packet
            return;
        }

        trace!("    -> tracking packet");

        self.packets[slot] = Some(packet);
    }

    pub fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let n = match self.data.front_mut() {
            Some(buf) => {
                let n = try!(buf.read(dst));

                if buf.has_remaining() {
                    return Ok(n);
                }

                n
            }
            None => {
                return Err(io::ErrorKind::WouldBlock.into());
            }
        };

        let _ = self.data.pop_front();

        Ok(n)
    }

    pub fn is_readable(&self) -> bool {
        !self.data.is_empty()
    }

    pub fn bytes_pending(&self) -> usize {
        self.data.iter()
            .map(|p| p.get_ref().len())
            .sum()
    }

    pub fn set_initial_ack_nr(&mut self, ack_nr: u16) {
        // This is the starting point
        self.ack_nr = Some(ack_nr);

        // Now, we prune the queue
        for p in self.packets.iter_mut() {
            let unset = p.as_ref()
                .map(|p| in_range(ack_nr, p.seq_nr()))
                .unwrap_or(false);

            if unset {
                *p = None;
            }
        }
    }
}

fn in_range(ack_nr: u16, seq_nr: u16) -> bool {
    let upper = ack_nr.wrapping_add(MAX_DELTA_SEQ as u16);

    if upper > ack_nr {
        // Non wrapping case
        seq_nr >= ack_nr && seq_nr < upper
    } else {
        // Wrapping case
        seq_nr >= ack_nr || seq_nr < upper
    }
}

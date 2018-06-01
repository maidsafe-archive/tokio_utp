use packet::{self, Packet};
use {MAX_DELTA_SEQ, MAX_WINDOW_SIZE};

use bytes::{Buf, BytesMut};

use std::collections::VecDeque;
use std::io::{self, Cursor, Read};
use std::{mem, u16};

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
            ack_nr,
        }
    }

    /// Returns the seq number of the last remote packet to ack
    pub fn ack_nr(&self) -> (u16, [u8; 4]) {
        let ack_nr = self.ack_nr.unwrap_or(0);
        let mut selective_acks = [0; 4];
        for i in 0..(MAX_DELTA_SEQ - 2) {
            let slot = (ack_nr as usize + i + 2) % MAX_DELTA_SEQ;
            if let Some(..) = self.packets[slot] {
                selective_acks[i / 8] |= 1 << (i % 8);
            }
        }
        (ack_nr, selective_acks)
    }

    /// Poll the next CTL packet for processing. Data packets are queued for
    /// read
    pub fn poll(&mut self) -> Option<Packet> {
        trace!("poll; ack_nr={:?}", self.ack_nr);

        // Get the current position, if none then no packets can be read
        let mut pos = match self.ack_nr {
            Some(ack_nr) => ack_nr as usize,
            None => return None,
        };

        loop {
            pos += 1;

            // Take the next packet
            let slot = pos % MAX_DELTA_SEQ;
            let p = mem::replace(&mut self.packets[slot], None);

            let p = match p {
                Some(p) => {
                    trace!("slot has packet; slot={:?}; packet={:?}", slot, p);
                    p
                }
                None => {
                    trace!("slot empty; slot={:?}", slot);
                    return None;
                }
            };

            // Update ack_nr
            self.ack_nr = Some((pos % (u16::MAX as usize + 1)) as u16);

            if p.ty() == packet::Type::Data {
                trace!(" -> got data");
                if !p.payload().is_empty() {
                    let buf = Cursor::new(p.into_payload());
                    self.data.push_back(buf);
                }
            } else {
                return Some(p);
            }
        }
    }

    /// Returns true, if packet was successfully pushed to the queue, false otherwise.
    /// Packets might not be accepted, if pending packets queue has reached the limit, etc.
    pub fn push(&mut self, packet: Packet) -> bool {
        trace!(
            "InQueue::push; packet={:?}; ack_nr={:?}",
            packet,
            self.ack_nr
        );

        // State packets are handled outside of this queue
        assert!(packet.ty() != packet::Type::State);

        // Just drop if our window is full
        if self.bytes_pending() >= MAX_WINDOW_SIZE as usize {
            trace!("    -> window full; dropping packet");
            return false;
        }

        let seq_nr = packet.seq_nr();

        if let Some(ack_nr) = self.ack_nr {
            if !in_range(ack_nr, seq_nr) {
                trace!("    -> not in range -- dropping");
                // Drop the packet
                return false;
            }
        }

        // Track the packet
        let slot = seq_nr as usize % MAX_DELTA_SEQ;

        if self.packets[slot].is_some() {
            trace!("    -> slot occupied -- dropping");
            // Slot already occupied, ignore the packet
            return false;
        }

        trace!(
            "    -> tracking packet; seq_nr={:?}; slot={:?}",
            seq_nr,
            slot
        );

        self.packets[slot] = Some(packet);
        true
    }

    pub fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let n = match self.data.front_mut() {
            Some(buf) => {
                let n = buf.read(dst)?;

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

    pub fn local_window(&self) -> usize {
        let pending = self.bytes_pending();

        if pending >= MAX_WINDOW_SIZE {
            0
        } else {
            MAX_WINDOW_SIZE - pending
        }
    }

    pub fn bytes_pending(&self) -> usize {
        self.data.iter().map(|p| p.get_ref().len()).sum()
    }

    pub fn set_initial_ack_nr(&mut self, ack_nr: u16) {
        // This is the starting point
        self.ack_nr = Some(ack_nr);

        // Now, we prune the queue
        for p in &mut self.packets {
            let keep = p
                .as_ref()
                .map(|p| in_range(ack_nr, p.seq_nr()))
                .unwrap_or(false);

            if !keep {
                *p = None;
            }
        }
    }
}

fn in_range(ack_nr: u16, seq_nr: u16) -> bool {
    let upper = ack_nr.wrapping_add(MAX_DELTA_SEQ as u16);

    if upper > ack_nr {
        // Non wrapping case
        seq_nr > ack_nr && seq_nr <= upper
    } else {
        // Wrapping case
        seq_nr > ack_nr || seq_nr <= upper
    }
}

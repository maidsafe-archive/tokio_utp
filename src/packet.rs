use bytes::{BytesMut, BufMut};
use byteorder::{ByteOrder, BigEndian};
use smallvec::SmallVec;

use std::{fmt, io};

/// Packet header
///
/// ```text
/// 0       4       8               16              24              32
/// +-------+-------+---------------+---------------+---------------+
/// | type  | ver   | extension     | connection_id                 |
/// +-------+-------+---------------+---------------+---------------+
/// | timestamp_microseconds                                        |
/// +---------------+---------------+---------------+---------------+
/// | timestamp_difference_microseconds                             |
/// +---------------+---------------+---------------+---------------+
/// | wnd_size                                                      |
/// +---------------+---------------+---------------+---------------+
/// | seq_nr                        | ack_nr                        |
/// +---------------+---------------+---------------+---------------+
/// ```
#[derive(Clone)]
pub struct Packet {
    padding: usize,
    data: BytesMut,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum Type {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

const DEFAULT: [u8; 26] = [
    0, 0, 0, 0, 0, 0,   // initial padding so that we can insert selective ack
                        // by moving the header back

    1, 0, 0, 0,
    0, 0, 0, 0,
    255, 255, 255, 255, // Default timestamp difference
    0, 1, 0, 0,         // Default window of 64kb
    0, 0, 0, 0];

const VERSION_MASK: u8 = 0b1111;

impl Packet {
    pub fn parse(packet: BytesMut) -> io::Result<Packet> {
        if packet.len() < 20 {
            return Err(io::ErrorKind::InvalidData.into());
        }

        let ret = Packet::new(0, packet);

        if ret.version() != 1 {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid packet version"));
        }

        if ret.ty_raw() >= 5 {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid packet type"));
        }

        Ok(ret)
    }

    pub fn new(padding: usize, packet: BytesMut) -> Packet {
        Packet {
            padding: padding,
            data: packet,
        }
    }

    pub fn syn() -> Packet {
        let mut p = Packet::default();
        p.set_ty(Type::Syn);
        p
    }

    pub fn fin() -> Packet {
        let mut p = Packet::default();
        p.set_ty(Type::Fin);
        p
    }

    pub fn state() -> Packet {
        let mut p = Packet::default();
        p.set_ty(Type::State);
        p
    }

    pub fn data(src: &[u8]) -> Packet {
        let mut data = BytesMut::with_capacity(DEFAULT.len() + src.len());

        data.put_slice(&DEFAULT);
        data.put_slice(src);

        let mut p = Packet::new(6, data);
        p.set_ty(Type::Data);
        p
    }

    pub fn reset() -> Packet {
        let mut p = Packet::default();
        p.set_ty(Type::Reset);
        p
    }

    fn ty_raw(&self) -> u8 {
        self.data[self.padding] >> 4
    }

    pub fn ty(&self) -> Type {
        match self.ty_raw() {
            0 => Type::Data,
            1 => Type::Fin,
            2 => Type::State,
            3 => Type::Reset,
            4 => Type::Syn,
            _ => unreachable!(),
        }
    }

    pub fn set_ty(&mut self, ty: Type) {
        self.data[self.padding] = (ty as u8) << 4 | self.version()
    }

    pub fn version(&self) -> u8 {
        self.data[self.padding] & VERSION_MASK
    }

    pub fn connection_id(&self) -> u16 {
        BigEndian::read_u16(&self.data[self.padding + 2 .. self.padding + 4])
    }

    pub fn set_connection_id(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[self.padding + 2 .. self.padding + 4], val)
    }

    pub fn timestamp(&self) -> u32 {
        BigEndian::read_u32(&self.data[self.padding + 4 .. self.padding + 8])
    }

    pub fn set_timestamp(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[self.padding + 4 .. self.padding + 8], val)
    }

    pub fn timestamp_diff(&self) -> u32 {
        BigEndian::read_u32(&self.data[self.padding + 8 .. self.padding + 12])
    }

    pub fn set_timestamp_diff(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[self.padding + 8 .. self.padding + 12], val)
    }

    pub fn wnd_size(&self) -> u32 {
        BigEndian::read_u32(&self.data[self.padding + 12 .. self.padding + 16])
    }

    pub fn set_wnd_size(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[self.padding + 12 .. self.padding + 16], val);
    }

    pub fn seq_nr(&self) -> u16 {
        BigEndian::read_u16(&self.data[self.padding + 16 .. self.padding + 18])
    }

    pub fn set_seq_nr(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[self.padding + 16 .. self.padding + 18], val);
    }

    pub fn ack_nr(&self) -> u16 {
        BigEndian::read_u16(&self.data[self.padding + 18 .. self.padding + 20])
    }

    pub fn set_ack_nr(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[self.padding + 18 .. self.padding + 20], val);
    }

    fn payload_start_index(&self) -> usize {
        let mut ext_index = self.padding + 1;
        let mut payload_start = self.padding + 20;
        while self.data[ext_index] != 0 {
            ext_index = payload_start;
            payload_start += 2 + self.data[ext_index + 1] as usize;
        }
        payload_start
    }

    pub fn payload(&self) -> &[u8] {
        &self.data[self.payload_start_index()..]
    }

    pub fn into_payload(mut self) -> BytesMut {
        let index = self.payload_start_index();
        self.data.split_to(index);
        self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.padding..]
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    pub fn set_selective_acks(&mut self, selective_acks: [u8; 4]) {
        if selective_acks != [0; 4] {
            if self.padding == 6 {
                self.padding = 0;
                for i in 0..20 {
                    self.data[i] = self.data[i + 6];
                }
                self.data[1] = 1;
                self.data[20] = 0;
                self.data[21] = 4;
            }
            self.data[22 .. 26].copy_from_slice(&selective_acks[..]);
        }
    }

    pub fn selective_acks(&self) -> SmallVec<[u8; 4]> {
        let mut ext_index = self.padding + 1;
        let mut next_index = self.padding + 20;
        loop {
            match self.data[ext_index] {
                0   => return SmallVec::new(),
                1   => {
                    ext_index = next_index;
                    let len = self.data[ext_index + 1] as usize;
                    let mut ret = SmallVec::new();
                    ret.reserve(len);
                    unsafe {
                        ret.set_len(len);
                    }
                    ret[0..].copy_from_slice(&self.data[ext_index + 2 .. ext_index + 2 + len]);
                    return ret;
                },
                _   => {
                    ext_index = next_index;
                    let len = self.data[ext_index + 1] as usize;
                    next_index += 2 + len;
                },
            }
        }
    }
}

impl Default for Packet {
    fn default() -> Packet {
        Packet {
            padding: 6,
            data: BytesMut::from(&DEFAULT[..])
        }
    }
}

impl fmt::Debug for Packet {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Packet")
            .field("type", &self.ty())
            .field("version", &self.version())
            // .field("extension", &self.extension())
            .field("connection_id", &self.connection_id())
            .field("timestamp", &self.timestamp())
            .field("timestamp_diff", &self.timestamp_diff())
            .field("wnd_size", &self.wnd_size())
            .field("seq_nr", &self.seq_nr())
            .field("ack_nr", &self.ack_nr())
            .field("payload", &self.payload().len())
            .finish()
    }
}

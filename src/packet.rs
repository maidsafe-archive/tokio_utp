use bytes::{BytesMut, BufMut};
use byteorder::{ByteOrder, BigEndian};

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

pub const HEADER_LEN: usize = 20;

const DEFAULT: [u8; 20] = [
    1, 0, 0, 0,
    0, 0, 0, 0,
    255, 255, 255, 255, // Default timestamp difference
    0, 1, 0, 0,         // Default window of 64kb
    0, 0, 0, 0];

const VERSION_MASK: u8 = 0b1111;

impl Packet {
    pub fn parse(packet: BytesMut) -> io::Result<Packet> {
        let ret = Packet::new(packet);

        if ret.version() != 1 {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid packet version"));
        }

        if ret.ty_raw() >= 5 {
            return Err(io::Error::new(io::ErrorKind::Other, "invalid packet type"));
        }

        Ok(ret)
    }

    pub fn new(packet: BytesMut) -> Packet {
        Packet {
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
        let mut data = BytesMut::with_capacity(HEADER_LEN + src.len());

        data.put_slice(&DEFAULT);
        data.put_slice(src);

        let mut p = Packet::new(data);
        p.set_ty(Type::Data);
        p
    }

    fn ty_raw(&self) -> u8 {
        self.data[0] >> 4
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
        self.data[0] = (ty as u8) << 4 | self.version()
    }

    pub fn version(&self) -> u8 {
        self.data[0] & VERSION_MASK
    }

    pub fn connection_id(&self) -> u16 {
        BigEndian::read_u16(&self.data[2..4])
    }

    pub fn set_connection_id(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[2..4], val)
    }

    pub fn timestamp(&self) -> u32 {
        BigEndian::read_u32(&self.data[4..8])
    }

    pub fn set_timestamp(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[4..8], val)
    }

    pub fn timestamp_diff(&self) -> u32 {
        BigEndian::read_u32(&self.data[8..12])
    }

    pub fn set_timestamp_diff(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[8..12], val)
    }

    pub fn wnd_size(&self) -> u32 {
        BigEndian::read_u32(&self.data[12..16])
    }

    pub fn set_wnd_size(&mut self, val: u32) {
        BigEndian::write_u32(&mut self.data[12..16], val);
    }

    pub fn seq_nr(&self) -> u16 {
        BigEndian::read_u16(&self.data[16..18])
    }

    pub fn set_seq_nr(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[16..18], val);
    }

    pub fn ack_nr(&self) -> u16 {
        BigEndian::read_u16(&self.data[18..20])
    }

    pub fn set_ack_nr(&mut self, val: u16) {
        BigEndian::write_u16(&mut self.data[18..20], val);
    }

    pub fn payload(&self) -> &[u8] {
        &self.data[HEADER_LEN..]
    }

    pub fn into_payload(mut self) -> BytesMut {
        self.data.split_to(HEADER_LEN);
        self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data[..]
    }

    pub fn len(&self) -> usize {
        self.as_slice().len()
    }
}

impl Default for Packet {
    fn default() -> Packet {
        Packet { data: BytesMut::from(&DEFAULT[..]) }
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

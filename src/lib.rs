extern crate byteorder;
extern crate bytes;
extern crate future_utils;
extern crate futures;
extern crate mio;
#[cfg(test)]
#[macro_use]
extern crate net_literals;
extern crate rand;
extern crate slab;
extern crate smallvec;
extern crate tokio_core;
extern crate tokio_io;
extern crate void;

#[macro_use]
extern crate log;
#[macro_use]
extern crate unwrap;

mod delays;
mod in_queue;
mod out_queue;
mod packet;
mod socket;
mod util;

#[cfg(test)]
extern crate env_logger;
#[cfg(test)]
extern crate netsim;

#[cfg(test)]
mod test;

pub use socket::{Incoming, RawChannel, RawReceiver, UtpListener, UtpSocket, UtpSocketFinalize,
                 UtpStream, UtpStreamConnect};

// max window size
const MAX_WINDOW_SIZE: usize = 64 * 1_024;
const MAX_DELTA_SEQ: usize = 32;
const TIMESTAMP_MASK: u32 = 0xFFFFFFFF;

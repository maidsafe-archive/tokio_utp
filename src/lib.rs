extern crate mio;
extern crate bytes;
extern crate slab;
extern crate rand;
extern crate byteorder;
extern crate smallvec;
extern crate tokio_core;
extern crate futures;
extern crate void;
extern crate tokio_io;

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
mod test;

pub use socket::{UtpSocket, UtpStream, UtpListener, UtpSocketFinalize, UtpStreamConnect, Incoming, Filter};

// max window size
const MAX_WINDOW_SIZE: usize = 64 * 1_024;
const MAX_DELTA_SEQ: usize = 32;
const TIMESTAMP_MASK: u32 = 0xFFFFFFFF;

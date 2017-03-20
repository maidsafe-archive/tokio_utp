#![allow(dead_code, unused_variables)]

extern crate mio;
extern crate bytes;
extern crate slab;
extern crate rand;
extern crate byteorder;

#[macro_use]
extern crate log;

mod in_queue;
mod out_queue;
mod packet;
mod socket;
mod util;

pub use socket::{UtpSocket, UtpStream, UtpListener};

// max window size
const MAX_WINDOW_SIZE: u32 = 1_024 * 1_024;
const MAX_DELTA_SEQ: usize = 32;

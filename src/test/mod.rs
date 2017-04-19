//! Tests are implemented in the source tree in order to have access to private
//! fields.

mod mock;
mod harness;


mod test_stream;
mod test_timeout;

/// Types that are imported in test modules
mod prelude {
    pub use super::harness::{Harness};
    pub use super::mock::{Mock};

    pub use packet::Packet;

    pub mod packet {
        pub use packet::Type;
    }
}

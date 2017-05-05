//! Tests are implemented in the source tree in order to have access to private
//! fields.

mod mock;
mod harness;

mod test_err;
mod test_flow;
mod test_listener;
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

    use std::time::{Instant, Duration};

    pub fn sleep(ms: u64) {
        use std::thread;
        thread::sleep(Duration::from_millis(ms));
    }

    pub struct Time {
        now: Instant,
    }

    impl Time {
        pub fn new() -> Time {
            Time { now: Instant::now() }
        }

        pub fn timestamp(&self) -> u32 {
            let dur = self.now.elapsed();
            as_micros(dur)
        }
    }

    fn as_micros(dur: Duration) -> u32 {
        let n = (dur.as_secs() as u32).wrapping_mul(1_000_000);
        n.wrapping_add(dur.subsec_nanos() / 1_000)
    }
}

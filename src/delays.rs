use util;
use super::TIMESTAMP_MASK;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct Delays {
    curr_delays: [u32; CURR_DELAY_LEN],
    curr_idx: usize,
    // the minimum delay between the hosts for the last N seconds. N = 13 * 60
    base_delay: u32,
    // sliding window of latest delays
    base_delays: [u32; BASE_DELAY_LEN],
    base_idx: usize,

    // The time when delay_base_idx was last stepped up. `None` indicates that
    // the struct is not initialized,
    last_step: Option<Instant>,
}

const CURR_DELAY_LEN: usize = 3;
const BASE_DELAY_LEN: usize = 13;

impl Delays {
    pub fn new() -> Delays {
        Delays::default()
    }

    /// Returns the minimum delay.
    pub fn get(&self) -> Option<u32> {
        self.last_step?;
        self.curr_delays.iter().min().cloned()
    }

    /// Returns the minimum delay between the hosts for the last N seconds.
    pub fn base_delay(&self) -> Option<u32> {
        self.last_step.map(|_| self.base_delay)
    }

    pub fn add_sample(&mut self, sample: u32, now: Instant) {
        if self.last_step.is_none() {
            self.last_step = Some(now);
            self.add_first_sample(sample);
        }

        self.keep_if_smaller(sample);
        self.set_curr_delay(sample);

        // Adds new base delay once every 60 seconds
        // NOTE, that's not what uTP specs say though:
        // "Each socket keeps a sliding minimum of the lowest value for the last two minutes."
        // in our case we keep minimum delays for about 10 mins.
        if now >= self.last_step.unwrap() + Duration::from_secs(60) {
            self.last_step = Some(now);
            self.advance_and_set_delay(sample);
            self.set_min_base_delay();
        }
    }

    pub fn shift(&mut self, offset: u32) {
        for e in &mut self.base_delays {
            *e = e.wrapping_add(offset);
        }

        self.base_delay = self.base_delay.wrapping_add(offset);
    }

    /// If given sample is smaller than our base delay, keep it.
    fn keep_if_smaller(&mut self, sample: u32) {
        let base = self.base_delays[self.base_idx];
        if util::wrapping_lt(sample, base, TIMESTAMP_MASK) {
            self.base_delays[self.base_idx] = sample;
        }
        if util::wrapping_lt(sample, self.base_delay, TIMESTAMP_MASK) {
            self.base_delay = sample;
        }
    }

    /// Our current delay is the difference between given sample and latest base(min) delay.
    /// I guess this is the part that uTP specs say:
    ///
    ///   When subtracting the base_delay from the timestamp difference in each packet you get a
    ///   measurement of the current buffering delay on the socket. This measurement is called
    ///   our_delay. It has a lot of noise it it, but is used as the driver to determine whether
    ///   to increase or decrease the send window (which controls the send rate).
    fn set_curr_delay(&mut self, sample: u32) {
        let delay = sample.wrapping_sub(self.base_delay);
        self.curr_delays[self.curr_idx] = delay;
        self.curr_idx = (self.curr_idx + 1) % self.curr_delays.len();
    }

    fn advance_and_set_delay(&mut self, sample: u32) {
        self.base_idx = (self.base_idx + 1) % self.base_delays.len();
        self.base_delays[self.base_idx] = sample;
    }

    fn add_first_sample(&mut self, sample: u32) {
        for entry in &mut self.base_delays {
            *entry = sample
        }
        self.base_delay = sample;
    }

    fn set_min_base_delay(&mut self) {
        self.base_delay = self.base_delays[0];
        for &base_delay in &self.base_delays {
            if util::wrapping_lt(base_delay, self.base_delay, TIMESTAMP_MASK) {
                self.base_delay = base_delay;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod add_first_sample {
        use super::*;

        #[test]
        fn it_fills_base_delays_with_given_sample() {
            let mut delays = Delays::new();

            delays.add_first_sample(10);

            assert_eq!(delays.base_delays, [10; BASE_DELAY_LEN]);
        }

        #[test]
        fn it_set_base_delay_to_given_sample() {
            let mut delays = Delays::new();

            delays.add_first_sample(10);

            assert_eq!(delays.base_delay, 10);
        }
    }

    mod keep_if_smaller {
        use super::*;

        #[test]
        fn when_given_sample_is_smaller_than_our_min_delay_it_keeps_it() {
            let mut delays = Delays::new();
            delays.add_first_sample(100);

            delays.keep_if_smaller(50);

            assert_eq!(delays.base_delay, 50);
        }

        #[test]
        fn when_given_sample_is_smaller_than_our_latest_registered_delay_it_is_replaced() {
            let mut delays = Delays::new();
            delays.add_first_sample(100);
            assert_eq!(delays.base_idx, 0);

            delays.keep_if_smaller(50);

            assert_eq!(delays.base_delays[0], 50);
        }
    }

    mod set_curr_delay {
        use super::*;

        #[test]
        fn it_subtracts_given_sample_from_base_delay_and_stores_it() {
            let mut delays = Delays::new();
            delays.base_delay = 80;

            delays.set_curr_delay(100);

            assert_eq!(delays.curr_delays[0], 20);
        }
    }

    mod add_sample {
        use super::*;

        #[test]
        fn when_last_step_is_none_it_is_set_to_given_time() {
            let mut delays = Delays::new();
            let now = Instant::now();

            delays.add_sample(100, now);

            assert_eq!(delays.last_step, Some(now));
        }
    }

    mod advance_and_set_delay {
        use super::*;

        #[test]
        fn it_advances_latest_delay_index() {
            let mut delays = Delays::new();
            assert_eq!(delays.base_idx, 0);

            delays.advance_and_set_delay(10);

            assert_eq!(delays.base_idx, 1);
        }

        #[test]
        fn when_latest_delay_index_is_last_it_wraps_it() {
            let mut delays = Delays::new();
            delays.base_idx = BASE_DELAY_LEN - 1; // last element at delays array

            delays.advance_and_set_delay(10);

            assert_eq!(delays.base_idx, 0);
        }

        #[test]
        fn it_registers_new_delay_sample_at_next_slot() {
            let mut delays = Delays::new();
            assert_eq!(delays.base_idx, 0);

            delays.advance_and_set_delay(10);

            assert_eq!(delays.base_delays[1], 10);
        }
    }

    #[test]
    fn set_min_base_delay_traverses_all_base_delays_and_sets_smallest_one() {
        let mut delays = Delays::new();
        delays.add_first_sample(100);
        delays.base_delays[0] = 50;

        delays.set_min_base_delay();

        assert_eq!(delays.base_delay, 50);
    }
}

use util;
use super::TIMESTAMP_MASK;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Default)]
pub struct Delays {
    curr_delays: [u32; CURR_DELAY_LEN],
    curr_idx: usize,
    base_delay: u32,
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

    pub fn base_delay(&self) -> Option<u32> {
        self.last_step.map(|_| self.base_delay)
    }

    pub fn add_sample(&mut self, sample: u32, now: Instant) {
        if self.last_step.is_none() {
            self.last_step = Some(now);

            for entry in &mut self.base_delays {
                *entry = sample
            }

            self.base_delay = sample;
        }

        let base = self.base_delays[self.base_idx];

        if util::wrapping_lt(sample, base, TIMESTAMP_MASK) {
            self.base_delays[self.base_idx] = sample;
        }

        if util::wrapping_lt(sample, self.base_delay, TIMESTAMP_MASK) {
            self.base_delay = sample;
        }

        let delay = sample.wrapping_sub(self.base_delay);

        self.curr_delays[self.curr_idx] = delay;
        self.curr_idx = (self.curr_idx + 1) % self.curr_delays.len();

        if now >= self.last_step.unwrap() + Duration::from_secs(60) {
            self.last_step = Some(now);
            self.base_idx = (self.base_idx + 1) % self.base_delays.len();

            self.base_delays[self.base_idx] = sample;
            self.base_delay = self.base_delays[0];

            for &base_delay in &self.base_delays {
                if util::wrapping_lt(base_delay, self.base_delay, TIMESTAMP_MASK) {
                    self.base_delay = base_delay;
                }
            }
        }
    }

    pub fn shift(&mut self, offset: u32) {
        for e in &mut self.base_delays {
            *e = e.wrapping_add(offset);
        }

        self.base_delay = self.base_delay.wrapping_add(offset);
    }
}

use std::time::Duration;

pub fn as_ms(duration: Duration) -> u64 {
    // Lets just limit to 30 seconds
    if duration.as_secs() > 30 {
        30_000
    } else {
        let sub_secs = duration.subsec_nanos() / NANOS_PER_MS;
        duration.as_secs() * 1000 + u64::from(sub_secs)
    }
}

/// Wrapping less than comparison
pub fn wrapping_lt(lhs: u32, rhs: u32, mask: u32) -> bool {
    let dist_dn = lhs.wrapping_sub(rhs) & mask;
    let dist_up = rhs.wrapping_sub(lhs) & mask;

    dist_up < dist_dn
}

const MICROS_PER_SEC: u32 = 1_000_000;
const NANOS_PER_MS: u32 = 1_000_000;
const NANOS_PER_MICRO: u32 = 1_000;

pub fn as_wrapping_micros(duration: Duration) -> u32 {
    // Wrapping is OK
    let mut ret = duration.as_secs().wrapping_mul(u64::from(MICROS_PER_SEC)) as u32;
    ret += duration.subsec_nanos() / NANOS_PER_MICRO;
    ret
}

/// Safely generates two sequential connection identifiers.
///
/// This avoids an overflow when the generated receiver identifier is the largest
/// representable value in u16 and it is incremented to yield the corresponding sender
/// identifier.
pub fn generate_sequential_identifiers() -> (u16, u16) {
    let id: u16 = rand();

    if id.checked_add(1).is_some() {
        (id, id + 1)
    } else {
        (id - 1, id)
    }
}

#[cfg(test)]
pub fn random_vec(num_bytes: usize) -> Vec<u8> {
    use rand::Rng;

    let mut ret = Vec::with_capacity(num_bytes);
    unsafe { ret.set_len(num_bytes) };
    THREAD_RNG.with(|r| r.borrow_mut().fill_bytes(&mut ret[..]));
    ret
}

#[cfg(not(test))]
pub fn rand<T: ::rand::Rand>() -> T {
    use rand::{self, Rng};

    let mut rng = rand::thread_rng();
    rng.gen::<T>()
}

#[cfg(test)]
pub use self::test::{rand, reset_rand, THREAD_RNG};

#[cfg(test)]
mod test {
    use rand::{Rand, Rng, XorShiftRng};
    use std::cell::RefCell;

    thread_local!(pub static THREAD_RNG: RefCell<XorShiftRng> = {
        RefCell::new(XorShiftRng::new_unseeded())
    });

    pub fn rand<T: Rand>() -> T {
        THREAD_RNG.with(|t| t.borrow_mut().gen::<T>())
    }

    #[cfg(test)]
    pub fn reset_rand() {
        THREAD_RNG.with(|t| *t.borrow_mut() = XorShiftRng::new_unseeded());
    }
}

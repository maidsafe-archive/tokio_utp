use rand::{self, Rng};

/// Safely generates two sequential connection identifiers.
///
/// This avoids an overflow when the generated receiver identifier is the largest
/// representable value in u16 and it is incremented to yield the corresponding sender
/// identifier.
pub fn generate_sequential_identifiers() -> (u16, u16) {
    let mut rng = rand::thread_rng();
    let id = rng.gen::<u16>();
    if id.checked_add(1).is_some() {
        (id, id + 1)
    } else {
        (id - 1, id)
    }
}

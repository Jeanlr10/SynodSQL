//! The simulator's random number generator.
//!
//! The simulator has its own generator (xoshiro256**, seeded by
//! SplitMix64) instead of a library one, so that a seed gives the same run
//! on every platform and with every version of the dependencies. The
//! generator uses only integer operations.

/// A seeded, deterministic random number generator.
#[derive(Clone, Debug)]
pub struct SimRng {
    s: [u64; 4],
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

impl SimRng {
    /// Makes a generator from a seed.
    pub fn new(seed: u64) -> SimRng {
        let mut state = seed;
        SimRng { s: [(); 4].map(|_| splitmix64(&mut state)) }
    }

    /// Returns the next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        let s = &mut self.s;
        let result = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        result
    }

    /// Returns a uniform value in `0..n`, with no bias (Lemire's method).
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        let threshold = n.wrapping_neg() % n;
        loop {
            let m = self.next_u64() as u128 * n as u128;
            if (m as u64) >= threshold {
                return (m >> 64) as u64;
            }
        }
    }

    /// Returns a uniform value in `lo..=hi`.
    ///
    /// # Panics
    ///
    /// Panics if `lo > hi`.
    pub fn range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "range({lo}, {hi})");
        match (hi - lo).checked_add(1) {
            Some(span) => lo + self.below(span),
            None => self.next_u64(),
        }
    }

    /// Returns true with probability `per_million / 1_000_000`.
    pub fn chance(&mut self, per_million: u32) -> bool {
        self.below(1_000_000) < per_million as u64
    }

    /// Returns a random item of `items`.
    ///
    /// # Panics
    ///
    /// Panics if `items` is empty.
    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    /// Makes an independent generator from this one.
    pub fn fork(&mut self) -> SimRng {
        SimRng::new(self.next_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_pinned() {
        // A change here changes every simulation run. Do not change it
        // without a reason.
        let mut r = SimRng::new(0);
        let got: Vec<u64> = (0..3).map(|_| r.next_u64()).collect();
        assert_eq!(got, PINNED);
    }

    // Checked against an independent Python implementation.
    const PINNED: [u64; 3] = [11091344671253066420, 13793997310169335082, 1900383378846508768];

    #[test]
    fn below_is_in_range() {
        let mut r = SimRng::new(7);
        for n in 1..200 {
            assert!(r.below(n) < n);
        }
        assert_eq!(r.range(5, 5), 5);
        r.range(0, u64::MAX);
    }
}

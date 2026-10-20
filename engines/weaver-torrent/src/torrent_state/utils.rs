// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::{AtomicU32, Ordering};

// A byte count with binary prefixes, one truncated decimal once a prefix
// applies: 999, 1.0Ki, 999.9Ki, 42.0Mi.
pub struct BinarySize(pub u64);

impl std::fmt::Display for BinarySize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const PREFIXES: [&str; 7] = ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"];
        let mut whole = self.0;
        let mut divisions = 0;
        while whole >= 1024 {
            whole /= 1024;
            divisions += 1;
        }
        if divisions == 0 {
            return write!(f, "{whole}");
        }
        let unit = 1u128 << (10 * divisions);
        let tenth = u128::from(self.0) % unit * 10 / unit;
        write!(f, "{whole}.{tenth}{}", PREFIXES[divisions])
    }
}

pub fn atomic_inc(c: &AtomicU32) -> u32 {
    c.fetch_add(1, Ordering::Relaxed)
}

pub fn atomic_dec(c: &AtomicU32) -> u32 {
    c.fetch_sub(1, Ordering::Relaxed)
}

// Used during debugging to see if some locks take too long.
mod timed_existence {
    use std::ops::{Deref, DerefMut};

    pub struct TimedExistence<T>(T);

    impl<T> TimedExistence<T> {
        #[inline(always)]
        pub fn new(object: T, _reason: &'static str) -> Self {
            Self(object)
        }
    }

    impl<T> Deref for TimedExistence<T> {
        type Target = T;

        #[inline(always)]
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl<T> DerefMut for TimedExistence<T> {
        #[inline(always)]
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    #[inline(always)]
    pub fn timeit<R>(_n: impl std::fmt::Display, f: impl FnOnce() -> R) -> R {
        f()
    }
}

pub use timed_existence::{TimedExistence, timeit};

#[cfg(test)]
mod tests {
    use super::BinarySize;

    #[test]
    fn binary_size_keeps_one_truncated_decimal() {
        for (bytes, text) in [
            (0, "0"),
            (999, "999"),
            (1023, "1023"),
            (1024, "1.0Ki"),
            (1535, "1.4Ki"),
            (55 * 1024, "55.0Ki"),
            (999 * 1024 + 1023, "999.9Ki"),
            (42 * 1024 * 1024, "42.0Mi"),
            (u64::MAX, "15.9Ei"),
        ] {
            assert_eq!(BinarySize(bytes).to_string(), text);
        }
    }
}

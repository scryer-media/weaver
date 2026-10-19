// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::{Duration, Instant};

// Token bucket rate limiter for bandwidth throttling.
//
// Tokens represent bytes. The bucket refills at `rate` bytes/sec up to a
// capacity of 1 second of bandwidth. Consuming more tokens than available
// drives the balance negative — callers should wait before consuming more.
//
// A rate of `None` is no limit. A rate of `Some(0)` lets nothing through:
// the bucket never refills and callers always wait.
pub struct TokenBucket {
    tokens: f64,
    rate: Option<u64>,
    capacity: f64,
    last_refill: Instant,
}

// How long a closed bucket reports until it is ready. It never refills; the
// change that reopens it wakes its caller first.
const CLOSED_WAIT: Duration = Duration::from_secs(3600);

impl TokenBucket {
    pub fn new(rate: Option<u64>) -> Self {
        let capacity = rate.unwrap_or(0) as f64;
        Self {
            tokens: capacity,
            rate,
            capacity,
            last_refill: Instant::now(),
        }
    }

    // The refill rate of a bucket that is limited and open.
    fn flowing(&self) -> Option<f64> {
        self.rate.filter(|&rate| rate > 0).map(|rate| rate as f64)
    }

    // Refill tokens based on elapsed time since last refill.
    fn refill(&mut self) {
        let Some(rate) = self.flowing() else {
            return;
        };
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * rate).min(self.capacity);
    }

    // Consume `bytes` tokens. May drive balance negative.
    pub fn consume(&mut self, bytes: u64) {
        if self.flowing().is_none() {
            return;
        }
        self.refill();
        self.tokens -= bytes as f64;
    }

    // Refund previously consumed bytes, capped at the bucket capacity.
    pub fn refund(&mut self, bytes: u64) {
        if self.flowing().is_none() {
            return;
        }
        self.refill();
        self.tokens = (self.tokens + bytes as f64).min(self.capacity);
    }

    // Reconcile an estimated consumption with the actual byte count.
    pub fn reconcile(&mut self, estimated_bytes: u64, actual_bytes: u64) {
        if actual_bytes > estimated_bytes {
            self.consume(actual_bytes - estimated_bytes);
        } else if estimated_bytes > actual_bytes {
            self.refund(estimated_bytes - actual_bytes);
        }
    }

    // Returns true if the caller should wait: the bucket is closed, or
    // limited with a balance below zero.
    pub fn should_wait(&mut self) -> bool {
        match self.rate {
            None => false,
            Some(0) => true,
            Some(_) => {
                self.refill();
                self.tokens < 0.0
            }
        }
    }

    // Duration until enough tokens are available. Returns `Duration::ZERO`
    // if ready or unlimited.
    pub fn time_until_ready(&mut self) -> Duration {
        let Some(rate) = self.rate else {
            return Duration::ZERO;
        };
        if rate == 0 {
            return CLOSED_WAIT;
        }
        self.refill();
        if self.tokens >= 0.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(-self.tokens / rate as f64)
    }

    // Update the rate limit.
    pub fn set_rate(&mut self, rate: Option<u64>) {
        self.refill();
        self.rate = rate;
        self.capacity = rate.unwrap_or(0) as f64;
        // Clamp existing tokens to new capacity (if capacity decreased).
        if rate.is_some() {
            self.tokens = self.tokens.min(self.capacity);
        }
    }

    pub fn rate(&self) -> Option<u64> {
        self.rate
    }

    // Whether rate limiting is active.
    pub fn is_limited(&self) -> bool {
        self.rate.is_some()
    }
}

#[cfg(test)]
mod tests;

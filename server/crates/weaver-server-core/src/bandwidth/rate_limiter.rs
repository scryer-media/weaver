use std::time::{Duration, Instant};

/// Token bucket rate limiter for bandwidth throttling.
///
/// Tokens represent bytes. The bucket refills at `rate` bytes/sec up to a
/// capacity of 1 second of bandwidth. Consuming more tokens than available
/// drives the balance negative — callers should wait before consuming more.
pub struct TokenBucket {
    tokens: f64,
    rate: f64,
    capacity: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new rate limiter. `rate` is bytes/sec; 0 means unlimited.
    pub fn new(rate: u64) -> Self {
        let rate_f = rate as f64;
        Self {
            tokens: rate_f,
            rate: rate_f,
            capacity: rate_f,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time since last refill.
    fn refill(&mut self) {
        if self.rate <= 0.0 {
            return;
        }
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
    }

    /// Consume `bytes` tokens. May drive balance negative.
    pub fn consume(&mut self, bytes: u64) {
        if self.rate <= 0.0 {
            return;
        }
        self.refill();
        self.tokens -= bytes as f64;
    }

    /// Returns true if the caller should wait (balance < 0 and rate limiting is active).
    pub fn should_wait(&mut self) -> bool {
        if self.rate <= 0.0 {
            return false;
        }
        self.refill();
        self.tokens < 0.0
    }

    /// Duration until enough tokens are available. Returns `Duration::ZERO` if ready or unlimited.
    pub fn time_until_ready(&mut self) -> Duration {
        if self.rate <= 0.0 {
            return Duration::ZERO;
        }
        self.refill();
        if self.tokens >= 0.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64(-self.tokens / self.rate)
    }

    /// Update the rate limit. 0 means unlimited.
    pub fn set_rate(&mut self, rate: u64) {
        self.refill();
        let rate_f = rate as f64;
        self.rate = rate_f;
        self.capacity = rate_f;
        // Clamp existing tokens to new capacity (if capacity decreased).
        if rate_f > 0.0 {
            self.tokens = self.tokens.min(self.capacity);
        }
    }

    /// Current rate in bytes/sec. 0 means unlimited.
    pub fn rate(&self) -> u64 {
        self.rate as u64
    }

    /// Whether rate limiting is active (rate > 0).
    pub fn is_limited(&self) -> bool {
        self.rate > 0.0
    }
}

#[cfg(test)]
mod tests;

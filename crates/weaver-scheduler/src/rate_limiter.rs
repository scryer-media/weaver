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
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn unlimited_never_waits() {
        let mut bucket = TokenBucket::new(0);
        bucket.consume(1_000_000_000);
        assert!(!bucket.should_wait());
        assert_eq!(bucket.time_until_ready(), Duration::ZERO);
    }

    #[test]
    fn limited_waits_after_exhaustion() {
        let mut bucket = TokenBucket::new(1_000);
        // Consume more than capacity to guarantee negative balance.
        bucket.consume(1_500);
        assert!(bucket.should_wait());
    }

    #[test]
    fn refill_over_time() {
        let mut bucket = TokenBucket::new(10_000);
        // Consume more than capacity to go clearly negative.
        bucket.consume(11_000);
        assert!(bucket.should_wait());

        // Sleep 150ms — should refill ~1500 tokens (10_000 * 0.15),
        // recovering from a ~1000 token deficit.
        thread::sleep(Duration::from_millis(150));

        // After refill, deficit should be recovered.
        assert!(!bucket.should_wait());
    }

    #[test]
    fn time_until_ready() {
        let mut bucket = TokenBucket::new(1_000);
        // Consume 1500 to create a ~500 token deficit.
        bucket.consume(1_500);
        let wait = bucket.time_until_ready();
        assert!(wait > Duration::ZERO);
        // Deficit is ~500 tokens at 1000/sec = ~0.5s
        assert!(wait <= Duration::from_millis(600));
        assert!(wait >= Duration::from_millis(400));
    }

    #[test]
    fn set_rate_changes_limit() {
        let mut bucket = TokenBucket::new(0);
        assert!(!bucket.is_limited());

        bucket.set_rate(1_000);
        assert!(bucket.is_limited());

        bucket.consume(1_000);
        assert!(bucket.should_wait());
    }

    #[test]
    fn set_rate_to_zero_unlimited() {
        let mut bucket = TokenBucket::new(1_000);
        bucket.consume(1_500);
        assert!(bucket.should_wait());

        bucket.set_rate(0);
        assert!(!bucket.is_limited());
        assert!(!bucket.should_wait());
    }

    #[test]
    fn negative_balance_recovery() {
        let mut bucket = TokenBucket::new(1_000);
        // Consume 2x capacity — deficit of 1000 tokens.
        bucket.consume(2_000);

        let wait = bucket.time_until_ready();
        // 1000 token deficit at 1000 tokens/sec ≈ 1s
        assert!(wait >= Duration::from_millis(900));
        assert!(wait <= Duration::from_millis(1_100));
    }
}

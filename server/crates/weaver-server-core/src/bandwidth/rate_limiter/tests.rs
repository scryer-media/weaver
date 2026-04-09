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

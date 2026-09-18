use super::*;

/// Forward-date the last refill so the next refill adds no tokens, whatever
/// time the runner lets pass between two calls. That refill stamps the real
/// `now` again, so the tests call this before every balance-touching step and
/// the balance only moves by what the test consumes, refunds or backdates.
fn freeze_refill(bucket: &mut TokenBucket) {
    bucket.last_refill = Instant::now() + Duration::from_secs(3600);
}

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
    freeze_refill(&mut bucket);
    bucket.consume(1_500);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());
}

#[test]
fn refill_over_time() {
    let mut bucket = TokenBucket::new(10_000);
    // Consume more than capacity to go clearly negative.
    freeze_refill(&mut bucket);
    bucket.consume(11_000);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());

    // Backdate the last refill by 150 ms: at least 1500 tokens (10_000 * 0.15)
    // come back, recovering the ~1000 token deficit.
    bucket.last_refill = bucket
        .last_refill
        .checked_sub(Duration::from_millis(150))
        .expect("instant 150 ms in the past");

    // After refill, deficit should be recovered.
    assert!(!bucket.should_wait());
}

#[test]
fn time_until_ready() {
    let mut bucket = TokenBucket::new(1_000);
    // Consume 1500 to create exactly a 500 token deficit.
    freeze_refill(&mut bucket);
    bucket.consume(1_500);
    freeze_refill(&mut bucket);
    let wait = bucket.time_until_ready();
    // Deficit is 500 tokens at 1000/sec = 0.5s
    assert_eq!(wait, Duration::from_millis(500));
}

#[test]
fn set_rate_changes_limit() {
    let mut bucket = TokenBucket::new(0);
    assert!(!bucket.is_limited());

    freeze_refill(&mut bucket);
    bucket.set_rate(1_000);
    assert!(bucket.is_limited());

    freeze_refill(&mut bucket);
    bucket.consume(1_000);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());
}

#[test]
fn set_rate_to_zero_unlimited() {
    let mut bucket = TokenBucket::new(1_000);
    freeze_refill(&mut bucket);
    bucket.consume(1_500);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());

    freeze_refill(&mut bucket);
    bucket.set_rate(0);
    assert!(!bucket.is_limited());
    freeze_refill(&mut bucket);
    assert!(!bucket.should_wait());
}

#[test]
fn negative_balance_recovery() {
    let mut bucket = TokenBucket::new(1_000);
    // Consume 2x capacity — deficit of 1000 tokens.
    freeze_refill(&mut bucket);
    bucket.consume(2_000);

    freeze_refill(&mut bucket);
    let wait = bucket.time_until_ready();
    // 1000 token deficit at 1000 tokens/sec = 1s
    assert_eq!(wait, Duration::from_secs(1));
}

#[test]
fn refund_reduces_wait_without_exceeding_capacity() {
    let mut bucket = TokenBucket::new(1_000);
    freeze_refill(&mut bucket);
    bucket.consume(1_500);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());

    freeze_refill(&mut bucket);
    bucket.refund(600);
    freeze_refill(&mut bucket);
    assert!(!bucket.should_wait());

    freeze_refill(&mut bucket);
    bucket.refund(10_000);
    freeze_refill(&mut bucket);
    bucket.consume(1_001);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());
}

#[test]
fn reconcile_adjusts_estimate_to_actual_bytes() {
    let mut bucket = TokenBucket::new(1_000);
    freeze_refill(&mut bucket);
    bucket.consume(500);
    freeze_refill(&mut bucket);
    bucket.reconcile(500, 1_200);
    freeze_refill(&mut bucket);
    assert!(bucket.should_wait());

    freeze_refill(&mut bucket);
    bucket.reconcile(1_200, 100);
    freeze_refill(&mut bucket);
    assert!(!bucket.should_wait());
}

use super::*;

#[test]
fn bandwidth_usage_minute_buckets_roundtrip_and_prune() {
    let db = Database::open_in_memory().unwrap();

    db.add_bandwidth_usage_minute(100, 10).unwrap();
    db.add_bandwidth_usage_minute(100, 5).unwrap();
    db.add_bandwidth_usage_minute(101, 20).unwrap();

    assert_eq!(db.sum_bandwidth_usage_minutes(100, 101).unwrap(), 15);
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 35);
    assert_eq!(db.sum_bandwidth_usage_minutes(99, 100).unwrap(), 0);

    let deleted = db.prune_bandwidth_usage_before(101).unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(db.sum_bandwidth_usage_minutes(100, 102).unwrap(), 20);
}

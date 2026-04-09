use super::*;

fn sample_feed(id: u32) -> RssFeedRow {
    RssFeedRow {
        id,
        name: format!("Feed {id}"),
        url: format!("https://example.com/{id}.xml"),
        enabled: true,
        poll_interval_secs: 900,
        username: Some("user".to_string()),
        password: Some("secret".to_string()),
        default_category: Some("tv".to_string()),
        default_metadata: vec![("source".to_string(), "rss".to_string())],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    }
}

fn sample_rule(id: u32, feed_id: u32) -> RssRuleRow {
    RssRuleRow {
        id,
        feed_id,
        sort_order: id as i32,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Frieren".to_string()),
        item_categories: vec!["tv".to_string()],
        min_size_bytes: Some(100),
        max_size_bytes: Some(1000),
        category_override: Some("anime".to_string()),
        metadata: vec![("profile".to_string(), "hevc".to_string())],
    }
}

#[test]
fn rss_feed_and_rule_roundtrip() {
    let db = Database::open_in_memory().unwrap();
    let feed = sample_feed(1);
    db.insert_rss_feed(&feed).unwrap();
    assert_eq!(db.get_rss_feed(1).unwrap(), Some(feed.clone()));

    let rule = sample_rule(10, 1);
    db.insert_rss_rule(&rule).unwrap();
    assert_eq!(db.list_rss_rules(1).unwrap(), vec![rule.clone()]);

    let mut updated = feed.clone();
    updated.last_error = Some("boom".to_string());
    updated.consecutive_failures = 2;
    db.update_rss_feed(&updated).unwrap();
    assert_eq!(db.get_rss_feed(1).unwrap(), Some(updated));

    assert!(db.delete_rss_rule(rule.id).unwrap());
    assert!(db.list_rss_rules(1).unwrap().is_empty());
}

#[test]
fn rss_seen_items_dedupe_and_purge() {
    let db = Database::open_in_memory().unwrap();
    db.insert_rss_feed(&sample_feed(1)).unwrap();
    let item = RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".to_string(),
        item_title: "Example".to_string(),
        published_at: Some(123),
        size_bytes: Some(456),
        decision: "accepted".to_string(),
        seen_at: 1_000,
        job_id: Some(42),
        item_url: Some("https://example.com/a.nzb".to_string()),
        error: None,
    };
    db.insert_rss_seen_item(&item).unwrap();
    assert!(db.rss_seen_item_exists(1, "guid-1").unwrap());
    assert_eq!(
        db.list_rss_seen_items(Some(1), Some(10)).unwrap(),
        vec![item]
    );
    assert!(db.delete_rss_seen_item(1, "guid-1").unwrap());
    assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());

    db.insert_rss_seen_item(&RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".to_string(),
        item_title: "Example".to_string(),
        published_at: Some(123),
        size_bytes: Some(456),
        decision: "accepted".to_string(),
        seen_at: 1_000,
        job_id: Some(42),
        item_url: Some("https://example.com/a.nzb".to_string()),
        error: None,
    })
    .unwrap();
    assert_eq!(db.purge_old_rss_seen_items(2_000).unwrap(), 1);
    assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());
}

#[test]
fn clear_rss_seen_items_can_scope_to_feed() {
    let db = Database::open_in_memory().unwrap();
    db.insert_rss_feed(&sample_feed(1)).unwrap();
    db.insert_rss_feed(&sample_feed(2)).unwrap();
    db.insert_rss_seen_item(&RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".to_string(),
        item_title: "Example".to_string(),
        published_at: None,
        size_bytes: None,
        decision: "submitted".to_string(),
        seen_at: 1_000,
        job_id: Some(1),
        item_url: None,
        error: None,
    })
    .unwrap();
    db.insert_rss_seen_item(&RssSeenItemRow {
        feed_id: 2,
        item_id: "guid-2".to_string(),
        item_title: "Example Two".to_string(),
        published_at: None,
        size_bytes: None,
        decision: "ignored".to_string(),
        seen_at: 2_000,
        job_id: None,
        item_url: None,
        error: None,
    })
    .unwrap();

    assert_eq!(db.clear_rss_seen_items(Some(1)).unwrap(), 1);
    let remaining = db.list_rss_seen_items(None, None).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].feed_id, 2);
    assert_eq!(db.clear_rss_seen_items(None).unwrap(), 1);
    assert!(db.list_rss_seen_items(None, None).unwrap().is_empty());
}

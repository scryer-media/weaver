mod common;

use common::{TestHarness, assert_has_errors, assert_no_errors, response_data};

// ---------------------------------------------------------------------------
// Feeds — CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_feeds_empty() {
    let h = TestHarness::new().await;
    let resp = h.execute("{ rssFeeds { id name url enabled rules { id } } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let feeds = data["rssFeeds"].as_array().unwrap();
    assert!(feeds.is_empty());
}

#[tokio::test]
async fn add_feed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "test", url: "https://example.com/rss", enabled: true }) {
                    id name url enabled
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let feed = &data["addRssFeed"];
    assert!(feed["id"].as_u64().unwrap() > 0);
    assert_eq!(feed["name"].as_str().unwrap(), "test");
    assert_eq!(feed["url"].as_str().unwrap(), "https://example.com/rss");
    assert!(feed["enabled"].as_bool().unwrap());
}

#[tokio::test]
async fn add_feed_invalid_url() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "bad", url: "not-a-url", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn add_feed_ftp_scheme() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "ftp", url: "ftp://example.com", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(
        err_msg.contains("unsupported"),
        "expected 'unsupported scheme' error, got: {err_msg}"
    );
}

#[tokio::test]
async fn add_feed_poll_interval_zero() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "zero", url: "https://example.com/rss", enabled: true, pollIntervalSecs: 0 }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_feed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "original", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateRssFeed(id: {id}, input: {{ name: "renamed", url: "https://example.com/rss", enabled: true }}) {{
                    id name
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["updateRssFeed"]["name"].as_str().unwrap(), "renamed");
}

#[tokio::test]
async fn update_nonexistent_feed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                updateRssFeed(id: 999, input: { name: "nope", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
    let err_msg = format!("{:?}", resp.errors);
    assert!(err_msg.contains("not found"), "expected 'not found' error, got: {err_msg}");
}

#[tokio::test]
async fn delete_feed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "delete-me", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(r#"mutation {{ deleteRssFeed(id: {id}) }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["deleteRssFeed"].as_bool().unwrap());
}

#[tokio::test]
async fn list_feeds_with_rules() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "with-rules", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0 }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);

    let resp = h.execute("{ rssFeeds { id name rules { id } } }").await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let feeds = data["rssFeeds"].as_array().unwrap();
    let feed = feeds.iter().find(|f| f["id"].as_u64().unwrap() == feed_id).unwrap();
    let rules = feed["rules"].as_array().unwrap();
    assert!(!rules.is_empty(), "feed should have at least one rule");
}

#[tokio::test]
async fn get_single_feed() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "single", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(r#"{{ rssFeed(id: {id}) {{ id name url enabled }} }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["rssFeed"]["name"].as_str().unwrap(), "single");
}

#[tokio::test]
async fn get_nonexistent_feed() {
    let h = TestHarness::new().await;
    let resp = h.execute(r#"{ rssFeed(id: 999) { id } }"#).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["rssFeed"].is_null());
}

// ---------------------------------------------------------------------------
// Rules — CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn add_rule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "rule-host", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0 }}) {{
                    id feedId enabled action sortOrder
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let rule = &data["addRssRule"];
    assert!(rule["id"].as_u64().unwrap() > 0);
    assert_eq!(rule["feedId"].as_u64().unwrap(), feed_id);
    assert_eq!(rule["action"].as_str().unwrap(), "ACCEPT");
}

#[tokio::test]
async fn add_rule_invalid_regex() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "regex-host", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0, titleRegex: "[invalid" }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn add_rule_min_gt_max() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "size-host", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0, minSizeBytes: 100, maxSizeBytes: 50 }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn add_rule_nonexistent_feed() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(
            r#"mutation {
                addRssRule(feedId: 999, input: { enabled: true, action: ACCEPT, sortOrder: 0 }) {
                    id
                }
            }"#,
        )
        .await;
    assert_has_errors(&resp);
}

#[tokio::test]
async fn update_rule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "update-rule-host", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0 }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let rule_id = response_data(&resp)["addRssRule"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                updateRssRule(id: {rule_id}, input: {{ enabled: false, action: REJECT, sortOrder: 1 }}) {{
                    id enabled action sortOrder
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let rule = &data["updateRssRule"];
    assert!(!rule["enabled"].as_bool().unwrap());
    assert_eq!(rule["action"].as_str().unwrap(), "REJECT");
    assert_eq!(rule["sortOrder"].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn delete_rule() {
    let h = TestHarness::new().await;

    let resp = h
        .execute(
            r#"mutation {
                addRssFeed(input: { name: "delete-rule-host", url: "https://example.com/rss", enabled: true }) {
                    id
                }
            }"#,
        )
        .await;
    assert_no_errors(&resp);
    let feed_id = response_data(&resp)["addRssFeed"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(
            r#"mutation {{
                addRssRule(feedId: {feed_id}, input: {{ enabled: true, action: ACCEPT, sortOrder: 0 }}) {{
                    id
                }}
            }}"#
        ))
        .await;
    assert_no_errors(&resp);
    let rule_id = response_data(&resp)["addRssRule"]["id"].as_u64().unwrap();

    let resp = h
        .execute(&format!(r#"mutation {{ deleteRssRule(id: {rule_id}) }}"#))
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert!(data["deleteRssRule"].as_bool().unwrap());
}

// ---------------------------------------------------------------------------
// Seen items
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_seen_items_empty() {
    let h = TestHarness::new().await;
    let resp = h
        .execute(r#"{ rssSeenItems { feedId itemId } }"#)
        .await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    let items = data["rssSeenItems"].as_array().unwrap();
    assert!(items.is_empty());
}

#[tokio::test]
async fn clear_seen_items() {
    let h = TestHarness::new().await;
    let resp = h.execute(r#"mutation { clearRssSeenItems }"#).await;
    assert_no_errors(&resp);
    let data = response_data(&resp);
    assert_eq!(data["clearRssSeenItems"].as_u64().unwrap(), 0);
}

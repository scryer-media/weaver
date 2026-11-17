// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn sample_feed(id: u32) -> RssFeedRow {
    RssFeedRow {
        scripts: Vec::new(),
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
        title_regex: Some("Silver Horizon".to_string()),
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
        decision: crate::RssSeenDecision::Accepted,
        seen_at: 1_000,
        job_id: Some(42),
        item_url: Some("https://example.com/a.nzb".to_string()),
        error: None,
    };
    db.insert_rss_seen_item(&item).unwrap();
    assert!(db.rss_seen_item_exists(1, "guid-1").unwrap());
    // Listed under its id's keyed digest, the rest as it was seen.
    assert_eq!(
        db.list_rss_seen_items(Some(1), Some(10)).unwrap(),
        vec![RssSeenItemRow {
            item_id: db.rss_seen_item_key("guid-1"),
            ..item
        }]
    );
    assert!(db.delete_rss_seen_item(1, "guid-1").unwrap());
    assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());

    db.insert_rss_seen_item(&RssSeenItemRow {
        feed_id: 1,
        item_id: "guid-1".to_string(),
        item_title: "Example".to_string(),
        published_at: Some(123),
        size_bytes: Some(456),
        decision: crate::RssSeenDecision::Accepted,
        seen_at: 1_000,
        job_id: Some(42),
        item_url: Some("https://example.com/a.nzb".to_string()),
        error: None,
    })
    .unwrap();
    assert_eq!(db.purge_old_rss_seen_items(2_000).unwrap(), 1);
    assert!(!db.rss_seen_item_exists(1, "guid-1").unwrap());
}

// Every feed's last items read in one call give, feed by feed, what a read
// of each feed alone gives: same items, same order, same cap. Items of one
// poll share their `seen_at`, so most of them tie.
#[test]
fn seen_items_per_feed_match_one_read_per_feed() {
    const CAP: u32 = 50;
    let db = Database::open_in_memory().unwrap();
    let counts = [0usize, 1, 49, 50, 51, 137];
    for (index, count) in counts.iter().enumerate() {
        let feed_id = index as u32 + 1;
        db.insert_rss_feed(&sample_feed(feed_id)).unwrap();
        for item in 0..*count {
            // Ids out of their insertion order, ten items a poll.
            let id = (item * 37) % count.max(&1);
            db.insert_rss_seen_item(&RssSeenItemRow {
                feed_id,
                item_id: format!("guid-{id:04}"),
                item_title: format!("Item {feed_id}/{id}"),
                published_at: (id % 3 == 0).then_some(id as i64),
                size_bytes: Some(id as u64 * 1_000),
                decision: crate::RssSeenDecision::Accepted,
                seen_at: 10_000 + (item / 10) as i64,
                job_id: None,
                item_url: Some(format!("https://example.com/{feed_id}/{id}.torrent")),
                error: None,
            })
            .unwrap();
        }
    }
    let feeds = db.list_rss_feeds().unwrap();

    crate::persistence::connection::blocking_call_probe::arm();
    let one = db.list_rss_seen_items_per_feed(CAP).unwrap();
    let one_calls = crate::persistence::connection::blocking_call_probe::take().len();

    crate::persistence::connection::blocking_call_probe::arm();
    let each: Vec<Vec<RssSeenItemRow>> = feeds
        .iter()
        .map(|feed| db.list_rss_seen_items(Some(feed.id), Some(CAP)).unwrap())
        .collect();
    let each_calls = crate::persistence::connection::blocking_call_probe::take().len();

    let grouped: Vec<Vec<RssSeenItemRow>> = feeds
        .iter()
        .map(|feed| {
            one.iter()
                .filter(|item| item.feed_id == feed.id)
                .cloned()
                .collect()
        })
        .collect();
    assert_eq!(grouped, each);
    assert_eq!(
        each.iter().map(Vec::len).collect::<Vec<_>>(),
        [0, 1, 49, 50, 50, 50]
    );
    assert_eq!((one_calls, each_calls), (1, feeds.len()));
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
        decision: crate::RssSeenDecision::Submitted,
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
        decision: crate::RssSeenDecision::Ignored,
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

// Each decision reads back as itself, its stored word stays what rows
// already hold, and a word no decision is reads back unchanged.
#[test]
fn a_seen_decision_keeps_its_stored_word() {
    use crate::RssSeenDecision;
    for decision in RssSeenDecision::KNOWN {
        assert_eq!(RssSeenDecision::parse(decision.as_str()), decision);
    }
    assert_eq!(
        RssSeenDecision::KNOWN.map(|decision| decision.as_str().to_owned()),
        [
            "accepted",
            "rejected",
            "ignored",
            "submitted",
            "error",
            "waiting"
        ]
    );
    assert_eq!(
        RssSeenDecision::parse("rejected_by_size"),
        RssSeenDecision::Other("rejected_by_size".to_owned())
    );

    let db = Database::open_in_memory().unwrap();
    db.insert_rss_feed(&sample_feed(1)).unwrap();
    for (index, decision) in RssSeenDecision::KNOWN
        .into_iter()
        .chain([RssSeenDecision::Other("rejected_by_size".to_owned())])
        .enumerate()
    {
        db.insert_rss_seen_item(&RssSeenItemRow {
            feed_id: 1,
            item_id: format!("guid-{index}"),
            item_title: "Example".to_string(),
            published_at: None,
            size_bytes: None,
            decision,
            seen_at: 1_000 + index as i64,
            job_id: None,
            item_url: None,
            error: None,
        })
        .unwrap();
    }
    let mut read: Vec<(String, String)> = db
        .list_rss_seen_items(None, None)
        .unwrap()
        .into_iter()
        .map(|row| (row.item_id, row.decision.as_str().to_owned()))
        .collect();
    read.sort();
    let mut kept = [
        ("guid-0", "accepted"),
        ("guid-1", "rejected"),
        ("guid-2", "ignored"),
        ("guid-3", "submitted"),
        ("guid-4", "error"),
        ("guid-5", "waiting"),
        ("guid-6", "rejected_by_size"),
    ]
    .map(|(id, word)| (db.rss_seen_item_key(id), word.to_owned()));
    kept.sort();
    assert_eq!(read, kept);
}

// Every feed URL and item link as the database holds them.
fn stored_links(db: &Database) -> (Vec<String>, Vec<Option<String>>) {
    use crate::persistence::sql_runtime::SqlRuntime;
    let datastore = db.datastore();
    db.run_sql_blocking_read(async move {
        let feeds = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT url FROM rss_feeds ORDER BY id",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.text("url"))
        .collect::<Result<Vec<_>, _>>()?;
        let items = SqlRuntime::fetch_all(
            datastore.read_exec(),
            "SELECT item_url FROM rss_seen_items ORDER BY feed_id, item_id",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.opt_text("item_url"))
        .collect::<Result<Vec<_>, _>>()?;
        Ok((feeds, items))
    })
    .unwrap()
}

// A private tracker's feed URL and item links carry its passkey: each is
// stored encrypted and reads back as it was. Rows an older start stored in
// the clear are encrypted by the next start's pass, a page at a time, and a
// pass with nothing left changes nothing.
#[test]
fn feed_urls_and_item_links_hold_no_passkey_at_rest() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    const PASSKEY: &str = "f00dfacecafe";
    let db = Database::open_in_memory().unwrap();
    let mut feed = sample_feed(1);
    feed.url = format!("https://tracker.invalid/rss?passkey={PASSKEY}");
    db.insert_rss_feed(&feed).unwrap();
    let item = |n: u32| RssSeenItemRow {
        feed_id: 1,
        item_id: format!("guid-{n:03}"),
        item_title: format!("Item {n}"),
        published_at: None,
        size_bytes: None,
        decision: crate::RssSeenDecision::Accepted,
        seen_at: i64::from(n),
        job_id: None,
        item_url: Some(format!("https://tracker.invalid/dl/{n}?passkey={PASSKEY}")),
        error: None,
    };
    let items: Vec<RssSeenItemRow> = (0..150).map(item).collect();
    db.insert_rss_seen_items(&items).unwrap();
    let sealed = stored_links(&db);
    assert!(sealed.0.iter().all(|url| !url.contains(PASSKEY)));
    assert!(sealed.1.iter().flatten().all(|url| !url.contains(PASSKEY)));
    assert_eq!(db.get_rss_feed(1).unwrap().unwrap().url, feed.url);
    // An item's id is often its link: it is stored as its keyed digest, and
    // found by the id the feed gives.
    let ids: Vec<String> = items.iter().map(|item| item.item_id.clone()).collect();
    let listed = db.list_rss_seen_items(Some(1), None).unwrap();
    assert_eq!(listed.len(), items.len());
    assert!(listed.iter().all(|item| !item.item_id.contains("guid-")));
    assert!(listed.iter().all(|item| {
        item.item_url
            .as_deref()
            .is_some_and(|url| url.contains(PASSKEY))
    }));
    assert_eq!(
        db.seen_rss_item_ids(1, &ids).unwrap(),
        ids.iter().cloned().collect()
    );
    assert!(db.rss_seen_item_exists(1, "guid-003").unwrap());
    assert!(!db.rss_seen_item_exists(1, "guid-900").unwrap());
    // Seen again, an item is the row it was.
    db.insert_rss_seen_item(&items[3]).unwrap();
    assert_eq!(db.list_rss_seen_items(Some(1), None).unwrap().len(), 150);

    // As an older start left them: the feed's URL, and 150 more items'
    // ids and links, in the clear.
    let datastore = db.datastore();
    let clear_url = feed.url.clone();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.write_exec(),
            "UPDATE rss_feeds SET url = {}",
            &[SqlArg::Text(clear_url)],
        )
        .await?;
        for n in 0..150 {
            SqlRuntime::execute(
                datastore.write_exec(),
                "INSERT INTO rss_seen_items
                    (feed_id, item_id, item_title, decision, seen_at, item_url)
                 VALUES (1, {}, 'Older', 'accepted', {}, {})",
                &[
                    SqlArg::Text(format!(
                        "https://tracker.invalid/old/{n:03}?passkey={PASSKEY}"
                    )),
                    SqlArg::I64(1000 + n),
                    SqlArg::Text(format!(
                        "https://tracker.invalid/old/{n:03}?passkey={PASSKEY}"
                    )),
                ],
            )
            .await?;
        }
        Ok(())
    })
    .unwrap();
    let older: Vec<String> = (0..150)
        .map(|n| format!("https://tracker.invalid/old/{n:03}?passkey={PASSKEY}"))
        .collect();
    // Until the pass, an older row is not found by its digest.
    assert!(db.seen_rss_item_ids(1, &older).unwrap().is_empty());

    db.seal_stored_secrets().unwrap();
    let resealed = stored_links(&db);
    assert!(resealed.0.iter().all(|url| !url.contains(PASSKEY)));
    assert_eq!(resealed.1.len(), 300);
    assert!(
        resealed
            .1
            .iter()
            .flatten()
            .all(|url| !url.contains(PASSKEY))
    );
    assert_eq!(db.get_rss_feed(1).unwrap().unwrap().url, feed.url);
    // Every item seen before the pass is seen after it, and no stored id
    // holds the passkey.
    assert_eq!(
        db.seen_rss_item_ids(1, &older).unwrap(),
        older.iter().cloned().collect()
    );
    assert_eq!(
        db.seen_rss_item_ids(1, &ids).unwrap(),
        ids.iter().cloned().collect()
    );
    let listed = db.list_rss_seen_items(Some(1), None).unwrap();
    assert_eq!(listed.len(), 300);
    assert!(listed.iter().all(|item| !item.item_id.contains(PASSKEY)));
    let last = listed.iter().find(|item| item.seen_at == 1149).unwrap();
    assert_eq!(last.item_url.as_deref(), Some(older[149].as_str()));

    // The pass is one pass: a start after it reads no row to look for
    // work, so one written in the clear past it stays as it is.
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.write_exec(),
            "INSERT INTO rss_seen_items (feed_id, item_id, item_title, decision, seen_at)
             VALUES (1, 'past-the-pass', 'Later', 'accepted', 5000)",
            &[],
        )
        .await
    })
    .unwrap();
    db.seal_stored_secrets().unwrap();
    assert!(
        db.list_rss_seen_items(Some(1), None)
            .unwrap()
            .iter()
            .any(|item| item.item_id == "past-the-pass")
    );
    // An item is forgotten by the id the list gives, or by the feed's.
    assert!(db.delete_rss_seen_item(1, &last.item_id).unwrap());
    assert!(db.delete_rss_seen_item(1, &older[0]).unwrap());
    assert!(db.delete_rss_seen_item(1, "past-the-pass").unwrap());
    assert_eq!(db.list_rss_seen_items(Some(1), None).unwrap().len(), 298);
}

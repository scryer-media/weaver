// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::categories::CategoryConfig;
use crate::settings::TorrentConfig;
use crate::torrents::service::{JobEnv, SystemClock, TorrentService, TorrentSettings};
use crate::torrents::sessions::tests::{FakeRoutes, env};
use crate::torrents::sessions::{Routes, Sessions};
use sha1::{Digest, Sha1};

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

struct Rig {
    service: RssService,
    db: Database,
    server: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.server.abort();
    }
}

// A feed of `items` served beside the torrent and NZB they link to, with one
// rule that takes every Silver Horizon item of `sizes`, and a torrent client
// that is on or off.
async fn rig(items: &str, sizes: Option<(u64, u64)>, torrents_on: bool) -> Rig {
    let dir = TempDir::new().unwrap();
    let db = Database::open(&dir.path().join("rss.sqlite")).unwrap();
    let feed = format!(
        r#"<?xml version="1.0"?>
        <rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
          <channel><title>Example</title>{items}</channel>
        </rss>"#
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let feed = feed.replace("http://localhost", &base_url);
    let app = Router::new()
        .route("/feed", get(move || async move { feed }))
        .route("/set.torrent", get(|| async { metainfo("set.bin") }))
        .route("/download.nzb", get(|| async { sample_nzb_bytes() }));
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    db.insert_rss_feed(&RssFeedRow {
        scripts: Vec::new(),
        id: 1,
        name: "Torrent Feed".to_string(),
        url: format!("{base_url}/feed"),
        enabled: true,
        poll_interval_secs: 900,
        username: None,
        password: None,
        default_category: Some("tv".to_string()),
        default_metadata: vec![],
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    })
    .unwrap();
    db.insert_rss_rule(&RssRuleRow {
        id: 1,
        feed_id: 1,
        sort_order: 0,
        enabled: true,
        action: RssRuleAction::Accept,
        title_regex: Some("Silver Horizon".to_string()),
        item_categories: vec![],
        min_size_bytes: sizes.map(|(min, _)| min),
        max_size_bytes: sizes.map(|(_, max)| max),
        category_override: None,
        metadata: vec![("tier".to_string(), "gold".to_string())],
    })
    .unwrap();

    let sessions = Sessions::new(
        Arc::new(FakeRoutes::default()) as Arc<dyn Routes>,
        env(&dir.path().join("sessions")),
        db.clone(),
    );
    sessions.turn(torrents_on).await;
    let torrents = TorrentService::new(
        db.clone(),
        sessions,
        TorrentSettings {
            complete_dir: dir.path().join("complete"),
            categories: vec![CategoryConfig {
                seeding: None,
                torrent_route: true,
                unpack: None,
                id: 1,
                name: "tv".to_string(),
                dest_dir: None,
                aliases: String::new(),
            }],
            seeding: None,
            torrent: TorrentConfig {
                enabled: torrents_on,
                ..Default::default()
            },
        },
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.open().await.unwrap();

    let service = build_service(dir.path(), db.clone());
    service.use_torrents(torrents);
    Rig {
        service,
        db,
        server,
        _dir: dir,
    }
}

impl Rig {
    // The NZB jobs the feed submitted to the queue.
    fn nzb_jobs(&self) -> Vec<crate::jobs::JobInfo> {
        self.service
            .inner
            .handle
            .list_jobs()
            .into_iter()
            .filter(|job| job.protocol == crate::jobs::Protocol::Nzb)
            .collect()
    }

    // The feed's seen rows, oldest first, as (item, decision, job, error).
    fn seen(&self) -> Vec<(String, String, Option<u64>, Option<String>)> {
        let mut rows = self.db.list_rss_seen_items(Some(1), None).unwrap();
        rows.sort_by(|a, b| a.item_id.cmp(&b.item_id));
        rows.into_iter()
            .map(|row| (row.item_id, row.decision, row.job_id, row.error))
            .collect()
    }

    fn torrent_jobs(&self) -> Vec<u64> {
        self.db
            .list_torrents()
            .unwrap()
            .into_iter()
            .map(|record| record.job_id.0)
            .collect()
    }
}

fn item(guid: &str, body: &str) -> String {
    format!("<item><guid>{guid}</guid><title>Silver Horizon {guid}</title>{body}</item>")
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

// A one-file torrent's metainfo.
fn metainfo(name: &str) -> Vec<u8> {
    let data = [7u8; 3000];
    let pieces: Vec<u8> = data
        .chunks(1024)
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect();
    let mut out = b"d4:infod6:lengthi3000e".to_vec();
    bstr(&mut out, b"name");
    bstr(&mut out, name.as_bytes());
    out.extend_from_slice(b"12:piece lengthi1024e");
    bstr(&mut out, b"pieces");
    bstr(&mut out, &pieces);
    out.extend_from_slice(b"ee");
    out
}

fn magnet() -> String {
    format!("magnet:?xt=urn:btih:{HASH}&amp;tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce")
}

// The enclosure's bytes say it is a torrent; its name plays no part.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enclosed_torrent_file_is_added_to_the_torrent_client() {
    let rig = rig(
        &item(
            "a",
            r#"<enclosure url="http://localhost/set.torrent" length="3000" type="application/x-bittorrent"/>"#,
        ),
        None,
        true,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 1, "{report:?}");
    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        rig.seen(),
        vec![("a".into(), "submitted".into(), Some(jobs[0]), None)]
    );
    let stored = rig.db.list_torrents().unwrap();
    assert_eq!(stored[0].name, "set.bin");
    assert_eq!(stored[0].category_id, Some(1));
    assert!(rig.nzb_jobs().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_link_is_added_without_a_fetch() {
    let rig = rig(
        &item("a", &format!("<link>{}</link>", magnet())),
        None,
        true,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 1, "{report:?}");
    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        rig.seen(),
        vec![("a".into(), "submitted".into(), Some(jobs[0]), None)]
    );
    // The job carries the feed's attributes as an NZB job does.
    let stored = rig.db.list_torrents().unwrap();
    assert_eq!(
        stored[0].metadata,
        [
            ("rss.feed_id", "1"),
            ("rss.feed_name", "Torrent Feed"),
            ("rss.item_id", "a"),
            ("rss.item_title", "Silver Horizon a"),
            ("rss.item_url", magnet().replace("&amp;", "&").as_str()),
            ("tier", "gold"),
        ]
        .map(|(key, value)| (key.to_string(), value.to_string()))
    );
}

// An indexer's attributes give the magnet and the size the rule reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torznab_magnet_with_its_size_matches_the_rule_and_is_added() {
    let rig = rig(
        &format!(
            "{}{}",
            item(
                "a",
                &format!(
                    r#"<link>https://example.com/details/a</link>
                    <torznab:attr name="size" value="5000"/>
                    <torznab:attr name="magneturl" value="{}"/>"#,
                    magnet()
                ),
            ),
            item(
                "b",
                &format!(
                    r#"<torznab:attr name="size" value="50000"/>
                    <torznab:attr name="magneturl" value="{}"/>"#,
                    magnet()
                ),
            )
        ),
        Some((1, 10_000)),
        true,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 1, "{report:?}");
    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        rig.seen(),
        vec![
            ("a".into(), "submitted".into(), Some(jobs[0]), None),
            ("b".into(), "ignored".into(), None, None),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_nzb_beside_a_torrent_client_is_submitted_as_before() {
    let rig = rig(
        &item(
            "a",
            r#"<enclosure url="http://localhost/download.nzb" length="321" type="application/x-nzb"/>"#,
        ),
        None,
        true,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 1, "{report:?}");
    assert!(rig.torrent_jobs().is_empty());
    let submissions = rig.nzb_jobs();
    assert_eq!(submissions.len(), 1);
    assert_eq!(submissions[0].category.as_deref(), Some("tv"));
    let seen = rig.seen();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].1, "submitted");
    assert!(seen[0].2.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_while_torrents_are_off_is_an_error_row() {
    let rig = rig(
        &item(
            "a",
            r#"<enclosure url="http://localhost/set.torrent" length="3000"/>"#,
        ),
        None,
        false,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 0, "{report:?}");
    assert!(rig.torrent_jobs().is_empty());
    assert_eq!(
        rig.seen(),
        vec![(
            "a".into(),
            "error".into(),
            None,
            Some(crate::torrents::status::OFF.into())
        )]
    );
}

// A torrent the client already holds, from this feed or any other origin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_the_client_already_holds_is_ignored_as_already_added() {
    let enclosure = r#"<enclosure url="http://localhost/set.torrent" length="3000"/>"#;
    let rig = rig(
        &format!("{}{}", item("a", enclosure), item("b", enclosure)),
        None,
        true,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 1, "{report:?}");
    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        rig.seen(),
        vec![
            ("a".into(), "submitted".into(), Some(jobs[0]), None),
            (
                "b".into(),
                "ignored".into(),
                None,
                Some("already added".into())
            ),
        ]
    );
}

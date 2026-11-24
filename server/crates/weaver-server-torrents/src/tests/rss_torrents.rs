// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline rss/service/tests/torrents.rs: an RSS
// feed's torrents go to the torrents crate's service.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use axum::Router;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use tempfile::TempDir;

use crate::Database;
use crate::rss::RssPersistenceInternalQueries;
use crate::rss::RssPersistenceQueries;
use crate::rss::RssQueries;
use crate::rss::service::test_fixture::{build_service, sample_nzb_bytes};
use crate::rss::service::*;
use crate::rss::*;
use crate::settings::TorrentConfig;
use crate::torrents::queries::TorrentsQueries;
use crate::torrents::service::{JobEnv, TorrentService, TorrentSettings};
use crate::torrents::sessions::tests::{FakeRoutes, env};
use crate::torrents::sessions::{Routes, Sessions};
use sha1::{Digest, Sha1};
use weaver_server_base::clock::SystemClock;
use weaver_server_settings::categories::CategoryConfig;

const HASH: &str = "0123456789abcdef0123456789abcdef01234567";

struct Rig {
    service: RssService,
    db: Database,
    torrents: TorrentService,
    // How many times the feed's body was served whole, not as unchanged.
    feed_reads: Arc<AtomicUsize>,
    // How many times the torrent file was fetched.
    torrent_fetches: Arc<AtomicUsize>,
    // Filled, the next whole read of the feed turns torrents off before it
    // answers: a switch that falls inside a sync.
    off_at_feed_read: OffAtRead,
    complete_dir: std::path::PathBuf,
    server: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

type OffAtRead = Arc<StdMutex<Option<(TorrentService, std::path::PathBuf)>>>;

impl Drop for Rig {
    fn drop(&mut self) {
        self.server.abort();
    }
}

// A feed of `items` served beside the torrent and NZB they link to, with one
// rule that takes every Silver Horizon item of `sizes`, and a torrent client
// that is on or off.
async fn rig(items: &str, sizes: Option<(u64, u64)>, torrents_on: bool) -> Rig {
    rig_seated(items, sizes, torrents_on, true).await
}

// `rig`, with the torrent client seated as the queue's torrent class or not.
async fn rig_seated(
    items: &str,
    sizes: Option<(u64, u64)>,
    torrents_on: bool,
    seated: bool,
) -> Rig {
    let dir = TempDir::new().unwrap();
    let mut db = Database::open(&dir.path().join("rss.sqlite")).unwrap();
    db.set_encryption_key(crate::persistence::encryption::EncryptionKey::generate());
    let feed = format!(
        r#"<?xml version="1.0"?>
        <rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
          <channel><title>Example</title>{items}</channel>
        </rss>"#
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let feed = feed.replace("http://localhost", &base_url);
    // The feed carries an ETag and answers a request that sends it back as
    // unchanged, as an indexer does.
    let feed_reads = Arc::new(AtomicUsize::new(0));
    let reads = feed_reads.clone();
    let torrent_fetches = Arc::new(AtomicUsize::new(0));
    let fetches = torrent_fetches.clone();
    let off_at_feed_read: OffAtRead = Arc::default();
    let off_at = off_at_feed_read.clone();
    let app = Router::new()
        .route(
            "/feed",
            get(move |headers: HeaderMap| async move {
                if headers
                    .get(header::IF_NONE_MATCH)
                    .is_some_and(|tag| tag == "\"v1\"")
                {
                    return StatusCode::NOT_MODIFIED.into_response();
                }
                reads.fetch_add(1, Ordering::SeqCst);
                let off = off_at.lock().unwrap().take();
                if let Some((torrents, complete_dir)) = off {
                    torrents
                        .set_settings(torrent_settings(&complete_dir, false))
                        .await;
                }
                ([(header::ETAG, "\"v1\"")], feed).into_response()
            }),
        )
        .route(
            "/set.torrent",
            get(move || async move {
                fetches.fetch_add(1, Ordering::SeqCst);
                metainfo("set.bin")
            }),
        )
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
    let complete_dir = dir.path().join("complete");
    let torrents = TorrentService::new(
        db.clone(),
        sessions,
        torrent_settings(&complete_dir, torrents_on),
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.open().await.unwrap();

    let service = build_service(dir.path(), db.clone());
    if seated {
        torrents.publish_to(service.inner.handle.clone());
    }
    Rig {
        service,
        db,
        torrents,
        feed_reads,
        torrent_fetches,
        off_at_feed_read,
        complete_dir,
        server,
        _dir: dir,
    }
}

fn torrent_settings(complete_dir: &std::path::Path, on: bool) -> TorrentSettings {
    TorrentSettings {
        upload_cap: None,
        complete_dir: complete_dir.to_path_buf(),
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            seeding: None,
            torrent_route: true,
            unpack: None,
            id: 1,
            name: "tv".to_string(),
            dest_dir: None,
            aliases: String::new(),
        }]
        .into(),
        seeding: None,
        torrent: TorrentConfig {
            enabled: on,
            ..Default::default()
        },
    }
}

impl Rig {
    async fn turn_torrents(&self, on: bool) {
        self.torrents
            .set_settings(torrent_settings(&self.complete_dir, on))
            .await;
    }

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

    // The feed's seen rows in their ids' order, as (item, decision, job,
    // error). A row is listed under its id's digest: each is named
    // back as the id these tests' feeds give.
    fn seen(&self) -> Vec<(String, String, Option<u64>, Option<String>)> {
        let mut rows = self.db.list_rss_seen_items(Some(1), None).unwrap();
        for row in &mut rows {
            if let Some(id) = ["a", "b", "c", "d", "old", "x"]
                .into_iter()
                .find(|id| self.db.rss_seen_item_key(id) == row.item_id)
            {
                row.item_id = id.to_owned();
            }
        }
        rows.sort_by(|a, b| a.item_id.cmp(&b.item_id));
        rows.into_iter()
            .map(|row| {
                (
                    row.item_id,
                    row.decision.as_str().to_owned(),
                    row.job_id,
                    row.error,
                )
            })
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
    // The job carries the feed's attributes as an NZB job does, but for
    // the item's link and its id, which is often the link: a private
    // tracker's carries the passkey, and the attributes are kept in the
    // clear.
    let stored = rig.db.list_torrents().unwrap();
    assert_eq!(
        stored[0].metadata,
        [
            ("rss.feed_id", "1"),
            ("rss.feed_name", "Torrent Feed"),
            ("rss.item_title", "Silver Horizon a"),
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

// A usenet job keeps the item's link and id sealed, in the queue and as
// stored, beside the feed's other attributes kept as they were. An admin
// reads them back opened; any other caller reads neither.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_usenet_jobs_rss_link_and_id_are_stored_sealed_and_shown_to_an_admin_only() {
    use crate::persistence::encryption::is_encrypted;
    use weaver_server_auth::auth::CallerScope;

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
    let job = rig.nzb_jobs().remove(0);
    let datastore = rig.db.datastore();
    let job_id = job.job_id.0 as i64;
    let stored = rig
        .db
        .run_sql_blocking_read(async move {
            crate::persistence::sql_runtime::SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT metadata FROM active_jobs WHERE job_id = {}",
                &[crate::persistence::sql_runtime::SqlArg::I64(job_id)],
            )
            .await?[0]
                .opt_text("metadata")
        })
        .unwrap();
    let stored = crate::history::parse_history_metadata(stored.as_deref());
    for metadata in [&job.metadata, &stored] {
        let keys: Vec<&str> = metadata
            .iter()
            .map(|(key, _)| key.as_str())
            .filter(|key| !key.starts_with("weaver.") && !key.starts_with("__weaver_"))
            .collect();
        assert_eq!(
            keys,
            [
                "rss.feed_id",
                "rss.feed_name",
                "rss.item_id",
                "rss.item_title",
                "rss.item_url",
                "tier"
            ]
        );
        for (key, value) in metadata {
            assert_eq!(
                is_encrypted(value),
                key == "rss.item_id" || key == "rss.item_url",
                "{key}"
            );
        }
        let link = crate::history::attributes::rss_link_pairs(metadata);
        let opened = rig.db.rss_link_attributes(&link, CallerScope::Admin);
        assert_eq!(opened[0], ("rss.item_id".to_owned(), "a".to_owned()));
        assert_eq!(opened[1].0, "rss.item_url");
        assert!(opened[1].1.ends_with("/download.nzb"), "{opened:?}");
        for scope in [CallerScope::Read, CallerScope::Control] {
            assert!(rig.db.rss_link_attributes(&link, scope).is_empty());
        }
    }
}

impl Rig {
    // Every idempotency row as stored: (scope, key, job), in key order.
    fn idempotency(&self) -> Vec<(String, String, i64)> {
        use crate::persistence::sql_runtime::SqlRuntime;
        let datastore = self.db.datastore();
        self.db
            .run_sql_blocking_read(async move {
                SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT caller_scope, idempotency_key, job_id FROM submission_idempotency \
                      ORDER BY caller_scope, idempotency_key",
                    &[],
                )
                .await?
                .iter()
                .map(|row| {
                    Ok((
                        row.text("caller_scope")?,
                        row.text("idempotency_key")?,
                        row.i64("job_id")?,
                    ))
                })
                .collect()
            })
            .unwrap()
    }

    // `sql` run as one write.
    fn write(&self, sql: &str) {
        use crate::persistence::sql_runtime::SqlRuntime;
        let (datastore, sql) = (self.db.datastore(), sql.to_owned());
        self.db
            .run_sql_blocking(async move {
                SqlRuntime::execute(datastore.write_exec(), &sql, &[]).await?;
                Ok(())
            })
            .unwrap();
    }

    // The feed polled again with `item` forgotten and read whole: the item
    // is submitted again as the feed gives it.
    async fn submit_again(&self, item: &str) {
        assert!(self.db.delete_rss_seen_item(1, item).unwrap());
        let mut feed = self.db.get_rss_feed(1).unwrap().unwrap();
        feed.etag = None;
        feed.last_modified = None;
        self.db.update_rss_feed(&feed).unwrap();
        self.service.run_all_sync().await.unwrap();
    }
}

// An RSS submission is kept idempotent under the item's seen key, never its
// id, which is often the link and its passkey: the same item submitted again
// is the job it made the first time, and no second is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_rss_submission_is_kept_under_the_items_seen_key_and_not_made_twice() {
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
    let job = rig.nzb_jobs().remove(0).job_id.0 as i64;
    let kept = vec![("rss:feed:1".to_owned(), rig.db.rss_seen_item_key("a"), job)];
    assert_eq!(rig.idempotency(), kept);

    rig.submit_again("a").await;
    assert_eq!(rig.nzb_jobs().len(), 1);
    assert_eq!(rig.seen()[0].2, Some(job as u64));
    assert_eq!(rig.idempotency(), kept);
}

// RSS idempotency rows kept under the item's id before are put back under
// its seen key by the one pass, across more than one page and a job's rows
// split between two pages. One whose seen key is there already leaves the
// row there as it is; another caller's rows, and a row keyed already, stay
// as they were. A legacy item submitted again is still the job it made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rss_idempotency_rows_kept_under_an_id_are_keyed_by_the_pass() {
    let rig = rig(
        &item(
            "a",
            r#"<enclosure url="http://localhost/download.nzb" length="321" type="application/x-nzb"/>"#,
        ),
        None,
        true,
    )
    .await;
    rig.service.run_all_sync().await.unwrap();
    let job = rig.nzb_jobs().remove(0).job_id.0 as i64;
    // The item's row as a build before keyed it.
    rig.write(
        "UPDATE submission_idempotency SET idempotency_key = 'a' WHERE caller_scope = 'rss:feed:1'",
    );

    const ROWS: i64 = 70;
    let id = |n: i64| format!("https://indexer.invalid/get/{n}?apikey=SECRET");
    // Two rows a job, the first page ending inside one.
    let job_of = |n: i64| 5000 + (n + 1) / 2;
    let snapshots = (job_of(0)..=job_of(ROWS + 2))
        .map(|job| format!("({job}, 'completed', 'n', 'accept', 'rss', 1, 1)"))
        .collect::<Vec<_>>()
        .join(", ");
    rig.write(&format!(
        "INSERT INTO duplicate_job_snapshots
            (job_id, lifecycle, normalized_name, admission_action, origin, created_at, updated_at)
         VALUES {snapshots}"
    ));
    let mut rows: Vec<String> = (0..ROWS)
        .map(|n| format!("('rss:feed:2', '{}', x'00', {}, 1)", id(n), job_of(n)))
        .collect();
    let dup = rig.db.rss_seen_item_key("dup");
    let keyed = rig.db.rss_seen_item_key("keyed");
    rows.extend([
        format!("('rss:feed:2', 'dup', x'00', {}, 1)", job_of(0)),
        format!("('rss:feed:2', '{dup}', x'00', {}, 1)", job_of(1)),
        format!("('rss:feed:2', '{keyed}', x'00', {}, 1)", job_of(2)),
        format!(
            "('api:key:1', 'client-key', x'00', {}, 1)",
            job_of(ROWS + 2)
        ),
    ]);
    rig.write(&format!(
        "INSERT INTO submission_idempotency
            (caller_scope, idempotency_key, request_hash, job_id, created_at)
         VALUES {}",
        rows.join(", ")
    ));

    rig.db.seal_stored_secrets().unwrap();

    let mut expected: Vec<(String, String, i64)> = (0..ROWS)
        .map(|n| {
            (
                "rss:feed:2".to_owned(),
                rig.db.rss_seen_item_key(&id(n)),
                job_of(n),
            )
        })
        .collect();
    expected.extend([
        (
            "api:key:1".to_owned(),
            "client-key".to_owned(),
            job_of(ROWS + 2),
        ),
        ("rss:feed:1".to_owned(), rig.db.rss_seen_item_key("a"), job),
        ("rss:feed:2".to_owned(), dup, job_of(1)),
        ("rss:feed:2".to_owned(), keyed, job_of(2)),
    ]);
    expected.sort();
    let stored = rig.idempotency();
    assert_eq!(stored, expected);
    assert!(stored.iter().all(|(_, key, _)| !key.contains("apikey")));

    // Nothing is left: the step reads one empty page and writes nothing.
    rig.db.key_rss_idempotency().unwrap();
    assert_eq!(rig.idempotency(), expected);

    rig.submit_again("a").await;
    assert_eq!(rig.nzb_jobs().len(), 1);
    assert_eq!(rig.seen()[0].2, Some(job as u64));
}

// A torrent while torrents are off is held, once: later polls neither read
// the unchanged feed nor write its row again. Once torrents are on, the
// feed is read whole and the item is added.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_while_torrents_are_off_waits_and_is_added_once_they_are_on() {
    let rig = rig(
        &format!(
            "{}{}",
            item(
                "a",
                r#"<enclosure url="http://localhost/set.torrent" length="3000"/>"#,
            ),
            item("b", &format!("<link>{}</link>", magnet())),
        ),
        None,
        false,
    )
    .await;

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(report.items_submitted, 0, "{report:?}");
    assert!(rig.torrent_jobs().is_empty());
    let held = |id: &str| {
        (
            id.to_string(),
            "waiting".to_string(),
            None,
            Some("torrents are off".to_string()),
        )
    };
    assert_eq!(rig.seen(), vec![held("a"), held("b")]);
    let rows = rig.db.list_rss_seen_items(Some(1), None).unwrap();
    assert_eq!(rig.feed_reads.load(Ordering::SeqCst), 1);

    // Still off: the feed is unchanged, nothing is read or written for it.
    let report = rig.service.run_all_sync().await.unwrap();
    assert_eq!(report.items_submitted, 0, "{report:?}");
    assert_eq!(rig.feed_reads.load(Ordering::SeqCst), 1);
    assert_eq!(rig.db.list_rss_seen_items(Some(1), None).unwrap(), rows);
    assert!(rig.torrent_jobs().is_empty());

    rig.turn_torrents(true).await;
    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(rig.feed_reads.load(Ordering::SeqCst), 2);
    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 2, "{report:?}");
    let seen = rig.seen();
    assert_eq!(
        seen.iter().map(|row| row.1.as_str()).collect::<Vec<_>>(),
        ["submitted", "submitted"]
    );

    // Taken up once: the next poll with torrents still on reads nothing.
    rig.service.run_all_sync().await.unwrap();
    assert_eq!(rig.feed_reads.load(Ordering::SeqCst), 2);
    assert_eq!(rig.seen(), seen);
}

// Torrents go off inside a sync, after it began with them on, and come back
// on before the next: the item held in between is still taken up then.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_item_held_by_a_switch_inside_a_sync_is_taken_up_at_the_next() {
    let rig = rig(
        &item("a", &format!("<link>{}</link>", magnet())),
        None,
        true,
    )
    .await;
    *rig.off_at_feed_read.lock().unwrap() = Some((rig.torrents.clone(), rig.complete_dir.clone()));

    rig.service.run_all_sync().await.unwrap();

    assert!(rig.torrent_jobs().is_empty());
    assert_eq!(rig.seen()[0].1, "waiting");

    rig.turn_torrents(true).await;
    rig.service.run_all_sync().await.unwrap();

    let jobs = rig.torrent_jobs();
    assert_eq!(jobs.len(), 1);
    assert_eq!(
        rig.seen(),
        vec![("a".into(), "submitted".into(), Some(jobs[0]), None)]
    );
}

// An enclosure the feed declares a torrent is not fetched while torrents are
// off; it is fetched once, when they are on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_declared_torrent_enclosure_is_not_fetched_while_torrents_are_off() {
    let rig = rig(
        &item(
            "a",
            r#"<enclosure url="http://localhost/set.torrent" length="3000" type="application/x-bittorrent"/>"#,
        ),
        None,
        false,
    )
    .await;

    rig.service.run_all_sync().await.unwrap();

    assert_eq!(rig.torrent_fetches.load(Ordering::SeqCst), 0);
    assert_eq!(rig.seen()[0].1, "waiting");

    rig.turn_torrents(true).await;
    rig.service.run_all_sync().await.unwrap();

    assert_eq!(rig.torrent_fetches.load(Ordering::SeqCst), 1);
    assert_eq!(rig.torrent_jobs().len(), 1);
    assert_eq!(rig.seen()[0].1, "submitted");
}

// With no torrent class seated, an item the feed declares a torrent fails
// unfetched, a magnet and a torrent whose bytes say so fail, each with the
// registry's refusal, and an NZB beside them is submitted as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_no_torrent_class_seated_torrent_items_fail_with_its_refusal() {
    let rig = rig_seated(
        &format!(
            "{}{}{}{}",
            item(
                "a",
                r#"<enclosure url="http://localhost/set.torrent" length="3000" type="application/x-bittorrent"/>"#,
            ),
            item("b", &format!(r#"<enclosure url="{}" length="3000"/>"#, magnet())),
            item("c", r#"<enclosure url="http://localhost/set.torrent" length="3000"/>"#),
            item("d", r#"<enclosure url="http://localhost/download.nzb" length="3000"/>"#),
        ),
        None,
        true,
        false,
    )
    .await;

    rig.service.run_all_sync().await.unwrap();

    let refusal = crate::jobs::class::AddRefusal::not_running(crate::jobs::Protocol::Torrent).text;
    // Only the undeclared torrent is fetched, its bytes telling what it is.
    assert_eq!(rig.torrent_fetches.load(Ordering::SeqCst), 1);
    assert!(rig.torrent_jobs().is_empty());
    let seen = rig.seen();
    for row in &seen[..3] {
        assert_eq!(
            (row.1.as_str(), row.3.as_deref()),
            ("error", Some(refusal.as_str())),
            "{seen:?}"
        );
    }
    assert_eq!(seen[3].1, "submitted", "{seen:?}");
    assert_eq!(rig.nzb_jobs().len(), 1);
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
                Some(format!("already added as job {}", jobs[0]))
            ),
        ]
    );
}

// One read of which items are seen and one write of what the poll records
// give the rows and counts the per-item check and write gave: an item seen
// before is left as it was, an item the page lists twice is judged once,
// and each new item gets the row it got before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_poll_records_its_items_together_with_the_rows_one_by_one_gave() {
    let page = format!(
        "{}{}{}{}{}",
        item("old", &format!("<link>{}</link>", magnet())),
        item("a", &format!("<link>{}</link>", magnet())),
        item("a", &format!("<link>{}</link>", magnet())),
        "<item><guid>x</guid><title>Other x</title></item>",
        item("b", &format!("<link>{}</link>", magnet())),
    );
    let rig = rig(&page, None, true).await;
    let before = RssSeenItemRow {
        feed_id: 1,
        item_id: "old".to_string(),
        item_title: "Silver Horizon old".to_string(),
        published_at: None,
        size_bytes: None,
        decision: crate::RssSeenDecision::Submitted,
        seen_at: crate::rss::model::unix_now_secs() - 60,
        job_id: Some(77),
        item_url: None,
        error: None,
    };
    rig.db.insert_rss_seen_item(&before).unwrap();

    let report = rig.service.run_all_sync().await.unwrap();

    assert_eq!(
        (
            report.items_fetched,
            report.items_new,
            report.items_accepted,
            report.items_submitted,
            report.items_ignored,
        ),
        (5, 3, 2, 1, 2),
        "{report:?}"
    );
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
                Some(format!("already added as job {}", jobs[0]))
            ),
            ("old".into(), "submitted".into(), Some(77), None),
            ("x".into(), "ignored".into(), None, None),
        ]
    );
    let rows = rig.db.list_rss_seen_items(Some(1), None).unwrap();
    let row = |id: &str| {
        let listed = rig.db.rss_seen_item_key(id);
        rows.iter().find(|row| row.item_id == listed).unwrap()
    };
    assert_eq!(
        row("old"),
        &RssSeenItemRow {
            item_id: rig.db.rss_seen_item_key("old"),
            ..before
        }
    );
    let a = row("a");
    assert_eq!(a.item_title, "Silver Horizon a");
    assert_eq!(a.item_url, Some(magnet().replace("&amp;", "&")));
    let x = row("x");
    assert_eq!((x.item_url.as_deref(), x.size_bytes), (None, None));
}

// A due pass with no feed due writes no row, and a poll the feed answers
// "not modified" writes only the feed's poll time: no seen row is written
// again. Triggers on the two tables count every row written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_pass_and_an_unchanged_feed_write_no_seen_rows() {
    use sqlx::Connection;
    let rig = rig(
        &item("a", &format!("<link>{}</link>", magnet())),
        None,
        true,
    )
    .await;
    rig.service.run_all_sync().await.unwrap();
    let seen = rig.db.list_rss_seen_items(Some(1), None).unwrap();

    let options =
        sqlx::sqlite::SqliteConnectOptions::new().filename(rig._dir.path().join("rss.sqlite"));
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE test_rss_writes (target TEXT NOT NULL);
         CREATE TRIGGER test_seen_insert AFTER INSERT ON rss_seen_items
           BEGIN INSERT INTO test_rss_writes VALUES ('seen'); END;
         CREATE TRIGGER test_seen_update AFTER UPDATE ON rss_seen_items
           BEGIN INSERT INTO test_rss_writes VALUES ('seen'); END;
         CREATE TRIGGER test_seen_delete AFTER DELETE ON rss_seen_items
           BEGIN INSERT INTO test_rss_writes VALUES ('seen'); END;
         CREATE TRIGGER test_feed_update AFTER UPDATE ON rss_feeds
           BEGIN INSERT INTO test_rss_writes VALUES ('feed'); END;",
    )
    .execute(&mut connection)
    .await
    .unwrap();
    async fn writes(connection: &mut sqlx::SqliteConnection) -> Vec<String> {
        sqlx::query_as::<_, (String,)>("SELECT target FROM test_rss_writes ORDER BY rowid")
            .fetch_all(connection)
            .await
            .unwrap()
            .into_iter()
            .map(|(target,)| target)
            .collect()
    }

    assert!(matches!(
        rig.service.try_run_due_sync().await.unwrap(),
        DueSyncOutcome::NoFeedsDue
    ));
    assert!(writes(&mut connection).await.is_empty());

    let report = rig.service.run_all_sync().await.unwrap();
    assert_eq!(report.items_fetched, 0, "{report:?}");
    assert_eq!(writes(&mut connection).await, ["feed"]);
    assert_eq!(rig.db.list_rss_seen_items(Some(1), None).unwrap(), seen);
    connection.close().await.unwrap();
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A list's rows read without their files answer as the rows read with them:
// `torrents/info` across filters, tags, sorts and pages, and `sync/maindata`
// across a run of ids with torrents added, changed and removed.

use std::collections::HashMap;

use weaver_server_core::torrents::service::fingerprint::ViewParts;
use weaver_server_core::torrents::service::listing::FileTotals;
use weaver_server_core::torrents::service::{TorrentPieces, ViewClock};
use weaver_server_core::torrents::{FilePriority, NamedBy, TorrentTracker, TrackerStatus};

use super::*;

const TORRENTS: usize = 10_000;

// The time the answers are read at, in seconds since the epoch.
const AT: i64 = 1_700_000_500;

const STATES: [TorrentState; 12] = [
    TorrentState::Queued,
    TorrentState::FetchingMetadata,
    TorrentState::CheckingResumeData,
    TorrentState::CheckingFiles,
    TorrentState::Downloading,
    TorrentState::Stalled,
    TorrentState::NeedsCertificate,
    TorrentState::Seeding,
    TorrentState::Paused,
    TorrentState::Moving,
    TorrentState::Stopped,
    TorrentState::FilesMissing,
];

// The bytes of the files a row fetches and of those it lacks, read from its
// files.
fn files_wanted_and_left(view: &TorrentView) -> (u64, u64) {
    if view.files.is_empty() {
        return (
            view.total_bytes,
            view.total_bytes.saturating_sub(view.have_bytes),
        );
    }
    view.files
        .iter()
        .filter(|file| file.selected())
        .fold((0, 0), |(wanted, left), file| {
            let have = view
                .file_have
                .get(file.index as usize)
                .copied()
                .unwrap_or(0);
            (
                wanted + file.length,
                left + file.length.saturating_sub(have),
            )
        })
}

// The folder a row names as its root, read from its files.
fn files_root_path(view: &TorrentView) -> String {
    let (_, content) = view.paths();
    let single = matches!(view.files.as_slice(), [file] if file.path == view.name);
    if single { String::new() } else { content }
}

fn tracker(index: usize) -> TorrentTracker {
    let status = [
        TrackerStatus::Working,
        TrackerStatus::NotWorking,
        TrackerStatus::TrackerError,
        TrackerStatus::Unreachable,
        TrackerStatus::NotContacted,
    ][index % 5];
    TorrentTracker {
        tier: 0,
        url: format!(
            "https://tracker{}.invalid/announce/passkey{index}",
            index % 3
        ),
        named_by: NamedBy::Torrent,
        status,
        updating: false,
        last_announce_ms: Some(1_700_000_000_000 + index as i64),
        next_announce_ms: None,
        min_announce_ms: None,
        message: index.is_multiple_of(2).then(|| "slow down".to_owned()),
        seeders: Some(index as u32 % 40),
        leechers: Some(index as u32 % 7),
        downloaded: None,
        peers: None,
        fails: 0,
        resolved_by: None,
    }
}

// Torrent `index`'s view with its files: none for a magnet, one named as the
// torrent, one in a folder, and 5, 40 and 150 files at mixed levels, each
// in hand whole, in half, not at all, or past its length, and some missing
// their figures.
fn seeded_view(index: usize) -> TorrentView {
    let mut view = view(STATES[index % STATES.len()]);
    view.job_id = JobId(index as u64 + 1);
    view.name = format!("t{index}");
    view.infohash = format!("{:040X}", index + 1);
    view.magnet = format!("magnet:?xt=urn:btih:{:040x}", index + 1);
    view.category = index.is_multiple_of(4).then(|| "tv".to_owned());
    view.tags = match index % 7 {
        0 => vec!["hd".to_owned()],
        3 => vec!["a".to_owned(), "hd".to_owned()],
        _ => Vec::new(),
    };
    view.download_bps = (index % 9) as u64 * 1000;
    view.upload_bps = u64::from(index.is_multiple_of(11)) * 500;
    view.completed_at_ms = index.is_multiple_of(3).then_some(1_700_000_100_000);
    view.forced = index.is_multiple_of(13);
    view.added_at_ms = 1_700_000_000_000 + (index % 997) as i64 * 1000;
    view.ratio = (index % 10) as f64 / 4.0;
    view.peers = (index % 23) as u32;
    view.downloaded_bytes = index as u64 * 1013;
    view.uploaded_bytes = index as u64 * 211;
    view.pieces = (!index.is_multiple_of(6)).then_some(TorrentPieces {
        count: 100,
        size: 16_384,
        have: (index % 100) as u32,
    });
    view.storage_dir = (index % 4 != 1).then(|| format!("/complete/tv/t{index}"));
    view.files_dir = (index % 5 == 2).then(|| format!("/complete/x/t{index}"));
    view.download_limit = match index % 17 {
        0 => Some(0),
        1 => Some(4096),
        _ => None,
    };
    view.upload_limit = index.is_multiple_of(19).then_some(0);
    view.trackers = (0..index % 3).map(|tier| tracker(index + tier)).collect();
    let paths: Vec<String> = match index % 6 {
        0 => Vec::new(),
        1 => vec![view.name.clone()],
        2 => vec![format!("t{index}/a.mkv")],
        3 => (0..5).map(|file| format!("t{index}/{file}.rar")).collect(),
        4 => (0..40)
            .map(|file| format!("t{index}/CD{}/{file}.flac", file % 2))
            .collect(),
        _ => (0..150)
            .map(|file| format!("t{index}/s01/e{file:03}.mkv"))
            .collect(),
    };
    let many = paths.len() > 2;
    view.files = paths
        .into_iter()
        .enumerate()
        .map(|(file, path)| TorrentFile {
            index: file as u32,
            path,
            length: ((index * 7919 + file * 104_729) % 4_000_000_000) as u64 + 1,
            priority: match (many, file % 4, index % 4) {
                (true, 1, _) => FilePriority::High,
                (true, 2, _) => FilePriority::Ignored,
                (true, 3, _) => FilePriority::Maximum,
                (false, _, 0) => FilePriority::Ignored,
                _ => FilePriority::Normal,
            },
        })
        .collect();
    view.file_have = view
        .files
        .iter()
        .map(|file| match (index + file.index as usize) % 4 {
            0 => 0,
            1 => file.length,
            2 => file.length / 2,
            _ => file.length + 10,
        })
        .collect();
    if index % 10 == 9 {
        view.file_have.truncate(view.files.len() / 2);
    }
    view.total_bytes = view.files.iter().map(|file| file.length).sum();
    view.have_bytes = view.file_have.iter().sum();
    view
}

fn without_files(view: &TorrentView) -> TorrentView {
    TorrentView {
        files: Vec::new(),
        ..view.clone()
    }
}

// The rows as the records read them without their files.
fn rows_of(views: &[TorrentView]) -> Vec<(TorrentView, FileTotals)> {
    views
        .iter()
        .map(|view| (without_files(view), FileTotals::of(view)))
        .collect()
}

fn params(pairs: &[(&str, &str)]) -> Params {
    Params {
        fields: pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<HashMap<_, _>>(),
        files: Vec::new(),
    }
}

async fn body(response: Response) -> (StatusCode, Vec<u8>) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

// What the records keep when they take the page: `torrents/info` asks them
// for it only when nothing else filters or sorts.
fn paged<T: Clone>(rows: &[T], ask: &InfoAsk<'_>) -> Vec<T> {
    let range = if ask.paged_first() {
        info_page(ask.limit, ask.offset, rows.len()).unwrap_or(0..rows.len())
    } else {
        0..rows.len()
    };
    rows[range].to_vec()
}

// Each row reads the same from its files' totals as from its files.
#[test]
fn a_row_reads_from_its_totals_what_its_files_give() {
    for index in 0..TORRENTS {
        let view = seeded_view(index);
        let (bare, totals) = (without_files(&view), FileTotals::of(&view));
        assert_eq!(
            (
                files_wanted_and_left(&view),
                files_root_path(&view),
                view.paths()
            ),
            (
                wanted_and_left(&bare, totals),
                root_path(&bare, totals),
                bare.paths_as(totals.one_file)
            ),
            "{index}"
        );
    }
}

// `torrents/info` over 10,000 torrents answers the same, byte for byte,
// whether its rows carry their files or only their totals.
#[tokio::test]
async fn torrents_info_answers_the_same_without_the_files() {
    let views: Vec<TorrentView> = (0..TORRENTS).map(seeded_view).collect();
    let whole: Vec<(TorrentView, FileTotals)> = views
        .iter()
        .map(|view| (view.clone(), FileTotals::of(view)))
        .collect();
    let rows = rows_of(&views);
    let files_carried: usize = views.iter().map(|view| view.files.len()).sum();
    assert!(files_carried > 300_000, "{files_carried}");
    assert!(rows.iter().all(|(view, _)| view.files.is_empty()));
    let asks: &[&[(&str, &str)]] = &[
        &[],
        &[("limit", "50")],
        &[("limit", "50"), ("offset", "9990")],
        &[("offset", "-25")],
        &[("limit", "0"), ("offset", "3")],
        &[("filter", "downloading")],
        &[("filter", "seeding"), ("limit", "10"), ("offset", "5")],
        &[("filter", "completed")],
        &[("filter", "stalled")],
        &[("filter", "active")],
        &[("filter", "errored")],
        &[("filter", "no-such-filter"), ("limit", "7")],
        &[("tag", "hd")],
        &[("tag", "")],
        &[("sort", "name")],
        &[("sort", "size"), ("reverse", "true"), ("limit", "100")],
        &[("sort", "amount_left"), ("offset", "20"), ("limit", "30")],
        &[("sort", "root_path")],
        &[("sort", "content_path"), ("reverse", "true")],
        &[("sort", "no_such_column")],
        &[("filter", "inactive"), ("sort", "progress"), ("tag", "a")],
        &[("includeTrackers", "true"), ("limit", "200")],
    ];
    // A read-only caller differs in its trackers' addresses alone.
    let read_asks: &[&[(&str, &str)]] = &[&[], &[("includeTrackers", "true"), ("limit", "200")]];
    for (scope, asks) in [(CallerScope::Admin, asks), (CallerScope::Read, read_asks)] {
        for pairs in asks {
            let params = params(pairs);
            let ask = InfoAsk::of(&params);
            let old = body(info_list(paged(&whole, &ask), &ask, scope, Api::LATEST, AT)).await;
            let new = body(info_list(paged(&rows, &ask), &ask, scope, Api::LATEST, AT)).await;
            assert_eq!(old.0, new.0, "{pairs:?}");
            assert!(old == new, "{pairs:?} {scope:?}");
        }
    }
    // The files themselves are listed only when asked for, from the rows
    // that carry them.
    let params = params(&[("includeFiles", "true"), ("limit", "60")]);
    let ask = InfoAsk::of(&params);
    let listed = body(info_list(
        paged(&whole, &ask),
        &ask,
        CallerScope::Admin,
        Api::LATEST,
        AT,
    ))
    .await;
    let listed: Vec<Value> = serde_json::from_slice(&listed.1).unwrap();
    assert!(listed.iter().any(|row| {
        row["files"]
            .as_array()
            .is_some_and(|files| files.len() == 150)
    }));
}

// One `sync/maindata` client's answers as the facade gave them before the
// snapshot: every torrent's view, with its files, read on every poll.
struct OldClient {
    table: sync::old::SyncTable,
    rid: u64,
    built: usize,
}

impl OldClient {
    fn poll(&mut self, rid: u64, views: &[&TorrentView], scope: CallerScope, at: i64) -> String {
        let built = std::cell::Cell::new(0);
        let fields = sync::old::Fields {
            steady: |view: &TorrentView| {
                built.set(built.get() + 1);
                let mut row = steady_fields(view, scope);
                row.retain(|(name, _)| *name != "hash");
                row.extend(announce_stats(&TrackerSummary::of(view)));
                row
            },
            clock: |view: &TorrentView| clock_fields(view, at),
        };
        let now = sync::old::Poll {
            torrents: views
                .iter()
                .map(|&view| sync::old::Row {
                    hash: view.infohash.to_ascii_lowercase(),
                    fingerprint: {
                        let mut hasher = DefaultHasher::new();
                        (view.fingerprint(ViewParts::Steady), scope.can_control())
                            .hash(&mut hasher);
                        hasher.finish()
                    },
                    closed: u8::from(view.download_limit == Some(0))
                        | u8::from(view.upload_limit == Some(0)) << 1,
                    view,
                })
                .collect(),
            categories: Vec::new(),
            tags: Vec::new(),
            server_state: Map::new(),
            server_closed: 0,
        };
        let start = self.table.begin(&SyncClient::Anonymous, rid);
        let (body, kept) = sync::old::answer(&start, now, &fields);
        self.table.finish(&SyncClient::Anonymous, start.id(), kept);
        self.rid = start.id();
        self.built += built.get();
        serde_json::to_string(&body).unwrap()
    }
}

// One `sync/maindata` client's answers as the facade gives them: from the
// snapshot's rows, read only where they moved.
#[derive(Default)]
struct NewClient {
    sync: ClientSync,
    rid: u64,
    built: usize,
    read: usize,
}

impl NewClient {
    fn poll(&mut self, rid: u64, snapshot: &Snapshot, scope: CallerScope, at: i64) -> String {
        let built = std::cell::Cell::new(0);
        let read = std::cell::Cell::new(0);
        let fields = Fields {
            steady: |row: &Arc<TorrentRow>| {
                built.set(built.get() + 1);
                maindata_steady(row, scope)
            },
            clock: |row: &Arc<TorrentRow>| {
                read.set(read.get() + 1);
                clock_fields(&row.view, at)
            },
        };
        let others = Others {
            categories: Vec::new(),
            tags: Vec::new(),
            server_state: Map::new(),
            server_closed: 0,
        };
        let start = self.sync.begin(rid, u8::from(scope.can_control()));
        let body = maindata_answer(
            &mut self.sync,
            &start,
            Some(snapshot),
            scope,
            others,
            &fields,
        );
        self.rid = start.id();
        self.built += built.get();
        self.read += read.get();
        serde_json::to_string(&body).unwrap()
    }
}

// `sync/maindata` over 10,000 torrents answers as it did before the
// snapshot, byte for byte, across a run of ids: unchanged polls, changed
// levels, bytes in hand and states, torrents added and removed, a file
// renamed, and an id the client never held. The old answer read every view
// with its files on every poll; the new one reads the snapshot's rows, which
// carry no files or trackers, and only the rows that moved.
#[test]
fn maindata_answers_from_the_snapshot_as_from_every_view() {
    for scope in [CallerScope::Admin, CallerScope::Read] {
        let mut views: Vec<TorrentView> = (0..TORRENTS).map(seeded_view).collect();
        let snapshot = Snapshot::default();
        let mut old = OldClient {
            table: sync::old::SyncTable::default(),
            rid: 0,
            built: 0,
        };
        let mut new = NewClient::default();
        let mut steps = 0;
        let mut poll = |views: &[TorrentView], rid: Option<u64>| {
            let before = (old.built, new.read);
            // The order the service lists them in.
            let mut listed: Vec<&TorrentView> = views.iter().collect();
            listed.sort_by_key(|view| (view.added_at_ms, view.job_id));
            let answer_old = old.poll(rid.unwrap_or(old.rid), &listed, scope, AT);
            snapshot.replace_every(&rows_of(views));
            let answer_new = new.poll(rid.unwrap_or(new.rid), &snapshot, scope, AT);
            assert_eq!(answer_old, answer_new, "step {steps}");
            steps += 1;
            (
                answer_new,
                old.built - before.0,
                new.rid,
                new.read - before.1,
            )
        };
        let (first, built, _, read) = poll(&views, Some(0));
        assert!(first.contains("\"full_update\":true"));
        assert_eq!((built, read), (TORRENTS, TORRENTS));
        // Nothing moved: the old answer read every row, the new one none.
        let (same, built, rid, read) = poll(&views, None);
        assert_eq!(same, format!("{{\"rid\":{rid}}}"));
        assert_eq!((built, read), (0, 0));
        // Levels, bytes in hand and states move.
        for index in (0..TORRENTS).step_by(97) {
            let view = &mut views[index];
            if let Some(file) = view.files.first_mut() {
                file.priority = match file.priority {
                    FilePriority::Ignored => FilePriority::Normal,
                    _ => FilePriority::Ignored,
                };
            }
            if let Some(have) = view.file_have.last_mut() {
                *have /= 3;
            }
            view.state = STATES[(index / 97) % STATES.len()];
        }
        let (.., read) = poll(&views, None);
        // The rows whose state moved, and none other.
        assert!(read > 0 && read <= TORRENTS.div_ceil(97), "{read}");
        // A level that leaves the totals as they were.
        for index in (5..TORRENTS).step_by(6).take(40) {
            views[index].files[1].priority = FilePriority::Normal;
        }
        poll(&views, None);
        // Torrents removed and added.
        views.drain(100..200);
        views.extend((TORRENTS..TORRENTS + 100).map(seeded_view));
        poll(&views, None);
        // A file of many renamed, and the one file of a torrent renamed off
        // the torrent's name.
        for index in (3..views.len()).step_by(500) {
            views[index].files[0].path.push_str(".renamed");
        }
        for index in (1..views.len()).step_by(600) {
            if views[index].is_one_file() {
                views[index].files[0].path = "other.mkv".to_owned();
            }
        }
        poll(&views, None);
        poll(&views, None);
        // An id the client never held: whole again.
        let (whole, ..) = poll(&views, Some(424_242));
        assert!(whole.contains("\"full_update\":true"));
        poll(&views, None);
        // The new path built no more rows than the old one.
        assert!(new.built <= old.built, "{} > {}", new.built, old.built);
        assert!(new.read < old.built, "{} >= {}", new.read, old.built);
    }
}

// What the table kept for a `torrents/info` request before the snapshot:
// the torrents of the hashes it names, each read as 40 hex digits in any
// case; of the category it names or one under it, in any case, a torrent in
// none being in the category ""; and of the privacy it asks.
fn table_keeps(params: &Params, view: &TorrentView) -> bool {
    let sent = info_hashes(params);
    let named: Vec<String> = sent
        .iter()
        .filter(|hash| hash.len() == 40 && hash.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|hash| hash.to_ascii_lowercase())
        .collect();
    let category = view
        .category
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    (sent.is_empty() || named.contains(&view.infohash))
        && params.get("category").is_none_or(|name| {
            let name = name.to_ascii_lowercase();
            category == name || category.starts_with(&format!("{name}/"))
        })
        && params
            .flag_value("private")
            .is_none_or(|private| view.private == private)
}

// The seeded torrents as a run of the service holds them: lowercase hashes,
// some private, some in a category under another, each read at `AT`.
fn info_views() -> Vec<TorrentView> {
    (0..TORRENTS)
        .map(|index| {
            let mut view = seeded_view(index);
            view.infohash = view.infohash.to_ascii_lowercase();
            view.private = index % 5 == 1;
            if index % 8 == 2 {
                view.category = Some("TV/HD".to_owned());
            }
            view.clock.at_ms = AT * 1000;
            view
        })
        .collect()
}

// `torrents/info` from the snapshot's rows answers as it did from the views
// built for each request, byte for byte, over 10,000 torrents of mixed
// states, categories, tags and privacy: across status filters, categories,
// tags, hashes, privacy, sorts, offsets, limits, and the files and trackers
// listed. The rows carry no files or trackers; those are read for the page
// alone.
#[tokio::test]
async fn torrents_info_answers_from_the_snapshot_as_from_built_views() {
    let views = info_views();
    // The order the service lists them in.
    let mut listed: Vec<&TorrentView> = views.iter().collect();
    listed.sort_by_key(|view| (view.added_at_ms, view.job_id));
    let by_id: HashMap<JobId, &TorrentView> =
        views.iter().map(|view| (view.job_id, view)).collect();
    let snapshot = Snapshot::default();
    snapshot.replace_every(&rows_of(&views));
    let (_, rows) = snapshot.every();
    assert_eq!(rows.len(), TORRENTS);
    assert!(rows.iter().all(|row| row.view.trackers.is_empty()));

    let some = format!(
        "{}|{}| nothex |{}|{}",
        views[5].infohash.to_ascii_uppercase(),
        views[77].infohash,
        views[9_000].infohash,
        views[77].infohash,
    );
    let private = format!("{}|{}", views[1].infohash, views[2].infohash);
    let asks: &[&[(&str, &str)]] = &[
        &[],
        &[("limit", "50"), ("offset", "9990")],
        &[("offset", "-25")],
        &[("offset", "-25"), ("limit", "10")],
        &[("offset", "-20000")],
        &[("limit", "0"), ("offset", "3")],
        &[("limit", "-4"), ("offset", "12")],
        &[("filter", "downloading")],
        &[("filter", "seeding"), ("limit", "10"), ("offset", "5")],
        &[("filter", "completed"), ("offset", "-3")],
        &[("filter", "stopped")],
        &[("filter", "running"), ("limit", "40")],
        &[("filter", "active")],
        &[("filter", "inactive"), ("offset", "100"), ("limit", "100")],
        &[("filter", "stalled")],
        &[("filter", "checking")],
        &[("filter", "moving")],
        &[("filter", "errored")],
        &[("filter", "no-such-filter"), ("limit", "7")],
        &[("category", "tv")],
        &[("category", "TV"), ("limit", "30"), ("offset", "30")],
        &[("category", "tv/hd")],
        &[("category", "")],
        &[("category", "movies")],
        &[("tag", "hd")],
        &[("tag", ""), ("limit", "25")],
        &[("tag", "a"), ("category", "tv")],
        &[("private", "true")],
        &[("private", "false"), ("limit", "60"), ("offset", "7")],
        &[
            ("private", "true"),
            ("category", "tv"),
            ("filter", "seeding"),
        ],
        &[("hashes", some.as_str())],
        &[
            ("hashes", some.as_str()),
            ("sort", "name"),
            ("reverse", "true"),
        ],
        &[("hashes", private.as_str()), ("private", "true")],
        &[("hashes", "nothex")],
        &[("hashes", "|")],
        &[("sort", "name")],
        &[("sort", "size"), ("reverse", "true"), ("limit", "100")],
        &[("sort", "amount_left"), ("offset", "20"), ("limit", "30")],
        &[("sort", "root_path")],
        &[("sort", "content_path"), ("reverse", "true")],
        &[("sort", "added_on"), ("offset", "-50")],
        &[("sort", "dl_limit")],
        &[("sort", "tracker"), ("limit", "300")],
        &[("sort", "seeding_time"), ("limit", "100")],
        &[
            ("sort", "time_active"),
            ("reverse", "true"),
            ("limit", "100"),
        ],
        &[("sort", "eta")],
        &[("sort", "no_such_column")],
        &[("sort", "files")],
        &[("sort", "trackers")],
        &[("filter", "inactive"), ("sort", "progress"), ("tag", "a")],
        &[("category", "tv"), ("sort", "ratio"), ("limit", "15")],
        &[("includeTrackers", "true"), ("limit", "200")],
        &[
            ("includeTrackers", "true"),
            ("sort", "trackers"),
            ("limit", "40"),
        ],
        &[("includeTrackers", "true"), ("hashes", some.as_str())],
        &[("includeFiles", "true"), ("limit", "60")],
        &[("includeFiles", "true"), ("sort", "files"), ("limit", "30")],
        &[
            ("includeFiles", "true"),
            ("sort", "files"),
            ("offset", "6"),
            ("limit", "30"),
        ],
        &[
            ("includeFiles", "true"),
            ("includeTrackers", "true"),
            ("filter", "downloading"),
            ("sort", "name"),
            ("offset", "-12"),
        ],
    ];
    let read_asks: &[&[(&str, &str)]] = &[
        &[("limit", "300")],
        &[("includeTrackers", "true"), ("limit", "200")],
        &[("sort", "tracker"), ("limit", "300")],
    ];
    let mut compared = 0;
    for (scope, asks) in [(CallerScope::Admin, asks), (CallerScope::Read, read_asks)] {
        for pairs in asks {
            let params = params(pairs);
            let ask = InfoAsk::of(&params);
            // Before: the table's pick, each view built for the request,
            // with its files only when they are listed.
            let kept: Vec<(TorrentView, FileTotals)> = listed
                .iter()
                .filter(|view| table_keeps(&params, view))
                .map(|&view| {
                    let built = if ask.with_files {
                        view.clone()
                    } else {
                        without_files(view)
                    };
                    (built, FileTotals::of(view))
                })
                .collect();
            let old = body(info_list(paged(&kept, &ask), &ask, scope, Api::LATEST, AT)).await;
            // Now: the rows, with the page's files and trackers read alone.
            let hashes = info_hashes(&params);
            let pick = info_pick(&params, &hashes);
            let new = match info_rows(&rows, &pick.of_rows(), &ask, scope, AT * 1000) {
                Err(refused) => *refused,
                Ok(mut page) => {
                    for (row, fields) in &mut page {
                        let view = by_id[&row.view.job_id];
                        let parts = RowParts {
                            files: view.files.clone(),
                            file_have: view.file_have.clone(),
                            trackers: view.trackers.clone(),
                        };
                        with_parts(fields, &row.view, parts, &ask, scope, Api::LATEST);
                    }
                    info_answer(page)
                }
            };
            let new = body(new).await;
            assert_eq!(old.0, new.0, "{pairs:?} {scope:?}");
            assert!(old.1 == new.1, "{pairs:?} {scope:?}");
            compared += 1;
        }
    }
    assert_eq!(compared, asks.len() + read_asks.len());
}

// A row's time active and seeding run on from the reading it came from to
// the time it is read, as a reading then gives them, and its countdown to
// the next announce runs down; a stopped torrent's times stand still.
#[test]
fn a_rows_clock_fields_run_on_to_the_time_it_is_read() {
    let read_at = |state: TorrentState, active: bool, now_ms: i64| {
        let mut view = view(state);
        view.infohash = format!("{:040x}", 1);
        view.active_seconds = 100;
        view.seeding_seconds = 40;
        view.consecutive_seeding_seconds = 10;
        view.reannounce_seconds = Some(30);
        view.clock = ViewClock {
            at_ms: 1_000_000,
            next_announce_ms: Some(1_030_000),
            active_ms: 400,
            finished_ms: 700,
            consecutive_ms: 0,
            active,
        };
        let snapshot = Snapshot::default();
        snapshot.replace_every(&[(view.clone(), FileTotals::of(&view))]);
        let (_, rows) = snapshot.every();
        let fields = info_fields(&rows[0], CallerScope::Admin, now_ms);
        [
            fields["time_active"].clone(),
            fields["seeding_time"].clone(),
            fields["reannounce"].clone(),
        ]
    };
    // Read as it was taken: as a view built then gives them.
    assert_eq!(
        read_at(TorrentState::Seeding, true, 1_000_000),
        [json!(100), json!(40), json!(30)]
    );
    // 2.5 seconds on: 99.4 s active is 101.9 s, counted up; 40.7 s seeding
    // is 43.2 s, counted down; 27.5 s to the announce, counted up.
    assert_eq!(
        read_at(TorrentState::Seeding, true, 1_002_500),
        [json!(102), json!(43), json!(28)]
    );
    // Stopped: only the countdown runs.
    assert_eq!(
        read_at(TorrentState::Stopped, false, 1_002_500),
        [json!(100), json!(40), json!(28)]
    );
    // Past the announce, and before the reading: nothing below zero, and
    // nothing run back.
    assert_eq!(read_at(TorrentState::Seeding, true, 1_040_000)[2], json!(0));
    assert_eq!(
        read_at(TorrentState::Seeding, true, 999_000),
        [json!(100), json!(40), json!(31)]
    );
}

// Stored torrents, `count` of them, written as the add writes them: magnets
// paused, resumed or stopped, some private.
fn stored_torrents(db: &Database, count: u64) {
    use weaver_server_core::torrents::model::{
        KeptIntent, TorrentIntent, TorrentRecord, TorrentSource,
    };
    for job_id in 1..=count {
        let mut hash = [0u8; 20];
        hash[..8].copy_from_slice(&job_id.to_be_bytes());
        hash[19] = 0x5a;
        let record = TorrentRecord {
            job_id: JobId(job_id),
            infohash_v1: Some(hash),
            infohash_v2: None,
            name: format!("t{job_id}"),
            source: TorrentSource::Magnet,
            metainfo: None,
            category_id: None,
            added_at_ms: 1_700_000_000_000 + (job_id % 997) as i64,
            // A third resumed, a third paused, a third stopped.
            kept: KeptIntent::Intent(match job_id % 3 {
                1 => TorrentIntent::Resumed,
                0 => TorrentIntent::Stopped,
                _ => TorrentIntent::Paused,
            }),
            forced: job_id.is_multiple_of(11),
            completed_at_ms: None,
            storage_dir: None,
            policy_override: None,
            unpack: None,
            unpack_in_force: None,
            metadata: Vec::new(),
            upload_limit: job_id.is_multiple_of(7).then_some(4096),
            download_limit: None,
            sequential: false,
            first_last_piece_first: false,
            save_path: None,
            content_layout: None,
            files_dir: None,
            seed_mode: false,
            move_to: None,
            queue_position: job_id as i64,
            pool_member: None,
            private: job_id % 5 == 1,
        };
        db.insert_torrent(&record, &[]).unwrap();
    }
}

// `count` stored torrents, loaded by a service with torrents off, every
// resumed magnet's fetch ended.
async fn started_with(count: u64) -> (tempfile::TempDir, Database, TorrentService) {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    stored_torrents(&db, count);
    let torrents = TorrentService::new(
        db.clone(),
        Sessions::new(
            Arc::new(Offline::default()),
            SessionEnv {
                root: dir.path().join("sessions"),
            },
            db.clone(),
        ),
        TorrentSettings {
            upload_cap: None,
            complete_dir: dir.path().join("complete"),
            categories: Vec::new(),
            seeding: None,
            torrent: TorrentConfig {
                enabled: false,
                ..TorrentConfig::default()
            },
        },
        JobEnv::unbounded(),
        Arc::new(SystemClock),
    );
    torrents.start().await.unwrap();
    torrents.loaded().await;
    // Every resumed magnet's fetch ends at once with torrents off; each
    // shows only its own row, so the start builds a few views a torrent.
    torrents.fetches_ended(count.div_ceil(3)).await;
    assert!(torrents.views_built() <= 4 * count as usize);
    assert_eq!(torrents.snapshot().len(), count as usize);
    (dir, db, torrents)
}

// The hash torrent `job_id` was stored with.
fn stored_hash(job_id: u64) -> String {
    format!("{job_id:016x}{}5a", "0".repeat(22))
}

// The rows of every torrent, by job, without the time each was built at.
fn shown(torrents: &TorrentService) -> Vec<TorrentView> {
    let mut views: Vec<TorrentView> = torrents
        .snapshot()
        .every()
        .1
        .iter()
        .map(|row| {
            let mut view = row.view.clone();
            view.clock.at_ms = 0;
            view
        })
        .collect();
    views.sort_by_key(|view| view.job_id);
    views
}

// Once no torrent's row shows a metadata fetch running.
async fn fetches_shown_ended(torrents: &TorrentService) {
    let mut revision = torrents.snapshot().subscribe();
    while torrents
        .snapshot()
        .every()
        .1
        .iter()
        .any(|row| row.view.state == TorrentState::FetchingMetadata)
    {
        revision.changed().await.unwrap();
    }
}

// An action on many torrents is one unit of work: each request builds a
// publish of the torrents, not one for each, and takes the database a few
// times, not once for each. What it stores and shows is what the same
// changes made one torrent at a time store and show.
#[tokio::test]
async fn actions_on_many_torrents_are_one_unit_each() {
    // Few enough that the torrents taken one at a time, a publish of every
    // torrent each, finish quickly.
    const STORED: u64 = 300;
    let (dir, db, many) = started_with(STORED).await;
    let (_alone_dir, alone_db, alone) = started_with(STORED).await;
    let rig = rig_around(
        dir,
        db,
        Some(many.clone()),
        RuntimeSecurityConfig::default(),
        scheduler(),
        Api::LATEST,
    );
    let hashes = (1..=STORED).map(stored_hash).collect::<Vec<_>>().join("|");
    for (action, form) in [
        ("setForceStart", "value=true"),
        ("toggleSequentialDownload", ""),
        ("toggleFirstLastPiecePrio", ""),
        ("setUploadLimit", "limit=1024"),
        ("setDownloadLimit", "limit=2048"),
        (
            "setShareLimits",
            "ratioLimit=2&seedingTimeLimit=60&inactiveSeedingTimeLimit=-2",
        ),
        ("stop", ""),
        ("start", ""),
        ("setCategory", "category="),
        ("setForceStart", "value=false"),
    ] {
        let (views, takes) = (many.views_built(), rig.db.datastore_takes());
        let reply = rig
            .post(
                &format!("/api/v2/torrents/{action}"),
                &format!("hashes={hashes}&{form}"),
            )
            .await;
        assert_eq!(reply.status, StatusCode::OK, "{action}: {}", reply.body);
        let built = many.views_built() - views;
        // One publish of every torrent, and on a start a row for each
        // magnet whose fetch ends while the request runs. One at a time
        // they built a publish of every torrent for each.
        assert!(built <= 3 * STORED as usize, "{action}: {built} views");
        let took = rig.db.datastore_takes() - takes;
        assert!(took <= 16, "{action}: {took} takes");
        // The same request, one torrent at a time.
        let before = shown(&alone);
        for view in &before {
            let job_id = view.job_id;
            let done = match action {
                "setForceStart" => alone.set_forced(job_id, form.ends_with("true")).await,
                "toggleSequentialDownload" => alone.set_sequential(job_id, !view.sequential).await,
                "toggleFirstLastPiecePrio" => {
                    alone
                        .set_first_last_piece_first(job_id, !view.first_last_piece_first)
                        .await
                }
                "setUploadLimit" => alone.set_rate_limits(job_id, Some(Some(1024)), None).await,
                "setDownloadLimit" => alone.set_rate_limits(job_id, None, Some(Some(2048))).await,
                // The goals the request names, as the facade reads them.
                "setShareLimits" => {
                    let policy = rig.db.get_torrent(job_id).unwrap().unwrap().policy_override;
                    assert!(policy.is_some());
                    alone.set_policy_override(job_id, policy).await
                }
                "stop" => alone.pause(job_id).await,
                "start" => alone.resume(job_id).await,
                _ => alone.set_category(job_id, None).await,
            };
            done.unwrap();
        }
        assert_eq!(
            rig.db.list_torrents().unwrap(),
            alone_db.list_torrents().unwrap(),
            "{action}"
        );
        // A resumed magnet fetches again, and its fetch ends on its own
        // turn.
        fetches_shown_ended(&many).await;
        fetches_shown_ended(&alone).await;
        assert!(shown(&many) == shown(&alone), "{action}");
    }
    many.stop().await;
    alone.stop().await;
}

// Each torrent's trackers, by tier and URL, by job.
async fn trackers_of(torrents: &TorrentService) -> Vec<(JobId, Vec<(u32, String)>)> {
    let ids: Vec<JobId> = shown(torrents).iter().map(|view| view.job_id).collect();
    let mut trackers: Vec<_> = torrents
        .row_parts(&ids, false, true)
        .await
        .into_iter()
        .map(|(job_id, parts)| {
            let listed = parts
                .trackers
                .into_iter()
                .map(|tracker| (tracker.tier, tracker.url))
                .collect();
            (job_id, listed)
        })
        .collect();
    trackers.sort_by_key(|(job_id, _)| *job_id);
    trackers
}

// The tracker edits, rechecks, announces and removals of many torrents are
// one unit of work each, as the other actions are: a request builds a
// publish of the torrents, not one for each, and takes the database a few
// times, not once for each, beside the history row each removal writes.
// What it answers, stores and shows is what the same changes made one
// torrent at a time answer, store and show.
#[tokio::test]
async fn edits_and_removals_of_many_torrents_are_one_unit_each() {
    const STORED: u64 = 300;
    let (dir, db, many) = started_with(STORED).await;
    let (_alone_dir, alone_db, alone) = started_with(STORED).await;
    let rig = rig_around(
        dir,
        db,
        Some(many.clone()),
        RuntimeSecurityConfig::default(),
        scheduler(),
        Api::LATEST,
    );
    let hashes = (1..=STORED).map(stored_hash).collect::<Vec<_>>().join("|");
    let tiers = vec![
        vec!["http://one.example:80/announce".to_owned()],
        vec!["http://two.example:80/announce".to_owned()],
    ];
    let one = "http://one.example:80/announce".to_owned();
    for (action, form) in [
        (
            "addTrackers",
            format!(
                "hash={hashes}&urls=http%3A%2F%2Fone.example%3A80%2Fannounce%0A%0Ahttp%3A%2F%2Ftwo.example%3A80%2Fannounce"
            ),
        ),
        (
            "reannounce",
            format!("hashes={hashes}&urls=http%3A%2F%2Fone.example%3A80%2Fannounce"),
        ),
        ("reannounce", format!("hashes={hashes}")),
        (
            "removeTrackers",
            "hash=*&urls=http%3A%2F%2Fone.example%3A80%2Fannounce".to_owned(),
        ),
        // In no session, each is refused, the first ending the request.
        ("recheck", format!("hashes={hashes}")),
        // With no files to move yet, each is refused, the first ending the
        // request.
        (
            "setLocation",
            format!("hashes={hashes}&location=%2Fweaver-nowhere"),
        ),
        ("delete", format!("hashes={hashes}&deleteFiles=false")),
    ] {
        let (views, takes) = (many.views_built(), rig.db.datastore_takes());
        let reply = rig.post(&format!("/api/v2/torrents/{action}"), &form).await;
        let built = many.views_built() - views;
        let took = rig.db.datastore_takes() - takes;
        // The same request, one torrent at a time, in the request's order,
        // answered by the first that failed.
        let mut answer = (StatusCode::OK, String::new());
        let before: Vec<TorrentView> = rig_order(&alone);
        for view in &before {
            let job_id = view.job_id;
            let done = match action {
                "addTrackers" => alone.add_trackers(job_id, tiers.clone()).await,
                "reannounce" if form.contains("urls") => {
                    alone.reannounce_tracker(job_id, &one).await
                }
                "reannounce" => match alone.reannounce(job_id).await {
                    Ok(()) => alone.dht_announce(job_id).await,
                    failed => failed,
                },
                "removeTrackers" => {
                    alone
                        .remove_trackers(job_id, std::slice::from_ref(&one))
                        .await
                }
                "recheck" => alone.recheck(job_id).await,
                "setLocation" => {
                    alone
                        .set_location(job_id, "/weaver-nowhere".to_owned())
                        .await
                }
                _ => alone.remove(job_id, false, true).await,
            };
            match done {
                Ok(()) | Err(ActionError::NotFound(_)) => {}
                Err(error) => {
                    answer = match (action, &error) {
                        ("delete", _) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
                        (_, ActionError::Invalid(_)) => (StatusCode::CONFLICT, error.to_string()),
                        _ => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()),
                    };
                    break;
                }
            }
        }
        // A success is answered as the API version answers it.
        if answer.0 == StatusCode::OK {
            assert!(reply.status.is_success(), "{action}: {}", reply.body);
        } else {
            assert_eq!((reply.status, reply.body.clone()), answer, "{action}");
        }
        // A publish of every torrent at most, and the view of each removed
        // torrent its history row reads, where one at a time built a
        // publish of every torrent for each.
        assert!(built <= 3 * STORED as usize, "{action}: {built} views");
        // A few statements, and one history row a removal.
        let bound = if action == "delete" {
            16 + STORED as usize
        } else {
            16
        };
        assert!(took <= bound, "{action}: {took} takes");
        assert_eq!(
            rig.db.list_torrents().unwrap(),
            alone_db.list_torrents().unwrap(),
            "{action}"
        );
        let ids: Vec<u64> = (1..=STORED).collect();
        assert_eq!(
            rig.db.list_history_job_ids(&ids).unwrap(),
            alone_db.list_history_job_ids(&ids).unwrap(),
            "{action}"
        );
        fetches_shown_ended(&many).await;
        fetches_shown_ended(&alone).await;
        assert!(shown(&many) == shown(&alone), "{action}");
        assert_eq!(
            trackers_of(&many).await,
            trackers_of(&alone).await,
            "{action}"
        );
    }
    assert_eq!(many.snapshot().len(), 0);
    many.stop().await;
    alone.stop().await;
}

// The torrents' views in the order a request naming them all takes them:
// the order they were added.
fn rig_order(torrents: &TorrentService) -> Vec<TorrentView> {
    torrents
        .snapshot()
        .every()
        .1
        .iter()
        .map(|row| row.view.clone())
        .collect()
}

// With 10,000 torrents loaded and nothing changing, `torrents/info` polls,
// across filters, sorts, pages, hashes, privacy and the trackers and files
// listed, and the limit readings of every torrent, build no torrent's view
// and take the database for no statement. Torrents are off, so no tick
// publishes while the polls run.
#[tokio::test]
async fn torrents_info_polls_build_no_view_and_read_nothing() {
    const STORED: u64 = 10_000;
    let (dir, db, torrents) = started_with(STORED).await;
    let rig = rig_around(
        dir,
        db,
        Some(torrents.clone()),
        RuntimeSecurityConfig::default(),
        scheduler(),
        Api::LATEST,
    );
    let named = format!(
        "{:016x}{zeros}5a|{:016X}{zeros}5A",
        3,
        8_000,
        zeros = "0".repeat(22)
    );
    // The facade's first request of any kind takes the database once, an
    // `app/version` as much as a list: that one is not a poll's.
    assert_eq!(rig.get("/api/v2/app/version").await.status, StatusCode::OK);
    let (views, statements) = (torrents.views_built(), rig.db.datastore_takes());
    let mut answered = Vec::new();
    for _ in 0..3 {
        for path in [
            "/api/v2/torrents/info".to_owned(),
            "/api/v2/torrents/info?filter=stopped&sort=name&limit=50".to_owned(),
            "/api/v2/torrents/info?private=true&offset=-20".to_owned(),
            "/api/v2/torrents/info?category=&sort=priority&reverse=true".to_owned(),
            "/api/v2/torrents/info?tag=&limit=500&offset=9000".to_owned(),
            "/api/v2/torrents/info?includeTrackers=true&limit=100".to_owned(),
            "/api/v2/torrents/info?includeFiles=true&limit=100".to_owned(),
            format!("/api/v2/torrents/info?hashes={named}"),
            "/api/v2/torrents/uploadLimit?hashes=all".to_owned(),
            format!("/api/v2/torrents/downloadLimit?hashes={named}"),
        ] {
            let reply = rig.get(&path).await;
            assert_eq!(reply.status, StatusCode::OK, "{path}: {}", reply.body);
            answered.push((path, reply.json()));
        }
    }
    assert_eq!(torrents.views_built(), views);
    assert_eq!(rig.db.datastore_takes(), statements);
    let every = &answered[0].1;
    assert_eq!(every.as_array().unwrap().len(), STORED as usize);
    let hashed = answered
        .iter()
        .find(|(path, _)| path.contains("info?hashes"))
        .unwrap();
    assert_eq!(hashed.1.as_array().unwrap().len(), 2, "{named}");
    let limits = answered
        .iter()
        .find(|(path, _)| path.contains("uploadLimit"))
        .unwrap();
    assert_eq!(limits.1.as_object().unwrap().len(), STORED as usize);
    // The same polls answer the same each time.
    let rounds: Vec<&[(String, Value)]> = answered.chunks(10).collect();
    assert!(rounds.windows(2).all(|pair| pair[0] == pair[1]));
    torrents.stop().await;
}

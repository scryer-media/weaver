// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A list's rows read without their files answer as the rows read with them:
// `torrents/info` across filters, tags, sorts and pages, and `sync/maindata`
// across a run of ids with torrents added, changed and removed.

use std::collections::HashMap;

use weaver_server_core::torrents::service::TorrentPieces;
use weaver_server_core::torrents::service::listing::FileTotals;
use weaver_server_core::torrents::{FilePriority, NamedBy, TorrentTracker, TrackerStatus};

use super::*;

const TORRENTS: usize = 10_000;

const STATES: [TorrentState; 11] = [
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
            let old = body(info_list(paged(&whole, &ask), &ask, scope, Api::LATEST)).await;
            let new = body(info_list(paged(&rows, &ask), &ask, scope, Api::LATEST)).await;
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
    ))
    .await;
    let listed: Vec<Value> = serde_json::from_slice(&listed.1).unwrap();
    assert!(listed.iter().any(|row| {
        row["files"]
            .as_array()
            .is_some_and(|files| files.len() == 150)
    }));
}

// One `sync/maindata` client's answers, built from the rows given.
struct Client<V> {
    table: SyncTable,
    rid: u64,
    built: usize,
    _rows: std::marker::PhantomData<V>,
}

impl<V> Client<V> {
    fn new() -> Self {
        Self {
            table: SyncTable::default(),
            rid: 0,
            built: 0,
            _rows: std::marker::PhantomData,
        }
    }

    fn poll(
        &mut self,
        rid: u64,
        rows: Vec<Row<'_, V>>,
        steady: impl Fn(&V) -> Vec<(&'static str, Value)>,
        clock: impl Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
    ) -> String {
        let built = std::cell::Cell::new(0);
        let fields = Fields {
            steady: |row: &V| {
                built.set(built.get() + 1);
                steady(row)
            },
            clock,
        };
        let now = Poll {
            torrents: rows,
            categories: Vec::new(),
            tags: Vec::new(),
            server_state: Map::new(),
            server_closed: 0,
        };
        let start = self.table.begin(&SyncClient::Anonymous, rid);
        let (body, kept) = sync::answer(&start, now, &fields);
        self.table.finish(&SyncClient::Anonymous, start.id(), kept);
        self.rid = start.id();
        self.built += built.get();
        serde_json::to_string(&body).unwrap()
    }
}

// `sync/maindata` over 10,000 torrents answers the same, byte for byte,
// whether its rows carry their files or only their totals, across a run of
// ids: unchanged polls, changed levels, bytes in hand and states, torrents
// added and removed, a file renamed, and an id the client never held.
#[test]
fn maindata_answers_the_same_without_the_files() {
    const AT: i64 = 1_700_000_500;
    for scope in [CallerScope::Admin, CallerScope::Read] {
        let mut views: Vec<TorrentView> = (0..TORRENTS).map(seeded_view).collect();
        let mut old: Client<TorrentView> = Client::new();
        let mut new: Client<(TorrentView, FileTotals)> = Client::new();
        let mut steps = 0;
        let mut poll = |views: &[TorrentView], rid: Option<u64>| {
            let old_rows: Vec<Row<'_, TorrentView>> = views
                .iter()
                .map(|view| Row {
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
                .collect();
            let before = old.built;
            let answer_old = old.poll(
                rid.unwrap_or(old.rid),
                old_rows,
                |view| {
                    let mut row = steady_fields(view, scope);
                    row.retain(|(name, _)| *name != "hash");
                    row.extend(announce_stats(view));
                    row
                },
                |view| clock_fields(view, AT),
            );
            let rows = rows_of(views);
            let answer_new = new.poll(
                rid.unwrap_or(new.rid),
                rows.iter().map(|row| maindata_row(row, scope)).collect(),
                |(view, totals)| maindata_steady(view, *totals, scope),
                |(view, _)| clock_fields(view, AT),
            );
            assert_eq!(answer_old, answer_new, "step {steps}");
            steps += 1;
            (answer_new, old.built - before, new.rid)
        };
        let (first, built, _) = poll(&views, Some(0));
        assert!(first.contains("\"full_update\":true"));
        assert_eq!(built, TORRENTS);
        // Nothing moved.
        let (same, built, rid) = poll(&views, None);
        assert_eq!(same, format!("{{\"rid\":{rid}}}"));
        assert_eq!(built, 0);
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
        poll(&views, None);
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
    }
}

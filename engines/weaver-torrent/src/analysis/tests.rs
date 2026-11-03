// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::render::MAX_COLUMNS;
use super::*;
use crate::core::v2_test_torrent::{TestTorrent, bstr, payload};

// 2026-10-01 00:00:00 UTC.
const NOW: u64 = 1_790_812_800;
const DAY: u64 = 86_400;
const PASSKEY: &str = "0f1e2d3c4b5a69788796a5b4c3d2e1f0";

// A bencoded value, for metainfo built by hand.
enum B {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<B>),
    // Keys must be given in order.
    Dict(Vec<(&'static str, B)>),
}

impl B {
    fn s(text: &str) -> Self {
        Self::Bytes(text.as_bytes().to_vec())
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Int(value) => out.extend_from_slice(format!("i{value}e").as_bytes()),
            Self::Bytes(bytes) => out.extend(bstr(bytes)),
            Self::List(items) => {
                out.push(b'l');
                items.iter().for_each(|item| item.encode(out));
                out.push(b'e');
            }
            Self::Dict(entries) => {
                out.push(b'd');
                for (key, value) in entries {
                    out.extend(bstr(key.as_bytes()));
                    value.encode(out);
                }
                out.push(b'e');
            }
        }
    }

    fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode(&mut out);
        out
    }
}

// A v1 torrent built from its parts. Piece hashes are filler of the right
// count unless `pieces` says otherwise.
struct V1 {
    // Each file as path components and length; one file with one
    // component and `single` set is a single-file torrent.
    files: Vec<(Vec<Vec<u8>>, u64)>,
    single: bool,
    piece_length: i64,
    pieces: Option<usize>,
    private: bool,
    tiers: Vec<Vec<String>>,
    created_by: Option<&'static str>,
    created: Option<u64>,
    comment: Option<&'static str>,
}

impl V1 {
    fn new(files: &[(&str, u64)]) -> Self {
        Self {
            files: files
                .iter()
                .map(|(path, len)| {
                    (
                        path.split('/').map(|c| c.as_bytes().to_vec()).collect(),
                        *len,
                    )
                })
                .collect(),
            single: false,
            piece_length: 16_384,
            pieces: None,
            private: false,
            tiers: vec![vec![tracker("https://tracker.example.org/announce")]],
            created_by: None,
            created: None,
            comment: None,
        }
    }

    fn single(name: &str, len: u64) -> Self {
        let mut v1 = Self::new(&[(name, len)]);
        v1.single = true;
        v1
    }

    fn raw_files(files: Vec<(Vec<Vec<u8>>, u64)>) -> Self {
        let mut v1 = Self::new(&[]);
        v1.files = files;
        v1
    }

    fn bytes(&self) -> Vec<u8> {
        let total: u64 = self.files.iter().map(|(_, len)| len).sum();
        let count = usize::try_from(total.div_ceil(self.piece_length as u64)).unwrap();
        let pieces = vec![7u8; 20 * self.pieces.unwrap_or(count)];
        let mut info: Vec<(&'static str, B)> = Vec::new();
        if !self.single {
            let files = self
                .files
                .iter()
                .map(|(path, len)| {
                    B::Dict(vec![
                        ("length", B::Int(*len as i64)),
                        (
                            "path",
                            B::List(path.iter().map(|c| B::Bytes(c.clone())).collect()),
                        ),
                    ])
                })
                .collect();
            info.push(("files", B::List(files)));
        } else {
            info.push(("length", B::Int(self.files[0].1 as i64)));
        }
        let name = if self.single {
            B::Bytes(self.files[0].0[0].clone())
        } else {
            B::s("content")
        };
        info.push(("name", name));
        info.push(("piece length", B::Int(self.piece_length)));
        info.push(("pieces", B::Bytes(pieces)));
        if self.private {
            info.push(("private", B::Int(1)));
        }

        let mut top: Vec<(&'static str, B)> = Vec::new();
        if let Some(first) = self.tiers.first().and_then(|tier| tier.first()) {
            top.push(("announce", B::s(first)));
        }
        if !self.tiers.is_empty() {
            let tiers = self
                .tiers
                .iter()
                .map(|tier| B::List(tier.iter().map(|url| B::s(url)).collect()))
                .collect();
            top.push(("announce-list", B::List(tiers)));
        }
        if let Some(comment) = self.comment {
            top.push(("comment", B::s(comment)));
        }
        if let Some(tool) = self.created_by {
            top.push(("created by", B::s(tool)));
        }
        if let Some(created) = self.created {
            top.push(("creation date", B::Int(created as i64)));
        }
        top.push(("info", B::Dict(info)));
        B::Dict(top).bytes()
    }

    fn report(&self) -> TorrentReport {
        analyze_metainfo(&self.bytes(), NOW).unwrap()
    }
}

fn tracker(url: &str) -> String {
    format!("{url}?passkey={PASSKEY}")
}

// A hybrid of two files, private, with a tracker in each of two tiers and
// a web seed; its v1 files are padded to the piece.
fn hybrid_private() -> Vec<u8> {
    let mut torrent = TestTorrent::new(
        vec![
            ("Holiday Clip.mkv", payload(40_000, 1)),
            ("notes.nfo", payload(900, 2)),
        ],
        16_384,
    );
    torrent.hybrid = true;
    let mut info = torrent.info_bytes();
    info.pop();
    info.extend_from_slice(b"7:privatei1ee");
    let rest = torrent.torrent_bytes_with_info(&info);
    let mut out = b"d".to_vec();
    out.extend(
        B::s(&tracker("https://tracker.example.org:8443/a"))
            .bytes()
            .iter()
            .copied()
            .fold(bstr(b"announce"), |mut acc, byte| {
                acc.push(byte);
                acc
            }),
    );
    out.extend(bstr(b"announce-list"));
    out.extend(
        B::List(vec![
            B::List(vec![B::s(&tracker("https://tracker.example.org:8443/a"))]),
            B::List(vec![B::s(&tracker("udp://203.0.113.9:6969/announce"))]),
        ])
        .bytes(),
    );
    out.extend(bstr(b"comment"));
    out.extend(bstr(b"Secret Group release, see https://example.org/x"));
    out.extend(bstr(b"created by"));
    out.extend(bstr(b"maker 2.1"));
    out.extend(bstr(b"creation date"));
    out.extend_from_slice(format!("i{}e", NOW - 12 * DAY).as_bytes());
    // The rest: `4:info...` and the piece layers, without the outer `d`
    // and `e`.
    out.extend_from_slice(&rest[1..rest.len() - 1]);
    out.extend(bstr(b"url-list"));
    out.extend(bstr(b"https://seed.example.net/files/"));
    out.push(b'e');
    out
}

fn assert_fits(text: &str) {
    for line in text.lines() {
        assert!(line.len() <= MAX_COLUMNS, "line too wide: {line:?}");
        assert!(line.is_ascii(), "line not ascii: {line:?}");
    }
}

fn assert_redacted(report: &TorrentReport) {
    for form in [report.to_text(), report.to_json()] {
        for secret in [
            PASSKEY,
            "passkey",
            "Holiday",
            "Secret Group",
            "content",
            "notes",
            "announce",
            "/files",
            "example.org/x",
        ] {
            assert!(!form.contains(secret), "{secret:?} leaked into {form}");
        }
    }
}

#[test]
fn a_hybrid_private_torrent_reports_both_hashes_and_its_trackers() {
    let report = analyze_metainfo(&hybrid_private(), NOW).unwrap();
    assert_eq!(report.identity.version, Some(Version::Hybrid));
    assert!(report.identity.has_v1_hash && report.identity.has_v2_hash);
    assert_eq!(report.red_flags, Vec::<RedFlag>::new());

    let shape = report.shape.as_ref().unwrap();
    assert!(shape.loads);
    assert_eq!(shape.piece_length, 16_384);
    assert_eq!(shape.file_count, 2);
    // Both files are padded to the piece: three pieces, then one.
    assert_eq!(shape.piece_count, 4);
    assert_eq!(shape.padding_files, 2);
    assert_eq!(shape.total_bytes, 4 * 16_384);
    assert_eq!(shape.largest.unwrap().bytes, 40_000);
    assert_eq!(shape.smallest.unwrap().bytes, 900);
    assert_eq!(
        shape.piece_hashes,
        Some(PieceHashes {
            expected: 80,
            listed: 80
        })
    );
    assert_eq!(
        shape.piece_layers,
        Some(PieceLayers {
            needed: 1,
            carried: 1
        })
    );

    let trackers = &report.trackers;
    assert_eq!(trackers.private, Some(true));
    assert_eq!(trackers.discovery.dht, Some(false));
    assert_eq!(trackers.discovery.pex, Some(false));
    assert_eq!(trackers.discovery.lsd, Some(false));
    assert_eq!(trackers.tiers, 2);
    let first = &trackers.trackers[0];
    assert_eq!(
        (first.tier, first.scheme, first.host.as_deref(), first.port),
        (0, Scheme::Https, Some("tracker.example.org"), Some(8443))
    );
    assert_eq!(first.host_kind, HostKind::Name);
    assert!(first.has_path);
    let second = &trackers.trackers[1];
    assert_eq!((second.tier, second.scheme), (1, Scheme::Udp));
    assert_eq!(second.host_kind, HostKind::PublicAddress);
    assert!(second.host_kind.is_literal());
    assert_eq!(trackers.web_seeds.len(), 1);
    assert_eq!(
        trackers.web_seeds[0].host.as_deref(),
        Some("seed.example.net")
    );

    assert_eq!(report.about.created_age_days, Some(12));
    assert_eq!(report.about.created_by.as_deref(), Some("maker 2.1"));
    assert!(report.about.comment.unwrap().has_url);

    let layout = report.layout.as_ref().unwrap();
    assert!(!layout.single_file);
    assert_eq!(layout.payload, PayloadKind::Direct);
    assert_redacted(&report);
    assert_fits(&report.to_text());
}

#[test]
fn a_v1_torrent_has_only_its_v1_hash_and_open_discovery() {
    let report = V1::new(&[("a/one.mkv", 50_000), ("a/two.srt", 2_000)]).report();
    assert_eq!(report.identity.version, Some(Version::V1));
    assert!(report.identity.has_v1_hash && !report.identity.has_v2_hash);
    assert_eq!(report.red_flags, Vec::<RedFlag>::new());
    let shape = report.shape.as_ref().unwrap();
    assert_eq!(shape.piece_count, 4);
    assert_eq!(shape.last_piece_bytes, 52_000 - 3 * 16_384);
    assert_eq!(shape.piece_layers, None);
    assert_eq!(report.trackers.private, Some(false));
    assert_eq!(report.trackers.discovery.dht, Some(true));
    assert_eq!(report.layout.as_ref().unwrap().max_depth, 1);
    assert_fits(&report.to_text());
}

#[test]
fn a_v2_torrent_counts_its_piece_layers() {
    let torrent = TestTorrent::new(vec![("big.mkv", payload(70_000, 3))], 16_384);
    let report = analyze_metainfo(&torrent.torrent_bytes(), NOW).unwrap();
    assert_eq!(report.identity.version, Some(Version::V2));
    assert!(!report.identity.has_v1_hash && report.identity.has_v2_hash);
    let shape = report.shape.as_ref().unwrap();
    assert_eq!(shape.piece_hashes, None);
    assert_eq!(
        shape.piece_layers,
        Some(PieceLayers {
            needed: 1,
            carried: 1
        })
    );
    assert!(report.layout.as_ref().unwrap().single_file);
    assert!(
        !report.red_flags.contains(&RedFlag::NoTrackers) || report.trackers.trackers.is_empty()
    );
}

#[test]
fn a_v2_torrent_without_its_layers_is_flagged() {
    let mut torrent = TestTorrent::new(vec![("big.mkv", payload(70_000, 3))], 16_384);
    torrent.layers = false;
    let report = analyze_metainfo(&torrent.torrent_bytes(), NOW).unwrap();
    assert!(
        report
            .red_flags
            .contains(&RedFlag::PieceLayersMissing { files: 1 })
    );
}

#[test]
fn a_magnet_reports_what_it_carries_and_marks_the_rest_unknown() {
    let hex = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
    let link = format!(
        "magnet:?xt=urn:btih:{hex}&dn=Holiday+Clip&xl=123456&tr={}&ws={}",
        url_escape(&tracker("udp://tracker.example.org:1337/announce")),
        url_escape("https://seed.example.net/files/x"),
    );
    let report = analyze_magnet_link(&link, NOW).unwrap();
    assert_eq!(report.identity.source, Source::Magnet);
    assert_eq!(report.identity.version, None);
    assert!(report.shape.is_none() && report.layout.is_none());
    assert_eq!(report.trackers.private, None);
    assert_eq!(report.trackers.discovery.dht, None);
    assert_eq!(report.trackers.trackers[0].scheme, Scheme::Udp);
    assert_eq!(report.trackers.web_seeds.len(), 1);
    let magnet = report.about.magnet.unwrap();
    assert_eq!(magnet.exact_length, Some(123_456));
    assert_eq!(report.about.name.unwrap().bytes, 12);
    assert_eq!(report.red_flags, Vec::<RedFlag>::new());
    let text = report.to_text();
    assert!(text.contains("unknown"));
    assert_redacted(&report);
    assert_fits(&text);

    let bare = analyze_magnet_link(&format!("magnet:?xt=urn:btih:{hex}"), NOW).unwrap();
    assert_eq!(bare.red_flags, vec![RedFlag::NoTrackers]);
    assert_eq!(bare.identity.fingerprint, report.identity.fingerprint);
}

#[test]
fn a_magnet_and_its_metainfo_share_a_fingerprint() {
    let bytes = hybrid_private();
    let meta = torrent_from_bytes(&bytes).unwrap();
    let magnet = Magnet::of(meta.info_hashes, None, Vec::new());
    assert_eq!(
        analyze_magnet(&magnet, NOW).identity.fingerprint,
        analyze_metainfo(&bytes, NOW).unwrap().identity.fingerprint
    );
}

#[test]
fn what_is_not_a_torrent_is_an_error_that_quotes_nothing() {
    let error = analyze_metainfo(b"Holiday Clip", NOW).unwrap_err();
    assert!(!error.to_string().contains("Holiday"));
    assert!(analyze_magnet_link("https://example.org/Holiday", NOW).is_err());
}

#[test]
fn a_private_torrent_without_trackers_is_flagged() {
    let mut v1 = V1::new(&[("a.mkv", 20_000)]);
    v1.private = true;
    v1.tiers.clear();
    let report = v1.report();
    assert_eq!(report.red_flags, vec![RedFlag::PrivateWithoutTrackers]);
    assert_eq!(report.trackers.discovery.pex, Some(false));

    let mut public = V1::new(&[("a.mkv", 20_000)]);
    public.tiers.clear();
    assert_eq!(public.report().red_flags, vec![RedFlag::NoTrackers]);
}

#[test]
fn trackers_weaver_cannot_reach_are_flagged() {
    let mut v1 = V1::new(&[("a.mkv", 20_000)]);
    v1.tiers = vec![
        vec!["wss://tracker.example.org/announce".to_owned()],
        vec!["udp://192.168.1.20:6969/announce".to_owned()],
        vec!["http://127.0.0.1:8080/announce".to_owned()],
    ];
    let report = v1.report();
    assert!(
        report
            .red_flags
            .contains(&RedFlag::UnusableTrackers { trackers: 1 })
    );
    assert!(
        report
            .red_flags
            .contains(&RedFlag::LocalTrackers { trackers: 2 })
    );
    let kinds: Vec<HostKind> = report
        .trackers
        .trackers
        .iter()
        .map(|t| t.host_kind)
        .collect();
    assert_eq!(
        kinds,
        vec![HostKind::Name, HostKind::LocalAddress, HostKind::Loopback]
    );
    assert_eq!(report.trackers.trackers[0].scheme, Scheme::Wss);
}

#[test]
fn empty_files_are_flagged() {
    let report = V1::new(&[("a.mkv", 20_000), ("b.txt", 0)]).report();
    assert!(report.red_flags.contains(&RedFlag::EmptyFiles { files: 1 }));
    assert!(report.to_text().contains("empty"));
}

#[test]
fn piece_hashes_that_do_not_match_the_lengths_are_flagged() {
    let mut v1 = V1::new(&[("a.mkv", 40_000)]);
    v1.pieces = Some(2);
    let report = v1.report();
    assert!(report.red_flags.contains(&RedFlag::PieceHashMismatch {
        expected: 60,
        listed: 40
    }));
    assert!(report.red_flags.contains(&RedFlag::DoesNotLoad {
        reason: "piece_hash_count"
    }));
    assert!(!report.shape.unwrap().loads);
}

#[test]
fn an_odd_piece_length_is_flagged() {
    let mut v1 = V1::new(&[("a.mkv", 40_000)]);
    v1.piece_length = 20_000;
    let report = v1.report();
    assert!(report.red_flags.contains(&RedFlag::OddPieceLength {
        piece_length: 20_000
    }));
}

#[test]
fn path_oddities_are_each_flagged() {
    let path = |parts: &[&[u8]]| parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>();
    let long = vec![b'n'; 300];
    let report = V1::raw_files(vec![
        (path(&[b"..", b"escape.mkv"]), 10),
        (path(&[b"C:", b"rooted.mkv"]), 10),
        (path(&[b"a\\b.mkv"]), 10),
        (path(&[b"same.mkv"]), 10),
        (path(&[b"same.mkv"]), 10),
        (path(&[b"Case.mkv"]), 10),
        (path(&[b"case.mkv"]), 10),
        (path(&[&long]), 10),
        (path(&[b"\xff\xfe.mkv"]), 10),
    ])
    .report();
    for flag in [
        RedFlag::PathTraversal { files: 1 },
        RedFlag::AbsolutePaths { files: 1 },
        RedFlag::SeparatorInNames { files: 1 },
        RedFlag::DuplicatePaths { files: 2 },
        RedFlag::CaseDuplicatePaths { files: 2 },
        RedFlag::OverlongNames { files: 1 },
        RedFlag::NonUtf8Names { files: 1 },
    ] {
        assert!(report.red_flags.contains(&flag), "{flag:?} missing");
    }
    // Weaver refuses the climb and the duplicate.
    assert!(!report.shape.as_ref().unwrap().loads);
    let text = report.to_text();
    assert_fits(&text);
    assert!(!text.contains("escape") && !text.contains("rooted"));
}

#[test]
fn programs_among_the_files_are_flagged() {
    let report = V1::new(&[("a/film.mkv", 40_000), ("a/codec.exe", 900)]).report();
    assert_eq!(
        report.red_flags,
        vec![RedFlag::ExecutableFiles { files: 1 }]
    );
    assert!(report.to_text().contains("program"));
}

#[test]
fn a_single_file_archive_is_flagged_by_its_extension() {
    let rar = V1::single("pack.rar", 40_000).report();
    assert_eq!(
        rar.red_flags,
        vec![RedFlag::SingleFileArchive {
            archive: ArchiveKind::Rar
        }]
    );
    assert_eq!(rar.layout.unwrap().payload, PayloadKind::Archive);
    let split = V1::single("pack.7z.001", 40_000).report();
    assert!(split.red_flags.contains(&RedFlag::SingleFileArchive {
        archive: ArchiveKind::SevenZip
    }));
    assert_eq!(V1::single("clip.mkv", 40_000).report().red_flags, vec![]);
    // Many files that are archives are a set, not one archive.
    let set = V1::new(&[("a/pack.rar", 40_000), ("a/pack.r00", 40_000)]).report();
    assert_eq!(set.red_flags, vec![]);
}

#[test]
fn a_creation_date_ahead_of_now_is_flagged() {
    let mut v1 = V1::new(&[("a.mkv", 40_000)]);
    v1.created = Some(NOW + 30 * DAY);
    let report = v1.report();
    assert_eq!(
        report.red_flags,
        vec![RedFlag::CreatedInFuture { days: 30 }]
    );
    assert!(report.about.created_in_future);
}

#[test]
fn the_creating_tool_is_kept_only_when_plain() {
    let mut v1 = V1::new(&[("a.mkv", 40_000)]);
    v1.created_by = Some("maker/1.4.2 (x)");
    assert_eq!(v1.report().about.created_by.as_deref(), Some("maker/1.4.2"));
    v1.created_by = Some("maker 2.1");
    assert_eq!(v1.report().about.created_by.as_deref(), Some("maker 2.1"));
    v1.created_by = Some("sometag");
    assert_eq!(v1.report().about.created_by.as_deref(), Some("other"));
    v1.created_by = Some("Packed by Secret Group, see www.example.org");
    assert_eq!(v1.report().about.created_by.as_deref(), Some("other"));
    v1.created_by = Some("https://example.org/");
    assert_eq!(v1.report().about.created_by.as_deref(), Some("other"));
}

#[test]
fn the_json_is_schema_versioned() {
    let report = V1::new(&[("a.mkv", 40_000)])
        .report()
        .with_weaver_version("9.9.9");
    let json: serde_json::Value = serde_json::from_str(&report.to_json()).unwrap();
    assert_eq!(json["identity"]["schema_version"], REPORT_SCHEMA_VERSION);
    assert_eq!(json["identity"]["weaver_version"], "9.9.9");
    assert_eq!(json["identity"]["source"], "metainfo");
    assert_eq!(json["files"][0]["token"], "f01");
    assert!(
        report
            .to_text()
            .starts_with("weaver torrent report  schema 1  weaver 9.9.9\n")
    );
}

fn url_escape(text: &str) -> String {
    text.bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

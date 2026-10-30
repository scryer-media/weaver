// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use super::*;
use crate::jobs::model::{DownloadState, PostState, RunState, TerminalDiscard};
use crate::jobs::server_attribution::JobServerAttribution;
use crate::{ActiveJob, JobStatus};

const NOW: u64 = 1_767_225_600;
const SENTINEL: &str = "qvzsentinel";

const ENVIRONMENT: ReportEnvironment = ReportEnvironment {
    weaver_version: "9.9.9",
    datastore: "sqlite",
    os: "testos",
    arch: "testarch",
    hardware_profile: Some("balanced"),
};

// An NZB whose every free-text field carries the sentinel.
fn sentinel_nzb() -> Vec<u8> {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="title">{s} title</meta>
    <meta type="password">{s}</meta>
  </head>
  <file poster="{s}@{s}.invalid" date="1767000000" subject="[1/2] - &quot;{s}.part1.rar&quot; yEnc (1/2) 2000">
    <groups><group>alt.binaries.{s}</group></groups>
    <segments>
      <segment bytes="1000" number="1">{s}-1@{s}.invalid</segment>
      <segment bytes="1000" number="2">{s}-2@{s}.invalid</segment>
    </segments>
  </file>
  <file poster="{s}@{s}.invalid" date="1767000000" subject="[2/2] - &quot;{s}.par2&quot; yEnc (1/1)">
    <groups><group>alt.binaries.{s}</group></groups>
    <segments>
      <segment bytes="100" number="1">{s}-3@{s}.invalid</segment>
    </segments>
  </file>
</nzb>"#,
        s = SENTINEL
    )
    .into_bytes()
}

fn active_job(id: u64) -> ActiveJob {
    ActiveJob {
        engine: crate::pipeline::graph::Engine::Classic,
        job_id: JobId(id),
        nzb_hash: [0x11; 32],
        nzb_path: PathBuf::from(format!("/srv/{SENTINEL}/{id}.nzb")),
        nzb_zstd: crate::ingest::compress_nzb_bytes(&sentinel_nzb()).unwrap(),
        output_dir: PathBuf::from(format!("/srv/{SENTINEL}/out")),
        created_at: 1_767_000_000,
        category: Some(SENTINEL.to_string()),
        metadata: vec![(SENTINEL.to_string(), SENTINEL.to_string())],
        status: "downloading",
        download_state: "downloading",
        post_state: "idle",
        run_state: "active",
        paused_resume_status: None,
        paused_resume_download_state: None,
        paused_resume_post_state: None,
        password_override: Some(SENTINEL.to_string()),
        supplied_password_candidates: Vec::new(),
    }
}

fn record_events(db: &Database, job_id: u64, kinds: &[&str]) {
    for (index, kind) in kinds.iter().enumerate() {
        db.insert_job_event(
            job_id,
            1_767_000_000 + index as i64,
            kind,
            &format!("{SENTINEL} message for {kind}"),
            Some(SENTINEL),
        )
        .unwrap();
    }
}

fn assert_redacted(report: &SupportReport) {
    for (form, body) in [("text", &report.text), ("json", &report.json)] {
        let lower = body.to_ascii_lowercase();
        assert!(
            !lower.contains(SENTINEL),
            "{form} leaks the sentinel:\n{body}"
        );
        assert!(!lower.contains("/srv"), "{form} leaks a path:\n{body}");
        assert!(!lower.contains(".invalid"), "{form} leaks a host:\n{body}");
    }
    for line in report.text.lines() {
        assert!(line.len() <= MAX_COLUMNS, "too wide: {line:?}");
    }
}

#[test]
fn history_report_carries_the_outcome_and_no_names() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(7)).unwrap();
    record_events(
        &db,
        7,
        &[
            "JobCreated",
            "FileMissing",
            "JobVerificationComplete",
            "RepairStarted",
            "RepairFailed",
            "JobFailed",
        ],
    );
    let mut attribution = JobServerAttribution::default();
    attribution.note_article(2, 1_000);
    attribution.note_article(5, 2_000);
    attribution.note_article(5, 2_000);
    db.archive_job(
        JobId(7),
        &JobHistoryRow {
            protocol: crate::jobs::Protocol::Nzb,
            job_id: 7,
            job_hash: None,
            name: SENTINEL.to_string(),
            status: "failed".to_string(),
            error_message: Some(format!("PAR2 repair failed for {SENTINEL}.part1.rar")),
            total_bytes: 2_100,
            downloaded_bytes: 1_100,
            optional_recovery_bytes: 100,
            optional_recovery_downloaded_bytes: 100,
            failed_bytes: 1_000,
            health: 523,
            category: Some(SENTINEL.to_string()),
            output_dir: Some(format!("/srv/{SENTINEL}/out")),
            nzb_path: Some(format!("/srv/{SENTINEL}/7.nzb")),
            created_at: 1_767_000_000,
            completed_at: 1_767_000_090,
            metadata: Some(format!("[[\"{SENTINEL}\",\"{SENTINEL}\"]]")),
            server_attribution: attribution.to_storage_json(),
        },
    )
    .unwrap();

    let report = build_job_support_report(&db, None, None, 7, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);

    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    let job = &json["job"];
    assert_eq!(job["source"], "history");
    assert_eq!(job["status"], "failed");
    assert_eq!(job["failure"], "repair");
    assert_eq!(job["repair"], "failed");
    assert_eq!(job["extraction"], "not_run");
    assert_eq!(job["files_reported_missing"], 1);
    assert_eq!(job["verification_runs"], 1);
    assert_eq!(job["health"], 523);
    assert_eq!(job["run_secs"], 90);
    assert_eq!(job["output"]["kind"], "absolute");
    assert_eq!(job["output"]["components"], 3);
    assert_eq!(job["providers"][0]["server"], "s5");
    assert_eq!(job["providers"][0]["articles"], 2);
    assert_eq!(job["providers"][1]["server"], "s2");
    assert_eq!(json["environment"]["datastore"], "sqlite");
    assert_eq!(json["nzb"]["identity"]["weaver_version"], "9.9.9");
    assert!(report.text.contains("verdict repair"));
    assert!(report.text.contains("env weaver 9.9.9  db sqlite"));
}

fn live_info(id: u64) -> JobInfo {
    JobInfo {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: JobId(id),
        job_hash: None,
        name: SENTINEL.to_string(),
        status: JobStatus::Extracting,
        download_state: DownloadState::Complete,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state: PostState::Extracting,
        run_state: RunState::Active,
        progress: 0.5,
        total_bytes: 2_100,
        downloaded_bytes: 2_100,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        phase_progress: Vec::new(),
        failed_bytes: 0,
        health: 1000,
        terminal_discards: vec![TerminalDiscard {
            file_index: 1,
            filename: format!("{SENTINEL}.par2"),
            kind: TerminalDiscardKind::UnneededRecoveryVolume,
            bytes: 100,
        }],
        total_files: 2,
        completed_files: 2,
        remaining_par_files: 0,
        password: Some(SENTINEL.to_string()),
        category: None,
        metadata: vec![(SENTINEL.to_string(), SENTINEL.to_string())],
        output_dir: Some(format!("C:\\{SENTINEL}\\out")),
        error: None,
        download_wait_reason: Some(SENTINEL.to_string()),
        download_retry_at_epoch_ms: None,
        server_attribution: Vec::new(),
        created_at_epoch_ms: 0.0,
    }
}

#[test]
fn live_report_reads_queue_state() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(3)).unwrap();
    record_events(
        &db,
        3,
        &[
            "ExtractionReady",
            "ExtractionMemberFailed",
            "ExtractionComplete",
        ],
    );
    let info = live_info(3);

    let report = build_job_support_report(&db, Some(info), None, 3, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    let job = &json["job"];
    assert_eq!(job["source"], "queue");
    assert_eq!(job["status"], "extracting");
    assert_eq!(job["download_state"], "complete");
    assert_eq!(job["extraction"], "complete");
    assert_eq!(job["extraction_member_failures"], 1);
    assert_eq!(job["password_supplied"], true);
    assert_eq!(job["output"]["kind"], "windows_drive");
    assert_eq!(job["discards"][0]["kind"], "unneeded_recovery_volume");
    assert!(report.text.contains("servers: none attributed"));
}

#[test]
fn missing_job_and_missing_nzb_are_distinct_errors() {
    let db = Database::open_in_memory().unwrap();
    assert!(matches!(
        build_job_support_report(&db, None, None, 99, &ENVIRONMENT, NOW),
        Err(SupportReportError::NotFound(99))
    ));
}

#[test]
fn standalone_analysis_has_no_job_section() {
    let report = analyze_nzb_bytes(&sentinel_nzb(), &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    assert!(json["job"].is_null());
    assert!(!report.text.contains("job ("));
    assert!(matches!(
        analyze_nzb_bytes(b"not xml at all <", &ENVIRONMENT, NOW),
        Err(SupportReportError::Parse(_))
    ));
}

#[test]
fn failure_messages_reduce_to_a_cause() {
    for (message, verdict) in [
        ("Cancelled by user", FailureVerdict::Cancelled),
        ("wrong password for archive", FailureVerdict::Password),
        ("PAR3 verification incomplete", FailureVerdict::Par3),
        ("not enough recovery blocks", FailureVerdict::Repair),
        ("extraction of member failed", FailureVerdict::Extraction),
        ("No space left on device", FailureVerdict::Disk),
        ("post-processing script exited 1", FailureVerdict::Script),
        ("CRC check failed", FailureVerdict::Checksum),
        (
            "articles missing on every server",
            FailureVerdict::MissingArticles,
        ),
        ("something else", FailureVerdict::Other),
    ] {
        assert_eq!(FailureVerdict::classify(message), verdict, "{message}");
    }
}

fn history_row(id: u64, status: &str) -> JobHistoryRow {
    JobHistoryRow {
        protocol: crate::jobs::Protocol::Nzb,
        job_id: id,
        job_hash: None,
        name: SENTINEL.to_string(),
        status: status.to_string(),
        error_message: None,
        total_bytes: 2_100,
        downloaded_bytes: 2_100,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at: 1_767_000_000,
        completed_at: 1_767_000_090,
        metadata: None,
        server_attribution: None,
    }
}

#[test]
fn demotion_and_gaps_reach_the_report_and_survive_the_archive() {
    use crate::jobs::support_facts::{GapKind, GapPosition, JobSupportFacts};

    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(11)).unwrap();
    let mut facts = JobSupportFacts::default();
    facts.note_demotion("member_checksum_mismatch", "downloading", NOW - 7_200);
    facts.note_demotion("par2_damaged", "repairing", NOW - 60);
    facts.note_gap(GapKind::Failed, GapPosition(0, 0));
    facts.note_gap_servers([7]);
    for segment in 0..20 {
        facts.note_gap(GapKind::Missing, GapPosition(2, 40 - segment));
        facts.note_gap_servers([4, 7]);
    }
    db.save_active_support_facts(vec![(JobId(11), facts.to_storage_json())])
        .unwrap();

    // Live: read from the active row.
    let live =
        build_job_support_report(&db, Some(live_info(11)), None, 11, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&live);
    assert!(
        live.text
            .contains("demoted member_checksum_mismatch  in downloading  sets 2  2h ago"),
        "{}",
        live.text
    );

    // Finished: the archive copies the column onto the history row.
    db.archive_job(JobId(11), &history_row(11, "failed"))
        .unwrap();
    let report = build_job_support_report(&db, None, None, 11, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    let job = &json["job"];
    assert_eq!(job["source"], "history");
    assert_eq!(job["demotion"]["reason"], "member_checksum_mismatch");
    assert_eq!(job["demotion"]["stage"], "downloading");
    assert_eq!(job["demotion"]["sets"], 2);
    assert_eq!(job["demotion"]["age_secs"], 7_200);
    assert_eq!(job["gaps"]["missing"], 20);
    assert_eq!(job["gaps"]["failed"], 1);
    assert_eq!(job["gaps"]["servers"][0]["server"], "s7");
    assert_eq!(job["gaps"]["servers"][0]["refused"], 21);
    assert_eq!(job["gaps"]["servers"][1]["server"], "s4");
    let sample = job["gaps"]["sample"].as_array().unwrap();
    assert_eq!(sample.len(), 16);
    assert_eq!(sample[0], "f01#1");
    assert_eq!(sample[1], "f03#27");
    assert!(
        report
            .text
            .contains("  gaps missing 20  failed 1  refused s7 21 s4 20"),
        "{}",
        report.text
    );
    assert!(
        report.text.contains("  gap at f01#1 f03#27"),
        "{}",
        report.text
    );
    assert!(report.text.contains("(first 16 of 21)"), "{}", report.text);
}

#[test]
fn a_job_without_facts_reports_no_demotion_or_gap_lines() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(12)).unwrap();
    db.archive_job(JobId(12), &history_row(12, "completed"))
        .unwrap();
    let report = build_job_support_report(&db, None, None, 12, &ENVIRONMENT, NOW).unwrap();
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    assert!(json["job"]["demotion"].is_null());
    assert!(json["job"]["gaps"].is_null());
    assert!(!report.text.contains("demoted"));
    assert!(!report.text.contains("gap"));
}

#[test]
fn a_hand_edited_row_cannot_carry_names_into_the_report() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(13)).unwrap();
    let raw = format!(r#"{{"d":{{"r":"{SENTINEL}.part01.rar","s":"{SENTINEL}","t":1}}}}"#);
    db.save_active_support_facts(vec![(JobId(13), Some(raw))])
        .unwrap();
    let report =
        build_job_support_report(&db, Some(live_info(13)), None, 13, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    assert!(
        report.text.contains("demoted unknown  in other"),
        "{}",
        report.text
    );
}

// A finished job the scheduler still caches is reported from history, where
// its NZB now lives.
#[test]
fn a_cached_finished_job_reports_from_history() {
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(14)).unwrap();
    db.archive_job(JobId(14), &history_row(14, "failed"))
        .unwrap();
    let report =
        build_job_support_report(&db, Some(live_info(14)), None, 14, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    assert_eq!(json["job"]["source"], "history");
    assert_eq!(json["job"]["status"], "failed");
}

// Torrents.

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

const TRACKER: &str = "udp://tracker.example:6969/qvzsentinel/announce?passkey=qvzsentinel";

// The info dictionary of a private one-file torrent named after the
// sentinel, and the whole metainfo around it.
fn sentinel_torrent() -> (Vec<u8>, [u8; 20]) {
    let mut info = b"d".to_vec();
    bstr(&mut info, b"length");
    info.extend_from_slice(b"i3000e");
    bstr(&mut info, b"name");
    bstr(&mut info, format!("{SENTINEL}.part1.rar").as_bytes());
    bstr(&mut info, b"piece length");
    info.extend_from_slice(b"i1024e");
    bstr(&mut info, b"pieces");
    bstr(&mut info, &[7u8; 60]);
    bstr(&mut info, b"private");
    info.extend_from_slice(b"i1e");
    info.push(b'e');
    let hash: [u8; 20] = {
        use sha1::{Digest, Sha1};
        Sha1::digest(&info).into()
    };
    let mut out = b"d".to_vec();
    bstr(&mut out, b"announce");
    bstr(&mut out, TRACKER.as_bytes());
    bstr(&mut out, b"comment");
    bstr(&mut out, format!("{SENTINEL} comment").as_bytes());
    bstr(&mut out, b"created by");
    bstr(&mut out, SENTINEL.as_bytes());
    bstr(&mut out, b"creation date");
    out.extend_from_slice(format!("i{}e", NOW - 86_400).as_bytes());
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    (out, hash)
}

fn sentinel_magnet(hash: [u8; 20]) -> String {
    format!(
        "magnet:?xt=urn:btih:{}&dn={SENTINEL}&tr=udp%3A%2F%2Ftracker.example%3A6969%2F{SENTINEL}%2Fannounce",
        hex::encode(hash)
    )
}

fn assert_torrent_redacted(report: &SupportReport, hash: [u8; 20]) {
    assert_redacted(report);
    for (form, body) in [("text", &report.text), ("json", &report.json)] {
        assert!(
            !body.to_ascii_lowercase().contains(&hex::encode(hash)),
            "{form} leaks the infohash:\n{body}"
        );
        assert!(!body.contains("passkey"), "{form} leaks a passkey:\n{body}");
    }
}

#[test]
fn a_torrent_file_and_its_magnet_report_without_names() {
    let (metainfo, hash) = sentinel_torrent();
    let file = analyze_torrent_bytes(&metainfo, &ENVIRONMENT, NOW).unwrap();
    assert_torrent_redacted(&file, hash);
    let json: serde_json::Value = serde_json::from_str(&file.json).unwrap();
    assert!(json["job"].is_null());
    assert_eq!(json["torrent"]["identity"]["weaver_version"], "9.9.9");
    assert_eq!(json["torrent"]["trackers"]["private"], true);
    assert_eq!(
        json["torrent"]["trackers"]["trackers"][0]["host"],
        "tracker.example"
    );
    assert_eq!(json["environment"]["datastore"], "sqlite");
    assert!(file.text.contains("tracker.example"), "{}", file.text);
    assert!(!file.text.contains("job ("));

    let magnet = analyze_magnet(&sentinel_magnet(hash), &ENVIRONMENT, NOW).unwrap();
    assert_torrent_redacted(&magnet, hash);
    let magnet_json: serde_json::Value = serde_json::from_str(&magnet.json).unwrap();
    assert_eq!(
        magnet_json["torrent"]["identity"]["fingerprint"],
        json["torrent"]["identity"]["fingerprint"],
    );
    assert_eq!(magnet_json["torrent"]["identity"]["source"], "magnet");

    assert!(matches!(
        analyze_torrent_bytes(b"not bencode", &ENVIRONMENT, NOW),
        Err(SupportReportError::TorrentParse(_))
    ));
    assert!(matches!(
        analyze_magnet("magnet:?dn=nothing", &ENVIRONMENT, NOW),
        Err(SupportReportError::TorrentParse(_))
    ));
}

fn torrent_history_row(
    id: u64,
    hash: [u8; 20],
    status: &str,
    error: Option<&str>,
) -> JobHistoryRow {
    JobHistoryRow {
        protocol: crate::jobs::Protocol::Torrent,
        job_hash: Some(hash.to_vec()),
        error_message: error.map(str::to_owned),
        output_dir: Some(format!("/srv/{SENTINEL}/out")),
        category: Some(SENTINEL.to_string()),
        status: status.to_string(),
        ..history_row(id, status)
    }
}

#[test]
fn a_retired_torrent_reports_from_its_history_row() {
    let (metainfo, hash) = sentinel_torrent();
    let db = Database::open_in_memory().unwrap();
    db.create_active_job(&active_job(21)).unwrap();
    record_events(&db, 21, &["JobCreated", "JobFailed"]);
    db.archive_job(
        JobId(21),
        &torrent_history_row(
            21,
            hash,
            "failed",
            Some(&format!("tracker {SENTINEL} refused the announce")),
        ),
    )
    .unwrap();

    let report = build_job_support_report(&db, None, None, 21, &ENVIRONMENT, NOW).unwrap();
    assert_torrent_redacted(&report, hash);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    let job = &json["job"];
    assert_eq!(job["source"], "history");
    assert_eq!(job["status"], "failed");
    assert_eq!(job["failure"], "tracker");
    assert_eq!(job["output"]["kind"], "absolute");
    assert_eq!(job["category_set"], true);
    assert_eq!(job["checkpoint_in_hand"], false);
    assert_eq!(job["events"][1]["kind"], "JobFailed");
    assert_eq!(json["torrent"]["identity"]["source"], "magnet");
    // The hash it went by is what ties the report to the file's.
    let file = analyze_torrent_bytes(&metainfo, &ENVIRONMENT, NOW).unwrap();
    let file_json: serde_json::Value = serde_json::from_str(&file.json).unwrap();
    assert_eq!(
        json["torrent"]["identity"]["fingerprint"],
        file_json["torrent"]["identity"]["fingerprint"],
    );
    assert!(
        report.text.contains("job (history, torrent)"),
        "{}",
        report.text
    );
    assert!(report.text.contains("verdict tracker"), "{}", report.text);

    // A torrent row without its hash has nothing left to report.
    db.create_active_job(&active_job(22)).unwrap();
    db.archive_job(
        JobId(22),
        &JobHistoryRow {
            job_hash: None,
            ..torrent_history_row(22, hash, "completed", None)
        },
    )
    .unwrap();
    assert!(matches!(
        build_job_support_report(&db, None, None, 22, &ENVIRONMENT, NOW),
        Err(SupportReportError::TorrentUnavailable(22))
    ));
}

fn held_torrent(
    metainfo: Option<Vec<u8>>,
    hash: [u8; 20],
) -> crate::torrents::service::TorrentSupport {
    use crate::proxies::TorrentScope;
    use crate::settings::SeedingPolicy;
    use crate::torrents::service::{
        PeerKinds, SeedingSource, TorrentLeg, TorrentPieces, TorrentSupport, TorrentView,
    };
    use crate::torrents::sessions::TorrentRouteReason;
    use crate::torrents::{
        NamedBy, TorrentCounters, TorrentSource, TorrentState, TorrentTracker, TrackerStatus,
    };

    let now_ms = NOW as i64 * 1000;
    let view = TorrentView {
        job_id: JobId(31),
        name: SENTINEL.into(),
        infohash: hex::encode(hash),
        infohash_v1: Some(hex::encode(hash)),
        infohash_v2: None,
        version: metainfo
            .as_ref()
            .map(|_| weaver_torrent::TorrentVersion::V1),
        magnet: sentinel_magnet(hash),
        source: TorrentSource::Metainfo,
        state: TorrentState::Downloading,
        detail: None,
        route_reason: Some(TorrentRouteReason::LegDown),
        paused_by: None,
        category: Some(SENTINEL.into()),
        scope: TorrentScope::Category(4),
        private: true,
        forced: false,
        progress: 0.25,
        total_bytes: 3000,
        have_bytes: 1024,
        uploaded_bytes: 512,
        downloaded_bytes: 2048,
        ratio: 0.5,
        download_bps: 500,
        upload_bps: 0,
        upload_limit: None,
        download_limit: None,
        eta_seconds: None,
        peers: 3,
        seeds: 1,
        leechers: 2,
        max_peers: 100,
        swarm_seeds: None,
        swarm_leechers: None,
        reannounce_seconds: None,
        active_seconds: 120,
        download_avg_bps: 0,
        upload_avg_bps: 0,
        wasted_bytes: 2048,
        last_seen_complete_ms: None,
        pieces: Some(TorrentPieces {
            count: 3,
            size: 1024,
            have: 1,
        }),
        created_by: Some(SENTINEL.into()),
        created_at_ms: None,
        comment: Some(SENTINEL.into()),
        added_at_ms: now_ms - 600_000,
        completed_at_ms: None,
        storage_dir: Some(format!("/srv/{SENTINEL}")),
        files_dir: None,
        queue_position: None,
        seeding_left: None,
        route: Some("0123456789abcdef".into()),
        files: Vec::new(),
        sequential: false,
        first_last_piece_first: false,
        trackers: vec![TorrentTracker {
            tier: 0,
            url: TRACKER.into(),
            named_by: NamedBy::Torrent,
            status: TrackerStatus::TrackerError,
            updating: false,
            last_announce_ms: Some(now_ms - 30_000),
            next_announce_ms: Some(now_ms + 60_000),
            min_announce_ms: None,
            message: Some(format!("passkey {SENTINEL} not authorized")),
            seeders: Some(4),
            leechers: Some(2),
            downloaded: Some(9),
            peers: Some(6),
            fails: 2,
            resolved_by: None,
        }],
        file_have: Vec::new(),
        metadata: Vec::new(),
        seeding_override: None,
        seeding_inherited: None,
        seeding_policy: SeedingPolicy {
            ratio_goal: Some(2.0),
            ..SeedingPolicy::default()
        },
        seeding_source: SeedingSource::Category,
        unpack: true,
    };
    TorrentSupport {
        view,
        metainfo: metainfo.map(Into::into),
        counters: TorrentCounters {
            uploaded_bytes: 512,
            downloaded_bytes: 2048,
            active_duration_s: 120,
            ..TorrentCounters::default()
        },
        failed_bytes: 1024,
        redundant_bytes: 1024,
        peers: (3, 1, 2),
        peer_kinds: PeerKinds {
            tcp: 2,
            utp: 1,
            outgoing: 3,
            incoming: 0,
            encrypted: 1,
        },
        fetch: None,
        in_session: true,
        leg: Some(TorrentLeg {
            position: 1,
            egress_id: 3,
            own_sockets: false,
        }),
        has_job: true,
        kept_pieces: 0,
        wanted_bytes: 3000,
        wanted_have_bytes: 1024,
        metrics: published_metrics(),
    }
}

// The exporter's snapshot with job 31, its route's session and another
// torrent's figures beside it.
fn published_metrics() -> crate::torrents::metrics::TorrentMetrics {
    use crate::torrents::metrics::{
        FetchOutcome, Published, SeedingGoal, SeedingSeries, SessionSeries, TorrentEvent,
        TorrentMetrics, TorrentSeries, TrackerSeries,
    };
    use crate::torrents::service::TorrentPieces;
    use crate::torrents::{TorrentState, TrackerStatus};

    let now_ms = NOW as i64 * 1000;
    let series = |job: u64| TorrentSeries {
        job_id: JobId(job),
        category: SENTINEL.into(),
        route: "0123456789abcdef".into(),
        state: TorrentState::Downloading,
        have_bytes: 1024,
        total_bytes: 3000,
        wanted_bytes: 3000,
        pieces: Some(TorrentPieces {
            count: 3,
            size: 1024,
            have: 1,
        }),
        downloaded_bytes: 2048,
        uploaded_bytes: 512,
        hash_failed_bytes: 1024,
        redundant_bytes: 1024,
        active_seconds: 120,
        seeding_seconds: 0,
        download_bps: 500,
        upload_bps: 0,
        ratio: 0.5,
        peers_established: 3,
        peers_connecting: 2,
        peer_seeders: 1,
        leg: Some(("torrent:category:4".into(), 1)),
        fetch: None,
        seeding: SeedingSeries {
            goals: vec![(SeedingGoal::Ratio, 2.0, 1.5)],
            met: false,
        },
        trackers: vec![TrackerSeries {
            host: "tracker.example".into(),
            status: TrackerStatus::TrackerError,
            last_announce_ms: Some(now_ms - 30_000),
            next_announce_ms: Some(now_ms + 60_000),
            interval_seconds: Some(1800),
            seeders: Some(4),
            leechers: Some(2),
            completed: Some(9),
            fails: 2,
        }],
    };
    let mut other = series(32);
    other.state = TorrentState::Seeding;
    TorrentMetrics {
        published: std::sync::Arc::new(Published {
            torrents: vec![other, series(31)],
            sessions: vec![SessionSeries {
                route: "0123456789abcdef".into(),
                open: true,
                listen_port: Some(6881),
                external_addresses: 1,
                uptime_seconds: 600,
                received_payload_bytes: 2048,
                received_protocol_bytes: 100,
                sent_payload_bytes: 512,
                sent_protocol_bytes: 80,
                blocked_incoming: 1,
                blocked_outgoing: 0,
                peers: vec![("established", 3), ("connecting", 2)],
                connects: vec![("tcp", "v4", [5, 3, 2])],
                dht: None,
            }],
        }),
        events: vec![(TorrentEvent::Added, 2), (TorrentEvent::Failed, 0)],
        fetches: vec![(FetchOutcome::Resolved, 0)],
    }
}

#[test]
fn a_held_torrent_reports_what_the_service_holds() {
    let (metainfo, hash) = sentinel_torrent();
    let db = Database::open_in_memory().unwrap();
    record_events(&db, 31, &["JobCreated"]);
    let held = held_torrent(Some(metainfo), hash);

    let report = build_job_support_report(&db, None, Some(held), 31, &ENVIRONMENT, NOW).unwrap();
    assert_torrent_redacted(&report, hash);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    let job = &json["job"];
    assert_eq!(json["torrent"]["identity"]["source"], "metainfo");
    assert_eq!(job["source"], "queue");
    assert_eq!(job["state"], "downloading");
    assert_eq!(job["route_reason"], "leg_down");
    assert_eq!(job["scope"], "category c4");
    assert_eq!(job["leg"]["egress"], "e3");
    assert_eq!(job["private"], true);
    assert_eq!(job["counts"]["ratio_milli"], 500);
    assert_eq!(job["counts"]["hash_failed_bytes"], 1024);
    assert_eq!(job["pieces"]["hash_failures"], 1);
    assert_eq!(job["pieces"]["have"], 1);
    assert_eq!(job["peers"]["utp"], 1);
    assert_eq!(job["trackers"][0]["status"], "tracker_error");
    assert_eq!(job["trackers"][0]["message"], "authorization");
    assert_eq!(job["trackers"][0]["last_answer_age_secs"], 30);
    assert_eq!(job["trackers"][0]["next_announce_in_secs"], 60);
    assert_eq!(job["seeding"]["source"], "category");
    assert_eq!(job["seeding"]["ratio_left_milli"], 1500);
    assert_eq!(job["output"]["kind"], "absolute");
    assert!(
        report.text.contains("job (queue, torrent)"),
        "{}",
        report.text
    );
    assert!(
        report
            .text
            .contains("  tracker 0 udp tracker.example  tracker_error x2 (authorization)"),
        "{}",
        report.text
    );

    // The exporter's figures for this job, not its neighbour's.
    let metrics = &job["metrics"];
    assert_eq!(metrics["torrent"]["state"], "downloading");
    assert_eq!(metrics["torrent"]["category_set"], true);
    assert_eq!(metrics["torrent"]["leg"][0], "torrent:category:4");
    assert_eq!(metrics["torrent"]["seeding_goals"][0][0], "ratio");
    assert_eq!(metrics["torrent"]["trackers"][0]["interval_secs"], 1800);
    assert_eq!(metrics["session"]["listening"], true);
    assert_eq!(metrics["session"]["connects"][0][2][1], 3);
    assert_eq!(metrics["events"][0][0], "added");
    assert_eq!(metrics["events"][0][1], 2);
    assert!(
        report.text.contains("metrics (as exported)"),
        "{}",
        report.text
    );
    assert!(
        report.text.contains("    connects tcp/v4 3/5 x2"),
        "{}",
        report.text
    );
    assert!(!report.json.contains("6881"), "{}", report.json);

    // Before its metadata arrives, the link it was added by stands in.
    let report = build_job_support_report(
        &db,
        None,
        Some(held_torrent(None, hash)),
        31,
        &ENVIRONMENT,
        NOW,
    )
    .unwrap();
    assert_torrent_redacted(&report, hash);
    let json: serde_json::Value = serde_json::from_str(&report.json).unwrap();
    assert_eq!(json["torrent"]["identity"]["source"], "magnet");
    assert!(json["job"]["private"].is_null());
}

#[test]
fn torrent_failures_reduce_to_a_cause() {
    for (message, verdict) in [
        ("certificate verify failed", FailureVerdict::Certificate),
        ("metadata never arrived", FailureVerdict::Metadata),
        ("tracker refused the announce", FailureVerdict::Tracker),
        ("no route: egress down", FailureVerdict::Route),
        ("listen port unavailable", FailureVerdict::Session),
    ] {
        assert_eq!(FailureVerdict::classify(message), verdict, "{message}");
    }
}

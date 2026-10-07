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

/// An NZB whose every free-text field carries the sentinel.
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

    let report = build_job_support_report(&db, None, 7, &ENVIRONMENT, NOW).unwrap();
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

    let report = build_job_support_report(&db, Some(info), 3, &ENVIRONMENT, NOW).unwrap();
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
        build_job_support_report(&db, None, 99, &ENVIRONMENT, NOW),
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
    let live = build_job_support_report(&db, Some(live_info(11)), 11, &ENVIRONMENT, NOW).unwrap();
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
    let report = build_job_support_report(&db, None, 11, &ENVIRONMENT, NOW).unwrap();
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
    let report = build_job_support_report(&db, None, 12, &ENVIRONMENT, NOW).unwrap();
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
    let report = build_job_support_report(&db, Some(live_info(13)), 13, &ENVIRONMENT, NOW).unwrap();
    assert_redacted(&report);
    assert!(
        report.text.contains("demoted unknown  in other"),
        "{}",
        report.text
    );
}

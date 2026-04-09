use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::{JobInfo, JobStatus};

use super::*;

fn history(created_at: i64, completed_at: i64) -> JobHistoryRow {
    JobHistoryRow {
        job_id: 42,
        name: "job".into(),
        status: "complete".into(),
        error_message: None,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: None,
        nzb_path: None,
        created_at,
        completed_at,
        metadata: None,
    }
}

fn job(status: JobStatus) -> JobInfo {
    JobInfo {
        job_id: JobId(42),
        name: "job".into(),
        status,
        progress: 0.0,
        total_bytes: 0,
        downloaded_bytes: 0,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        password: None,
        category: None,
        metadata: Vec::new(),
        output_dir: None,
        error: None,
        created_at_epoch_ms: 1_000.0,
    }
}

fn event(kind: &str, timestamp: i64, file_id: Option<String>, message: &str) -> StoredJobEvent {
    StoredJobEvent {
        job_id: 42,
        timestamp,
        kind: kind.to_string(),
        message: message.to_string(),
        file_id,
    }
}

#[test]
fn builds_overlapping_member_timeline() {
    let member =
        crate::history::types::encode_timeline_member_subject("set", "episode01.mkv", None);
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        None,
        &[
            event("DownloadStarted", 2_000, None, ""),
            event("ExtractionMemberStarted", 2_500, member.clone(), ""),
            event("ExtractionMemberAppendStarted", 2_800, member.clone(), ""),
            event("ExtractionMemberAppendFinished", 3_000, member.clone(), ""),
            event("DownloadFinished", 3_200, None, ""),
            event("ExtractionMemberFinished", 3_300, member, ""),
            event("MoveToCompleteStarted", 3_400, None, ""),
            event("MoveToCompleteFinished", 3_500, None, ""),
            event("JobCompleted", 3_600, None, ""),
        ],
    );

    assert_eq!(timeline.lanes.len(), 4);
    assert_eq!(timeline.extraction_groups.len(), 1);
    assert_eq!(timeline.extraction_groups[0].members.len(), 1);
    assert_eq!(timeline.extraction_groups[0].members[0].spans.len(), 2);
}

#[test]
fn marks_failed_member_as_awaiting_repair_when_job_survives() {
    let member =
        crate::history::types::encode_timeline_member_subject("set", "episode02.mkv", None);
    let timeline = build_job_timeline(
        &job(JobStatus::Repairing),
        None,
        &[
            event("ExtractionMemberStarted", 2_000, member.clone(), ""),
            event("ExtractionMemberFailed", 2_500, member, "crc failed"),
            event("RepairStarted", 3_000, None, ""),
        ],
    );

    assert_eq!(
        timeline.extraction_groups[0].members[0].state,
        ExtractionMemberState::AwaitingRepair
    );
}

#[test]
fn preserves_pre_restart_history_and_marks_recovery_gap() {
    let member =
        crate::history::types::encode_timeline_member_subject("set", "episode03.mkv", None);
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(9, 12)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("ExtractionMemberStarted", 2_500, member.clone(), ""),
            event("JobCreated", 8_000, None, ""),
            event("DownloadStarted", 8_000, None, ""),
            event("JobFailed", 8_500, None, "restart"),
            event("JobResumed", 9_000, None, "resumed"),
            event("ExtractionMemberStarted", 9_200, member.clone(), ""),
            event("ExtractionMemberFinished", 9_600, member, ""),
            event("JobCompleted", 12_000, None, "completed"),
        ],
    );

    assert_eq!(timeline.started_at, 1_000.0);
    assert_eq!(timeline.lanes.len(), 4);
    assert_eq!(timeline.lanes[0].stage, TimelineStage::PendingDownload);
    assert_eq!(timeline.lanes[0].spans.len(), 1);
    assert_eq!(timeline.lanes[0].spans[0].started_at, 1_000.0);
    assert_eq!(timeline.lanes[0].spans[0].ended_at, Some(2_000.0));
    assert_eq!(
        timeline.lanes[0].spans[0].state,
        TimelineSpanState::Complete
    );
    assert_eq!(timeline.lanes[1].stage, TimelineStage::Downloading);
    assert_eq!(timeline.lanes[1].spans.len(), 2);
    assert_eq!(timeline.lanes[1].spans[0].started_at, 2_000.0);
    assert_eq!(timeline.lanes[1].spans[0].ended_at, Some(8_000.0));
    assert_eq!(
        timeline.lanes[1].spans[0].state,
        TimelineSpanState::Complete
    );
    assert_eq!(timeline.lanes[1].spans[1].started_at, 8_000.0);
    assert_eq!(timeline.lanes[1].spans[1].ended_at, Some(8_500.0));
    assert_eq!(timeline.lanes[1].spans[1].state, TimelineSpanState::Failed);
    assert_eq!(timeline.lanes[2].stage, TimelineStage::Extracting);
    assert_eq!(timeline.lanes[2].spans.len(), 2);
    assert_eq!(timeline.lanes[2].spans[0].started_at, 2_500.0);
    assert_eq!(timeline.lanes[2].spans[0].ended_at, Some(8_000.0));
    assert_eq!(timeline.lanes[2].spans[1].started_at, 9_200.0);
    assert_eq!(timeline.lanes[2].spans[1].ended_at, Some(9_600.0));
    assert_eq!(timeline.lanes[3].stage, TimelineStage::Interrupted);
    assert_eq!(timeline.lanes[3].spans.len(), 1);
    assert_eq!(timeline.lanes[3].spans[0].started_at, 8_500.0);
    assert_eq!(timeline.lanes[3].spans[0].ended_at, Some(9_000.0));
    assert_eq!(
        timeline.lanes[3].spans[0].label.as_deref(),
        Some("Recovery gap")
    );
    assert_eq!(timeline.extraction_groups.len(), 1);
    assert_eq!(timeline.extraction_groups[0].members.len(), 1);
    assert_eq!(
        timeline.extraction_groups[0].members[0].state,
        ExtractionMemberState::Complete
    );
    assert_eq!(timeline.extraction_groups[0].members[0].spans.len(), 2);
    assert_eq!(
        timeline.extraction_groups[0].members[0].spans[0].started_at,
        2_500.0
    );
    assert_eq!(
        timeline.extraction_groups[0].members[0].spans[1].started_at,
        9_200.0
    );
}

#[test]
fn tracks_pause_only_when_downloading() {
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(1, 10)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("JobPaused", 1_500, None, "paused while queued"),
            event("JobResumed", 2_000, None, "resumed while queued"),
            event("DownloadStarted", 3_000, None, ""),
            event("JobPaused", 4_000, None, "paused while downloading"),
            event("JobResumed", 6_000, None, "resumed"),
            event("DownloadStarted", 6_500, None, ""),
            event("JobCompleted", 10_000, None, "done"),
        ],
    );

    let paused_lane = timeline
        .lanes
        .iter()
        .find(|lane| lane.stage == TimelineStage::Paused)
        .expect("paused lane");
    assert_eq!(paused_lane.spans.len(), 1);
    assert_eq!(paused_lane.spans[0].started_at, 4_000.0);
    assert_eq!(paused_lane.spans[0].ended_at, Some(6_000.0));
    assert_eq!(paused_lane.spans[0].label.as_deref(), Some("Paused"));
    assert_eq!(
        timeline
            .lanes
            .iter()
            .find(|lane| lane.stage == TimelineStage::PendingDownload)
            .map(|lane| lane.spans.len()),
        Some(1)
    );
}

#[test]
fn tracks_pause_when_pause_follows_download_finish_at_same_timestamp() {
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(1, 12)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("DownloadFinished", 5_000, None, ""),
            event("JobPaused", 5_000, None, "paused"),
            event("JobResumed", 9_000, None, "resumed"),
            event("DownloadStarted", 9_000, None, ""),
            event("JobCompleted", 12_000, None, "done"),
        ],
    );

    let paused_lane = timeline
        .lanes
        .iter()
        .find(|lane| lane.stage == TimelineStage::Paused)
        .expect("paused lane");
    assert_eq!(paused_lane.spans.len(), 1);
    assert_eq!(paused_lane.spans[0].started_at, 5_000.0);
    assert_eq!(paused_lane.spans[0].ended_at, Some(9_000.0));
    assert_eq!(paused_lane.spans[0].label.as_deref(), Some("Paused"));
}

#[test]
fn keeps_pause_lane_open_for_currently_paused_job() {
    let timeline = build_job_timeline(
        &job(JobStatus::Paused),
        None,
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("DownloadFinished", 4_000, None, ""),
            event("JobPaused", 4_000, None, "paused"),
        ],
    );

    let paused_lane = timeline
        .lanes
        .iter()
        .find(|lane| lane.stage == TimelineStage::Paused)
        .expect("paused lane");
    assert_eq!(paused_lane.spans.len(), 1);
    assert_eq!(paused_lane.spans[0].started_at, 4_000.0);
    assert_eq!(paused_lane.spans[0].ended_at, None);
    assert_eq!(paused_lane.spans[0].state, TimelineSpanState::Running);
}

#[test]
fn pause_does_not_split_extraction_spans() {
    let member =
        crate::history::types::encode_timeline_member_subject("set", "episode04.mkv", None);
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(1, 8)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("ExtractionMemberStarted", 3_000, member.clone(), ""),
            event("JobPaused", 4_000, None, "paused"),
            event("JobResumed", 6_000, None, "resumed"),
            event("ExtractionMemberFinished", 7_000, member, ""),
            event("JobCompleted", 8_000, None, "done"),
        ],
    );

    let paused_lane = timeline
        .lanes
        .iter()
        .find(|lane| lane.stage == TimelineStage::Paused)
        .expect("paused lane");
    assert_eq!(paused_lane.spans.len(), 1);
    assert_eq!(paused_lane.spans[0].started_at, 4_000.0);
    assert_eq!(paused_lane.spans[0].ended_at, Some(6_000.0));

    assert_eq!(timeline.extraction_groups.len(), 1);
    assert_eq!(timeline.extraction_groups[0].members.len(), 1);
    assert_eq!(timeline.extraction_groups[0].members[0].spans.len(), 1);
    assert_eq!(
        timeline.extraction_groups[0].members[0].spans[0].started_at,
        3_000.0
    );
    assert_eq!(
        timeline.extraction_groups[0].members[0].spans[0].ended_at,
        Some(7_000.0)
    );
}

#[test]
fn download_resumes_after_pause_without_explicit_second_start() {
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(1, 10)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("JobPaused", 4_000, None, "paused"),
            event("JobResumed", 6_000, None, "resumed"),
            event("JobCompleted", 10_000, None, "done"),
        ],
    );

    let download_lane = timeline
        .lanes
        .iter()
        .find(|lane| lane.stage == TimelineStage::Downloading)
        .expect("download lane");
    assert_eq!(download_lane.spans.len(), 2);
    assert_eq!(download_lane.spans[0].started_at, 2_000.0);
    assert_eq!(download_lane.spans[0].ended_at, Some(4_000.0));
    assert_eq!(download_lane.spans[1].started_at, 6_000.0);
    assert_eq!(download_lane.spans[1].ended_at, Some(10_000.0));
}

#[test]
fn tracks_waiting_for_volume_as_distinct_member_span() {
    let member =
        crate::history::types::encode_timeline_member_subject("set", "episode05.mkv", None);
    let waiting_subject =
        crate::history::types::encode_timeline_member_subject("set", "episode05.mkv", Some(7));
    let timeline = build_job_timeline(
        &job(JobStatus::Complete),
        Some(&history(1, 9)),
        &[
            event("JobCreated", 1_000, None, ""),
            event("DownloadStarted", 2_000, None, ""),
            event("ExtractionMemberStarted", 3_000, member.clone(), ""),
            event(
                "ExtractionMemberWaitingStarted",
                3_500,
                waiting_subject.clone(),
                "waiting",
            ),
            event(
                "ExtractionMemberWaitingFinished",
                5_000,
                waiting_subject,
                "resumed",
            ),
            event("ExtractionMemberAppendStarted", 5_500, member.clone(), ""),
            event("ExtractionMemberAppendFinished", 5_800, member.clone(), ""),
            event("ExtractionMemberFinished", 5_800, member, ""),
            event("JobCompleted", 9_000, None, ""),
        ],
    );

    let member_timeline = &timeline.extraction_groups[0].members[0];
    assert_eq!(member_timeline.spans.len(), 4);
    assert_eq!(
        member_timeline.spans[1].kind,
        ExtractionMemberSpanKind::WaitingForVolume
    );
    assert_eq!(member_timeline.spans[1].started_at, 3_500.0);
    assert_eq!(member_timeline.spans[1].ended_at, Some(5_000.0));
    assert_eq!(member_timeline.spans[1].label.as_deref(), Some("Volume 7"));
    assert_eq!(
        member_timeline.spans[2].kind,
        ExtractionMemberSpanKind::Extracting
    );
    assert_eq!(member_timeline.spans[2].started_at, 5_000.0);
}

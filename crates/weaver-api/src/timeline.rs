use std::collections::BTreeMap;

use weaver_scheduler::JobInfo;
use weaver_state::{JobEvent as StoredJobEvent, JobHistoryRow};

use crate::types::{
    EventKind, ExtractionMemberSpanKind, ExtractionMemberState, ExtractionMemberTimeline,
    ExtractionMemberTimelineSpan, ExtractionTimelineGroup, JobStatusGql, JobTimeline,
    JobTimelineLane, JobTimelineSpan, TimelineSpanState, TimelineStage,
    decode_timeline_member_subject,
};

#[derive(Default)]
struct MemberAccumulator {
    spans: Vec<ExtractionMemberTimelineSpan>,
    open_extract_idx: Option<usize>,
    open_wait_idx: Option<usize>,
    open_append_idx: Option<usize>,
    last_error: Option<String>,
    completed: bool,
    interrupted: bool,
}

pub(crate) fn build_job_timeline(
    job: &JobInfo,
    history: Option<&JobHistoryRow>,
    events: &[StoredJobEvent],
) -> JobTimeline {
    let current_attempt_started_at = history
        .map(|row| row.created_at as f64 * 1000.0)
        .unwrap_or(job.created_at_epoch_ms);
    let started_at = events
        .first()
        .map(|event| (event.timestamp as f64).min(current_attempt_started_at))
        .unwrap_or(current_attempt_started_at);

    let outcome = JobStatusGql::from(&job.status);
    let now = epoch_ms_now();
    let terminal_event_at =
        events
            .iter()
            .rev()
            .find_map(|event| match event.kind.parse::<EventKind>().ok()? {
                EventKind::JobCompleted | EventKind::JobFailed => Some(event.timestamp as f64),
                _ => None,
            });
    let ended_at = terminal_event_at.or_else(|| {
        history.and_then(|row| {
            matches!(outcome, JobStatusGql::Complete | JobStatusGql::Failed)
                .then_some(row.completed_at as f64 * 1000.0)
        })
    });

    let download_spans = collect_download_spans(events);
    let pause_spans = collect_pause_spans(events);
    let verify_spans = collect_job_spans(
        events,
        EventKind::JobVerificationStarted,
        EventKind::JobVerificationComplete,
        stage_boundary_state,
    );
    let repair_spans = collect_job_spans(
        events,
        EventKind::RepairStarted,
        EventKind::RepairComplete,
        stage_boundary_state,
    );
    let final_move_spans = collect_job_spans(
        events,
        EventKind::MoveToCompleteStarted,
        EventKind::MoveToCompleteFinished,
        stage_boundary_state,
    );
    let interruption_spans = collect_interruption_spans(events);

    let first_download_started = download_spans.first().map(|span| span.started_at);
    let pending_spans = match first_download_started {
        Some(first) if first > started_at => vec![JobTimelineSpan {
            started_at,
            ended_at: Some(first),
            state: TimelineSpanState::Complete,
            label: None,
        }],
        None if matches!(outcome, JobStatusGql::Queued | JobStatusGql::Paused) => {
            vec![JobTimelineSpan {
                started_at,
                ended_at: None,
                state: TimelineSpanState::Running,
                label: None,
            }]
        }
        _ => Vec::new(),
    };

    let (extraction_groups, extracting_spans) =
        build_extraction_groups(events, outcome, ended_at, now);

    let mut lanes = Vec::new();
    push_lane(&mut lanes, TimelineStage::PendingDownload, pending_spans);
    push_lane(
        &mut lanes,
        TimelineStage::Downloading,
        close_open_job_spans(download_spans, ended_at, outcome, now),
    );
    push_lane(
        &mut lanes,
        TimelineStage::Paused,
        close_open_job_spans(pause_spans, ended_at, outcome, now),
    );
    push_lane(
        &mut lanes,
        TimelineStage::Verifying,
        close_open_job_spans(verify_spans, ended_at, outcome, now),
    );
    push_lane(
        &mut lanes,
        TimelineStage::Repairing,
        close_open_job_spans(repair_spans, ended_at, outcome, now),
    );
    push_lane(&mut lanes, TimelineStage::Extracting, extracting_spans);
    push_lane(&mut lanes, TimelineStage::Interrupted, interruption_spans);
    push_lane(
        &mut lanes,
        TimelineStage::FinalMove,
        close_open_job_spans(final_move_spans, ended_at, outcome, now),
    );

    JobTimeline {
        started_at,
        ended_at,
        outcome,
        lanes,
        extraction_groups,
    }
}

fn collect_job_spans(
    events: &[StoredJobEvent],
    start_kind: EventKind,
    end_kind: EventKind,
    boundary_state: impl Fn(EventKind) -> Option<TimelineSpanState>,
) -> Vec<JobTimelineSpan> {
    let mut spans = Vec::new();
    let mut open: Option<usize> = None;

    for event in events {
        let Ok(kind) = event.kind.parse::<EventKind>() else {
            continue;
        };
        if kind == start_kind {
            if open.is_none() {
                spans.push(JobTimelineSpan {
                    started_at: event.timestamp as f64,
                    ended_at: None,
                    state: TimelineSpanState::Running,
                    label: None,
                });
                open = Some(spans.len() - 1);
            }
        } else if kind == end_kind {
            close_open_job_span(
                &mut spans,
                &mut open,
                event.timestamp as f64,
                TimelineSpanState::Complete,
            );
        } else if let Some(boundary_state) = boundary_state(kind) {
            close_open_job_span(
                &mut spans,
                &mut open,
                event.timestamp as f64,
                boundary_state,
            );
        }
    }

    spans
}

fn collect_download_spans(events: &[StoredJobEvent]) -> Vec<JobTimelineSpan> {
    let mut spans = Vec::new();
    let mut open: Option<usize> = None;
    let mut reopen_after_pause = false;

    for event in events {
        let Ok(kind) = event.kind.parse::<EventKind>() else {
            continue;
        };
        let at = event.timestamp as f64;

        match kind {
            EventKind::DownloadStarted => {
                if open.is_none() {
                    spans.push(JobTimelineSpan {
                        started_at: at,
                        ended_at: None,
                        state: TimelineSpanState::Running,
                        label: None,
                    });
                    open = Some(spans.len() - 1);
                }
                reopen_after_pause = false;
            }
            EventKind::DownloadFinished => {
                close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Complete);
                reopen_after_pause = false;
            }
            EventKind::JobPaused => {
                if open.is_some() {
                    close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Complete);
                    reopen_after_pause = true;
                }
            }
            EventKind::JobResumed => {
                if reopen_after_pause && open.is_none() {
                    spans.push(JobTimelineSpan {
                        started_at: at,
                        ended_at: None,
                        state: TimelineSpanState::Running,
                        label: None,
                    });
                    open = Some(spans.len() - 1);
                }
                reopen_after_pause = false;
            }
            EventKind::JobCreated | EventKind::JobCompleted => {
                close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Complete);
                reopen_after_pause = false;
            }
            EventKind::JobFailed => {
                close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Failed);
                reopen_after_pause = false;
            }
            _ => {}
        }
    }

    spans
}

fn collect_pause_spans(events: &[StoredJobEvent]) -> Vec<JobTimelineSpan> {
    let mut spans = Vec::new();
    let mut open: Option<usize> = None;
    let mut download_active = false;
    let mut pause_boundary_at: Option<f64> = None;

    for event in events {
        let Ok(kind) = event.kind.parse::<EventKind>() else {
            continue;
        };
        let at = event.timestamp as f64;

        match kind {
            EventKind::DownloadStarted => {
                download_active = true;
                pause_boundary_at = None;
            }
            EventKind::DownloadFinished
            | EventKind::JobFailed
            | EventKind::JobCompleted
            | EventKind::JobCreated => {
                if kind == EventKind::DownloadFinished && download_active {
                    pause_boundary_at = Some(at);
                } else {
                    pause_boundary_at = None;
                }
                download_active = false;
                close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Complete);
            }
            EventKind::JobPaused => {
                if open.is_none() && (download_active || pause_boundary_at == Some(at)) {
                    spans.push(JobTimelineSpan {
                        started_at: at,
                        ended_at: None,
                        state: TimelineSpanState::Running,
                        label: Some("Paused".to_string()),
                    });
                    open = Some(spans.len() - 1);
                }
                download_active = false;
                pause_boundary_at = None;
            }
            EventKind::JobResumed => {
                close_open_job_span(&mut spans, &mut open, at, TimelineSpanState::Complete);
                pause_boundary_at = None;
            }
            _ => {
                if pause_boundary_at.is_some() && pause_boundary_at != Some(at) {
                    pause_boundary_at = None;
                }
            }
        }
    }

    spans
}

fn close_open_job_span(
    spans: &mut [JobTimelineSpan],
    open: &mut Option<usize>,
    ended_at: f64,
    state: TimelineSpanState,
) {
    if let Some(index) = open.take() {
        spans[index].ended_at = Some(ended_at);
        spans[index].state = state;
    }
}

fn stage_boundary_state(kind: EventKind) -> Option<TimelineSpanState> {
    match kind {
        EventKind::JobCreated | EventKind::JobCompleted => Some(TimelineSpanState::Complete),
        EventKind::JobFailed => Some(TimelineSpanState::Failed),
        _ => None,
    }
}

fn collect_interruption_spans(events: &[StoredJobEvent]) -> Vec<JobTimelineSpan> {
    let mut spans = Vec::new();
    let mut pending_gap: Option<(f64, TimelineSpanState, &'static str)> = None;

    for event in events {
        let Ok(kind) = event.kind.parse::<EventKind>() else {
            continue;
        };
        let at = event.timestamp as f64;

        match kind {
            EventKind::JobFailed => {
                pending_gap = Some((at, TimelineSpanState::Failed, "Recovery gap"));
            }
            EventKind::JobResumed | EventKind::JobCreated => {
                if let Some((start, state, label)) = pending_gap.take()
                    && at > start
                {
                    spans.push(JobTimelineSpan {
                        started_at: start,
                        ended_at: Some(at),
                        state,
                        label: Some(label.to_string()),
                    });
                }
            }
            _ => {}
        }
    }

    spans
}

fn close_open_job_spans(
    mut spans: Vec<JobTimelineSpan>,
    ended_at: Option<f64>,
    outcome: JobStatusGql,
    now: f64,
) -> Vec<JobTimelineSpan> {
    for span in &mut spans {
        if span.ended_at.is_none() {
            match outcome {
                JobStatusGql::Failed => {
                    span.ended_at = Some(ended_at.unwrap_or(now));
                    span.state = TimelineSpanState::Failed;
                }
                JobStatusGql::Complete => {
                    span.ended_at = ended_at.or(Some(now));
                    span.state = TimelineSpanState::Complete;
                }
                _ => {
                    span.state = TimelineSpanState::Running;
                }
            }
        }
    }
    spans
}

fn build_extraction_groups(
    events: &[StoredJobEvent],
    outcome: JobStatusGql,
    ended_at: Option<f64>,
    now: f64,
) -> (Vec<ExtractionTimelineGroup>, Vec<JobTimelineSpan>) {
    let mut groups: BTreeMap<String, BTreeMap<String, MemberAccumulator>> = BTreeMap::new();

    for event in events {
        let Ok(kind) = event.kind.parse::<EventKind>() else {
            continue;
        };
        let at = event.timestamp as f64;
        if let Some(state) = extraction_boundary_state(kind) {
            close_open_extraction_groups(&mut groups, at, state);
            continue;
        }
        let Some(subject) = decode_timeline_member_subject(event.file_id.as_deref()) else {
            continue;
        };

        let members = groups.entry(subject.set_name.clone()).or_default();
        let member = members.entry(subject.member.clone()).or_default();

        match kind {
            EventKind::ExtractionMemberStarted => {
                member.spans.push(ExtractionMemberTimelineSpan {
                    kind: ExtractionMemberSpanKind::Extracting,
                    started_at: at,
                    ended_at: None,
                    state: TimelineSpanState::Running,
                    label: None,
                });
                member.open_extract_idx = Some(member.spans.len() - 1);
                member.completed = false;
                member.last_error = None;
            }
            EventKind::ExtractionMemberWaitingStarted => {
                close_open_member_extract_span(member, at, TimelineSpanState::Complete);
                member.spans.push(ExtractionMemberTimelineSpan {
                    kind: ExtractionMemberSpanKind::WaitingForVolume,
                    started_at: at,
                    ended_at: None,
                    state: TimelineSpanState::Running,
                    label: subject.volume_index.map(|index| format!("Volume {index}")),
                });
                member.open_wait_idx = Some(member.spans.len() - 1);
            }
            EventKind::ExtractionMemberWaitingFinished => {
                if let Some(index) = member.open_wait_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
                member.spans.push(ExtractionMemberTimelineSpan {
                    kind: ExtractionMemberSpanKind::Extracting,
                    started_at: at,
                    ended_at: None,
                    state: TimelineSpanState::Running,
                    label: None,
                });
                member.open_extract_idx = Some(member.spans.len() - 1);
            }
            EventKind::ExtractionMemberAppendStarted => {
                if let Some(index) = member.open_wait_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
                member.spans.push(ExtractionMemberTimelineSpan {
                    kind: ExtractionMemberSpanKind::Appending,
                    started_at: at,
                    ended_at: None,
                    state: TimelineSpanState::Running,
                    label: None,
                });
                member.open_append_idx = Some(member.spans.len() - 1);
            }
            EventKind::ExtractionMemberAppendFinished => {
                if let Some(index) = member.open_append_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
            }
            EventKind::ExtractionMemberFinished => {
                if let Some(index) = member.open_wait_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
                if let Some(index) = member.open_append_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
                if let Some(index) = member.open_extract_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Complete;
                }
                member.completed = true;
                member.interrupted = false;
                member.last_error = None;
            }
            EventKind::ExtractionMemberFailed => {
                if let Some(index) = member.open_wait_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Failed;
                }
                if let Some(index) = member.open_append_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Failed;
                }
                if let Some(index) = member.open_extract_idx.take() {
                    member.spans[index].ended_at = Some(at);
                    member.spans[index].state = TimelineSpanState::Failed;
                }
                member.completed = false;
                member.interrupted = false;
                member.last_error = Some(event.message.clone());
            }
            _ => {}
        }
    }

    let mut extraction_groups = Vec::new();
    let mut aggregate_ranges = Vec::new();

    for (set_name, members) in groups {
        let mut member_rows = Vec::new();
        for (member_name, mut member) in members {
            for span in &mut member.spans {
                if span.ended_at.is_none() {
                    match outcome {
                        JobStatusGql::Failed => {
                            span.ended_at = Some(ended_at.unwrap_or(now));
                            span.state = TimelineSpanState::Failed;
                        }
                        JobStatusGql::Complete => {
                            span.ended_at = ended_at.or(Some(now));
                            span.state = TimelineSpanState::Complete;
                        }
                        _ => {
                            span.state = TimelineSpanState::Running;
                        }
                    }
                }
                let range_end = span.ended_at.unwrap_or(now);
                aggregate_ranges.push((span.started_at, range_end, span.state));
            }

            let state = if member.completed {
                ExtractionMemberState::Complete
            } else if member.open_extract_idx.is_some()
                || member.open_wait_idx.is_some()
                || member.open_append_idx.is_some()
            {
                ExtractionMemberState::Running
            } else if member.last_error.is_some() && outcome == JobStatusGql::Failed {
                ExtractionMemberState::Failed
            } else if member.last_error.is_some() {
                ExtractionMemberState::AwaitingRepair
            } else if member.interrupted {
                ExtractionMemberState::Interrupted
            } else {
                ExtractionMemberState::Running
            };

            member_rows.push(ExtractionMemberTimeline {
                member: member_name,
                state,
                error: member.last_error,
                spans: member.spans,
            });
        }

        extraction_groups.push(ExtractionTimelineGroup {
            set_name,
            members: member_rows,
        });
    }

    let extracting_spans = merge_ranges(aggregate_ranges);
    (extraction_groups, extracting_spans)
}

fn extraction_boundary_state(kind: EventKind) -> Option<TimelineSpanState> {
    match kind {
        EventKind::JobCreated | EventKind::JobCompleted => Some(TimelineSpanState::Complete),
        EventKind::JobFailed => Some(TimelineSpanState::Failed),
        _ => None,
    }
}

fn close_open_extraction_groups(
    groups: &mut BTreeMap<String, BTreeMap<String, MemberAccumulator>>,
    at: f64,
    state: TimelineSpanState,
) {
    for members in groups.values_mut() {
        for member in members.values_mut() {
            if close_open_member_spans(member, at, state) {
                member.interrupted = true;
            }
        }
    }
}

fn close_open_member_spans(
    member: &mut MemberAccumulator,
    at: f64,
    state: TimelineSpanState,
) -> bool {
    let mut closed = false;
    if let Some(index) = member.open_wait_idx.take() {
        member.spans[index].ended_at = Some(at);
        member.spans[index].state = state;
        closed = true;
    }
    if let Some(index) = member.open_append_idx.take() {
        member.spans[index].ended_at = Some(at);
        member.spans[index].state = state;
        closed = true;
    }
    if let Some(index) = member.open_extract_idx.take() {
        member.spans[index].ended_at = Some(at);
        member.spans[index].state = state;
        closed = true;
    }
    closed
}

fn close_open_member_extract_span(
    member: &mut MemberAccumulator,
    at: f64,
    state: TimelineSpanState,
) {
    if let Some(index) = member.open_extract_idx.take() {
        member.spans[index].ended_at = Some(at);
        member.spans[index].state = state;
    }
}

fn merge_ranges(mut ranges: Vec<(f64, f64, TimelineSpanState)>) -> Vec<JobTimelineSpan> {
    if ranges.is_empty() {
        return Vec::new();
    }
    ranges.sort_by(|left, right| left.0.total_cmp(&right.0));

    let mut merged = Vec::new();
    let mut current_start = ranges[0].0;
    let mut current_end = ranges[0].1;
    let mut current_state = ranges[0].2;

    for (start, end, state) in ranges.into_iter().skip(1) {
        if start <= current_end {
            current_end = current_end.max(end);
            if matches!(state, TimelineSpanState::Failed) {
                current_state = TimelineSpanState::Failed;
            } else if matches!(state, TimelineSpanState::Running)
                && !matches!(current_state, TimelineSpanState::Failed)
            {
                current_state = TimelineSpanState::Running;
            }
            continue;
        }

        merged.push(JobTimelineSpan {
            started_at: current_start,
            ended_at: Some(current_end),
            state: current_state,
            label: None,
        });
        current_start = start;
        current_end = end;
        current_state = state;
    }

    merged.push(JobTimelineSpan {
        started_at: current_start,
        ended_at: Some(current_end),
        state: current_state,
        label: None,
    });
    merged
}

fn push_lane(lanes: &mut Vec<JobTimelineLane>, stage: TimelineStage, spans: Vec<JobTimelineSpan>) {
    if spans.is_empty() {
        return;
    }
    lanes.push(JobTimelineLane { stage, spans });
}

fn epoch_ms_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as f64
}

#[cfg(test)]
mod tests {
    use weaver_core::id::JobId;
    use weaver_scheduler::{JobInfo, JobStatus};

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
        let member = crate::types::encode_timeline_member_subject("set", "episode01.mkv", None);
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
        let member = crate::types::encode_timeline_member_subject("set", "episode02.mkv", None);
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
        let member = crate::types::encode_timeline_member_subject("set", "episode03.mkv", None);
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
        let member = crate::types::encode_timeline_member_subject("set", "episode04.mkv", None);
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
        let member = crate::types::encode_timeline_member_subject("set", "episode05.mkv", None);
        let waiting_subject =
            crate::types::encode_timeline_member_subject("set", "episode05.mkv", Some(7));
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
}

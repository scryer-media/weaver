use std::collections::BTreeMap;

use weaver_server_core::JobInfo;
use weaver_server_core::{JobEvent as StoredJobEvent, JobHistoryRow};

use crate::history::types::{
    EventKind, ExtractionMemberSpanKind, ExtractionMemberState, ExtractionMemberTimeline,
    ExtractionMemberTimelineSpan, ExtractionTimelineGroup, JobTimeline, JobTimelineLane,
    JobTimelineSpan, TimelineSpanState, TimelineStage, decode_timeline_member_subject,
};
use crate::jobs::types::JobStatusGql;

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
            EventKind::JobFailed | EventKind::JobCancelled => {
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
            | EventKind::JobCancelled
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
        EventKind::JobFailed | EventKind::JobCancelled => Some(TimelineSpanState::Failed),
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
            EventKind::JobFailed | EventKind::JobCancelled => {
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
                close_open_member_extract_span(member, at, TimelineSpanState::Complete);
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
mod tests;

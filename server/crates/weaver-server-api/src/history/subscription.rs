// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[derive(Default)]
pub(crate) struct HistorySubscription;

const JOB_DETAIL_HEARTBEAT: Duration = Duration::from_millis(500);
const JOB_DETAIL_THROTTLE: Duration = Duration::from_millis(250);
const JOB_DETAIL_SETTLE_DELAY: Duration = Duration::from_millis(25);
// Slow cadence at which a terminal (archived) job's detail is still re-read, to
// pick up DB-only changes that emit no pipeline event (e.g. a background
// history-delete operation) without the fast per-tab heartbeat load.
const JOB_DETAIL_TERMINAL_HEARTBEAT: Duration = Duration::from_secs(5);

#[Subscription]
impl HistorySubscription {
    /// Subscribe to all pipeline events.
    #[graphql(guard = "AdminGuard")]
    async fn events(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        Ok(raw_pipeline_event_stream(handle))
    }
    /// Admin-facing raw event stream.
    #[graphql(guard = "AdminGuard")]
    async fn admin_events(
        &self,
        ctx: &Context<'_>,
    ) -> Result<impl Stream<Item = PipelineEventGql>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        Ok(raw_pipeline_event_stream(handle))
    }

    /// Read-facing live snapshot for the job detail page.
    #[graphql(guard = "ReadGuard")]
    async fn job_detail_updates(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<impl Stream<Item = JobDetailSnapshot>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<Database>()?.clone();
        // Resolved once per subscription: a server rename mid-stream is a
        // cosmetic staleness that the next page load corrects, and re-reading
        // the config on every 500 ms heartbeat is not worth that.
        let server_hosts =
            crate::schema::history_query::server_hosts_by_id(ctx.data::<SharedConfig>()?).await;
        let event_rx = handle.subscribe_events();

        // Only events for THIS job should force a database reload; an unrelated
        // job's event must not trigger a full re-read of this job's event log.
        let event_stream =
            tokio_stream::wrappers::BroadcastStream::new(event_rx).filter_map(move |result| {
                result.ok().and_then(|event| {
                    // Reload only on meaningful events for THIS job. `should_record_job_event`
                    // is the same predicate that gates the persisted event log the detail view
                    // reads, so it triggers exactly when the log or lifecycle changes while
                    // excluding the lower-frequency file and phase events (the per-article
                    // stream is a separate channel). Live progress is covered by the heartbeat. Uses the
                    // core job_id accessor (all variants, no allocation).
                    use weaver_server_core::events::publish::{
                        pipeline_job_id, should_record_job_event,
                    };
                    (should_record_job_event(&event) && pipeline_job_id(&event) == Some(job_id))
                        .then_some(JobDetailTrigger::Event)
                })
            });
        let heartbeat = tokio_stream::wrappers::IntervalStream::new(tokio::time::interval(
            JOB_DETAIL_HEARTBEAT,
        ))
        .map(|_| JobDetailTrigger::Heartbeat);
        let initial = tokio_stream::once(JobDetailTrigger::Initial);
        let triggers = throttle(
            initial.merge(event_stream).merge(heartbeat),
            JOB_DETAIL_THROTTLE,
        );

        Ok(async_stream::stream! {
            tokio::pin!(triggers);
            let mut terminal_settled = false;
            let mut last_terminal_reload = tokio::time::Instant::now();
            let mut gate = JobDetailReloadGate::default();

            while let Some(trigger) = triggers.next().await {
                let heartbeat = matches!(trigger, JobDetailTrigger::Heartbeat);
                // A terminal (archived) job's event log and history are
                // immutable, so the fast per-tab heartbeat need not re-read the
                // database. But DB-only changes that emit no pipeline event
                // (e.g. a background history-delete) must still surface, so a
                // terminal job is checked at a slow cadence, and rebuilt only
                // when its history row or delete state moved.
                if heartbeat && terminal_settled {
                    if last_terminal_reload.elapsed() < JOB_DETAIL_TERMINAL_HEARTBEAT {
                        continue;
                    }
                    last_terminal_reload = tokio::time::Instant::now();
                    match terminal_mark(db.clone(), job_id).await {
                        Ok(mark) => {
                            if !gate.terminal_moved(mark) {
                                continue;
                            }
                        }
                        Err(error) => {
                            tracing::warn!(job_id, error = ?error, "failed to check job detail history");
                        }
                    }
                }
                // A live job's heartbeat rebuilds only when this job's own
                // queue row changed: its status, phases, counters or files.
                // Another job's progress leaves this snapshot as it was sent.
                let live = live_fingerprint(&handle, job_id);
                if heartbeat && !terminal_settled && !gate.live_moved(live) {
                    continue;
                }
                if matches!(trigger, JobDetailTrigger::Event) {
                    tokio::time::sleep(JOB_DETAIL_SETTLE_DELAY).await;
                }

                match crate::schema::history_query::load_job_detail_snapshot(
                    handle.clone(),
                    db.clone(),
                    job_id,
                    server_hosts.clone(),
                ).await {
                    Ok(snapshot) => {
                        // A job with no live queue entry has settled to the slow
                        // JOB_DETAIL_TERMINAL_HEARTBEAT cadence, whether it is
                        // archived (history row present) OR fully gone (history
                        // row deleted while this tab was open). Not gating on
                        // `history_item.is_some()` is what keeps a just-deleted
                        // job from flipping back to the 500ms heartbeat and
                        // reloading the DB forever. This is safe for the brief
                        // completion gap (queue entry removed before the archive
                        // row is visible) because every real transition emits
                        // pipeline events that pass the should_record_job_event
                        // filter and force an immediate reload regardless of
                        // settling.
                        terminal_settled = snapshot.queue_item.is_none();
                        if terminal_settled {
                            last_terminal_reload = tokio::time::Instant::now();
                        }
                        yield snapshot;
                    }
                    Err(error) => tracing::warn!(job_id, error = ?error, "failed to build job detail snapshot"),
                }
            }
        })
    }
}

// What a job detail heartbeat compares to decide whether a rebuild could
// differ from the last snapshot sent.
#[derive(Default)]
struct JobDetailReloadGate {
    live: Option<Vec<u8>>,
    terminal: Option<TerminalMark>,
}

impl JobDetailReloadGate {
    // Records `live` and answers whether it differs from the last one. A job
    // the queue no longer holds always counts as moved.
    fn live_moved(&mut self, live: Option<Vec<u8>>) -> bool {
        let moved = live.is_none() || live != self.live;
        self.live = live;
        moved
    }

    // Records `mark` and answers whether it differs from the last one.
    fn terminal_moved(&mut self, mark: TerminalMark) -> bool {
        let moved = self.terminal.as_ref() != Some(&mark);
        self.terminal = Some(mark);
        moved
    }
}

// This job's queue row, serialized, or `None` when the queue holds no row
// for it. Read from the queue's shared rows; copies this one row alone.
fn live_fingerprint(handle: &SchedulerHandle, job_id: u64) -> Option<Vec<u8>> {
    let info = handle.get_job(JobId(job_id)).ok()?;
    serde_json::to_vec(&info).ok()
}

// What a terminal job's snapshot can still change by: whether its history row
// exists, and its delete state.
#[derive(Debug, PartialEq, Eq)]
struct TerminalMark {
    archived: bool,
    delete_state: Option<weaver_server_core::HistoryDeleteRowState>,
}

async fn terminal_mark(
    db: Database,
    job_id: u64,
) -> Result<TerminalMark, weaver_server_core::StateError> {
    tokio::task::spawn_blocking(move || {
        let archived = !db.job_history_ids_among(&[job_id])?.is_empty();
        let delete_state = db
            .list_history_delete_row_states(&[job_id])?
            .remove(&job_id);
        Ok(TerminalMark {
            archived,
            delete_state,
        })
    })
    .await
    .map_err(|error| weaver_server_core::StateError::Database(error.to_string()))?
}

fn raw_pipeline_event_stream(handle: SchedulerHandle) -> impl Stream<Item = PipelineEventGql> {
    // The raw stream is every event: job-level and per-article, which the
    // pipeline publishes on separate channels. Each channel keeps its own
    // publication order; across the two there is none. A `SegmentCommitted`
    // may surface after the `FileComplete` that followed it, and that is by
    // design: the per-article stream is a trace, not a ledger. Nothing that
    // derives state from this subscription may sequence a job-level event
    // against a segment event, and no consumer of ours does — the queue
    // subscriptions treat both channels as an opaque "something changed"
    // trigger and reload a snapshot.
    let job_events = tokio_stream::wrappers::BroadcastStream::new(handle.subscribe_events());
    let segment_events =
        tokio_stream::wrappers::BroadcastStream::new(handle.subscribe_segment_events());
    job_events
        .merge(segment_events)
        .filter_map(|result| result.ok().map(|event| PipelineEventGql::from(&event)))
}

#[derive(Clone, Copy)]
enum JobDetailTrigger {
    Initial,
    Event,
    Heartbeat,
}

fn throttle<S, T>(stream: S, period: Duration) -> impl Stream<Item = T>
where
    S: Stream<Item = T> + Unpin,
{
    async_stream::stream! {
        tokio::pin!(stream);
        let mut last = tokio::time::Instant::now() - period;

        while let Some(item) = stream.next().await {
            let now = tokio::time::Instant::now();
            let elapsed = now.duration_since(last);
            if elapsed < period {
                tokio::time::sleep(period - elapsed).await;
            }
            last = tokio::time::Instant::now();
            yield item;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle_with(jobs: Vec<weaver_server_core::JobInfo>) -> SchedulerHandle {
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(1);
        SchedulerHandle::new(
            event_tx,
            weaver_server_core::jobs::handle::SharedPipelineState::new(
                weaver_server_core::operations::metrics::PipelineMetrics::new(),
                jobs,
            ),
        )
    }

    fn history_row(job_id: u64) -> weaver_server_core::JobHistoryRow {
        weaver_server_core::JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("job-{job_id}"),
            status: "complete".to_string(),
            error_message: None,
            total_bytes: 1_000,
            downloaded_bytes: 1_000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1_000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: 100,
            completed_at: 200,
            metadata: None,
            server_attribution: None,
        }
    }

    fn job(job_id: u64, downloaded_bytes: u64) -> weaver_server_core::JobInfo {
        let row = history_row(job_id);
        let mut info = crate::schema::history_query::job_info_from_history_row(&row);
        info.status = weaver_server_core::JobStatus::Downloading;
        info.downloaded_bytes = downloaded_bytes;
        info
    }

    #[test]
    fn a_heartbeat_rebuilds_only_when_this_jobs_row_moved() {
        let mut gate = JobDetailReloadGate::default();
        let before = handle_with(vec![job(1, 10), job(2, 10)]);
        assert!(gate.live_moved(live_fingerprint(&before, 1)));

        let other_moved = handle_with(vec![job(1, 10), job(2, 99)]);
        assert!(!gate.live_moved(live_fingerprint(&other_moved, 1)));

        let this_moved = handle_with(vec![job(1, 11), job(2, 99)]);
        assert!(gate.live_moved(live_fingerprint(&this_moved, 1)));
        assert!(!gate.live_moved(live_fingerprint(&this_moved, 1)));

        let gone = handle_with(vec![job(2, 99)]);
        assert!(gate.live_moved(live_fingerprint(&gone, 1)));
    }

    #[tokio::test]
    async fn a_terminal_job_rebuilds_only_when_its_history_moved() {
        let db = Database::open_in_memory().unwrap();
        db.insert_job_history(&history_row(7)).unwrap();
        let mut gate = JobDetailReloadGate::default();
        assert!(gate.terminal_moved(terminal_mark(db.clone(), 7).await.unwrap()));
        assert!(!gate.terminal_moved(terminal_mark(db.clone(), 7).await.unwrap()));

        db.delete_job_history(7).unwrap();
        let gone = terminal_mark(db.clone(), 7).await.unwrap();
        assert!(!gone.archived);
        assert!(gate.terminal_moved(gone));
        assert!(!gate.terminal_moved(terminal_mark(db, 7).await.unwrap()));
    }
}

use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::*;
use crate::history::types::{history_delete_row_state_from_core, history_diagnostic_run_from_core};

#[derive(Default)]
pub(crate) struct HistoryQuery;

#[Object]
impl HistoryQuery {
    /// Public history facade.
    #[graphql(guard = "ReadGuard")]
    async fn history_items(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<HistoryItem>> {
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = first.unwrap_or(u32::MAX) as usize;
        let db = ctx.data::<Database>()?.clone();
        let items = tokio::task::spawn_blocking(move || {
            let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
            let delete_states = load_history_delete_states(&db, rows.iter().map(|row| row.job_id))?;
            let diagnostic_states =
                load_history_diagnostic_states(&db, rows.iter().map(|row| row.job_id))?;
            Ok::<_, weaver_server_core::StateError>(
                rows.into_iter()
                    .map(|row| {
                        let diagnostic_run = diagnostic_states
                            .get(&row.job_id)
                            .cloned()
                            .map(history_diagnostic_run_from_core);
                        let delete_state = delete_states
                            .get(&row.job_id)
                            .cloned()
                            .map(history_delete_row_state_from_core);
                        history_item_from_row(&row, diagnostic_run, delete_state)
                    })
                    .filter(|item| matches_history_filter(item, filter.as_ref()))
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(items)
    }
    /// Server-backed history table page with search, counts, sorting, and pagination.
    #[graphql(guard = "ReadGuard")]
    async fn history_page(
        &self,
        ctx: &Context<'_>,
        input: HistoryPageInput,
    ) -> Result<HistoryPage> {
        let db = ctx.data::<Database>()?.clone();
        load_history_page(db, input).await
    }
    /// Public history facade for one completed or failed item.
    #[graphql(guard = "ReadGuard")]
    async fn history_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<HistoryItem>> {
        let db = ctx.data::<Database>()?.clone();
        let row = tokio::task::spawn_blocking(move || {
            let row = db.get_job_history(id)?;
            let delete_states =
                load_history_delete_states(&db, row.iter().map(|entry| entry.job_id))?;
            let diagnostic_states =
                load_history_diagnostic_states(&db, row.iter().map(|entry| entry.job_id))?;
            Ok::<_, weaver_server_core::StateError>((row, delete_states, diagnostic_states))
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(row.0.as_ref().map(|history_row| {
            history_item_from_row(
                history_row,
                row.2
                    .get(&history_row.job_id)
                    .cloned()
                    .map(history_diagnostic_run_from_core),
                row.1
                    .get(&history_row.job_id)
                    .cloned()
                    .map(history_delete_row_state_from_core),
            )
        }))
    }
    /// Count history items exposed by the public history facade.
    #[graphql(guard = "ReadGuard")]
    async fn history_items_count(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<u32> {
        let db = ctx.data::<Database>()?.clone();
        let count = tokio::task::spawn_blocking(move || {
            let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
            let delete_states = load_history_delete_states(&db, rows.iter().map(|row| row.job_id))?;
            let diagnostic_states =
                load_history_diagnostic_states(&db, rows.iter().map(|row| row.job_id))?;
            Ok::<_, weaver_server_core::StateError>(
                rows.into_iter()
                    .map(|row| {
                        let diagnostic_run = diagnostic_states
                            .get(&row.job_id)
                            .cloned()
                            .map(history_diagnostic_run_from_core);
                        let delete_state = delete_states
                            .get(&row.job_id)
                            .cloned()
                            .map(history_delete_row_state_from_core);
                        history_item_from_row(&row, diagnostic_run, delete_state)
                    })
                    .filter(|item| matches_history_filter(item, filter.as_ref()))
                    .count() as u32,
            )
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(count)
    }
    /// Compatibility facade for recent queue lifecycle history.
    ///
    /// This is backed by the bounded in-memory replay window rather than
    /// persisted queue events. Older entries fall out of the ring buffer, and
    /// stale cursors degrade to `[]` for compatibility with the old table.
    #[graphql(guard = "ReadGuard")]
    async fn history_events(
        &self,
        ctx: &Context<'_>,
        item_id: u64,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueEvent>> {
        let after = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let limit = (first.unwrap_or(u32::MAX) as usize).min(replay.capacity());
        replay
            .replay_for_item(item_id, after, limit)
            .await
            .or_else(|_| Ok(Vec::new()))
    }
    /// Atomic snapshot for the job detail page.
    #[graphql(guard = "ReadGuard")]
    async fn job_detail_snapshot(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<JobDetailSnapshot> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        load_job_detail_snapshot(handle, db, job_id).await
    }
    /// Active or recent background history delete operations.
    #[graphql(guard = "ReadGuard")]
    async fn history_delete_operations(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = true)] active_only: bool,
    ) -> Result<Vec<crate::history::types::HistoryDeleteOperation>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.list_history_delete_operations(active_only))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))
            .map(|operations| {
                operations
                    .into_iter()
                    .map(crate::history::types::history_delete_operation_from_core)
                    .collect()
            })
    }
    /// Get synthesized waterfall/timeline data for a job.
    async fn job_timeline(&self, ctx: &Context<'_>, job_id: u64) -> Result<Option<JobTimeline>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_server_core::Database>()?.clone();

        let live_job = match handle.get_job(JobId(job_id)) {
            Ok(info) => Some(info),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };
        let live_only = live_job.is_some();

        let (events, history) = tokio::task::spawn_blocking(move || {
            let events = db.get_job_events(job_id)?;
            let history = if live_only {
                None
            } else {
                db.get_job_history(job_id)?
            };
            Ok::<_, weaver_server_core::StateError>((events, history))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        let Some(job) = live_job.or_else(|| history.as_ref().map(job_info_from_history_row)) else {
            return Ok(None);
        };

        Ok(Some(build_job_timeline(&job, history.as_ref(), &events)))
    }
    /// Get the event log for a specific job.
    async fn job_events(&self, ctx: &Context<'_>, job_id: u64) -> Result<Vec<JobEvent>> {
        let db = ctx.data::<weaver_server_core::Database>()?;
        let db = db.clone();
        let events = tokio::task::spawn_blocking(move || db.get_job_events(job_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(events
            .into_iter()
            .map(|e| JobEvent {
                kind: e.kind.parse::<EventKind>().unwrap_or(EventKind::JobCreated),
                job_id: e.job_id,
                file_id: e.file_id,
                message: e.message,
                timestamp: e.timestamp as f64,
            })
            .collect())
    }
}

pub(crate) async fn load_job_detail_snapshot(
    handle: SchedulerHandle,
    db: weaver_server_core::Database,
    job_id: u64,
) -> Result<JobDetailSnapshot> {
    let snapshot_started = Instant::now();
    let live_job = match handle.get_job(JobId(job_id)) {
        Ok(info) => Some(info),
        Err(weaver_server_core::SchedulerError::JobNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    let live_only = live_job.is_some();

    let (events, history, delete_states, diagnostic_states) =
        tokio::task::spawn_blocking(move || {
            let events = db.get_job_events(job_id)?;
            let history = if live_only {
                None
            } else {
                db.get_job_history(job_id)?
            };
            let delete_states =
                load_history_delete_states(&db, history.iter().map(|row| row.job_id))?;
            let diagnostic_states =
                load_history_diagnostic_states(&db, history.iter().map(|row| row.job_id))?;
            Ok::<_, weaver_server_core::StateError>((
                events,
                history,
                delete_states,
                diagnostic_states,
            ))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;

    let queue_item = live_job.as_ref().map(queue_item_from_job);
    let history_item = history.as_ref().map(|row| {
        history_item_from_row(
            row,
            diagnostic_states
                .get(&row.job_id)
                .cloned()
                .map(history_diagnostic_run_from_core),
            delete_states
                .get(&row.job_id)
                .cloned()
                .map(history_delete_row_state_from_core),
        )
    });
    let job_timeline = live_job
        .as_ref()
        .cloned()
        .or_else(|| history.as_ref().map(job_info_from_history_row))
        .map(|job| build_job_timeline(&job, history.as_ref(), &events));
    let job_events = events
        .into_iter()
        .map(|event| JobEvent {
            kind: event
                .kind
                .parse::<EventKind>()
                .unwrap_or(EventKind::JobCreated),
            job_id: event.job_id,
            file_id: event.file_id,
            message: event.message,
            timestamp: event.timestamp as f64,
        })
        .collect();

    let elapsed = snapshot_started.elapsed();
    if elapsed > Duration::from_millis(200) {
        tracing::debug!(
            job_id,
            elapsed_ms = elapsed.as_millis(),
            snapshot_mode = if queue_item.is_some() && history_item.is_none() {
                "live-only"
            } else if queue_item.is_some() && history_item.is_some() {
                "live+history"
            } else {
                "history-only"
            },
            "slow job detail snapshot"
        );
    }

    Ok(JobDetailSnapshot {
        queue_item,
        history_item,
        job_timeline,
        job_events,
    })
}

async fn load_history_page(db: Database, input: HistoryPageInput) -> Result<HistoryPage> {
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
        let delete_states = load_history_delete_states(&db, rows.iter().map(|row| row.job_id))?;
        let diagnostic_states =
            load_history_diagnostic_states(&db, rows.iter().map(|row| row.job_id))?;
        Ok::<_, weaver_server_core::StateError>(build_history_page(
            rows,
            delete_states,
            diagnostic_states,
            input,
        ))
    })
    .await
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))
}

fn load_history_delete_states(
    db: &Database,
    ids: impl Iterator<Item = u64>,
) -> Result<HashMap<u64, weaver_server_core::HistoryDeleteRowState>, weaver_server_core::StateError>
{
    let ids = ids.collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    db.list_history_delete_row_states(&ids)
}

fn load_history_diagnostic_states(
    db: &Database,
    ids: impl Iterator<Item = u64>,
) -> Result<HashMap<u64, weaver_server_core::DiagnosticRunRow>, weaver_server_core::StateError> {
    let ids = ids.collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let wanted = ids.into_iter().collect::<std::collections::HashSet<_>>();
    Ok(db
        .list_pending_diagnostic_runs()?
        .into_iter()
        .filter(|row| wanted.contains(&row.source_job_id))
        .map(|row| (row.source_job_id, row))
        .collect())
}

fn build_history_page(
    rows: Vec<JobHistoryRow>,
    delete_states: HashMap<u64, weaver_server_core::HistoryDeleteRowState>,
    diagnostic_states: HashMap<u64, weaver_server_core::DiagnosticRunRow>,
    input: HistoryPageInput,
) -> HistoryPage {
    let page_size = sanitize_page_size(input.page_size);
    let page_index = input.page_index as usize;
    let search = normalize_history_search(input.search);
    let status = input.status.unwrap_or(HistoryStatusFilter::All);
    let sort_field = input.sort_field.unwrap_or(HistorySortField::CompletedAt);
    let sort_direction = input.sort_direction.unwrap_or(HistorySortDirection::Desc);

    let filtered_by_search: Vec<HistoryItem> = rows
        .into_iter()
        .map(|row| {
            let diagnostic_run = diagnostic_states
                .get(&row.job_id)
                .cloned()
                .map(history_diagnostic_run_from_core);
            let delete_state = delete_states
                .get(&row.job_id)
                .cloned()
                .map(history_delete_row_state_from_core);
            history_item_from_row(&row, diagnostic_run, delete_state)
        })
        .filter(|item| history_matches_search(item, search.as_deref()))
        .collect();

    let counts = HistoryPageCounts {
        all: filtered_by_search.len() as u32,
        success: filtered_by_search
            .iter()
            .filter(|item| item.state == QueueItemState::Completed)
            .count() as u32,
        failure: filtered_by_search
            .iter()
            .filter(|item| item.state == QueueItemState::Failed)
            .count() as u32,
    };

    let mut matching_items: Vec<HistoryItem> = filtered_by_search
        .into_iter()
        .filter(|item| history_matches_status(item, status))
        .collect();
    matching_items
        .sort_by(|left, right| compare_history_items(left, right, sort_field, sort_direction));

    let total_count = matching_items.len() as u32;
    let items = matching_items
        .into_iter()
        .skip(page_index.saturating_mul(page_size))
        .take(page_size)
        .collect();

    HistoryPage {
        items,
        total_count,
        counts,
    }
}

fn sanitize_page_size(page_size: u32) -> usize {
    match page_size {
        0 => 25,
        value => value.min(500) as usize,
    }
}

fn normalize_history_search(search: Option<String>) -> Option<String> {
    let trimmed = search?.trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn history_matches_search(item: &HistoryItem, search: Option<&str>) -> bool {
    let Some(search) = search else {
        return true;
    };

    [
        item.display_title.as_str(),
        item.original_title.as_str(),
        item.name.as_str(),
    ]
    .into_iter()
    .any(|value| value.to_lowercase().contains(search))
}

fn history_matches_status(item: &HistoryItem, status: HistoryStatusFilter) -> bool {
    match status {
        HistoryStatusFilter::All => true,
        HistoryStatusFilter::Success => item.state == QueueItemState::Completed,
        HistoryStatusFilter::Failure => item.state == QueueItemState::Failed,
    }
}

fn compare_history_items(
    left: &HistoryItem,
    right: &HistoryItem,
    sort_field: HistorySortField,
    sort_direction: HistorySortDirection,
) -> Ordering {
    let ordering = match sort_field {
        HistorySortField::CompletedAt => left.completed_at.cmp(&right.completed_at),
        HistorySortField::Name => {
            normalized_history_name(left).cmp(&normalized_history_name(right))
        }
        HistorySortField::State => {
            history_state_key(left.state).cmp(history_state_key(right.state))
        }
        HistorySortField::Health => left.health.cmp(&right.health),
        HistorySortField::Size => left.total_bytes.cmp(&right.total_bytes),
        HistorySortField::Category => left
            .category
            .as_deref()
            .unwrap_or_default()
            .to_lowercase()
            .cmp(&right.category.as_deref().unwrap_or_default().to_lowercase()),
    };

    let stable = if ordering == Ordering::Equal {
        left.id.cmp(&right.id)
    } else {
        ordering
    };

    match sort_direction {
        HistorySortDirection::Asc => stable,
        HistorySortDirection::Desc => stable.reverse(),
    }
}

fn normalized_history_name(item: &HistoryItem) -> String {
    let title = item.original_title.trim();
    if title.is_empty() {
        item.display_title.to_lowercase()
    } else {
        title.to_lowercase()
    }
}

fn history_state_key(state: QueueItemState) -> &'static str {
    match state {
        QueueItemState::Completed => "completed",
        QueueItemState::Failed => "failed",
        QueueItemState::Paused => "paused",
        QueueItemState::Queued => "queued",
        QueueItemState::Downloading => "downloading",
        QueueItemState::Checking => "checking",
        QueueItemState::Verifying => "verifying",
        QueueItemState::Repairing => "repairing",
        QueueItemState::Extracting => "extracting",
        QueueItemState::Finalizing => "finalizing",
    }
}

fn job_info_from_history_row(row: &JobHistoryRow) -> JobInfo {
    let metadata = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();
    let error = row.error_message.clone();
    let status = weaver_server_core::job_status_from_persisted_str(
        &row.status,
        row.error_message.as_deref(),
    );
    let (download_state, post_state, run_state) =
        weaver_server_core::runtime_lanes_from_status_snapshot(&status);
    let progress = if row.total_bytes == 0 {
        0.0
    } else {
        (row.downloaded_bytes as f64 / row.total_bytes as f64).clamp(0.0, 1.0)
    };

    JobInfo {
        job_id: JobId(row.job_id),
        name: row.name.clone(),
        status,
        download_state,
        post_state,
        run_state,
        progress,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        failed_bytes: row.failed_bytes,
        health: row.health,
        password: None,
        category: row.category.clone(),
        metadata,
        output_dir: row.output_dir.clone(),
        error,
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}

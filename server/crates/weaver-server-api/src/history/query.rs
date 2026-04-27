use super::*;

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
            Ok::<_, weaver_server_core::StateError>(
                rows.into_iter()
                    .map(|row| history_item_from_row(&row))
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
    /// Public history facade for one completed or failed item.
    #[graphql(guard = "ReadGuard")]
    async fn history_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<HistoryItem>> {
        let db = ctx.data::<Database>()?.clone();
        let row = tokio::task::spawn_blocking(move || db.get_job_history(id))
            .await
            .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
            .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(row.as_ref().map(history_item_from_row))
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
            Ok::<_, weaver_server_core::StateError>(
                rows.into_iter()
                    .map(|row| history_item_from_row(&row))
                    .filter(|item| matches_history_filter(item, filter.as_ref()))
                    .count() as u32,
            )
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(count)
    }
    /// Semantic lifecycle history for a queue/history item.
    #[graphql(guard = "ReadGuard")]
    async fn history_events(
        &self,
        ctx: &Context<'_>,
        item_id: u64,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueEvent>> {
        let cursor = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let db = ctx.data::<Database>()?.clone();
        let rows = tokio::task::spawn_blocking(move || {
            db.list_integration_events_after(cursor, Some(item_id), first)
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let record = serde_json::from_str(&row.payload_json).map_err(|e| {
                    graphql_error(
                        "INTERNAL",
                        format!("invalid integration event payload: {e}"),
                    )
                })?;
                Ok(queue_event_from_record(row.id, record))
            })
            .collect()
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
    /// Get synthesized waterfall/timeline data for a job.
    async fn job_timeline(&self, ctx: &Context<'_>, job_id: u64) -> Result<Option<JobTimeline>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_server_core::Database>()?.clone();

        let live_job = match handle.get_job(JobId(job_id)) {
            Ok(info) => Some(info),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };

        let (events, history) = tokio::task::spawn_blocking(move || {
            let events = db.get_job_events(job_id)?;
            let history = db.get_job_history(job_id)?;
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
    let live_job = match handle.get_job(JobId(job_id)) {
        Ok(info) => Some(info),
        Err(weaver_server_core::SchedulerError::JobNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };

    let (events, history) = tokio::task::spawn_blocking(move || {
        let events = db.get_job_events(job_id)?;
        let history = db.get_job_history(job_id)?;
        Ok::<_, weaver_server_core::StateError>((events, history))
    })
    .await
    .map_err(|error| async_graphql::Error::new(error.to_string()))?
    .map_err(|error| async_graphql::Error::new(error.to_string()))?;

    let queue_item = live_job.as_ref().map(queue_item_from_job);
    let history_item = history.as_ref().map(history_item_from_row);
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

    Ok(JobDetailSnapshot {
        queue_item,
        history_item,
        job_timeline,
        job_events,
    })
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

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::path::PathBuf;

use super::*;
use crate::jobs::types::{
    DuplicateIdentitySnapshotInfo, DuplicateSummaryInfo, PreparedQueueFilter,
    load_duplicate_summaries_chunked, matches_queue_filter_prepared, queue_display_titles_from_job,
    queue_item_state_from_job_info, queue_table_item_from_job,
};
use crate::observability::with_timed_config_read;

#[derive(Default)]
pub(crate) struct JobsQuery;

const MAX_QUEUE_PAGE_SIZE: usize = 500;

fn queue_page_display_name(original_title: &str, name: &str, display_title: &str) -> String {
    let release_name = if original_title.trim().is_empty() {
        name.trim()
    } else {
        original_title.trim()
    };
    if release_name.is_empty() {
        return display_title.to_string();
    }

    release_name
        .replace('.', " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn queue_page_job_display_name(info: &weaver_server_core::JobInfo) -> String {
    let (original_title, display_title) = queue_display_titles_from_job(info);
    queue_page_display_name(&original_title, &info.name, &display_title)
}

fn queue_display_state(state: QueueItemState) -> &'static str {
    match state {
        QueueItemState::Queued => "QUEUED",
        QueueItemState::Downloading => "DOWNLOADING",
        QueueItemState::Checking | QueueItemState::Verifying => "VERIFYING",
        QueueItemState::Repairing => "REPAIRING",
        QueueItemState::Extracting => "EXTRACTING",
        QueueItemState::Finalizing => "MOVING",
        QueueItemState::PostProcessing => "POST_PROCESSING",
        QueueItemState::Completed => "COMPLETE",
        QueueItemState::Failed => "FAILED",
        QueueItemState::Paused => "PAUSED",
    }
}

fn queue_page_job_matches(info: &weaver_server_core::JobInfo, input: &QueuePageInput) -> bool {
    let state = queue_item_state_from_job_info(info);
    if let Some(states) = input.states.as_ref()
        && !states.is_empty()
        && !states.contains(&state)
    {
        return false;
    }

    if let Some(priorities) = input.priorities.as_ref()
        && !priorities.is_empty()
        && !priorities.contains(&QueuePriority::from_metadata(&info.metadata))
    {
        return false;
    }

    if input.categories.as_ref().is_some_and(|categories| {
        !categories.is_empty()
            && !info
                .category
                .as_ref()
                .is_some_and(|category| categories.contains(category))
    }) {
        return false;
    }

    let Some(search) = input
        .search
        .as_deref()
        .map(str::trim)
        .filter(|search| !search.is_empty())
    else {
        return true;
    };
    let (original_title, display_title) = queue_display_titles_from_job(info);
    let search = search.to_lowercase();
    [
        queue_page_display_name(&original_title, &info.name, &display_title),
        info.name.clone(),
        display_title,
    ]
    .into_iter()
    .any(|value| value.to_lowercase().contains(&search))
}

fn queue_page_job_progress(info: &weaver_server_core::JobInfo) -> f64 {
    let progress = info.progress.clamp(0.0, 1.0);
    if info.total_bytes == 0 {
        return progress;
    }
    let processed_bytes = info
        .downloaded_bytes
        .saturating_add(info.failed_bytes)
        .min(info.total_bytes);
    progress.max(processed_bytes as f64 / info.total_bytes as f64)
}

fn queue_page_job_live_rate(info: &weaver_server_core::JobInfo) -> u64 {
    info.phase_progress
        .iter()
        .filter_map(|phase| phase.rate_bps)
        .max()
        .unwrap_or_default()
}

fn queue_page_default_order(
    left: &weaver_server_core::JobInfo,
    right: &weaver_server_core::JobInfo,
) -> Ordering {
    queue_page_job_live_rate(right).cmp(&queue_page_job_live_rate(left))
}

fn queue_page_job_order(
    left: &weaver_server_core::JobInfo,
    right: &weaver_server_core::JobInfo,
    input: &QueuePageInput,
) -> Ordering {
    let Some(sort_field) = input.sort_field else {
        return Ordering::Equal;
    };
    let order = match sort_field {
        QueueSortField::Name => {
            queue_page_job_display_name(left).cmp(&queue_page_job_display_name(right))
        }
        QueueSortField::State => queue_display_state(queue_item_state_from_job_info(left))
            .cmp(queue_display_state(queue_item_state_from_job_info(right))),
        QueueSortField::Priority => QueuePriority::from_metadata(&left.metadata)
            .rank()
            .cmp(&QueuePriority::from_metadata(&right.metadata).rank()),
        QueueSortField::Category => left
            .category
            .as_deref()
            .unwrap_or("\u{2014}")
            .cmp(right.category.as_deref().unwrap_or("\u{2014}")),
        QueueSortField::Progress => {
            queue_page_job_progress(left).total_cmp(&queue_page_job_progress(right))
        }
        QueueSortField::Size => left.total_bytes.cmp(&right.total_bytes),
    };
    let order = match input.sort_direction.unwrap_or(QueueSortDirection::Desc) {
        QueueSortDirection::Asc => order,
        QueueSortDirection::Desc => order.reverse(),
    };
    order.then_with(|| left.job_id.0.cmp(&right.job_id.0))
}

fn queue_page_summary(
    jobs: &[weaver_server_core::JobInfo],
    metrics: &weaver_server_core::operations::metrics::MetricsSnapshot,
) -> QueueSummary {
    let mut summary = QueueSummary {
        total_items: jobs.len() as u32,
        queued_items: 0,
        active_items: 0,
        paused_items: 0,
        failed_items: 0,
        total_bytes: jobs.iter().map(|job| job.total_bytes).sum(),
        downloaded_bytes: jobs.iter().map(|job| job.downloaded_bytes).sum(),
        current_download_speed: metrics.current_download_speed,
        verifying_items: 0,
        repairing_items: 0,
        extracting_items: 0,
    };

    for job in jobs {
        match queue_item_state_from_job_info(job) {
            QueueItemState::Queued => summary.queued_items += 1,
            QueueItemState::Paused => summary.paused_items += 1,
            QueueItemState::Failed => summary.failed_items += 1,
            QueueItemState::Verifying | QueueItemState::Checking => {
                summary.active_items += 1;
                summary.verifying_items += 1;
            }
            QueueItemState::Repairing => {
                summary.active_items += 1;
                summary.repairing_items += 1;
            }
            QueueItemState::Extracting => {
                summary.active_items += 1;
                summary.extracting_items += 1;
            }
            QueueItemState::Downloading
            | QueueItemState::Finalizing
            | QueueItemState::PostProcessing => summary.active_items += 1,
            QueueItemState::Completed => {}
        }
    }

    summary
}

#[Object]
impl JobsQuery {
    /// Public queue facade for active or in-flight items.
    #[graphql(guard = "ReadGuard")]
    async fn queue_items(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = first.unwrap_or(u32::MAX) as usize;
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());

        let mut items: Vec<QueueItem> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .filter(|item| matches_queue_filter_prepared(item, prepared_filter.as_ref()))
            .skip(offset)
            .take(limit)
            .collect();
        attach_duplicate_summaries(ctx.data::<Database>()?.clone(), &mut items).await?;
        Ok(items)
    }
    /// Server-paginated queue rows for the interactive queue table.
    #[graphql(guard = "ReadGuard")]
    async fn queue_page(&self, ctx: &Context<'_>, input: QueuePageInput) -> Result<QueuePage> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        // Capture before reading the scheduler so events produced while this page
        // is assembled remain eligible for replay by queueEvents(after: ...).
        let latest_cursor = replay.latest_cursor().await;
        let mut all_jobs: Vec<weaver_server_core::JobInfo> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .collect();
        let categories = all_jobs
            .iter()
            .filter_map(|job| job.category.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let metrics = handle.get_metrics();
        let summary = queue_page_summary(&all_jobs, &metrics);
        let page_size = (input.page_size as usize).clamp(1, MAX_QUEUE_PAGE_SIZE);
        let offset = (input.page_index as usize).saturating_mul(page_size);
        all_jobs.retain(|job| queue_page_job_matches(job, &input));
        if input.sort_field.is_some() {
            all_jobs.sort_by(|left, right| queue_page_job_order(left, right, &input));
        } else {
            all_jobs.sort_by(queue_page_default_order);
        }
        let total_count = u32::try_from(all_jobs.len()).unwrap_or(u32::MAX);
        let mut items = all_jobs
            .into_iter()
            .skip(offset)
            .take(page_size)
            .map(|job| queue_table_item_from_job(&job))
            .collect::<Vec<_>>();
        attach_duplicate_summaries(ctx.data::<Database>()?.clone(), &mut items).await?;

        Ok(QueuePage {
            items,
            total_count,
            summary,
            categories,
            latest_cursor,
        })
    }
    /// Public queue facade for one active item.
    #[graphql(guard = "ReadGuard")]
    async fn queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<QueueItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let Some(info) = handle.list_jobs().into_iter().find(|info| {
            info.job_id.0 == id
                && !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
        }) else {
            return Ok(None);
        };
        let mut item = queue_item_from_job(&info);
        attach_duplicate_summaries(
            ctx.data::<Database>()?.clone(),
            std::slice::from_mut(&mut item),
        )
        .await?;
        Ok(Some(item))
    }
    /// Summary of the active queue and live throughput.
    #[graphql(guard = "ReadGuard")]
    async fn queue_summary(&self, ctx: &Context<'_>) -> Result<QueueSummary> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let items: Vec<QueueItem> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .collect();
        Ok(queue_summary(&items, &handle.get_metrics()))
    }
    /// Atomic queue bootstrap snapshot for initial page load and reconnect polling.
    #[graphql(guard = "ReadGuard")]
    async fn queue_snapshot(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<QueueSnapshot> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx.data::<SharedConfig>()?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());

        let mut items: Vec<QueueItem> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .filter(|item| matches_queue_filter_prepared(item, prepared_filter.as_ref()))
            .collect();
        attach_duplicate_summaries(ctx.data::<Database>()?.clone(), &mut items).await?;
        let metrics = handle.get_metrics();
        let latest_cursor = replay.latest_cursor().await;
        let max_download_speed = with_timed_config_read(
            config,
            "jobs.query.queue_snapshot.max_download_speed",
            |cfg| cfg.max_download_speed.unwrap_or(0),
        )
        .await;

        Ok(QueueSnapshot {
            summary: queue_summary(&items, &metrics),
            metrics: metrics_from_snapshot(&metrics),
            global_state: global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                max_download_speed,
            ),
            items,
            latest_cursor,
            generated_at: chrono::Utc::now(),
        })
    }
    /// Current global queue state facade.
    #[graphql(guard = "ReadGuard")]
    async fn global_queue_state(&self, ctx: &Context<'_>) -> Result<GlobalQueueState> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        let max_download_speed = with_timed_config_read(
            config,
            "jobs.query.global_queue_state.max_download_speed",
            |cfg| cfg.max_download_speed.unwrap_or(0),
        )
        .await;
        Ok(global_queue_state(
            handle.is_globally_paused(),
            &handle.get_download_block(),
            max_download_speed,
        ))
    }
    /// Compatibility cursor for the live-only `queueEvents` stream.
    #[graphql(guard = "ReadGuard")]
    async fn latest_queue_cursor(&self, ctx: &Context<'_>) -> Result<String> {
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        Ok(replay.latest_cursor().await)
    }
    /// List jobs, optionally filtered by status, category, or metadata key.
    /// Supports pagination via `limit` and `offset`.
    async fn jobs(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let infos = handle.list_jobs();
        let filtered = infos.iter().filter(|info| {
            if let Some(ref statuses) = status {
                let gql_status = JobStatusGql::from(&info.status);
                if !statuses.contains(&gql_status) {
                    return false;
                }
            }
            if let Some(ref cat) = category
                && info.category.as_ref() != Some(cat)
            {
                return false;
            }
            if let Some(ref key) = has_metadata_key
                && !info.metadata.iter().any(|(k, _)| k == key)
            {
                return false;
            }
            true
        });
        let jobs = if let Some(lim) = limit {
            filtered
                .skip(offset.unwrap_or(0) as usize)
                .take(lim as usize)
                .map(Job::from)
                .collect()
        } else {
            filtered.map(Job::from).collect()
        };
        Ok(jobs)
    }
    /// Count jobs matching the given filters (same filters as `jobs`).
    async fn job_count(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
    ) -> Result<u32> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let infos = handle.list_jobs();
        let count = infos
            .iter()
            .filter(|info| {
                if let Some(ref statuses) = status {
                    let gql_status = JobStatusGql::from(&info.status);
                    if !statuses.contains(&gql_status) {
                        return false;
                    }
                }
                if let Some(ref cat) = category
                    && info.category.as_ref() != Some(cat)
                {
                    return false;
                }
                if let Some(ref key) = has_metadata_key
                    && !info.metadata.iter().any(|(k, _)| k == key)
                {
                    return false;
                }
                true
            })
            .count();
        Ok(count as u32)
    }
    /// Get a specific job by ID.
    async fn job(&self, ctx: &Context<'_>, id: u64) -> Result<Option<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        match handle.get_job(weaver_server_core::jobs::ids::JobId(id)) {
            Ok(info) => Ok(Some(Job::from(&info))),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    /// Durable duplicate identity and semantic candidate state for a queue,
    /// history, or job-detail item. Parked source material is never exposed.
    #[graphql(guard = "ReadGuard")]
    async fn duplicate_snapshot(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<Option<DuplicateIdentitySnapshotInfo>> {
        let db = ctx.data::<Database>()?.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            db.duplicate_snapshot(weaver_server_core::jobs::ids::JobId(id))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))??;
        let Some(snapshot) = snapshot else {
            return Ok(None);
        };

        let db = ctx.data::<Database>()?.clone();
        let semantic = tokio::task::spawn_blocking(move || {
            db.semantic_candidate_snapshot(weaver_server_core::jobs::ids::JobId(id))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))??;
        Ok(Some(DuplicateIdentitySnapshotInfo::from_parts(
            &snapshot,
            semantic.as_ref(),
        )))
    }
    /// List files in a completed job's output directory.
    async fn job_output_files(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<Option<JobOutputResult>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let output_dir = match handle.get_job(weaver_server_core::jobs::ids::JobId(job_id)) {
            Ok(info) => info.output_dir.clone(),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => {
                let db = ctx.data::<Database>()?.clone();
                tokio::task::spawn_blocking(move || {
                    db.get_job_history_profiled(
                        job_id,
                        "db.get_job_history.api_job_output_dir_fallback",
                    )
                })
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .and_then(|row| row.output_dir)
            }
            Err(e) => return Err(e.into()),
        };
        let Some(output_dir) = output_dir else {
            return Ok(None);
        };

        let dir_path = PathBuf::from(&output_dir);
        let result = tokio::task::spawn_blocking(move || list_output_files(&dir_path))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))??;
        Ok(Some(result))
    }
}

fn list_output_files(dir: &std::path::Path) -> Result<JobOutputResult> {
    let output_dir = dir.to_string_lossy().into_owned();

    if !dir.is_dir() {
        return Ok(JobOutputResult {
            output_dir,
            files: Vec::new(),
            total_bytes: 0,
        });
    }

    let mut files = Vec::new();
    collect_files_recursive(dir, &mut files)?;
    files.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
    });
    let total_bytes = files.iter().map(|f| f.size_bytes).sum();

    Ok(JobOutputResult {
        output_dir,
        files,
        total_bytes,
    })
}

async fn attach_duplicate_summaries(db: Database, items: &mut [QueueItem]) -> Result<()> {
    let job_ids = items
        .iter()
        .map(|item| weaver_server_core::jobs::ids::JobId(item.id))
        .collect::<Vec<_>>();
    let summaries =
        tokio::task::spawn_blocking(move || load_duplicate_summaries_chunked(&db, job_ids))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
    for item in items {
        if let Some(summary) = summaries.get(&weaver_server_core::jobs::ids::JobId(item.id)) {
            item.duplicate_summary = Some(DuplicateSummaryInfo::from_summary(summary));
        }
    }
    Ok(())
}

fn collect_files_recursive(dir: &std::path::Path, out: &mut Vec<JobOutputFile>) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| async_graphql::Error::new(format!("failed to read directory: {e}")))?;
    for entry in entries.flatten() {
        let path = entry.path();
        let meta = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_dir() {
            collect_files_recursive(&path, out)?;
        } else {
            out.push(JobOutputFile {
                name: path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                path: path.to_string_lossy().into_owned(),
                size_bytes: meta.len(),
            });
        }
    }
    Ok(())
}

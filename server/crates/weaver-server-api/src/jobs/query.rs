// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;

use super::*;
use crate::jobs::paging::{PageRead, PageWants, read_row};
use crate::jobs::types::{
    DuplicateIdentitySnapshotInfo, DuplicateSummaryInfo, PreparedQueueFilter,
    load_duplicate_summaries_chunked, matches_queue_filter_prepared, queue_table_item_from_job,
};
use crate::observability::with_timed_config_read;

#[derive(Default)]
pub(crate) struct JobsQuery;

#[Object]
impl JobsQuery {
    #[graphql(guard = "crate::auth::AdminGuard")]
    async fn validated_archive_password(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<Option<String>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(tokio::task::spawn_blocking(move || db.validated_archive_password(id)).await??)
    }

    /// A report on a queued or finished job's NZB and how the job went, with
    /// every name, path, host and password left out so it can be shared.
    #[graphql(guard = "ReadGuard")]
    async fn job_support_report(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<crate::jobs::support_report::SupportReport> {
        crate::jobs::support_report::resolve_job_support_report(ctx, job_id).await
    }

    /// Public queue facade for active or in-flight items.
    #[graphql(guard = "ReadGuard")]
    async fn queue_items(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueItem>> {
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = first.unwrap_or(u32::MAX) as usize;
        let prepared_filter = PreparedQueueFilter::new(filter.as_ref());

        let mut items: Vec<QueueItem> = crate::jobs::paging::read_listing(
            ctx.data::<weaver_server_core::SharedPipelineState>()?,
        )
        .iter()
        .map(queue_item_from_job)
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
        build_queue_page(
            ctx.data::<SchedulerHandle>()?,
            ctx.data::<weaver_server_core::SharedPipelineState>()?,
            ctx.data::<crate::jobs::replay::QueueEventReplay>()?,
            ctx.data::<Database>()?.clone(),
            &input,
            page_wants(ctx),
            &mut DuplicateSummaryCache::default(),
        )
        .await
    }
    /// Public queue facade for one active item.
    #[graphql(guard = "ReadGuard")]
    async fn queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<QueueItem>> {
        let state = ctx.data::<weaver_server_core::SharedPipelineState>()?;
        let Some(info) = read_row(state, weaver_server_core::jobs::ids::JobId(id)) else {
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
        let state = ctx.data::<weaver_server_core::SharedPipelineState>()?;
        Ok(crate::jobs::paging::summary(state, &handle.get_metrics()))
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

        let mut items: Vec<QueueItem> = crate::jobs::paging::read_listing(
            ctx.data::<weaver_server_core::SharedPipelineState>()?,
        )
        .iter()
        .map(queue_item_from_job)
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
        let state = ctx.data::<weaver_server_core::SharedPipelineState>()?;
        let filter = JobFilter {
            status,
            category,
            has_metadata_key,
        };
        let offset = offset.unwrap_or(0) as usize;
        let limit = limit.map_or(usize::MAX, |limit| limit as usize);
        let history = state.history_rows();
        // Only the jobs on the page are copied; the rest are matched in place.
        let (mut infos, queued_matches) = state.read_queue(|inner| {
            let mut infos = Vec::new();
            let mut matched = 0usize;
            for row in &inner.jobs {
                let view = row.view(&inner.globals);
                if !filter.matches(view.status(), view.job()) {
                    continue;
                }
                if matched >= offset && infos.len() < limit {
                    infos.push(row.info(&inner.globals));
                }
                matched += 1;
            }
            (infos, matched)
        });
        let room = limit - infos.len();
        infos.extend(
            history
                .iter()
                .filter(|info| filter.matches(&info.status, info))
                .skip(offset.saturating_sub(queued_matches))
                .take(room)
                .cloned(),
        );
        Ok(infos.iter().map(Job::from).collect())
    }
    /// Count jobs matching the given filters (same filters as `jobs`).
    async fn job_count(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
    ) -> Result<u32> {
        let state = ctx.data::<weaver_server_core::SharedPipelineState>()?;
        let filter = JobFilter {
            status,
            category,
            has_metadata_key,
        };
        let history = state.history_rows();
        let count = state.read_queue(|inner| {
            inner
                .jobs
                .iter()
                .filter(|row| {
                    let view = row.view(&inner.globals);
                    filter.matches(view.status(), view.job())
                })
                .count()
        }) + history
            .iter()
            .filter(|info| filter.matches(&info.status, info))
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

/// What a page request selects beyond the counts: a selection that names no
/// row field (the screen-wide totals) is planned without any row.
pub(crate) fn page_wants(ctx: &Context<'_>) -> PageWants {
    let fields = ctx.look_ahead();
    PageWants {
        rows: fields.field("items").exists() || fields.field("etaThrough").exists(),
    }
}

/// The duplicate summaries a page answer last read, kept while the queue
/// revision they were read at stands. A job's summary changes only with the
/// queue (an add, a removal or a retirement moves the revision), so a push
/// at the same revision reads nothing, and a page with no rows never reads.
#[derive(Default)]
pub(crate) struct DuplicateSummaryCache {
    revision: Option<u64>,
    by_id: std::collections::HashMap<u64, Option<DuplicateSummaryInfo>>,
    reads: u32,
}

impl DuplicateSummaryCache {
    pub(crate) async fn attach(
        &mut self,
        db: Database,
        revision: u64,
        items: &mut [QueueItem],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        if self.revision != Some(revision) {
            self.by_id.clear();
            self.revision = Some(revision);
        }
        let missing = items
            .iter()
            .filter(|item| !self.by_id.contains_key(&item.id))
            .map(|item| weaver_server_core::jobs::ids::JobId(item.id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            self.reads += 1;
            let wanted = missing.clone();
            let summaries =
                tokio::task::spawn_blocking(move || load_duplicate_summaries_chunked(&db, wanted))
                    .await
                    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
                    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
            for id in missing {
                self.by_id.insert(
                    id.0,
                    summaries.get(&id).map(DuplicateSummaryInfo::from_summary),
                );
            }
        }
        for item in items {
            item.duplicate_summary = self.by_id.get(&item.id).cloned().flatten();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reads(&self) -> u32 {
        self.reads
    }
}

/// One queue page: the rows `input` asks for, and the counts and totals
/// over every row it matches.
pub(crate) async fn build_queue_page(
    handle: &SchedulerHandle,
    state: &weaver_server_core::SharedPipelineState,
    replay: &crate::jobs::replay::QueueEventReplay,
    db: Database,
    input: &QueuePageInput,
    wants: PageWants,
    duplicates: &mut DuplicateSummaryCache,
) -> Result<QueuePage> {
    // Capture before reading the scheduler so events produced while this page
    // is assembled remain eligible for replay by queueEvents(after: ...).
    let latest_cursor = replay.latest_cursor().await;
    let PageRead {
        walk,
        plan,
        rows,
        revision,
    } = PageRead::read(state, input, wants);
    let metrics = handle.get_metrics();
    let summary = walk.summary(&metrics);
    let categories = walk.categories();
    drop(walk);
    let mut items = rows
        .iter()
        .map(queue_table_item_from_job)
        .collect::<Vec<_>>();
    duplicates.attach(db, revision, &mut items).await?;

    Ok(QueuePage {
        items,
        total_count: plan.total_count,
        summary,
        categories,
        category_counts: plan.category_counts,
        state_counts: plan.state_counts,
        remaining_bytes: plan.remaining_bytes,
        eta_total_bytes: plan.eta_total_bytes,
        eta_downloaded_bytes: plan.eta_downloaded_bytes,
        eta_through: plan.eta_through,
        latest_cursor,
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

// The filters `jobs` and `jobCount` share.
struct JobFilter {
    status: Option<Vec<JobStatusGql>>,
    category: Option<String>,
    has_metadata_key: Option<String>,
}

impl JobFilter {
    // `status` is the job's status as published; `job` carries the rest.
    fn matches(
        &self,
        status: &weaver_server_core::JobStatus,
        job: &weaver_server_core::JobInfo,
    ) -> bool {
        if let Some(statuses) = &self.status
            && !statuses.contains(&JobStatusGql::from(status))
        {
            return false;
        }
        if let Some(category) = &self.category
            && job.category.as_ref() != Some(category)
        {
            return false;
        }
        if let Some(key) = &self.has_metadata_key
            && !job.metadata.iter().any(|(k, _)| k == key)
        {
            return false;
        }
        true
    }
}

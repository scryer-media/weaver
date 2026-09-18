use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::*;
use crate::history::types::{history_delete_row_state_from_core, history_table_item_from_row};
use crate::jobs::types::load_duplicate_summaries_chunked;

enum HistoryQueryPlan {
    Empty,
    Query(weaver_server_core::HistoryFilter),
}

/// Upper bound on job-event rows loaded for the polled detail snapshot. Large
/// RAR jobs write ~5-7 event rows per extracted member, so an unbounded read of
/// a big job's log can be thousands of rows re-fetched a few times a second
/// while a tab is open. 2000 is a generous tail — enough to render every
/// lifecycle/verification/repair marker plus recent extraction activity — while
/// capping the per-poll cost. Only the detail-view read is bounded; callers that
/// must see the full log use `get_job_events`.
const JOB_DETAIL_EVENT_CAP: u32 = 2000;

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
        let db = ctx.data::<Database>()?.clone();
        let live_jobs = live_job_ids(ctx.data::<SchedulerHandle>()?);
        let plan = history_query_plan(filter.as_ref(), first, Some(offset));
        let HistoryQueryPlan::Query(history_filter) = plan else {
            return Ok(Vec::new());
        };
        let items = tokio::task::spawn_blocking(move || {
            // The live exclusion is applied after the query's LIMIT/OFFSET, so a
            // page that contains a live job's row comes back short by that many
            // rows rather than pulling the next row forward. The cursor stays a
            // plain row offset, which is what the next page is decoded against.
            let rows = exclude_live_rows(db.list_job_history(&history_filter)?, &live_jobs);
            let delete_states = load_history_delete_states(&db, rows.iter().map(|row| row.job_id))?;
            let duplicate_summaries = load_duplicate_summaries_chunked(
                &db,
                rows.iter()
                    .map(|row| weaver_server_core::jobs::ids::JobId(row.job_id)),
            )?;
            Ok::<_, weaver_server_core::StateError>(
                rows.into_iter()
                    .map(|row| {
                        let delete_state = delete_states
                            .get(&row.job_id)
                            .cloned()
                            .map(history_delete_row_state_from_core);
                        let mut item = history_item_from_row(&row, delete_state);
                        item.duplicate_summary = duplicate_summaries
                            .get(&weaver_server_core::jobs::ids::JobId(row.job_id))
                            .map(crate::jobs::types::DuplicateSummaryInfo::from_summary);
                        item
                    })
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
        let live_jobs = live_job_ids(ctx.data::<SchedulerHandle>()?);
        load_history_page(db, live_jobs, input).await
    }
    /// Public history facade for one completed or failed item.
    #[graphql(guard = "ReadGuard")]
    async fn history_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<HistoryItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        if live_job_ids(handle).contains(&id) {
            return Ok(None);
        }
        let db = ctx.data::<Database>()?.clone();
        let row = tokio::task::spawn_blocking(move || {
            let row = db.get_job_history_profiled(id, "db.get_job_history.api_history_item")?;
            let delete_states =
                load_history_delete_states(&db, row.iter().map(|entry| entry.job_id))?;
            let duplicate_summaries = load_duplicate_summaries_chunked(
                &db,
                row.iter()
                    .map(|entry| weaver_server_core::jobs::ids::JobId(entry.job_id)),
            )?;
            Ok::<_, weaver_server_core::StateError>((row, delete_states, duplicate_summaries))
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(row.0.as_ref().map(|history_row| {
            let mut item = history_item_from_row(
                history_row,
                row.1
                    .get(&history_row.job_id)
                    .cloned()
                    .map(history_delete_row_state_from_core),
            );
            item.duplicate_summary = row
                .2
                .get(&weaver_server_core::jobs::ids::JobId(history_row.job_id))
                .map(crate::jobs::types::DuplicateSummaryInfo::from_summary);
            item
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
        let plan = history_query_plan(filter.as_ref(), None, None);
        let HistoryQueryPlan::Query(history_filter) = plan else {
            return Ok(0);
        };
        let count = tokio::task::spawn_blocking(move || db.count_job_history(&history_filter))
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
        let server_hosts = server_hosts_by_id(ctx.data::<SharedConfig>()?).await;
        load_job_detail_snapshot(handle, db, job_id, server_hosts).await
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
                db.get_job_history_profiled(job_id, "db.get_job_history.api_history_events")?
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

/// Host label per configured server id, for naming a job's contributing servers
/// without denormalizing the host into every archived job.
pub(crate) async fn server_hosts_by_id(config: &SharedConfig) -> HashMap<u32, String> {
    crate::observability::with_timed_config_read(
        config,
        "history.query.job_detail_server_hosts",
        |cfg| {
            cfg.servers
                .iter()
                .map(|server| (server.id, server.host.clone()))
                .collect()
        },
    )
    .await
}

pub(crate) async fn load_job_detail_snapshot(
    handle: SchedulerHandle,
    db: weaver_server_core::Database,
    job_id: u64,
    server_hosts: HashMap<u32, String>,
) -> Result<JobDetailSnapshot> {
    let snapshot_started = Instant::now();
    let live_job = match handle.get_job(JobId(job_id)) {
        Ok(info) => Some(info),
        Err(weaver_server_core::SchedulerError::JobNotFound(_)) => None,
        Err(error) => return Err(error.into()),
    };
    let live_only = live_job.is_some();

    let (events, history, delete_states) = tokio::task::spawn_blocking(move || {
        // The detail view is a read-only, high-frequency poll (heartbeat + event
        // triggers, up to a few Hz per open tab). Large RAR jobs write thousands
        // of event rows, so read only the newest slice instead of scanning the
        // whole log. This is the ONLY detail-snapshot event read switched to the
        // capped variant; the pipeline restore path still uses get_job_events so
        // it can scan the full log for its finalization marker.
        let events = db.get_job_events_latest(job_id, JOB_DETAIL_EVENT_CAP)?;
        let history = if live_only {
            None
        } else {
            db.get_job_history_profiled(
                job_id,
                "db.get_job_history.api_history_integration_events",
            )?
        };
        let delete_states = load_history_delete_states(&db, history.iter().map(|row| row.job_id))?;
        Ok::<_, weaver_server_core::StateError>((events, history, delete_states))
    })
    .await
    .map_err(|error| async_graphql::Error::new(error.to_string()))?
    .map_err(|error| async_graphql::Error::new(error.to_string()))?;

    // Prefer the live counters while the job is still resident; once it has been
    // archived they only exist in the history row. Both are already aggregated
    // per server, so this is a sort over a handful of entries.
    let mut server_attribution: Vec<crate::jobs::types::JobServerContribution> = live_job
        .as_ref()
        .map(|job| job.server_attribution.clone())
        .or_else(|| {
            history.as_ref().map(|row| {
                weaver_server_core::jobs::server_attribution::contributions_from_storage(
                    row.server_attribution.as_deref(),
                )
            })
        })
        .unwrap_or_default()
        .into_iter()
        .map(|contribution| {
            crate::jobs::types::JobServerContribution::from_core(contribution, &server_hosts)
        })
        .collect();
    server_attribution.sort_by(|a, b| {
        b.articles
            .cmp(&a.articles)
            .then_with(|| a.server_id.cmp(&b.server_id))
    });

    let queue_item = live_job.as_ref().map(queue_item_from_job);
    let history_item = history.as_ref().map(|row| {
        history_item_from_row(
            row,
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
        server_attribution,
    })
}

async fn load_history_page(
    db: Database,
    live_jobs: HashSet<u64>,
    input: HistoryPageInput,
) -> Result<HistoryPage> {
    tokio::task::spawn_blocking(move || {
        // Rows for jobs the scheduler still owns are not history. Excluding them
        // after a SQL LIMIT/OFFSET would leave `total_count` and the bucket counts
        // describing rows the page does not contain, so when the overlap is
        // non-empty the page is built from the full row set with those rows
        // removed: `build_history_page` derives counts, total_count, and
        // pagination from the rows it is handed, which keeps all three
        // consistent. The overlap is normally empty and this costs one indexed
        // lookup over the live ids.
        let live_in_history = live_jobs_in_history(&db, &live_jobs)?;
        if !live_in_history.is_empty() {
            let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
            let page = build_history_page(exclude_live_rows(rows, &live_in_history), input);
            return attach_history_page_badges(&db, page);
        }
        // Two code paths, selected by whether the request's semantics map exactly
        // onto SQL. The SQL path (`build_history_page_sql`) pushes the status
        // filter, ordering, counts, and LIMIT/OFFSET into the database, backed by
        // idx_job_history_completed_job, so the default History view and the
        // completion-driven refetch never load the whole table. Any request that
        // does NOT map exactly — a free-text search (its Rust semantics are
        // `to_lowercase().contains()` over computed titles, which sqlite's
        // ASCII-only LOWER and LIKE cannot reproduce) or a non-default sort key
        // (not covered by the completed_at index) — keeps the original full-scan
        // Rust path (`build_history_page`) so results stay byte-identical.
        let page = if let Some(plan) = HistoryPageSqlPlan::for_input(&input) {
            build_history_page_sql(&db, plan)?
        } else {
            let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
            build_history_page(rows, input)
        };
        attach_history_page_badges(&db, page)
    })
    .await
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))
}

/// Attach the per-row delete-operation and duplicate badges to a built page.
///
/// Delete-operation badges are only needed for the rows actually shown on this
/// page. Load them for the page's job ids instead of issuing an `IN (…)` over
/// every row in the history table. Every page-building path shares this tail so
/// the response (page shape, counts, per-page delete states) is identical for
/// identical data.
fn attach_history_page_badges(
    db: &Database,
    mut page: HistoryPage,
) -> Result<HistoryPage, weaver_server_core::StateError> {
    let delete_states = load_history_delete_states(db, page.items.iter().map(|item| item.id))?;
    let duplicate_summaries = load_duplicate_summaries_chunked(
        db,
        page.items
            .iter()
            .map(|item| weaver_server_core::jobs::ids::JobId(item.id)),
    )?;
    for item in &mut page.items {
        item.delete_operation = delete_states
            .get(&item.id)
            .cloned()
            .map(history_delete_row_state_from_core);
        item.duplicate_summary = duplicate_summaries
            .get(&weaver_server_core::jobs::ids::JobId(item.id))
            .map(crate::jobs::types::DuplicateSummaryInfo::from_summary);
    }
    Ok(page)
}

/// A History-page request whose semantics map exactly onto SQL filtering,
/// ordering, counting, and pagination.
///
/// This is only constructed for requests with no free-text search and the
/// default `completed_at DESC` ordering; see [`HistoryPageSqlPlan::for_input`].
struct HistoryPageSqlPlan {
    status: HistoryStatusFilter,
    page_index: usize,
    page_size: usize,
}

impl HistoryPageSqlPlan {
    /// Returns a SQL plan when the request maps exactly onto SQL, or `None` when
    /// the caller must fall back to the full-scan Rust path in
    /// [`build_history_page`].
    fn for_input(input: &HistoryPageInput) -> Option<Self> {
        // A search term uses Rust `to_lowercase().contains()` over computed
        // display titles; SQL LOWER/LIKE has ASCII-only + Unicode divergence and
        // cannot see the computed fields, so it must not be approximated in SQL.
        if normalize_history_search(input.search.clone()).is_some() {
            return None;
        }
        // A category filter could be pushed into SQL, but the fast path exists
        // for the default view and the completion-driven refetch, neither of
        // which sets one. Keeping it on the Rust path means the two paths stay
        // one predicate apart instead of two, and a facet pays exactly what a
        // search already pays.
        if normalize_history_categories(input.categories.clone()).is_some() {
            return None;
        }
        // Only the default `completed_at DESC` ordering matches the indexed SQL
        // order (idx_job_history_completed_job → completed_at DESC, job_id DESC).
        // Any other sort field/direction is not indexed and its Rust tie-breaks
        // differ, so it stays on the Rust path.
        let sort_field = input.sort_field.unwrap_or(HistorySortField::CompletedAt);
        let sort_direction = input.sort_direction.unwrap_or(HistorySortDirection::Desc);
        if sort_field != HistorySortField::CompletedAt
            || sort_direction != HistorySortDirection::Desc
        {
            return None;
        }

        Some(Self {
            status: input.status.unwrap_or(HistoryStatusFilter::All),
            page_index: input.page_index as usize,
            page_size: sanitize_page_size(input.page_size),
        })
    }
}

/// SQL statuses that map to the "success" bucket (state == Completed): exactly
/// `complete`. Kept in lockstep with `history_state_from_row`.
const HISTORY_SUCCESS_STATUSES: &[&str] = &["complete"];
/// Statuses that are NEITHER success nor paused, i.e. the "failure" bucket
/// (state == Failed): every status except `complete` and `paused`. Expressed as
/// an exclusion because `history_state_from_row` maps every unknown status to
/// Failed, so an inclusion list could not be exhaustive.
const HISTORY_NON_FAILURE_STATUSES: &[&str] = &["complete", "paused"];

fn success_history_filter() -> weaver_server_core::HistoryFilter {
    weaver_server_core::HistoryFilter {
        statuses: Some(
            HISTORY_SUCCESS_STATUSES
                .iter()
                .map(|status| status.to_string())
                .collect(),
        ),
        ..Default::default()
    }
}

fn failure_history_filter() -> weaver_server_core::HistoryFilter {
    weaver_server_core::HistoryFilter {
        status_not_in: Some(
            HISTORY_NON_FAILURE_STATUSES
                .iter()
                .map(|status| status.to_string())
                .collect(),
        ),
        ..Default::default()
    }
}

fn build_history_page_sql(
    db: &Database,
    plan: HistoryPageSqlPlan,
) -> Result<HistoryPage, weaver_server_core::StateError> {
    // Counts are always computed over the entire (unfiltered-by-status) set, just
    // like the Rust path computes them before applying the status filter.
    let counts = HistoryPageCounts {
        all: db.count_job_history(&weaver_server_core::HistoryFilter::default())?,
        success: db.count_job_history(&success_history_filter())?,
        failure: db.count_job_history(&failure_history_filter())?,
    };

    // The status filter selects which rows are paginated and counted for
    // total_count. `All` needs no status predicate.
    let mut items_filter = match plan.status {
        HistoryStatusFilter::All => weaver_server_core::HistoryFilter::default(),
        HistoryStatusFilter::Success => success_history_filter(),
        HistoryStatusFilter::Failure => failure_history_filter(),
    };

    let total_count = db.count_job_history(&items_filter)?;

    items_filter.limit = Some(plan.page_size as u32);
    items_filter.offset =
        Some(u32::try_from(plan.page_index.saturating_mul(plan.page_size)).unwrap_or(u32::MAX));
    let rows = db.list_job_history(&items_filter)?;
    let items = rows
        .into_iter()
        .map(|row| history_table_item_from_row(&row, None))
        .collect();

    Ok(HistoryPage {
        items,
        total_count,
        counts,
    })
}

/// Ids of the jobs the scheduler still owns, i.e. whose status is neither of
/// the two terminal ones.
///
/// A job can have a history row while the scheduler still holds it: the row is
/// written before the job leaves the scheduler, and a re-queued job keeps its
/// old row. Such a row is not history — the job can still change state, and a
/// caller that treats it as history will try to remove it and be told the job
/// is still live. Every history read surface applies this same predicate so the
/// single-item read, the lists, and the queue cannot disagree.
pub(crate) fn live_job_ids(handle: &SchedulerHandle) -> HashSet<u64> {
    handle
        .list_jobs()
        .into_iter()
        .filter(|info| {
            !matches!(
                info.status,
                weaver_server_core::JobStatus::Complete
                    | weaver_server_core::JobStatus::Failed { .. }
            )
        })
        .map(|info| info.job_id.0)
        .collect()
}

/// Drop the history rows whose job is still live (see [`live_job_ids`]).
pub(crate) fn exclude_live_rows(
    rows: Vec<JobHistoryRow>,
    live_jobs: &HashSet<u64>,
) -> Vec<JobHistoryRow> {
    if live_jobs.is_empty() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| !live_jobs.contains(&row.job_id))
        .collect()
}

/// The subset of `live_jobs` that actually has a history row.
///
/// This is a point lookup over a handful of ids, not a scan: it answers whether
/// the live/archived overlap is non-empty before a caller decides how much work
/// the exclusion is worth. The overlap is normally empty, because a live job has
/// not been archived yet.
fn live_jobs_in_history(
    db: &Database,
    live_jobs: &HashSet<u64>,
) -> Result<HashSet<u64>, weaver_server_core::StateError> {
    if live_jobs.is_empty() {
        return Ok(HashSet::new());
    }
    let filter = weaver_server_core::HistoryFilter {
        item_ids: Some(live_jobs.iter().copied().collect()),
        ..Default::default()
    };
    Ok(db
        .list_job_history(&filter)?
        .into_iter()
        .map(|row| row.job_id)
        .collect())
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

fn history_query_plan(
    filter: Option<&QueueFilterInput>,
    limit: Option<u32>,
    offset: Option<usize>,
) -> HistoryQueryPlan {
    let mut history_filter = weaver_server_core::HistoryFilter {
        limit,
        offset: offset.map(|value| u32::try_from(value).unwrap_or(u32::MAX)),
        ..Default::default()
    };

    let Some(filter) = filter else {
        return HistoryQueryPlan::Query(history_filter);
    };

    if let Some(states) = &filter.states {
        let mut statuses = Vec::new();
        for state in states {
            match state {
                QueueItemState::Completed => statuses.push("complete".to_string()),
                QueueItemState::Failed => {
                    statuses.push("failed".to_string());
                    statuses.push("cancelled".to_string());
                }
                QueueItemState::Paused => statuses.push("paused".to_string()),
                QueueItemState::Queued
                | QueueItemState::Downloading
                | QueueItemState::FetchingRepairData
                | QueueItemState::FinalizingDownload
                | QueueItemState::Checking
                | QueueItemState::Verifying
                | QueueItemState::Repairing
                | QueueItemState::Extracting
                | QueueItemState::Finalizing
                | QueueItemState::PostProcessing => {}
            }
        }
        if statuses.is_empty() {
            return HistoryQueryPlan::Empty;
        }
        statuses.sort();
        statuses.dedup();
        history_filter.statuses = Some(statuses);
    }

    history_filter.item_ids = filter.item_ids.clone();
    history_filter.category = filter.category.clone();
    history_filter.metadata_has_key = filter.has_attribute_key.clone();
    history_filter.metadata_equals = filter.attribute_equals.as_ref().map(|attribute| {
        weaver_server_core::history::model::HistoryMetadataEquals {
            key: attribute.key.clone(),
            value: attribute.value.clone(),
        }
    });

    HistoryQueryPlan::Query(history_filter)
}

fn build_history_page(rows: Vec<JobHistoryRow>, input: HistoryPageInput) -> HistoryPage {
    let page_size = sanitize_page_size(input.page_size);
    let page_index = input.page_index as usize;
    let search = normalize_history_search(input.search);
    let categories = normalize_history_categories(input.categories);
    let status = input.status.unwrap_or(HistoryStatusFilter::All);
    let sort_field = input.sort_field.unwrap_or(HistorySortField::CompletedAt);
    let sort_direction = input.sort_direction.unwrap_or(HistorySortDirection::Desc);
    let requires_full_display_projection = search.is_some() || sort_field == HistorySortField::Name;

    // Delete-operation badges are attached by the caller for the final page only
    // (see `load_history_page`); they do not affect search, sort, status, or
    // counts, so they are omitted here.
    let filtered_by_search: Vec<HistoryItem> = rows
        .into_iter()
        .map(|row| {
            if requires_full_display_projection {
                history_item_from_row(&row, None)
            } else {
                history_table_item_from_row(&row, None)
            }
        })
        .filter(|item| history_matches_search(item, search.as_deref()))
        .filter(|item| history_matches_categories(item, categories.as_deref()))
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

/// Drop blank entries and an empty list, so a request that filters by nothing
/// is indistinguishable from one that omits the filter.
///
/// A blank entry is not dropped: the empty string is how a caller asks for rows
/// with no category, so it is trimmed but kept.
fn normalize_history_categories(categories: Option<Vec<String>>) -> Option<Vec<String>> {
    let trimmed: Vec<String> = categories?
        .into_iter()
        .map(|category| category.trim().to_string())
        .collect();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn history_matches_categories(item: &HistoryItem, categories: Option<&[String]>) -> bool {
    let Some(categories) = categories else {
        return true;
    };
    let category = item.category.as_deref().unwrap_or_default();
    categories.iter().any(|wanted| wanted == category)
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
        QueueItemState::FetchingRepairData => "fetching_repair_data",
        QueueItemState::FinalizingDownload => "downloading",
        QueueItemState::Checking => "checking",
        QueueItemState::Verifying => "verifying",
        QueueItemState::Repairing => "repairing",
        QueueItemState::Extracting => "extracting",
        QueueItemState::Finalizing => "finalizing",
        QueueItemState::PostProcessing => "post_processing",
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
        job_hash: row.job_hash.as_ref().and_then(|value| {
            (value.len() == 32).then(|| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(value);
                hash
            })
        }),
        name: row.name.clone(),
        status,
        download_state,
        finalizing_download: false,
        fetching_repair_data: false,
        post_state,
        run_state,
        progress,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        phase_progress: Vec::new(),
        failed_bytes: row.failed_bytes,
        health: row.health,
        terminal_discards: Vec::new(),
        total_files: 0,
        completed_files: 0,
        remaining_par_files: 0,
        password: None,
        category: row.category.clone(),
        metadata,
        output_dir: row.output_dir.clone(),
        error,
        download_wait_reason: None,
        download_retry_at_epoch_ms: None,
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
        server_attribution:
            weaver_server_core::jobs::server_attribution::contributions_from_storage(
                row.server_attribution.as_deref(),
            ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::JobHistoryRow;

    fn history_row(job_id: u64, status: &str, completed_at: i64) -> JobHistoryRow {
        JobHistoryRow {
            job_id,
            job_hash: None,
            name: format!("Release.{job_id}.1080p.WEB"),
            status: status.to_string(),
            error_message: (status != "complete" && status != "paused").then(|| "boom".to_string()),
            total_bytes: 1_000 + job_id,
            downloaded_bytes: 1_000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1_000,
            category: None,
            output_dir: None,
            nzb_path: None,
            created_at: completed_at - 10,
            completed_at,
            metadata: None,
            server_attribution: None,
        }
    }

    fn seed(db: &Database) {
        // A mix that exercises every state bucket, including statuses that only
        // arise via direct inserts (`cancelled`, `paused`) and map to Failed /
        // Paused in `history_state_from_row`.
        let rows = [
            history_row(1, "complete", 1_000),
            history_row(2, "failed", 1_100),
            history_row(3, "complete", 1_200),
            history_row(4, "cancelled", 1_300),
            history_row(5, "paused", 1_400),
            history_row(6, "complete", 1_500),
            history_row(7, "failed", 1_600),
        ];
        for row in &rows {
            db.insert_job_history(row).unwrap();
        }
    }

    fn page_input(
        page_index: u32,
        page_size: u32,
        status: Option<HistoryStatusFilter>,
    ) -> HistoryPageInput {
        HistoryPageInput {
            page_index,
            page_size,
            search: None,
            status,
            categories: None,
            sort_field: None,
            sort_direction: None,
        }
    }

    fn rust_page(db: &Database, input: HistoryPageInput) -> HistoryPage {
        let rows = db
            .list_job_history(&weaver_server_core::HistoryFilter::default())
            .unwrap();
        build_history_page(rows, input)
    }

    fn assert_parity(db: &Database, input: HistoryPageInput) {
        let plan = HistoryPageSqlPlan::for_input(&input)
            .expect("input should be SQL-eligible for this parity check");
        let sql = build_history_page_sql(db, plan).unwrap();
        let rust = rust_page(db, input);

        assert_eq!(
            sql.total_count, rust.total_count,
            "total_count mismatch: sql={} rust={}",
            sql.total_count, rust.total_count
        );
        assert_eq!(sql.counts.all, rust.counts.all, "counts.all mismatch");
        assert_eq!(
            sql.counts.success, rust.counts.success,
            "counts.success mismatch"
        );
        assert_eq!(
            sql.counts.failure, rust.counts.failure,
            "counts.failure mismatch"
        );
        let sql_ids: Vec<u64> = sql.items.iter().map(|item| item.id).collect();
        let rust_ids: Vec<u64> = rust.items.iter().map(|item| item.id).collect();
        assert_eq!(sql_ids, rust_ids, "page item ids/order mismatch");
        // Items are produced by the same `history_item_from_row`, so equal ids in
        // equal order means byte-identical items for identical data.
        assert_eq!(sql.items, rust.items, "page items mismatch");
    }

    #[test]
    fn sql_path_matches_rust_path_across_status_and_pages() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);

        for status in [
            None,
            Some(HistoryStatusFilter::All),
            Some(HistoryStatusFilter::Success),
            Some(HistoryStatusFilter::Failure),
        ] {
            // Multiple page sizes, including one that splits the result set and
            // one page fully past the end.
            for (page_index, page_size) in [(0, 25), (0, 2), (1, 2), (2, 2), (5, 2)] {
                assert_parity(&db, page_input(page_index, page_size, status));
            }
        }
    }

    #[test]
    fn sql_path_matches_rust_path_on_empty_table() {
        let db = Database::open_in_memory().unwrap();
        assert_parity(&db, page_input(0, 25, None));
        assert_parity(&db, page_input(0, 25, Some(HistoryStatusFilter::Failure)));
    }

    #[test]
    fn search_term_forces_rust_path() {
        let mut input = page_input(0, 25, None);
        input.search = Some("release".to_string());
        assert!(
            HistoryPageSqlPlan::for_input(&input).is_none(),
            "a free-text search term must not take the SQL path"
        );
        // Whitespace-only search is normalized away and stays SQL-eligible.
        input.search = Some("   ".to_string());
        assert!(HistoryPageSqlPlan::for_input(&input).is_some());
    }

    #[test]
    fn category_filter_forces_rust_path() {
        let mut input = page_input(0, 25, None);
        input.categories = Some(vec!["movies".to_string()]);
        assert!(
            HistoryPageSqlPlan::for_input(&input).is_none(),
            "a category filter must not take the SQL path"
        );
        // An empty list filters by nothing, so it stays SQL-eligible.
        input.categories = Some(Vec::new());
        assert!(HistoryPageSqlPlan::for_input(&input).is_some());
    }

    #[test]
    fn category_filter_unions_and_scopes_the_counts() {
        let db = Database::open_in_memory().unwrap();
        let mut rows = [
            history_row(1, "complete", 1_000),
            history_row(2, "failed", 1_100),
            history_row(3, "complete", 1_200),
            history_row(4, "complete", 1_300),
        ];
        rows[0].category = Some("movies".to_string());
        rows[1].category = Some("movies".to_string());
        rows[2].category = Some("tv".to_string());
        // Row 4 keeps `None`, which the empty string selects.
        for row in &rows {
            db.insert_job_history(row).unwrap();
        }

        let mut input = page_input(0, 25, None);
        input.categories = Some(vec!["movies".to_string()]);
        let page = rust_page(&db, input.clone());
        assert_eq!(page.total_count, 2);
        assert_eq!(page.counts.all, 2, "counts describe the filtered set");
        assert_eq!(page.counts.success, 1);
        assert_eq!(page.counts.failure, 1);

        // A second facet widens rather than narrows.
        input.categories = Some(vec!["movies".to_string(), "tv".to_string()]);
        let page = rust_page(&db, input.clone());
        assert_eq!(page.total_count, 3);
        assert_eq!(page.counts.success, 2);

        // The empty string is how a caller asks for rows with no category.
        input.categories = Some(vec![String::new()]);
        let page = rust_page(&db, input.clone());
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![4]);

        // A status filter still applies on top of the facets.
        input.categories = Some(vec!["movies".to_string(), "tv".to_string()]);
        input.status = Some(HistoryStatusFilter::Failure);
        let page = rust_page(&db, input);
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![2]);
    }

    #[test]
    fn non_default_sort_forces_rust_path() {
        let mut input = page_input(0, 25, None);
        input.sort_field = Some(HistorySortField::Name);
        assert!(HistoryPageSqlPlan::for_input(&input).is_none());

        let mut input = page_input(0, 25, None);
        input.sort_direction = Some(HistorySortDirection::Asc);
        assert!(HistoryPageSqlPlan::for_input(&input).is_none());

        // Explicit default sort field + direction is still SQL-eligible.
        let mut input = page_input(0, 25, None);
        input.sort_field = Some(HistorySortField::CompletedAt);
        input.sort_direction = Some(HistorySortDirection::Desc);
        assert!(HistoryPageSqlPlan::for_input(&input).is_some());
    }

    /// A scheduler handle that reports `jobs` as its resident job list. Nothing
    /// here drives the pipeline; the reads under test go straight to the
    /// published list.
    fn scheduler_with_jobs(jobs: Vec<JobInfo>) -> SchedulerHandle {
        let (command_tx, _command_rx) = tokio::sync::mpsc::channel(1);
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(1);
        SchedulerHandle::new(
            command_tx,
            event_tx,
            weaver_server_core::jobs::handle::SharedPipelineState::new(
                weaver_server_core::operations::metrics::PipelineMetrics::new(),
                jobs,
            ),
        )
    }

    /// The scheduler's view of an already-archived job, in `status`.
    fn resident_job(row: &JobHistoryRow, status: weaver_server_core::JobStatus) -> JobInfo {
        let mut info = job_info_from_history_row(row);
        info.status = status;
        info
    }

    #[tokio::test]
    async fn a_job_the_scheduler_still_owns_is_not_on_the_history_page() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        // Job 3 has a history row and is also resident in the scheduler in a
        // non-terminal status, the state a re-queued job passes through.
        let row = history_row(3, "complete", 1_200);
        let handle = scheduler_with_jobs(vec![resident_job(
            &row,
            weaver_server_core::JobStatus::Downloading,
        )]);
        let live = live_job_ids(&handle);
        assert!(live.contains(&3));

        let page = load_history_page(db.clone(), live.clone(), page_input(0, 25, None))
            .await
            .unwrap();
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert!(!ids.contains(&3), "a live job must not appear as history");
        // Excluding the row also removes it from the counts and total, so the
        // page's own numbers describe the rows it returns.
        assert_eq!(page.total_count, 6);
        assert_eq!(page.counts.all, 6);
        assert_eq!(page.counts.success, 2);
        assert_eq!(page.counts.failure, 3);

        // The list surfaces share the exclusion, so they agree with the page.
        let rows = db
            .list_job_history(&weaver_server_core::HistoryFilter::default())
            .unwrap();
        let list_ids: Vec<u64> = exclude_live_rows(rows, &live)
            .iter()
            .map(|row| row.job_id)
            .collect();
        assert!(!list_ids.contains(&3));

        // Once the scheduler reports it terminal, the same row is history again.
        let handle = scheduler_with_jobs(vec![resident_job(
            &row,
            weaver_server_core::JobStatus::Complete,
        )]);
        let live = live_job_ids(&handle);
        assert!(live.is_empty());
        let page = load_history_page(db, live, page_input(0, 25, None))
            .await
            .unwrap();
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert!(ids.contains(&3));
        assert_eq!(page.total_count, 7);
        assert_eq!(page.counts.success, 3);
    }

    #[tokio::test]
    async fn a_live_job_without_a_history_row_changes_nothing() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        // The normal case: the resident job has never been archived, so the
        // exclusion finds no overlap and the page is built exactly as before.
        let unarchived = history_row(42, "downloading", 2_000);
        let handle = scheduler_with_jobs(vec![resident_job(
            &unarchived,
            weaver_server_core::JobStatus::Downloading,
        )]);
        let live = live_job_ids(&handle);
        assert_eq!(live.len(), 1);

        let page = load_history_page(db, live, page_input(0, 25, None))
            .await
            .unwrap();
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(page.total_count, 7);
        assert_eq!(page.counts.all, 7);
    }

    #[test]
    fn a_limited_list_page_is_short_by_its_live_rows() {
        // `historyItems` applies the exclusion after the query's LIMIT, so a
        // page holding a live job's row comes back short rather than pulling the
        // next row forward onto it.
        let rows = vec![
            history_row(1, "complete", 1_000),
            history_row(2, "complete", 1_100),
        ];
        let live = HashSet::from([2]);
        let kept: Vec<u64> = exclude_live_rows(rows, &live)
            .iter()
            .map(|row| row.job_id)
            .collect();
        assert_eq!(kept, vec![1]);
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#[cfg(test)]
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::*;
use crate::history::types::{history_delete_row_state_from_core, history_table_item_from_row};
use crate::jobs::types::load_duplicate_summaries_chunked;
use weaver_server_core::HistoryRows;

enum HistoryQueryPlan {
    Empty,
    Query(Box<weaver_server_core::HistoryFilter>),
}

// Upper bound on job-event rows loaded for the polled detail snapshot. Large
// RAR jobs write ~5-7 event rows per extracted member, so an unbounded read of
// a big job's log can be thousands of rows re-fetched a few times a second
// while a tab is open. 2000 is a generous tail — enough to render every
// lifecycle/verification/repair marker plus recent extraction activity — while
// capping the per-poll cost. Only the detail-view read is bounded; callers that
// must see the full log use `get_job_events`.
const JOB_DETAIL_EVENT_CAP: u32 = 2000;
// The page `historyItems` returns when the caller names no size, and the
// largest it returns when one does.
const HISTORY_ITEMS_DEFAULT_FIRST: u32 = 1000;
const HISTORY_ITEMS_MAX_FIRST: u32 = 1000;

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
        let first = first
            .unwrap_or(HISTORY_ITEMS_DEFAULT_FIRST)
            .min(HISTORY_ITEMS_MAX_FIRST);
        let plan = history_query_plan(filter.as_ref(), Some(first), Some(offset));
        let HistoryQueryPlan::Query(mut history_filter) = plan else {
            return Ok(Vec::new());
        };
        let want_delete_states = ctx.look_ahead().field("deleteOperation").exists();
        let want_duplicates = ctx.look_ahead().field("duplicateSummary").exists();
        let items = tokio::task::spawn_blocking(move || {
            // Live rows are left out in SQL, before the page is cut, so a page
            // comes back full and the cursor's offset counts only rows that are
            // history.
            let live_in_history = live_jobs_in_history(&db, &live_jobs)?;
            if !live_in_history.is_empty() {
                let mut ids: Vec<u64> = live_in_history.into_iter().collect();
                ids.sort_unstable();
                history_filter.item_ids_not_in = Some(ids);
            }
            let rows = db.list_job_history(&history_filter, HistoryRows::Shown)?;
            let delete_states = if want_delete_states {
                load_history_delete_states(&db, rows.iter().map(|row| row.job_id))?
            } else {
                HashMap::new()
            };
            let duplicate_summaries = if want_duplicates {
                load_duplicate_summaries_chunked(
                    &db,
                    rows.iter()
                        .map(|row| weaver_server_core::jobs::ids::JobId(row.job_id)),
                )?
            } else {
                Default::default()
            };
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
        load_history_page(db, live_jobs, input, HistoryPageFields::selected(ctx)).await
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
        let live_jobs = live_job_ids(ctx.data::<SchedulerHandle>()?);
        let plan = history_query_plan(filter.as_ref(), None, None);
        let HistoryQueryPlan::Query(history_filter) = plan else {
            return Ok(0);
        };
        // The count answers for the same rows the list returns, so the rows the
        // list excludes as still-live are taken off it too.
        let count = tokio::task::spawn_blocking(move || {
            let counted = db.count_job_history(&history_filter, HistoryRows::Shown)?;
            let live = count_live_rows_matching(&db, &history_filter, &live_jobs)?;
            Ok::<_, weaver_server_core::StateError>(counted.saturating_sub(live))
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
    #[graphql(guard = "ReadGuard")]
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
            // Every event the timeline acts on, however old: a newest-N
            // window would drop the early stage and member boundaries.
            let events =
                db.get_job_events_of_kinds(job_id, crate::history::timeline::TIMELINE_EVENT_KINDS)?;
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
    #[graphql(guard = "ReadGuard")]
    async fn job_events(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<JobEvent>> {
        let db = ctx.data::<weaver_server_core::Database>()?;
        let db = db.clone();
        // The newest events, oldest-first: `offset` counts back from the
        // newest, and a page is at most the window the detail snapshot reads.
        let limit = limit
            .unwrap_or(JOB_DETAIL_EVENT_CAP)
            .min(JOB_DETAIL_EVENT_CAP);
        let offset = offset.unwrap_or(0);
        let events =
            tokio::task::spawn_blocking(move || db.get_job_events_window(job_id, limit, offset))
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(events
            .into_iter()
            .filter_map(crate::history::types::job_event_from_core)
            .collect())
    }
}

// Host label per configured server id, for naming a job's contributing servers
// without denormalizing the host into every archived job.
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
        // whole log.
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
        .filter_map(crate::history::types::job_event_from_core)
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

// The parts of a History page a query selected; the rest are left at their
// defaults without being read.
#[derive(Debug, Clone, Copy)]
struct HistoryPageFields {
    total_count: bool,
    counts: bool,
    category_counts: bool,
    delete_operation: bool,
    duplicate_summary: bool,
}

impl HistoryPageFields {
    #[cfg(test)]
    const ALL: Self = Self {
        total_count: true,
        counts: true,
        category_counts: true,
        delete_operation: true,
        duplicate_summary: true,
    };

    fn selected(ctx: &Context<'_>) -> Self {
        let page = ctx.look_ahead();
        let items = page.field("items");
        Self {
            total_count: page.field("totalCount").exists(),
            counts: page.field("counts").exists(),
            category_counts: page.field("categoryCounts").exists(),
            delete_operation: items.field("deleteOperation").exists(),
            duplicate_summary: items.field("duplicateSummary").exists(),
        }
    }
}

async fn load_history_page(
    db: Database,
    live_jobs: HashSet<u64>,
    input: HistoryPageInput,
    fields: HistoryPageFields,
) -> Result<HistoryPage> {
    tokio::task::spawn_blocking(move || {
        // Rows for jobs the scheduler still owns are not history. They are
        // excluded before counting and pagination, on whichever path builds the
        // page, so `total_count` and the bucket counts describe exactly the
        // rows the page draws from. The overlap is normally empty and costs one
        // indexed lookup over the live ids; when it is not, the SQL path still
        // pages in SQL with a `NOT IN` over that handful of ids.
        let live_in_history = live_jobs_in_history(&db, &live_jobs)?;
        // The search, the status and category filters, the order, the counts
        // and LIMIT/OFFSET all run in the database, each order served by its
        // own index.
        let plan = HistoryPageSqlPlan::for_input(&input);
        let page = build_history_page_sql(&db, plan, &live_in_history, fields)?;
        attach_history_page_badges(&db, page, fields)
    })
    .await
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))
}

// Attach the per-row delete-operation and duplicate badges to a built page.
//
// Delete-operation badges are only needed for the rows actually shown on this
// page. Load them for the page's job ids instead of issuing an `IN (…)` over
// every row in the history table. Every page-building path shares this tail so
// the response (page shape, counts, per-page delete states) is identical for
// identical data.
fn attach_history_page_badges(
    db: &Database,
    mut page: HistoryPage,
    fields: HistoryPageFields,
) -> Result<HistoryPage, weaver_server_core::StateError> {
    let delete_states = if fields.delete_operation {
        load_history_delete_states(db, page.items.iter().map(|item| item.id))?
    } else {
        HashMap::new()
    };
    let duplicate_summaries = if fields.duplicate_summary {
        load_duplicate_summaries_chunked(
            db,
            page.items
                .iter()
                .map(|item| weaver_server_core::jobs::ids::JobId(item.id)),
        )?
    } else {
        Default::default()
    };
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

// A History-page request as SQL filtering, ordering, counting, and
// pagination.
struct HistoryPageSqlPlan {
    // Text the full release name contains, ASCII letters folded.
    search: Option<String>,
    status: HistoryStatusFilter,
    categories: Option<Vec<String>>,
    order: weaver_server_core::HistoryOrder,
    page_index: usize,
    page_size: usize,
}

impl HistoryPageSqlPlan {
    fn for_input(input: &HistoryPageInput) -> Self {
        // Every sort compares stored columns, and ties go to the job id in the
        // sort's direction, which is the order of the index on each key.
        let key = history_order_key(input.sort_field.unwrap_or(HistorySortField::CompletedAt));
        let descending = input.sort_direction.unwrap_or(HistorySortDirection::Desc)
            == HistorySortDirection::Desc;

        Self {
            // A search matches the full release name, as the name sort orders
            // it.
            search: normalize_history_search(input.search.clone()),
            status: input.status.unwrap_or(HistoryStatusFilter::All),
            // A category is the row's own column, compared exactly, with a row
            // that has none matching the empty string.
            categories: normalize_history_categories(input.categories.clone()),
            order: weaver_server_core::HistoryOrder { key, descending },
            page_index: input.page_index as usize,
            page_size: sanitize_page_size(input.page_size),
        }
    }
}

fn history_order_key(field: HistorySortField) -> weaver_server_core::HistoryOrderKey {
    match field {
        HistorySortField::CompletedAt => weaver_server_core::HistoryOrderKey::CompletedAt,
        HistorySortField::Size => weaver_server_core::HistoryOrderKey::TotalBytes,
        HistorySortField::Health => weaver_server_core::HistoryOrderKey::Health,
        HistorySortField::Name => weaver_server_core::HistoryOrderKey::Name,
        HistorySortField::State => weaver_server_core::HistoryOrderKey::State,
        HistorySortField::Category => weaver_server_core::HistoryOrderKey::Category,
    }
}

// SQL statuses that map to the "success" bucket (state == Completed): exactly
// `complete`. Kept in lockstep with `history_state_from_row`.
const HISTORY_SUCCESS_STATUSES: &[&str] = &["complete"];
// Statuses that are NEITHER success nor paused, i.e. the "failure" bucket
// (state == Failed): every status except `complete` and `paused`. Expressed as
// an exclusion because `history_state_from_row` maps every unknown status to
// Failed, so an inclusion list could not be exhaustive.
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
    live_in_history: &HashSet<u64>,
    fields: HistoryPageFields,
) -> Result<HistoryPage, weaver_server_core::StateError> {
    // Every count and the page itself keep only the rows the search matches,
    // and leave out rows of jobs still live.
    let searched = |mut filter: weaver_server_core::HistoryFilter| {
        filter.name_contains = plan.search.clone();
        if !live_in_history.is_empty() {
            let mut ids: Vec<u64> = live_in_history.iter().copied().collect();
            ids.sort_unstable();
            filter.item_ids_not_in = Some(ids);
        }
        filter
    };
    // The bucket counts, the total and the page keep only the selected
    // categories.
    let in_categories = |mut filter: weaver_server_core::HistoryFilter| {
        filter.categories_any = plan.categories.clone();
        searched(filter)
    };

    // Counts are computed over the category-filtered set before the status
    // filter. The total is the bucket
    // count of the requested status, so it is read once for either field.
    let bucket = |status: HistoryStatusFilter| -> Result<u32, weaver_server_core::StateError> {
        if !(fields.counts || (fields.total_count && plan.status == status)) {
            return Ok(0);
        }
        let filter = match status {
            HistoryStatusFilter::All => weaver_server_core::HistoryFilter::default(),
            HistoryStatusFilter::Success => success_history_filter(),
            HistoryStatusFilter::Failure => failure_history_filter(),
        };
        db.count_job_history(&in_categories(filter), HistoryRows::Shown)
    };
    let counts = HistoryPageCounts {
        all: bucket(HistoryStatusFilter::All)?,
        success: bucket(HistoryStatusFilter::Success)?,
        failure: bucket(HistoryStatusFilter::Failure)?,
    };
    let total_count = match plan.status {
        HistoryStatusFilter::All => counts.all,
        HistoryStatusFilter::Success => counts.success,
        HistoryStatusFilter::Failure => counts.failure,
    };

    // The status filter selects which rows are paginated. `All` needs no
    // status predicate.
    let mut items_filter = in_categories(match plan.status {
        HistoryStatusFilter::All => weaver_server_core::HistoryFilter::default(),
        HistoryStatusFilter::Success => success_history_filter(),
        HistoryStatusFilter::Failure => failure_history_filter(),
    });

    // Facet counts describe every category the search matches, before the
    // category filter: a facet's count has to survive that facet's rivals
    // being selected.
    let mut category_counts: Vec<HistoryCategoryCount> = if fields.category_counts {
        db.count_job_history_by_category(
            &searched(weaver_server_core::HistoryFilter::default()),
            HistoryRows::Shown,
        )?
        .into_iter()
        .map(|(category, count)| HistoryCategoryCount { category, count })
        .collect()
    } else {
        Vec::new()
    };
    category_counts.sort_by(|left, right| left.category.cmp(&right.category));

    items_filter.order = plan.order;
    items_filter.limit = Some(plan.page_size as u32);
    items_filter.offset =
        Some(u32::try_from(plan.page_index.saturating_mul(plan.page_size)).unwrap_or(u32::MAX));
    let rows = db.list_job_history(&items_filter, HistoryRows::Shown)?;
    let items = rows
        .into_iter()
        .map(|row| history_table_item_from_row(&row, None))
        .collect();

    Ok(HistoryPage {
        items,
        total_count,
        counts,
        category_counts,
    })
}

// Ids of every row the queue still owns, including hidden cancellations and
// terminal rows whose archive acknowledgement has not retired them yet.
//
// A job can have a history row while the scheduler still holds it: the row is
// written before the job leaves the scheduler, and a re-queued job keeps its
// old row. Such a row is not history — the job can still change state, and a
// caller that treats it as history will try to remove it and be told the job
// is still live. Every history read surface applies this same predicate so the
// single-item read, the lists, and the queue cannot disagree.
pub(crate) fn live_job_ids(handle: &SchedulerHandle) -> HashSet<u64> {
    handle.queued_job_ids().into_iter().map(|id| id.0).collect()
}

// Drop the history rows whose job is still live (see [`live_job_ids`]).
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

// The subset of `live_jobs` that actually has a history row.
//
// This is a point lookup over a handful of ids, not a scan: it answers whether
// the live/archived overlap is non-empty before a caller decides how much work
// the exclusion is worth. The overlap is normally empty, because a live job has
// not been archived yet.
fn live_jobs_in_history(
    db: &Database,
    live_jobs: &HashSet<u64>,
) -> Result<HashSet<u64>, weaver_server_core::StateError> {
    if live_jobs.is_empty() {
        return Ok(HashSet::new());
    }
    let live_jobs: Vec<u64> = live_jobs.iter().copied().collect();
    // An exclusion list: an id that is hidden anyway changes nothing.
    Ok(db.job_history_ids_among(&live_jobs)?.into_iter().collect())
}

// How many of the rows `filter` counts belong to jobs the scheduler still
// owns (see [`live_job_ids`]). A point lookup over the live ids, narrowed to
// the filter's own id list when it has one.
fn count_live_rows_matching(
    db: &Database,
    filter: &weaver_server_core::HistoryFilter,
    live_jobs: &HashSet<u64>,
) -> Result<u32, weaver_server_core::StateError> {
    if live_jobs.is_empty() {
        return Ok(0);
    }
    let item_ids: Vec<u64> = match &filter.item_ids {
        Some(ids) => ids
            .iter()
            .copied()
            .filter(|id| live_jobs.contains(id))
            .collect(),
        None => live_jobs.iter().copied().collect(),
    };
    if item_ids.is_empty() {
        return Ok(0);
    }
    let filter = weaver_server_core::HistoryFilter {
        item_ids: Some(item_ids),
        limit: None,
        offset: None,
        ..filter.clone()
    };
    // Taken off a count of shown rows, so it counts shown rows too.
    db.count_job_history(&filter, HistoryRows::Shown)
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
        return HistoryQueryPlan::Query(Box::new(history_filter));
    };

    if let Some(states) = &filter.states {
        let mut statuses = Vec::new();
        for state in states {
            match state {
                QueueItemState::Completed => statuses.push("complete".to_string()),
                // Clients see a partial delivery as failed, so asking for
                // failed rows asks for partial ones too.
                QueueItemState::Failed => {
                    statuses.push("failed".to_string());
                    statuses.push("partial".to_string());
                    statuses.push("cancelled".to_string());
                }
                QueueItemState::Paused => statuses.push("paused".to_string()),
                QueueItemState::AwaitingQueueScripts
                | QueueItemState::Queued
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

    HistoryQueryPlan::Query(Box::new(history_filter))
}

// The page decided from whole items. Tests hold the SQL path to it.
#[cfg(test)]
fn build_history_page(rows: Vec<JobHistoryRow>, input: HistoryPageInput) -> HistoryPage {
    let page_size = sanitize_page_size(input.page_size);
    let page_index = input.page_index as usize;
    let search = normalize_history_search(input.search);
    let categories = normalize_history_categories(input.categories);
    let status = input.status.unwrap_or(HistoryStatusFilter::All);
    let sort_field = input.sort_field.unwrap_or(HistorySortField::CompletedAt);
    let sort_direction = input.sort_direction.unwrap_or(HistorySortDirection::Desc);
    let matched_search: Vec<HistoryItem> = rows
        .into_iter()
        .map(|row| history_table_item_from_row(&row, None))
        .filter(|item| {
            search
                .as_deref()
                .is_none_or(|search| history_sort_name(&item.original_title).contains(search))
        })
        .collect();

    let category_counts =
        count_history_categories(matched_search.iter().map(|item| item.category.as_deref()));

    let filtered_by_search: Vec<HistoryItem> = matched_search
        .into_iter()
        .filter(|item| history_category_selected(item.category.as_deref(), categories.as_deref()))
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
        .filter(|item| history_state_matches(item.state, status))
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
        category_counts,
    }
}

// Tally the rows per category, ordered by category so the response does not
// depend on hash iteration order.
#[cfg(test)]
fn count_history_categories<'a>(
    categories: impl Iterator<Item = Option<&'a str>>,
) -> Vec<HistoryCategoryCount> {
    let mut counts: HashMap<&str, u32> = HashMap::new();
    for category in categories {
        *counts.entry(category.unwrap_or_default()).or_default() += 1;
    }
    let mut counts: Vec<HistoryCategoryCount> = counts
        .into_iter()
        .map(|(category, count)| HistoryCategoryCount {
            category: category.to_string(),
            count,
        })
        .collect();
    counts.sort_by(|left, right| left.category.cmp(&right.category));
    counts
}

fn sanitize_page_size(page_size: u32) -> usize {
    match page_size {
        0 => 25,
        value => value.min(500) as usize,
    }
}

// The search text, trimmed, its ASCII letters folded as the name's are.
fn normalize_history_search(search: Option<String>) -> Option<String> {
    let trimmed = search?.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

// Drop blank entries and an empty list, so a request that filters by nothing
// is indistinguishable from one that omits the filter.
//
// A blank entry is not dropped: the empty string is how a caller asks for rows
// with no category, so it is trimmed but kept.
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

#[cfg(test)]
fn history_category_selected(category: Option<&str>, categories: Option<&[String]>) -> bool {
    let Some(categories) = categories else {
        return true;
    };
    let category = category.unwrap_or_default();
    categories.iter().any(|wanted| wanted == category)
}

#[cfg(test)]
fn history_state_matches(state: QueueItemState, status: HistoryStatusFilter) -> bool {
    match status {
        HistoryStatusFilter::All => true,
        HistoryStatusFilter::Success => state == QueueItemState::Completed,
        HistoryStatusFilter::Failure => state == QueueItemState::Failed,
    }
}

// The order of two whole items; the database's order is held to it.
#[cfg(test)]
fn compare_history_items(
    left: &HistoryItem,
    right: &HistoryItem,
    sort_field: HistorySortField,
    sort_direction: HistorySortDirection,
) -> Ordering {
    let ordering = match sort_field {
        HistorySortField::CompletedAt => left.completed_at.cmp(&right.completed_at),
        HistorySortField::Name => {
            history_sort_name(&left.original_title).cmp(&history_sort_name(&right.original_title))
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
            .to_ascii_lowercase()
            .cmp(
                &right
                    .category
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase(),
            ),
    };
    directed_history_order(ordering, left.id, right.id, sort_direction)
}

// Ties go to the job id; the direction applies to both.
#[cfg(test)]
fn directed_history_order(
    ordering: Ordering,
    left_id: u64,
    right_id: u64,
    sort_direction: HistorySortDirection,
) -> Ordering {
    let stable = if ordering == Ordering::Equal {
        left_id.cmp(&right_id)
    } else {
        ordering
    };
    match sort_direction {
        HistorySortDirection::Asc => stable,
        HistorySortDirection::Desc => stable.reverse(),
    }
}

// The full release name with its ASCII letters folded, as the database's
// name order compares it.
#[cfg(test)]
fn history_sort_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

#[cfg(test)]
fn history_state_key(state: QueueItemState) -> &'static str {
    match state {
        QueueItemState::AwaitingQueueScripts => "awaiting_queue_scripts",
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

pub(crate) fn job_info_from_history_row(row: &JobHistoryRow) -> JobInfo {
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
        file_remaining_bytes: Vec::new(),
        file_parked_bytes: Vec::new(),
        remaining_bytes: 0,
        wanted_bytes: 0,
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

    fn rust_page(db: &Database, input: HistoryPageInput, live: &HashSet<u64>) -> HistoryPage {
        let rows = db
            .list_job_history(
                &weaver_server_core::HistoryFilter::default(),
                HistoryRows::Shown,
            )
            .unwrap();
        build_history_page(exclude_live_rows(rows, live), input)
    }

    fn assert_parity(db: &Database, input: HistoryPageInput) {
        assert_parity_excluding(db, input, &HashSet::new());
    }

    fn assert_parity_excluding(db: &Database, input: HistoryPageInput, live: &HashSet<u64>) {
        let plan = HistoryPageSqlPlan::for_input(&input);
        let sql = build_history_page_sql(db, plan, live, HistoryPageFields::ALL).unwrap();
        let rust = rust_page(db, input, live);

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
        assert_eq!(
            sql.category_counts, rust.category_counts,
            "category counts mismatch"
        );
        let sql_ids: Vec<u64> = sql.items.iter().map(|item| item.id).collect();
        let rust_ids: Vec<u64> = rust.items.iter().map(|item| item.id).collect();
        assert_eq!(sql_ids, rust_ids, "page item ids/order mismatch");
        // Items are produced by the same `history_item_from_row`, so equal ids in
        // equal order means byte-identical items for identical data.
        assert_eq!(sql.items, rust.items, "page items mismatch");
    }

    // The bug this guards: a facet list built from the rows a filtered page
    // returned shows only the facet already selected, so there is no way back
    // to the others and no way to discover them in the first place.
    #[test]
    fn category_counts_survive_the_category_filter() {
        let db = Database::open_in_memory().unwrap();
        let mut rows = [
            history_row(1, "complete", 1_000),
            history_row(2, "complete", 1_100),
            history_row(3, "failed", 1_200),
            history_row(4, "complete", 1_300),
        ];
        rows[0].category = Some("anime".to_string());
        rows[1].category = Some("movie".to_string());
        rows[2].category = Some("movie".to_string());
        for row in &rows {
            db.insert_job_history(row).unwrap();
        }

        let mut input = page_input(0, 25, None);
        input.categories = Some(vec!["anime".to_string()]);
        let page = rust_page(&db, input, &HashSet::new());

        // One anime row on the page, and every category still countable.
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.category_counts,
            vec![
                HistoryCategoryCount {
                    category: String::new(),
                    count: 1
                },
                HistoryCategoryCount {
                    category: "anime".to_string(),
                    count: 1
                },
                HistoryCategoryCount {
                    category: "movie".to_string(),
                    count: 2
                },
            ]
        );
    }

    // A search is the one filter the counts do follow: it changes which rows
    // are on offer at all, so a facet's number has to answer for the search
    // the person typed.
    #[test]
    fn category_counts_follow_the_search() {
        let db = Database::open_in_memory().unwrap();
        let mut rows = [
            history_row(1, "complete", 1_000),
            history_row(2, "complete", 1_100),
        ];
        rows[0].category = Some("anime".to_string());
        rows[1].category = Some("movie".to_string());
        for row in &rows {
            db.insert_job_history(row).unwrap();
        }

        let mut input = page_input(0, 25, None);
        input.search = Some("Release.1.".to_string());
        let page = rust_page(&db, input, &HashSet::new());

        assert_eq!(
            page.category_counts,
            vec![HistoryCategoryCount {
                category: "anime".to_string(),
                count: 1
            }]
        );
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

    // A page that selects only some parts reads only those, and the parts it
    // does select match the whole page.
    #[test]
    fn a_page_reads_only_the_parts_it_selects() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        let live = HashSet::from([6]);
        let none = HistoryPageFields {
            total_count: false,
            counts: false,
            category_counts: false,
            delete_operation: false,
            duplicate_summary: false,
        };
        for status in [
            HistoryStatusFilter::All,
            HistoryStatusFilter::Success,
            HistoryStatusFilter::Failure,
        ] {
            let plan = || HistoryPageSqlPlan::for_input(&page_input(0, 3, Some(status)));
            let whole = build_history_page_sql(&db, plan(), &live, HistoryPageFields::ALL).unwrap();
            let total_only = build_history_page_sql(
                &db,
                plan(),
                &live,
                HistoryPageFields {
                    total_count: true,
                    ..none
                },
            )
            .unwrap();
            assert_eq!(total_only.total_count, whole.total_count, "{status:?}");
            assert_eq!(total_only.items, whole.items, "{status:?}");
            assert!(total_only.category_counts.is_empty());

            let bare = build_history_page_sql(&db, plan(), &live, none).unwrap();
            assert_eq!(bare.total_count, 0);
            assert_eq!(
                (bare.counts.all, bare.counts.success, bare.counts.failure),
                (0, 0, 0)
            );
            assert_eq!(
                bare.items.iter().map(|item| item.id).collect::<Vec<_>>(),
                whole.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            );

            let counts_only = build_history_page_sql(
                &db,
                plan(),
                &live,
                HistoryPageFields {
                    counts: true,
                    ..none
                },
            )
            .unwrap();
            assert_eq!(
                (
                    counts_only.counts.all,
                    counts_only.counts.success,
                    counts_only.counts.failure
                ),
                (whole.counts.all, whole.counts.success, whole.counts.failure),
            );
        }
    }

    // A re-queued job's old row stays on the SQL path: the live ids are
    // excluded in SQL, and the page, its total and every bucket count match
    // the Rust path over the same rows with those ids removed.
    #[test]
    fn sql_path_excludes_live_rows_and_matches_rust_path() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        let live: HashSet<u64> = [2, 6].into_iter().collect();

        for status in [
            None,
            Some(HistoryStatusFilter::Success),
            Some(HistoryStatusFilter::Failure),
        ] {
            for (page_index, page_size) in [(0, 25), (0, 2), (1, 2), (3, 2)] {
                assert_parity_excluding(&db, page_input(page_index, page_size, status), &live);
            }
        }

        let plan = HistoryPageSqlPlan::for_input(&page_input(0, 25, None));
        let page = build_history_page_sql(&db, plan, &live, HistoryPageFields::ALL).unwrap();
        assert_eq!(page.total_count, 5);
        assert!(page.items.iter().all(|item| !live.contains(&item.id)));
    }

    #[test]
    fn sql_path_matches_rust_path_on_empty_table() {
        let db = Database::open_in_memory().unwrap();
        assert_parity(&db, page_input(0, 25, None));
        assert_parity(&db, page_input(0, 25, Some(HistoryStatusFilter::Failure)));
    }

    // A search is trimmed and its ASCII letters folded, as the name is.
    #[test]
    fn a_search_is_trimmed_and_folded() {
        let mut input = page_input(0, 25, None);
        input.search = Some("  ReLeAse.ÜNÏ ".to_string());
        assert_eq!(
            HistoryPageSqlPlan::for_input(&input).search.as_deref(),
            Some("release.Ünï")
        );
        // A whitespace-only search searches nothing.
        input.search = Some("   ".to_string());
        assert_eq!(HistoryPageSqlPlan::for_input(&input).search, None);
    }

    // The page searches and sorts by the full release name, not the shortened
    // name stored beside it: the original title, or a rename that replaced it.
    #[test]
    fn search_and_name_sort_use_the_full_release_name() {
        let db = Database::open_in_memory().unwrap();
        let mut original = history_row(1, "complete", 1_000);
        original.name = "The Movie".to_string();
        original.metadata = Some(
            serde_json::to_string(&vec![(
                "weaver.original_title".to_string(),
                "Zeta.The.Movie.2024.2160p.WEB".to_string(),
            )])
            .unwrap(),
        );
        let mut renamed = history_row(2, "complete", 1_000);
        renamed.name = "Zz Renamed".to_string();
        renamed.metadata = Some(
            serde_json::to_string(&vec![
                (
                    "weaver.original_title".to_string(),
                    "Before.Rename.1080p".to_string(),
                ),
                (
                    "weaver.renamed_title".to_string(),
                    "Beta.Renamed".to_string(),
                ),
            ])
            .unwrap(),
        );
        let mut plain = history_row(3, "complete", 1_000);
        plain.name = "Alpha.Plain.720p".to_string();
        for row in [&original, &renamed, &plain] {
            db.insert_job_history(row).unwrap();
        }
        let page = |search: Option<&str>| {
            let mut input = page_input(0, 25, None);
            input.search = search.map(str::to_string);
            input.sort_field = Some(HistorySortField::Name);
            input.sort_direction = Some(HistorySortDirection::Asc);
            let page = build_history_page_sql(
                &db,
                HistoryPageSqlPlan::for_input(&input),
                &HashSet::new(),
                HistoryPageFields::ALL,
            )
            .unwrap();
            page.items.iter().map(|item| item.id).collect::<Vec<_>>()
        };
        assert_eq!(page(Some("2024.2160P")), vec![1]);
        assert_eq!(page(Some("the movie")), Vec::<u64>::new());
        assert_eq!(page(Some("before.rename")), Vec::<u64>::new());
        assert_eq!(page(Some("beta.renamed")), vec![2]);
        // Alpha.Plain, Beta.Renamed, Zeta.The.Movie. The stored names would
        // give Alpha.Plain, The Movie, Zz Renamed: 3, 1, 2.
        assert_eq!(page(None), vec![3, 2, 1]);
    }

    #[test]
    fn category_filter_takes_sql_path() {
        let mut input = page_input(0, 25, None);
        input.categories = Some(vec!["movies".to_string()]);
        let plan = HistoryPageSqlPlan::for_input(&input);
        assert_eq!(plan.categories, Some(vec!["movies".to_string()]));
        // An empty list filters by nothing.
        input.categories = Some(Vec::new());
        let plan = HistoryPageSqlPlan::for_input(&input);
        assert_eq!(plan.categories, None);
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
        let page = rust_page(&db, input.clone(), &HashSet::new());
        assert_eq!(page.total_count, 2);
        assert_eq!(page.counts.all, 2, "counts describe the filtered set");
        assert_eq!(page.counts.success, 1);
        assert_eq!(page.counts.failure, 1);

        // A second facet widens rather than narrows.
        input.categories = Some(vec!["movies".to_string(), "tv".to_string()]);
        let page = rust_page(&db, input.clone(), &HashSet::new());
        assert_eq!(page.total_count, 3);
        assert_eq!(page.counts.success, 2);

        // The empty string is how a caller asks for rows with no category.
        input.categories = Some(vec![String::new()]);
        let page = rust_page(&db, input.clone(), &HashSet::new());
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![4]);

        // A status filter still applies on top of the facets.
        input.categories = Some(vec!["movies".to_string(), "tv".to_string()]);
        input.status = Some(HistoryStatusFilter::Failure);
        let page = rust_page(&db, input, &HashSet::new());
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![2]);
    }

    // Many rows with ties on every sort column, categories that differ only by
    // case, rows with no category or an empty one, and titles whose lowercase
    // form is outside ASCII.
    fn seed_varied(db: &Database) {
        let statuses = ["complete", "failed", "partial", "cancelled", "paused"];
        let categories = [
            None,
            Some(""),
            Some("movies"),
            Some("tv"),
            Some("Anime"),
            Some("anime"),
        ];
        let names = [
            "Ünïcödé.Show.S01E01.1080p.WEB",
            "ÉCOLE.Documentary.2019.720p",
            "Straße.Film.2021.2160p.BluRay",
            "MiXeD.CaSe.Release.1080p.WEB",
            "plain.release.name.720p",
            "Ωmega.Ψ.Collection.1080p",
            "Release.Year.2024.1080p.WEB-DL",
            "Under_Score.100%.Done.720p",
        ];
        let mut rows = Vec::new();
        for job_id in 1..=280u64 {
            let mut row = history_row(
                job_id,
                statuses[(job_id % 5) as usize],
                // Four rows share each completion time.
                10_000 + (job_id / 4) as i64,
            );
            row.name = format!("{}.{}", names[job_id as usize % names.len()], job_id % 11);
            row.category = categories[(job_id % 6) as usize].map(str::to_string);
            row.total_bytes = 1_000 * (job_id % 9);
            row.health = 100 * (job_id % 4) as u32;
            if job_id % 13 == 0 {
                row.metadata = Some(
                    serde_json::to_string(&vec![(
                        "title".to_string(),
                        format!("Gâteau Spécial {job_id}"),
                    )])
                    .unwrap(),
                );
            } else if job_id % 7 == 0 {
                // The stored name is the shortened title; the full release
                // name the page sorts and searches by is the original title.
                row.metadata = Some(
                    serde_json::to_string(&vec![(
                        "weaver.original_title".to_string(),
                        format!("Zz.Original.Search.Me.{job_id}.2160p"),
                    )])
                    .unwrap(),
                );
            } else if job_id % 11 == 0 {
                row.metadata = Some(
                    serde_json::to_string(&vec![
                        (
                            "weaver.original_title".to_string(),
                            format!("Aa.Before.Rename.{job_id}"),
                        ),
                        (
                            "weaver.renamed_title".to_string(),
                            format!("Mm.Renamed.{job_id}"),
                        ),
                    ])
                    .unwrap(),
                );
            }
            rows.push(row);
        }
        for row in &rows {
            db.insert_job_history(row).unwrap();
        }
    }

    fn assert_pages_equal(label: &str, left: &HistoryPage, right: &HistoryPage) {
        assert_eq!(left.total_count, right.total_count, "{label}: total_count");
        assert_eq!(left.counts.all, right.counts.all, "{label}: counts.all");
        assert_eq!(
            left.counts.success, right.counts.success,
            "{label}: counts.success"
        );
        assert_eq!(
            left.counts.failure, right.counts.failure,
            "{label}: counts.failure"
        );
        assert_eq!(
            left.category_counts, right.category_counts,
            "{label}: category counts"
        );
        assert_eq!(left.items, right.items, "{label}: items");
    }

    // Every way of building a page returns what deciding it over every whole
    // row returns, for every filter and sort, with and without live rows.
    #[test]
    fn every_page_path_matches_the_whole_row_page() {
        let db = Database::open_in_memory().unwrap();
        seed_varied(&db);

        let searches = [
            None,
            Some("école"),
            Some("ÜNÏ"),
            Some("straße"),
            Some("mixed.case"),
            Some("ωmega"),
            Some("gâteau"),
            Some("release"),
            Some("no such title"),
            // LIKE's wildcards, which a search must match as themselves.
            Some("%"),
            Some("_score"),
            Some("100%.done"),
            Some("e_s"),
            // Text only the full release name holds, and text a rename replaced.
            Some("search.me"),
            Some("mm.renamed"),
            Some("before.rename"),
        ];
        let category_sets: [Option<Vec<String>>; 5] = [
            None,
            Some(vec!["anime".to_string()]),
            Some(vec!["Anime".to_string(), "tv".to_string()]),
            Some(vec![String::new()]),
            Some(vec!["missing".to_string()]),
        ];
        let fields = [
            HistorySortField::CompletedAt,
            HistorySortField::Name,
            HistorySortField::State,
            HistorySortField::Health,
            HistorySortField::Size,
            HistorySortField::Category,
        ];
        let statuses = [
            HistoryStatusFilter::All,
            HistoryStatusFilter::Success,
            HistoryStatusFilter::Failure,
        ];
        let lives: [HashSet<u64>; 2] = [HashSet::new(), [3, 40, 41, 200].into_iter().collect()];

        let mut matched = 0;
        for live in &lives {
            let rows = exclude_live_rows(
                db.list_job_history(
                    &weaver_server_core::HistoryFilter::default(),
                    HistoryRows::Shown,
                )
                .unwrap(),
                live,
            );
            for search in searches {
                for categories in &category_sets {
                    for field in fields {
                        for direction in [HistorySortDirection::Asc, HistorySortDirection::Desc] {
                            for status in statuses {
                                for (page_index, page_size) in [(0, 25), (2, 7), (40, 7)] {
                                    let input = HistoryPageInput {
                                        page_index,
                                        page_size,
                                        search: search.map(str::to_string),
                                        status: Some(status),
                                        categories: categories.clone(),
                                        sort_field: Some(field),
                                        sort_direction: Some(direction),
                                    };
                                    let label = format!("{input:?} live={live:?}");
                                    let whole = build_history_page(rows.clone(), input.clone());
                                    let sql = build_history_page_sql(
                                        &db,
                                        HistoryPageSqlPlan::for_input(&input),
                                        live,
                                        HistoryPageFields::ALL,
                                    )
                                    .unwrap();
                                    assert_pages_equal(&label, &sql, &whole);
                                    if whole.total_count > 0 {
                                        matched += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        assert!(matched > 0);
    }

    // A scheduler handle that reports `jobs` as its resident job list. Nothing
    // here drives the pipeline; the reads under test go straight to the
    // published list.
    fn scheduler_with_jobs(jobs: Vec<JobInfo>) -> SchedulerHandle {
        let (event_tx, _event_rx) = tokio::sync::broadcast::channel(1);
        SchedulerHandle::new(
            event_tx,
            weaver_server_core::jobs::handle::SharedPipelineState::new(
                weaver_server_core::operations::metrics::PipelineMetrics::new(),
                jobs,
            ),
        )
    }

    // The scheduler's view of an already-archived job, in `status`.
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

        let page = load_history_page(
            db.clone(),
            live.clone(),
            page_input(0, 25, None),
            HistoryPageFields::ALL,
        )
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
            .list_job_history(
                &weaver_server_core::HistoryFilter::default(),
                HistoryRows::Shown,
            )
            .unwrap();
        let list_ids: Vec<u64> = exclude_live_rows(rows, &live)
            .iter()
            .map(|row| row.job_id)
            .collect();
        assert!(!list_ids.contains(&3));
        // And so does the count: the live row comes off it, but only when the
        // filter would have counted that row in the first place.
        let all = weaver_server_core::HistoryFilter::default();
        assert_eq!(count_live_rows_matching(&db, &all, &live).unwrap(), 1);
        assert_eq!(
            db.count_job_history(&all, HistoryRows::Shown).unwrap()
                - count_live_rows_matching(&db, &all, &live).unwrap(),
            6
        );
        let failed_only = weaver_server_core::HistoryFilter {
            statuses: Some(vec!["failed".to_string()]),
            ..Default::default()
        };
        assert_eq!(
            count_live_rows_matching(&db, &failed_only, &live).unwrap(),
            0
        );

        // A terminal report still belongs to the queue until retirement.
        let handle = scheduler_with_jobs(vec![resident_job(
            &row,
            weaver_server_core::JobStatus::Complete,
        )]);
        let live = live_job_ids(&handle);
        assert!(live.contains(&3));
        let page = load_history_page(
            db.clone(),
            live,
            page_input(0, 25, None),
            HistoryPageFields::ALL,
        )
        .await
        .unwrap();
        assert!(!page.items.iter().any(|item| item.id == 3));

        // Only removal from the queue makes its durable row history.
        let handle = scheduler_with_jobs(vec![]);
        let live = live_job_ids(&handle);
        assert!(live.is_empty());
        let page = load_history_page(db, live, page_input(0, 25, None), HistoryPageFields::ALL)
            .await
            .unwrap();
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert!(ids.contains(&3));
        assert_eq!(page.total_count, 7);
        assert_eq!(page.counts.success, 3);
    }

    #[tokio::test]
    async fn a_cancelled_row_is_not_history_until_retirement() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        let row = history_row(3, "cancelled", 1_200);
        let handle = scheduler_with_jobs(vec![resident_job(
            &row,
            weaver_server_core::JobStatus::Queued,
        )]);
        handle
            .cancel_job(weaver_server_core::JobId(3))
            .await
            .unwrap();
        // Still listed: the job leaves the queue only once its history row commits.
        assert!(handle.list_jobs().iter().any(|job| job.job_id.0 == 3));
        let live = live_job_ids(&handle);
        assert!(live.contains(&3));
        let page = load_history_page(db, live, page_input(0, 25, None), HistoryPageFields::ALL)
            .await
            .unwrap();
        assert!(!page.items.iter().any(|item| item.id == 3));
        assert_eq!(page.total_count, 6);
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

        let page = load_history_page(db, live, page_input(0, 25, None), HistoryPageFields::ALL)
            .await
            .unwrap();
        let ids: Vec<u64> = page.items.iter().map(|item| item.id).collect();
        assert_eq!(ids, vec![7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(page.total_count, 7);
        assert_eq!(page.counts.all, 7);
    }

    // `historyItems` leaves live rows out before the page is cut, so a page
    // holding a live job's row is filled from the next row on, and the next
    // page starts where this one ended.
    #[test]
    fn a_limited_list_page_is_full_without_its_live_rows() {
        let db = Database::open_in_memory().unwrap();
        seed(&db);
        let live = HashSet::from([6, 4]);
        let live_in_history = live_jobs_in_history(&db, &live).unwrap();
        let page = |offset: u32| {
            let HistoryQueryPlan::Query(mut filter) =
                history_query_plan(None, Some(3), Some(offset as usize))
            else {
                panic!("an unfiltered plan reads");
            };
            filter.item_ids_not_in = Some(live_in_history.iter().copied().collect());
            db.list_job_history(&filter, HistoryRows::Shown)
                .unwrap()
                .into_iter()
                .map(|row| row.job_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(page(0), vec![7, 5, 3]);
        assert_eq!(page(3), vec![2, 1]);
    }
}

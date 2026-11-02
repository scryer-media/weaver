// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Queue pages, totals and batch selections, read in place.
//!
//! A walk reads every queue row once under the queue lock and keeps a few
//! scalars per row; it copies no report. Searching, counting and sorting run
//! on those scalars after the lock is let go, and only the rows a page
//! returns are copied whole, each found through the queue's position index.

use std::cmp::Ordering;
use std::collections::HashMap;

use weaver_server_core::pipeline::queue::QueueInner;
use weaver_server_core::pipeline::queue::paging::JobView;
use weaver_server_core::{JobId, JobInfo, SharedPipelineState};

use super::types::{
    QueueCategoryCount, QueueEtaBytes, QueueItemState, QueuePageInput, QueuePriority,
    QueueSelectionInput, QueueSortDirection, QueueSortField, QueueStateCount, QueueStateFacts,
    QueueSummary, queue_item_state,
};

pub(crate) const MAX_QUEUE_PAGE_SIZE: usize = 500;

/// The release name a queue row shows: the original title, or the job name,
/// with dots read as spaces and runs of space collapsed.
pub(crate) fn queue_page_display_name(
    original_title: &str,
    name: &str,
    display_title: &str,
) -> String {
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

/// The scalars a page, a total or a selection needs of one queue row.
#[derive(Debug, Clone)]
pub(crate) struct WalkedRow {
    pub(crate) id: JobId,
    pub(crate) state: QueueItemState,
    priority: QueuePriority,
    // Index into the walk's category names; `None` for no category.
    category: Option<u32>,
    total_bytes: u64,
    downloaded_bytes: u64,
    progress: f64,
    rate: u64,
    propagating: bool,
    // The job name and its original title, copied only when a search or a
    // name sort needs them.
    titles: Option<(String, String)>,
}

impl WalkedRow {
    // Queue order puts transferring jobs first, fastest first.
    fn download_rank(&self) -> u8 {
        match self.state {
            QueueItemState::Downloading | QueueItemState::FetchingRepairData => 1,
            _ => 0,
        }
    }

    fn leads(&self) -> bool {
        self.download_rank() == 1 || self.rate > 0
    }

    fn remaining_bytes(&self) -> u64 {
        self.total_bytes.saturating_sub(self.downloaded_bytes)
    }

    // Counted in the queue's bytes still to fetch: whatever waits for or
    // holds a download lane.
    fn awaits_download(&self) -> bool {
        matches!(
            self.state,
            QueueItemState::Queued
                | QueueItemState::Downloading
                | QueueItemState::FinalizingDownload
                | QueueItemState::AwaitingQueueScripts
        )
    }

    // Counted by a time-left estimate: downloading or waiting, propagation
    // holds aside.
    fn counts_toward_eta(&self) -> bool {
        matches!(
            self.state,
            QueueItemState::Queued | QueueItemState::Downloading
        ) && !self.propagating
    }
}

/// One walk of the queue: a row per job, in queue order.
#[derive(Debug, Default)]
pub(crate) struct QueueWalk {
    pub(crate) rows: Vec<WalkedRow>,
    category_names: Vec<String>,
}

/// What the walk copies beyond the scalars.
#[derive(Clone, Copy, Default)]
pub(crate) struct WalkNeeds {
    pub(crate) titles: bool,
}

fn row_state(view: &JobView<'_>) -> QueueItemState {
    queue_item_state(QueueStateFacts::of_view(view))
}

fn job_progress(job: &JobInfo) -> f64 {
    let progress = job.progress.clamp(0.0, 1.0);
    if job.total_bytes == 0 {
        return progress;
    }
    let processed_bytes = job
        .downloaded_bytes
        .saturating_add(job.failed_bytes)
        .min(job.total_bytes);
    progress.max(processed_bytes as f64 / job.total_bytes as f64)
}

impl QueueWalk {
    /// Reads every queue row once under the queue lock.
    pub(crate) fn read(state: &SharedPipelineState, needs: WalkNeeds) -> Self {
        state.read_queue(|inner| Self::of(inner, needs))
    }

    fn of(inner: &QueueInner, needs: WalkNeeds) -> Self {
        let mut walk = QueueWalk {
            rows: Vec::with_capacity(inner.jobs.len()),
            category_names: Vec::new(),
        };
        let mut category_index: HashMap<String, u32> = HashMap::new();
        for row in &inner.jobs {
            let view = row.view(&inner.globals);
            let job = view.job();
            let category = job.category.as_deref().map(|name| {
                if let Some(index) = category_index.get(name) {
                    return *index;
                }
                let index = walk.category_names.len() as u32;
                walk.category_names.push(name.to_string());
                category_index.insert(name.to_string(), index);
                index
            });
            walk.rows.push(WalkedRow {
                id: row.id,
                state: row_state(&view),
                priority: QueuePriority::from_metadata(&job.metadata),
                category,
                total_bytes: job.total_bytes,
                downloaded_bytes: job.downloaded_bytes,
                progress: job_progress(job),
                rate: view
                    .phase_progress()
                    .iter()
                    .filter_map(|phase| phase.rate_bps)
                    .max()
                    .unwrap_or_default(),
                propagating: view.download_wait_reason()
                    == Some(weaver_server_core::jobs::handle::PROPAGATION_WAIT_REASON),
                titles: needs.titles.then(|| {
                    (
                        job.name.clone(),
                        weaver_server_core::ingest::original_release_title(
                            &job.name,
                            &job.metadata,
                        ),
                    )
                }),
            });
        }
        walk
    }

    fn category(&self, row: &WalkedRow) -> &str {
        row.category
            .map_or("", |index| self.category_names[index as usize].as_str())
    }

    /// Every category a queued job carries, sorted.
    pub(crate) fn categories(&self) -> Vec<String> {
        let mut names = self.category_names.clone();
        names.sort();
        names
    }

    /// The queue's totals: counts by state and bytes, over every row.
    pub(crate) fn summary(
        &self,
        metrics: &weaver_server_core::operations::metrics::MetricsSnapshot,
    ) -> QueueSummary {
        let mut summary = QueueSummary::empty(metrics);
        for row in &self.rows {
            summary.total_items += 1;
            summary.total_bytes += row.total_bytes;
            summary.downloaded_bytes += row.downloaded_bytes;
            summary.count_state(row.state);
        }
        summary
    }

    /// Row indexes in queue order as the default page shows it:
    /// transferring jobs first, fastest first, then the rest as queued.
    fn queue_order(&self, indexes: &[usize]) -> Vec<usize> {
        let mut lead: Vec<usize> = indexes
            .iter()
            .copied()
            .filter(|&index| self.rows[index].leads())
            .collect();
        lead.sort_by(|&left, &right| {
            let (left_row, right_row) = (&self.rows[left], &self.rows[right]);
            right_row
                .download_rank()
                .cmp(&left_row.download_rank())
                .then_with(|| right_row.rate.cmp(&left_row.rate))
                .then_with(|| left.cmp(&right))
        });
        lead.extend(
            indexes
                .iter()
                .copied()
                .filter(|&index| !self.rows[index].leads()),
        );
        lead
    }
}

/// Which rows a page or a batch keeps. One predicate serves both.
pub(crate) struct QueueSelection<'a> {
    search: Option<String>,
    states: Option<&'a [QueueItemState]>,
    priorities: Option<&'a [QueuePriority]>,
    categories: Option<&'a [String]>,
}

fn non_empty<T>(values: Option<&Vec<T>>) -> Option<&[T]> {
    values
        .map(Vec::as_slice)
        .filter(|values| !values.is_empty())
}

impl<'a> QueueSelection<'a> {
    pub(crate) fn of_page(input: &'a QueuePageInput) -> Self {
        Self::new(
            input.search.as_deref(),
            input.states.as_ref(),
            input.priorities.as_ref(),
            input.categories.as_ref(),
        )
    }

    pub(crate) fn of_input(input: &'a QueueSelectionInput) -> Self {
        Self::new(
            input.search.as_deref(),
            input.states.as_ref(),
            input.priorities.as_ref(),
            input.categories.as_ref(),
        )
    }

    fn new(
        search: Option<&str>,
        states: Option<&'a Vec<QueueItemState>>,
        priorities: Option<&'a Vec<QueuePriority>>,
        categories: Option<&'a Vec<String>>,
    ) -> Self {
        Self {
            search: search
                .map(str::trim)
                .filter(|search| !search.is_empty())
                .map(str::to_lowercase),
            states: non_empty(states),
            priorities: non_empty(priorities),
            categories: non_empty(categories),
        }
    }

    pub(crate) fn needs(&self, sort: Option<QueueSortField>) -> WalkNeeds {
        WalkNeeds {
            titles: self.search.is_some() || sort == Some(QueueSortField::Name),
        }
    }

    // Search and priorities: the stage category counts are taken after.
    fn matches_search(&self, row: &WalkedRow) -> bool {
        if self
            .priorities
            .is_some_and(|priorities| !priorities.contains(&row.priority))
        {
            return false;
        }
        let Some(search) = self.search.as_deref() else {
            return true;
        };
        let Some((name, original_title)) = row.titles.as_ref() else {
            return false;
        };
        if name.to_lowercase().contains(search) {
            return true;
        }
        let display_title =
            weaver_server_core::ingest::derive_release_name(Some(original_title), Some(name));
        queue_page_display_name(original_title, name, &display_title)
            .to_lowercase()
            .contains(search)
            || display_title.to_lowercase().contains(search)
    }

    // Categories: the stage state counts are taken after.
    fn matches_category(&self, walk: &QueueWalk, row: &WalkedRow) -> bool {
        self.categories.is_none_or(|categories| {
            let category = walk.category(row);
            categories.iter().any(|wanted| wanted == category)
        })
    }

    fn matches_state(&self, row: &WalkedRow) -> bool {
        self.states.is_none_or(|states| states.contains(&row.state))
    }

    /// Ids of every walked row the selection keeps, in queue order.
    pub(crate) fn ids(&self, walk: &QueueWalk) -> Vec<JobId> {
        walk.rows
            .iter()
            .filter(|row| {
                self.matches_search(row)
                    && self.matches_category(walk, row)
                    && self.matches_state(row)
            })
            .map(|row| row.id)
            .collect()
    }
}

/// A page's rows (ids, in page order) and its counts, before the rows
/// themselves are read.
pub(crate) struct PagePlan {
    pub(crate) ids: Vec<JobId>,
    pub(crate) total_count: u32,
    pub(crate) category_counts: Vec<QueueCategoryCount>,
    pub(crate) state_counts: Vec<QueueStateCount>,
    pub(crate) remaining_bytes: u64,
    pub(crate) eta_total_bytes: u64,
    pub(crate) eta_downloaded_bytes: u64,
    pub(crate) eta_through: Vec<QueueEtaBytes>,
}

fn sort_name(row: &WalkedRow) -> String {
    let Some((name, original_title)) = row.titles.as_ref() else {
        return String::new();
    };
    // The display title is the fallback only when both names are blank.
    if original_title.trim().is_empty() && name.trim().is_empty() {
        let display_title =
            weaver_server_core::ingest::derive_release_name(Some(original_title), Some(name));
        return queue_page_display_name(original_title, name, &display_title);
    }
    queue_page_display_name(original_title, name, "")
}

fn display_state(state: QueueItemState) -> &'static str {
    match state {
        QueueItemState::AwaitingQueueScripts => "AWAITING_QUEUE_SCRIPTS",
        QueueItemState::Queued => "QUEUED",
        QueueItemState::Downloading => "DOWNLOADING",
        QueueItemState::FetchingRepairData => "FETCHING_REPAIR_DATA",
        QueueItemState::FinalizingDownload => "FINALIZING_DOWNLOAD",
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

impl PagePlan {
    pub(crate) fn plan(walk: &QueueWalk, input: &QueuePageInput) -> Self {
        let selection = QueueSelection::of_page(input);

        // Everything a page counts follows from three nested stages: search
        // (with priorities), then categories, then states.
        let mut category_counts: HashMap<&str, u32> = HashMap::new();
        let mut state_counts: Vec<(QueueItemState, u32)> = Vec::new();
        let mut matching: Vec<usize> = Vec::new();
        for (index, row) in walk.rows.iter().enumerate() {
            if !selection.matches_search(row) {
                continue;
            }
            *category_counts.entry(walk.category(row)).or_default() += 1;
            if !selection.matches_category(walk, row) {
                continue;
            }
            match state_counts
                .iter_mut()
                .find(|(state, _)| *state == row.state)
            {
                Some((_, count)) => *count += 1,
                None => state_counts.push((row.state, 1)),
            }
            if selection.matches_state(row) {
                matching.push(index);
            }
        }

        let ordered = match input.sort_field {
            None => walk.queue_order(&matching),
            Some(field) => sorted(walk, matching, field, input.sort_direction),
        };
        let page_size = (input.page_size as usize).clamp(1, MAX_QUEUE_PAGE_SIZE);
        let offset = (input.page_index as usize).saturating_mul(page_size);
        let page: Vec<usize> = ordered
            .iter()
            .copied()
            .skip(offset)
            .take(page_size)
            .collect();

        // Time left counts every job ahead in queue order, filters aside.
        let every_row: Vec<usize> = (0..walk.rows.len()).collect();
        let mut through = HashMap::new();
        let (mut eta_total_bytes, mut eta_downloaded_bytes, mut ahead) = (0u64, 0u64, 0u64);
        for index in walk.queue_order(&every_row) {
            let row = &walk.rows[index];
            if row.counts_toward_eta() {
                eta_total_bytes += row.total_bytes;
                eta_downloaded_bytes += row.downloaded_bytes;
                ahead += row.remaining_bytes();
                through.insert(index, ahead);
            }
        }
        let eta_through = page
            .iter()
            .filter_map(|index| {
                through.get(index).map(|bytes_through| QueueEtaBytes {
                    id: walk.rows[*index].id.0,
                    bytes_through: *bytes_through,
                })
            })
            .collect();

        let mut category_counts: Vec<QueueCategoryCount> = category_counts
            .into_iter()
            .map(|(category, count)| QueueCategoryCount {
                category: category.to_string(),
                count,
            })
            .collect();
        category_counts.sort_by(|left, right| left.category.cmp(&right.category));

        PagePlan {
            ids: page.iter().map(|&index| walk.rows[index].id).collect(),
            total_count: u32::try_from(ordered.len()).unwrap_or(u32::MAX),
            category_counts,
            state_counts: state_counts
                .into_iter()
                .map(|(state, count)| QueueStateCount { state, count })
                .collect(),
            remaining_bytes: walk
                .rows
                .iter()
                .filter(|row| row.awaits_download())
                .map(WalkedRow::remaining_bytes)
                .sum(),
            eta_total_bytes,
            eta_downloaded_bytes,
            eta_through,
        }
    }
}

fn sorted(
    walk: &QueueWalk,
    mut indexes: Vec<usize>,
    field: QueueSortField,
    direction: Option<QueueSortDirection>,
) -> Vec<usize> {
    // Names are worked out once per row, never per comparison.
    let names: HashMap<usize, String> = if field == QueueSortField::Name {
        indexes
            .iter()
            .map(|&index| (index, sort_name(&walk.rows[index])))
            .collect()
    } else {
        HashMap::new()
    };
    let by_field = |left: usize, right: usize| -> Ordering {
        let (left_row, right_row) = (&walk.rows[left], &walk.rows[right]);
        match field {
            QueueSortField::Name => names[&left].cmp(&names[&right]),
            QueueSortField::State => {
                display_state(left_row.state).cmp(display_state(right_row.state))
            }
            QueueSortField::Priority => left_row.priority.rank().cmp(&right_row.priority.rank()),
            QueueSortField::Category => {
                let category = |row: &WalkedRow| match row.category {
                    Some(_) => walk.category(row),
                    None => "\u{2014}",
                };
                category(left_row).cmp(category(right_row))
            }
            QueueSortField::Progress => left_row.progress.total_cmp(&right_row.progress),
            QueueSortField::Size => left_row.total_bytes.cmp(&right_row.total_bytes),
        }
    };
    indexes.sort_by(|&left, &right| {
        let (left_row, right_row) = (&walk.rows[left], &walk.rows[right]);
        let order = match direction.unwrap_or(QueueSortDirection::Desc) {
            QueueSortDirection::Asc => by_field(left, right),
            QueueSortDirection::Desc => by_field(left, right).reverse(),
        };
        right_row
            .download_rank()
            .cmp(&left_row.download_rank())
            .then(order)
            .then_with(|| left_row.id.0.cmp(&right_row.id.0))
    });
    indexes
}

/// Copies the named rows, found through the position index, in the order
/// given. A row that has left the queue since it was planned is skipped.
pub(crate) fn read_rows(state: &SharedPipelineState, ids: &[JobId]) -> Vec<JobInfo> {
    state.read_queue(|inner| {
        ids.iter()
            .filter_map(|id| inner.row(*id).map(|row| row.info(&inner.globals)))
            .collect()
    })
}

/// Copies one row, found through the position index.
pub(crate) fn read_row(state: &SharedPipelineState, id: JobId) -> Option<JobInfo> {
    state.read_queue(|inner| inner.row(id).map(|row| row.info(&inner.globals)))
}

/// The queue's totals, counted under the lock without copying a row.
pub(crate) fn summary(
    state: &SharedPipelineState,
    metrics: &weaver_server_core::operations::metrics::MetricsSnapshot,
) -> QueueSummary {
    state.read_queue(|inner| {
        let mut summary = QueueSummary::empty(metrics);
        for row in &inner.jobs {
            let view = row.view(&inner.globals);
            summary.total_items += 1;
            summary.total_bytes += view.job().total_bytes;
            summary.downloaded_bytes += view.job().downloaded_bytes;
            summary.count_state(row_state(&view));
        }
        summary
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::{DownloadState, JobStatus, PipelineMetrics, PostState, RunState};

    fn job(id: u64, status: JobStatus, category: Option<&str>, total_bytes: u64) -> JobInfo {
        let (download_state, run_state) = match status {
            JobStatus::Downloading => (DownloadState::Downloading, RunState::Active),
            JobStatus::Paused => (DownloadState::Queued, RunState::Paused),
            _ => (DownloadState::Queued, RunState::Active),
        };
        JobInfo {
            job_id: JobId(id),
            job_hash: None,
            name: format!("Show.{id:05}.1080p"),
            status,
            download_state,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state: PostState::Idle,
            run_state,
            progress: 0.0,
            total_bytes,
            downloaded_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            phase_progress: Vec::new(),
            failed_bytes: 0,
            health: 1000,
            terminal_discards: Vec::new(),
            total_files: 1,
            completed_files: 0,
            file_remaining_bytes: Vec::new(),
            remaining_par_files: 0,
            password: None,
            category: category.map(str::to_string),
            metadata: Vec::new(),
            output_dir: None,
            server_attribution: Vec::new(),
            error: None,
            download_wait_reason: None,
            download_retry_at_epoch_ms: None,
            created_at_epoch_ms: 0.0,
        }
    }

    fn page_input(page_index: u32, page_size: u32) -> QueuePageInput {
        QueuePageInput {
            page_index,
            page_size,
            search: None,
            states: None,
            priorities: None,
            categories: None,
            sort_field: None,
            sort_direction: None,
        }
    }

    fn ids(plan: &PagePlan) -> Vec<u64> {
        plan.ids.iter().map(|id| id.0).collect()
    }

    #[test]
    fn a_default_page_is_a_range_of_queue_order_with_downloads_first() {
        let jobs = (1..=1_200)
            .map(|id| {
                let status = if id == 700 || id == 900 {
                    JobStatus::Downloading
                } else {
                    JobStatus::Queued
                };
                job(id, status, None, 10)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let walk = QueueWalk::read(&state, WalkNeeds::default());
        assert_eq!(walk.rows.len(), 1_200);

        let first = PagePlan::plan(&walk, &page_input(0, 500));
        assert_eq!(first.total_count, 1_200);
        let mut expected = vec![700, 900];
        expected.extend((1..=498).collect::<Vec<_>>());
        assert_eq!(ids(&first), expected);

        let last = PagePlan::plan(&walk, &page_input(2, 500));
        let expected: Vec<u64> = (1_001..=1_200).collect();
        assert_eq!(ids(&last), expected);

        // Time left counts every waiting job ahead in queue order, so a row on
        // the last page counts the pages before it.
        let through: HashMap<u64, u64> = last
            .eta_through
            .iter()
            .map(|eta| (eta.id, eta.bytes_through))
            .collect();
        assert_eq!(through[&1_200], 12_000);
        assert_eq!(last.remaining_bytes, 12_000);
        assert_eq!(last.eta_total_bytes, 12_000);

        let summary = walk.summary(&state.metrics_snapshot());
        assert_eq!(summary.total_items, 1_200);
        assert_eq!(summary.queued_items, 1_198);
        assert_eq!(summary.active_items, 2);
    }

    #[test]
    fn a_filtered_page_counts_each_facet_before_its_own_filter() {
        let mut jobs = Vec::new();
        for id in 1..=60u64 {
            let category = match id % 3 {
                0 => Some("tv"),
                1 => Some("movies"),
                _ => None,
            };
            let status = if id % 4 == 0 {
                JobStatus::Paused
            } else {
                JobStatus::Queued
            };
            jobs.push(job(id, status, category, id * 100));
        }
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let mut input = page_input(0, 5);
        input.search = Some("show 0001".to_string());
        input.categories = Some(vec!["tv".to_string(), String::new()]);
        input.states = Some(vec![QueueItemState::Queued]);
        input.sort_field = Some(QueueSortField::Size);
        input.sort_direction = Some(QueueSortDirection::Desc);
        let selection = QueueSelection::of_page(&input);
        let walk = QueueWalk::read(&state, selection.needs(input.sort_field));
        let plan = PagePlan::plan(&walk, &input);

        // "show 0001" finds ids 10..=19 through the display name.
        let searched: Vec<u64> = (10..=19).collect();
        let in_category: Vec<u64> = searched.iter().copied().filter(|id| id % 3 != 1).collect();
        let mut expected: Vec<u64> = in_category
            .iter()
            .copied()
            .filter(|id| id % 4 != 0)
            .collect();
        assert_eq!(plan.total_count as usize, expected.len());
        expected.sort_by(|left, right| right.cmp(left));
        expected.truncate(5);
        assert_eq!(ids(&plan), expected);

        let count_of = |category: &str| {
            plan.category_counts
                .iter()
                .find(|count| count.category == category)
                .map_or(0, |count| count.count)
        };
        assert_eq!(
            count_of("tv"),
            searched.iter().filter(|id| *id % 3 == 0).count() as u32
        );
        assert_eq!(
            count_of("movies"),
            searched.iter().filter(|id| *id % 3 == 1).count() as u32
        );
        assert_eq!(
            count_of(""),
            searched.iter().filter(|id| *id % 3 == 2).count() as u32
        );
        let paused = plan
            .state_counts
            .iter()
            .find(|count| count.state == QueueItemState::Paused)
            .map_or(0, |count| count.count);
        assert_eq!(
            paused,
            in_category.iter().filter(|id| *id % 4 == 0).count() as u32
        );
    }

    #[test]
    fn a_name_sort_reads_each_name_once_and_orders_by_it() {
        let jobs = [3u64, 1, 2]
            .into_iter()
            .map(|id| job(id, JobStatus::Queued, None, 1))
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let mut input = page_input(0, 10);
        input.sort_field = Some(QueueSortField::Name);
        input.sort_direction = Some(QueueSortDirection::Asc);
        let walk = QueueWalk::read(
            &state,
            QueueSelection::of_page(&input).needs(input.sort_field),
        );
        assert_eq!(ids(&PagePlan::plan(&walk, &input)), vec![1, 2, 3]);
    }

    #[test]
    fn a_selection_keeps_every_matching_job_in_queue_order() {
        let jobs = (1..=40u64)
            .map(|id| {
                let status = if id % 2 == 0 {
                    JobStatus::Paused
                } else {
                    JobStatus::Queued
                };
                job(id, status, (id % 5 == 0).then_some("tv"), 1)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let input = QueueSelectionInput {
            states: Some(vec![QueueItemState::Paused]),
            categories: Some(vec!["tv".to_string()]),
            ..Default::default()
        };
        let selection = QueueSelection::of_input(&input);
        let walk = QueueWalk::read(&state, selection.needs(None));
        let selected: Vec<u64> = selection.ids(&walk).iter().map(|id| id.0).collect();
        assert_eq!(selected, vec![10, 20, 30, 40]);

        let everything = QueueSelectionInput::default();
        let selection = QueueSelection::of_input(&everything);
        assert_eq!(selection.ids(&walk).len(), 40);
    }

    #[test]
    fn a_single_row_is_read_through_the_position_index() {
        let jobs = (1..=50u64)
            .map(|id| job(id, JobStatus::Queued, None, 1))
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        assert_eq!(
            read_row(&state, JobId(37)).map(|job| job.job_id),
            Some(JobId(37))
        );
        assert!(read_row(&state, JobId(51)).is_none());
        let rows = read_rows(&state, &[JobId(9), JobId(99), JobId(3)]);
        assert_eq!(
            rows.iter().map(|job| job.job_id.0).collect::<Vec<_>>(),
            vec![9, 3]
        );
    }
}

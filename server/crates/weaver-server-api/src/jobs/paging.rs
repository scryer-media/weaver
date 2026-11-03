// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Queue pages, totals and batch selections, read in place.
//!
//! A walk reads every queue row once under the queue lock and keeps a few
//! scalars per row; it copies no report. A page is planned on those scalars
//! and its rows are copied whole, each found through the queue's position
//! index, under the same lock, so the rows agree with the counts beside them.

use std::cmp::Ordering;
use std::collections::HashMap;

use weaver_server_core::pipeline::queue::QueueInner;
use weaver_server_core::pipeline::queue::paging::JobView;
use weaver_server_core::{JobId, JobInfo, SharedPipelineState};

use super::types::{
    ProtocolGql, QueueCategoryCount, QueueEtaBytes, QueueItemState, QueuePageInput, QueuePriority,
    QueueProtocolCount, QueueSelectionInput, QueueSortDirection, QueueSortField, QueueStateCount,
    QueueStateFacts, QueueSummary, queue_item_state,
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
    protocol: ProtocolGql,
    // Where the row's report is read from.
    source: RowSource,
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
    // The section the Downloads screen draws the row in, in the screen's
    // order: active (any phase from downloading to the final move), paused,
    // queued, then failed. Matches the screen's own grouping of a status.
    fn group_rank(&self) -> u8 {
        match self.state {
            QueueItemState::Paused => 1,
            QueueItemState::Failed => 3,
            QueueItemState::Queued | QueueItemState::Completed => 2,
            // A held propagation reads as queued on the screen.
            QueueItemState::Downloading if self.propagating => 2,
            QueueItemState::AwaitingQueueScripts
            | QueueItemState::Downloading
            | QueueItemState::FetchingRepairData
            | QueueItemState::FinalizingDownload
            | QueueItemState::Checking
            | QueueItemState::Verifying
            | QueueItemState::Repairing
            | QueueItemState::Extracting
            | QueueItemState::Finalizing
            | QueueItemState::PostProcessing
            | QueueItemState::FetchingMetadata
            | QueueItemState::CheckingFiles
            | QueueItemState::Stalled => 0,
            QueueItemState::Seeding => 2,
        }
    }

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

/// Where a walked row's report lives: the queue's own rows, or the published
/// torrents, by index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowSource {
    Queue,
    Torrent(usize),
}

/// One walk of the queue: a row per job, in queue order.
#[derive(Debug, Default)]
pub(crate) struct QueueWalk {
    pub(crate) rows: Vec<WalkedRow>,
    category_names: Vec<String>,
    // Rows that lead queue order, so their list is sized once.
    leading: usize,
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
            leading: 0,
        };
        let mut category_index: HashMap<String, u32> = HashMap::new();
        let titles = |job: &JobInfo| {
            needs.titles.then(|| {
                (
                    job.name.clone(),
                    weaver_server_core::ingest::original_release_title(&job.name, &job.metadata),
                )
            })
        };
        // The torrents fall in among the queue's rows by when each was
        // added, as the job list orders them.
        let mut torrents = inner.torrents.iter().enumerate().peekable();
        let push_torrent = |walk: &mut QueueWalk,
                            category_index: &mut HashMap<String, u32>,
                            index: usize,
                            job: &JobInfo| {
            let category = walk.category_of(category_index, job);
            walk.rows.push(WalkedRow {
                id: job.job_id,
                state: queue_item_state(QueueStateFacts::of_job(job)),
                protocol: job.protocol.into(),
                source: RowSource::Torrent(index),
                priority: QueuePriority::from_metadata(&job.metadata),
                category,
                total_bytes: job.total_bytes,
                downloaded_bytes: job.downloaded_bytes,
                progress: job_progress(job),
                rate: job
                    .phase_progress
                    .iter()
                    .filter_map(|phase| phase.rate_bps)
                    .max()
                    .unwrap_or_default(),
                propagating: false,
                titles: titles(job),
            });
            walk.leading += usize::from(walk.rows.last().is_some_and(WalkedRow::leads));
        };
        for row in &inner.jobs {
            let view = row.view(&inner.globals);
            let job = view.job();
            while let Some((index, torrent)) = torrents
                .next_if(|(_, torrent)| torrent.created_at_epoch_ms < job.created_at_epoch_ms)
            {
                push_torrent(&mut walk, &mut category_index, index, torrent);
            }
            let category = walk.category_of(&mut category_index, job);
            walk.rows.push(WalkedRow {
                id: row.id,
                state: row_state(&view),
                protocol: job.protocol.into(),
                source: RowSource::Queue,
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
                titles: titles(job),
            });
            walk.leading += usize::from(walk.rows.last().is_some_and(WalkedRow::leads));
        }
        for (index, torrent) in torrents {
            push_torrent(&mut walk, &mut category_index, index, torrent);
        }
        walk
    }

    // The index of the job's category name, added to the walk's names the
    // first time it is seen; `None` for no category.
    fn category_of(&mut self, names: &mut HashMap<String, u32>, job: &JobInfo) -> Option<u32> {
        job.category.as_deref().map(|name| {
            if let Some(index) = names.get(name) {
                return *index;
            }
            let index = self.category_names.len() as u32;
            self.category_names.push(name.to_string());
            names.insert(name.to_string(), index);
            index
        })
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

    /// Queue order: transferring jobs first, fastest first, then the rest
    /// as queued. Time left counts in this order.
    fn download_cmp(&self, left: usize, right: usize) -> Ordering {
        let (left_row, right_row) = (&self.rows[left], &self.rows[right]);
        right_row
            .leads()
            .cmp(&left_row.leads())
            .then_with(|| {
                if !left_row.leads() {
                    return Ordering::Equal;
                }
                right_row
                    .download_rank()
                    .cmp(&left_row.download_rank())
                    .then_with(|| right_row.rate.cmp(&left_row.rate))
            })
            .then_with(|| left.cmp(&right))
    }

    /// The default page order: section by section as the screen draws them
    /// (active, paused, queued, failed), queue order within each. Sorted in
    /// place, with no copy of the indexes.
    fn display_order(&self, indexes: &mut [usize]) {
        indexes.sort_unstable_by(|&left, &right| {
            self.rows[left]
                .group_rank()
                .cmp(&self.rows[right].group_rank())
                .then_with(|| self.download_cmp(left, right))
        });
    }
}

/// Which rows a page or a batch keeps. One predicate serves both.
pub(crate) struct QueueSelection<'a> {
    search: Option<String>,
    protocol: Option<ProtocolGql>,
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
            input.protocol,
            input.states.as_ref(),
            input.priorities.as_ref(),
            input.categories.as_ref(),
        )
    }

    pub(crate) fn of_input(input: &'a QueueSelectionInput) -> Self {
        Self::new(
            input.search.as_deref(),
            input.protocol,
            input.states.as_ref(),
            input.priorities.as_ref(),
            input.categories.as_ref(),
        )
    }

    fn new(
        search: Option<&str>,
        protocol: Option<ProtocolGql>,
        states: Option<&'a Vec<QueueItemState>>,
        priorities: Option<&'a Vec<QueuePriority>>,
        categories: Option<&'a Vec<String>>,
    ) -> Self {
        Self {
            search: search
                .map(str::trim)
                .filter(|search| !search.is_empty())
                .map(str::to_lowercase),
            protocol,
            states: non_empty(states),
            priorities: non_empty(priorities),
            categories: non_empty(categories),
        }
    }

    /// Whether the selection keeps every job: no search, protocol, state,
    /// priority or category filter.
    pub(crate) fn is_unfiltered(&self) -> bool {
        self.search.is_none()
            && self.protocol.is_none()
            && self.states.is_none()
            && self.priorities.is_none()
            && self.categories.is_none()
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

    // Protocol: counted beside categories, each under the other's filter.
    fn matches_protocol(&self, row: &WalkedRow) -> bool {
        self.protocol
            .is_none_or(|protocol| protocol == row.protocol)
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
                    && self.matches_protocol(row)
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
    // Where each page row's report is read from, in page order.
    sources: Vec<RowSource>,
    pub(crate) total_count: u32,
    pub(crate) category_counts: Vec<QueueCategoryCount>,
    pub(crate) protocol_counts: Vec<QueueProtocolCount>,
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
        QueueItemState::FetchingMetadata => "FETCHING_METADATA",
        QueueItemState::CheckingFiles => "CHECKING_FILES",
        QueueItemState::Seeding => "SEEDING",
        QueueItemState::Stalled => "STALLED",
    }
}

/// What a page answer carries beyond its counts.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PageWants {
    /// The page's rows and their time left. A totals-only answer (the
    /// screen-wide counts every screen reads) asks for neither, and is then
    /// planned without listing, ordering or copying any row.
    pub(crate) rows: bool,
}

impl PagePlan {
    pub(crate) fn plan(walk: &QueueWalk, input: &QueuePageInput, wants: PageWants) -> Self {
        let selection = QueueSelection::of_page(input);

        // Everything a page counts follows from three nested stages: search
        // (with priorities), then categories and protocol (each counted under
        // the other), then states.
        let mut category_counts: HashMap<&str, u32> = HashMap::new();
        let mut protocol_counts: Vec<(ProtocolGql, u32)> = Vec::new();
        let mut state_counts: Vec<(QueueItemState, u32)> = Vec::new();
        let mut matching: Vec<usize> = if wants.rows {
            Vec::with_capacity(walk.rows.len())
        } else {
            Vec::new()
        };
        let mut total_count = 0u32;
        for (index, row) in walk.rows.iter().enumerate() {
            if !selection.matches_search(row) {
                continue;
            }
            let in_protocol = selection.matches_protocol(row);
            let in_category = selection.matches_category(walk, row);
            if in_protocol {
                *category_counts.entry(walk.category(row)).or_default() += 1;
            }
            if in_category {
                match protocol_counts
                    .iter_mut()
                    .find(|(protocol, _)| *protocol == row.protocol)
                {
                    Some((_, count)) => *count += 1,
                    None => protocol_counts.push((row.protocol, 1)),
                }
            }
            if !(in_protocol && in_category) {
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
                total_count = total_count.saturating_add(1);
                if wants.rows {
                    matching.push(index);
                }
            }
        }

        let page: &[usize] = if wants.rows {
            match input.sort_field {
                None => walk.display_order(&mut matching),
                Some(field) => sorted(walk, &mut matching, field, input.sort_direction),
            }
            let page_size = (input.page_size as usize).clamp(1, MAX_QUEUE_PAGE_SIZE);
            let offset = (input.page_index as usize)
                .saturating_mul(page_size)
                .min(matching.len());
            let end = offset.saturating_add(page_size).min(matching.len());
            &matching[offset..end]
        } else {
            &[]
        };

        let eta = EtaTotals::of(walk, page);

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
            sources: page.iter().map(|&index| walk.rows[index].source).collect(),
            total_count,
            category_counts,
            protocol_counts: protocol_counts
                .into_iter()
                .map(|(protocol, count)| QueueProtocolCount { protocol, count })
                .collect(),
            state_counts: state_counts
                .into_iter()
                .map(|(state, count)| QueueStateCount { state, count })
                .collect(),
            remaining_bytes: eta.remaining_bytes,
            eta_total_bytes: eta.total_bytes,
            eta_downloaded_bytes: eta.downloaded_bytes,
            eta_through: eta.through,
        }
    }
}

/// The queue's byte totals and each page row's bytes through, in one pass
/// over the rows. Time left counts every job ahead in queue order, filters
/// aside; only the jobs that lead the queue (transferring, or with a rate)
/// are listed, to be ranked, and nothing is kept per row.
struct EtaTotals {
    remaining_bytes: u64,
    total_bytes: u64,
    downloaded_bytes: u64,
    through: Vec<QueueEtaBytes>,
}

impl EtaTotals {
    fn of(walk: &QueueWalk, page: &[usize]) -> Self {
        // A page row's slot by its row index. An empty page allocates none.
        let slots: HashMap<usize, usize> = page
            .iter()
            .enumerate()
            .map(|(slot, &index)| (index, slot))
            .collect();
        let slot = |index: usize| slots.get(&index).copied();
        let mut through: Vec<Option<u64>> = vec![None; page.len()];
        // Only a page ranks the leading jobs; a totals answer lists none.
        let mut leads: Vec<usize> = if page.is_empty() {
            Vec::new()
        } else {
            Vec::with_capacity(walk.leading)
        };
        let (mut remaining_bytes, mut total_bytes, mut downloaded_bytes) = (0u64, 0u64, 0u64);
        // Bytes of the waiting jobs that do not lead, ahead of and including
        // each one, in queue position.
        let mut trailing = 0u64;
        for (index, row) in walk.rows.iter().enumerate() {
            if row.awaits_download() {
                remaining_bytes += row.remaining_bytes();
            }
            if !row.counts_toward_eta() {
                continue;
            }
            total_bytes += row.total_bytes;
            downloaded_bytes += row.downloaded_bytes;
            if row.leads() {
                if !page.is_empty() {
                    leads.push(index);
                }
                continue;
            }
            trailing += row.remaining_bytes();
            if !page.is_empty()
                && let Some(slot) = slot(index)
            {
                through[slot] = Some(trailing);
            }
        }
        if !page.is_empty() {
            leads.sort_unstable_by(|&left, &right| walk.download_cmp(left, right));
            let mut ahead = 0u64;
            for &index in &leads {
                ahead += walk.rows[index].remaining_bytes();
                if let Some(slot) = slot(index) {
                    through[slot] = Some(ahead);
                }
            }
            // Every leading job is ahead of every one that does not lead.
            for (slot, bytes) in through.iter_mut().enumerate() {
                if let Some(bytes) = bytes
                    && !walk.rows[page[slot]].leads()
                {
                    *bytes += ahead;
                }
            }
        }
        EtaTotals {
            remaining_bytes,
            total_bytes,
            downloaded_bytes,
            through: page
                .iter()
                .zip(through)
                .filter_map(|(&index, bytes)| {
                    bytes.map(|bytes_through| QueueEtaBytes {
                        id: walk.rows[index].id.0,
                        bytes_through,
                    })
                })
                .collect(),
        }
    }
}

fn sorted(
    walk: &QueueWalk,
    indexes: &mut [usize],
    field: QueueSortField,
    direction: Option<QueueSortDirection>,
) {
    // Names are worked out once per row, never per comparison, each as a
    // folded key and the raw name that breaks a tie between equal keys.
    let names: HashMap<usize, (String, String)> = if field == QueueSortField::Name {
        indexes
            .iter()
            .map(|&index| {
                let name = sort_name(&walk.rows[index]);
                (index, (fold_name(&name), name))
            })
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
    // Section by section as the screen draws them, the field within each.
    indexes.sort_unstable_by(|&left, &right| {
        let (left_row, right_row) = (&walk.rows[left], &walk.rows[right]);
        let order = match direction.unwrap_or(QueueSortDirection::Desc) {
            QueueSortDirection::Asc => by_field(left, right),
            QueueSortDirection::Desc => by_field(left, right).reverse(),
        };
        left_row
            .group_rank()
            .cmp(&right_row.group_rank())
            .then(order)
            .then_with(|| left_row.id.0.cmp(&right_row.id.0))
    });
}

/// A name as a sort compares it: lower case, with the accents of Latin
/// letters dropped, so "alpha", "Beta", "Émile" and "Zulu" sort in that
/// order, as a reader's alphabet has them.
pub(crate) fn fold_name(name: &str) -> String {
    let mut folded = String::with_capacity(name.len());
    for lower in name.chars().flat_map(char::to_lowercase) {
        match lower {
            // Combining accents, as a decomposed name carries them.
            '\u{0300}'..='\u{036F}' => {}
            'ß' => folded.push_str("ss"),
            'ĳ' => folded.push_str("ij"),
            'æ' => folded.push_str("ae"),
            'œ' => folded.push_str("oe"),
            _ => folded.push(base_letter(lower)),
        }
    }
    folded
}

// The Latin-1 and Latin Extended-A letters, in code point order from
// U+0100, as the letter each is an accented form of: one run per letter.
const EXTENDED_A: [(char, u8); 21] = [
    ('a', 6),
    ('c', 8),
    ('d', 4),
    ('e', 10),
    ('g', 8),
    ('h', 4),
    ('i', 10),
    ('i', 2),
    ('j', 2),
    ('k', 3),
    ('l', 10),
    ('n', 9),
    ('o', 8),
    ('r', 6),
    ('s', 8),
    ('t', 6),
    ('u', 12),
    ('w', 2),
    ('y', 3),
    ('z', 6),
    ('s', 1),
];

fn base_letter(letter: char) -> char {
    match letter {
        'à'..='å' => 'a',
        'ç' => 'c',
        'è'..='ë' => 'e',
        'ì'..='ï' => 'i',
        'ð' => 'd',
        'ñ' => 'n',
        'ò'..='ö' | 'ø' => 'o',
        'ù'..='ü' => 'u',
        'ý' | 'ÿ' => 'y',
        '\u{0100}'..='\u{017F}' => {
            let mut offset = letter as u32 - 0x0100;
            for (base, run) in EXTENDED_A {
                if offset < u32::from(run) {
                    return base;
                }
                offset -= u32::from(run);
            }
            letter
        }
        _ => letter,
    }
}

/// One page as a page answer reads it: the walk, the plan and the page's
/// rows, all read under one queue lock, with the queue revision they show.
pub(crate) struct PageRead {
    pub(crate) walk: QueueWalk,
    pub(crate) plan: PagePlan,
    pub(crate) rows: Vec<JobInfo>,
    pub(crate) revision: u64,
}

impl PageRead {
    pub(crate) fn read(
        state: &SharedPipelineState,
        input: &QueuePageInput,
        wants: PageWants,
    ) -> Self {
        let needs = QueueSelection::of_page(input).needs(input.sort_field);
        state.read_queue(|inner| {
            let walk = QueueWalk::of(inner, needs);
            let plan = PagePlan::plan(&walk, input, wants);
            let rows = plan
                .ids
                .iter()
                .zip(&plan.sources)
                .filter_map(|(id, source)| match source {
                    RowSource::Queue => inner.row(*id).map(|row| row.info(&inner.globals)),
                    RowSource::Torrent(index) => inner.torrents.get(*index).cloned(),
                })
                .collect();
            PageRead {
                walk,
                plan,
                rows,
                revision: state.queue_revision(),
            }
        })
    }
}

/// Copies every queued row in queue order, as a listing shows it: no per-file
/// bytes, server attribution or discards. The torrents fall in among them by
/// when each was added.
pub(crate) fn read_listing(state: &SharedPipelineState) -> Vec<JobInfo> {
    state.read_queue(|inner| {
        let mut torrents = inner.torrents.iter().peekable();
        let mut listing = Vec::with_capacity(inner.jobs.len() + inner.torrents.len());
        for row in &inner.jobs {
            let info = row.view(&inner.globals).list_info();
            while let Some(torrent) =
                torrents.next_if(|torrent| torrent.created_at_epoch_ms < info.created_at_epoch_ms)
            {
                listing.push(torrent.clone());
            }
            listing.push(info);
        }
        listing.extend(torrents.cloned());
        listing
    })
}

/// Copies one row, found through the position index, or one torrent.
pub(crate) fn read_row(state: &SharedPipelineState, id: JobId) -> Option<JobInfo> {
    state.read_queue(|inner| {
        inner
            .row(id)
            .map(|row| row.info(&inner.globals))
            .or_else(|| inner.torrents.iter().find(|job| job.job_id == id).cloned())
    })
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
        for job in &inner.torrents {
            summary.total_items += 1;
            summary.total_bytes += job.total_bytes;
            summary.downloaded_bytes += job.downloaded_bytes;
            summary.count_state(queue_item_state(QueueStateFacts::of_job(job)));
        }
        summary
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_server_core::{DownloadState, JobStatus, PipelineMetrics, PostState, RunState};

    fn job(id: u64, status: JobStatus, category: Option<&str>, total_bytes: u64) -> JobInfo {
        let (download_state, run_state, post_state) = match status {
            JobStatus::Downloading => (
                DownloadState::Downloading,
                RunState::Active,
                PostState::Idle,
            ),
            JobStatus::Paused => (DownloadState::Queued, RunState::Paused, PostState::Idle),
            JobStatus::Extracting => (
                DownloadState::Complete,
                RunState::Active,
                PostState::Extracting,
            ),
            JobStatus::Failed { .. } => (DownloadState::Failed, RunState::Active, PostState::Idle),
            _ => (DownloadState::Queued, RunState::Active, PostState::Idle),
        };
        JobInfo {
            job_id: JobId(id),
            protocol: Default::default(),
            job_hash: None,
            name: format!("Show.{id:05}.1080p"),
            status,
            download_state,
            finalizing_download: false,
            fetching_repair_data: false,
            post_state,
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
            protocol: None,
            states: None,
            priorities: None,
            categories: None,
            sort_field: None,
            sort_direction: None,
        }
    }

    const ROWS: PageWants = PageWants { rows: true };
    const TOTALS: PageWants = PageWants { rows: false };

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

        let first = PagePlan::plan(&walk, &page_input(0, 500), ROWS);
        assert_eq!(first.total_count, 1_200);
        let mut expected = vec![700, 900];
        expected.extend((1..=498).collect::<Vec<_>>());
        assert_eq!(ids(&first), expected);

        let last = PagePlan::plan(&walk, &page_input(2, 500), ROWS);
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
        let plan = PagePlan::plan(&walk, &input, ROWS);

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
        assert_eq!(ids(&PagePlan::plan(&walk, &input, ROWS)), vec![1, 2, 3]);
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
    }

    #[test]
    fn a_page_is_read_with_its_rows_under_one_lock() {
        let jobs = (1..=50u64)
            .map(|id| job(id, JobStatus::Queued, None, 1))
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let read = PageRead::read(&state, &page_input(1, 20), ROWS);
        assert_eq!(ids(&read.plan), (21..=40).collect::<Vec<_>>());
        assert_eq!(
            read.rows.iter().map(|job| job.job_id.0).collect::<Vec<_>>(),
            (21..=40).collect::<Vec<_>>()
        );
        assert_eq!(read.revision, state.queue_revision());
    }

    #[test]
    fn every_active_job_leads_the_default_page_then_paused_queued_and_failed() {
        // Queue position 1 500 extracts, 1 600 is paused, 1 700 failed and
        // 1 800 downloads; everything else waits.
        let jobs = (1..=2_000)
            .map(|id| {
                let status = match id {
                    1_500 => JobStatus::Extracting,
                    1_600 => JobStatus::Paused,
                    1_700 => JobStatus::Failed {
                        error: "lost".to_string(),
                    },
                    1_800 => JobStatus::Downloading,
                    _ => JobStatus::Queued,
                };
                job(id, status, None, 10)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let walk = QueueWalk::read(&state, WalkNeeds::default());
        let first = PagePlan::plan(&walk, &page_input(0, 4), ROWS);
        assert_eq!(ids(&first), vec![1_800, 1_500, 1_600, 1]);
        let last = PagePlan::plan(&walk, &page_input(499, 4), ROWS);
        assert_eq!(ids(&last), vec![1_998, 1_999, 2_000, 1_700]);

        // A sort keeps the sections and orders by its field within each.
        let mut by_size = page_input(0, 3);
        by_size.sort_field = Some(QueueSortField::Size);
        assert_eq!(
            ids(&PagePlan::plan(&walk, &by_size, ROWS)),
            vec![1_500, 1_800, 1_600]
        );
    }

    #[test]
    fn a_name_sort_ignores_case_and_accents() {
        let names = ["Zulu", "Émile", "alpha", "Beta", "émile", "Alpha"];
        let jobs = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                let mut job = job(index as u64 + 1, JobStatus::Queued, None, 1);
                job.name = (*name).to_string();
                job
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let mut input = page_input(0, 10);
        input.sort_field = Some(QueueSortField::Name);
        input.sort_direction = Some(QueueSortDirection::Asc);
        let walk = QueueWalk::read(
            &state,
            QueueSelection::of_page(&input).needs(input.sort_field),
        );
        let ordered: Vec<&str> = ids(&PagePlan::plan(&walk, &input, ROWS))
            .iter()
            .map(|id| names[*id as usize - 1])
            .collect();
        // Equal folded names fall back to the raw name, capitals first.
        assert_eq!(
            ordered,
            ["Alpha", "alpha", "Beta", "Émile", "émile", "Zulu"]
        );
        assert_eq!(
            fold_name("Ĳssel Łódź Straße Œuvre"),
            "ijssel lodz strasse oeuvre"
        );
    }

    #[test]
    fn time_left_counts_leading_jobs_then_the_queue_ahead() {
        let jobs = (1..=6)
            .map(|id| {
                let status = if id == 5 {
                    JobStatus::Downloading
                } else {
                    JobStatus::Queued
                };
                job(id, status, None, id * 100)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let walk = QueueWalk::read(&state, WalkNeeds::default());
        let plan = PagePlan::plan(&walk, &page_input(0, 6), ROWS);
        let through: Vec<(u64, u64)> = plan
            .eta_through
            .iter()
            .map(|eta| (eta.id, eta.bytes_through))
            .collect();
        assert_eq!(
            through,
            [
                (5, 500),
                (1, 600),
                (2, 800),
                (3, 1_100),
                (4, 1_500),
                (6, 2_100)
            ]
        );
        assert_eq!(plan.eta_total_bytes, 2_100);
    }

    // Allocations the totals answer makes, at `rows` queued jobs.
    fn totals_allocations(rows: u64) -> u64 {
        let jobs = (1..=rows)
            .map(|id| {
                let status = if id % 50 == 0 {
                    JobStatus::Downloading
                } else {
                    JobStatus::Queued
                };
                job(id, status, (id % 3 == 0).then_some("tv"), 10)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let walk = QueueWalk::read(&state, WalkNeeds::default());
        let before = crate::alloc_probe::allocations();
        let plan = PagePlan::plan(&walk, &page_input(0, 1), TOTALS);
        let made = crate::alloc_probe::allocations() - before;
        assert_eq!(plan.total_count as u64, rows);
        assert!(plan.ids.is_empty() && plan.eta_through.is_empty());
        made
    }

    // Allocations a 50-row page makes, at `rows` queued jobs.
    fn page_allocations(rows: u64) -> u64 {
        let jobs = (1..=rows)
            .map(|id| {
                let status = if id % 50 == 0 {
                    JobStatus::Downloading
                } else {
                    JobStatus::Queued
                };
                job(id, status, None, 10)
            })
            .collect();
        let state = SharedPipelineState::new(PipelineMetrics::new(), jobs);
        let walk = QueueWalk::read(&state, WalkNeeds::default());
        let before = crate::alloc_probe::allocations();
        let plan = PagePlan::plan(&walk, &page_input(3, 50), ROWS);
        let made = crate::alloc_probe::allocations() - before;
        assert_eq!(plan.ids.len(), 50);
        made
    }

    #[test]
    fn a_push_allocates_by_its_page_not_by_the_queue() {
        // The totals answer lists no row: the same few allocations whether
        // the queue holds two thousand jobs or four thousand.
        let small = totals_allocations(2_000);
        assert_eq!(totals_allocations(4_000), small);
        assert!(small <= 8, "{small} allocations for the totals");
        // A page lists the matching rows once and its own rows' time left;
        // nothing is allocated per queued job.
        let small = page_allocations(2_000);
        assert_eq!(page_allocations(4_000), small);
        assert!(small <= 16, "{small} allocations for a page");
    }

    #[tokio::test]
    async fn a_page_push_reads_duplicate_summaries_once_per_queue_revision() {
        let db = weaver_server_core::Database::open_in_memory().unwrap();
        let rows: Vec<JobInfo> = (1..=3_000)
            .map(|id| job(id, JobStatus::Queued, None, 1))
            .collect();
        let mut page: Vec<_> = rows[100..150]
            .iter()
            .map(crate::jobs::types::queue_table_item_from_job)
            .collect();
        let mut cache = crate::jobs::query::DuplicateSummaryCache::default();
        cache.attach(db.clone(), 7, &mut page).await.unwrap();
        cache.attach(db.clone(), 7, &mut page).await.unwrap();
        assert_eq!(cache.reads(), 1, "the same revision reads once");
        // A totals push carries no row and reads nothing.
        cache.attach(db.clone(), 8, &mut []).await.unwrap();
        assert_eq!(cache.reads(), 1);
        cache.attach(db.clone(), 8, &mut page).await.unwrap();
        assert_eq!(cache.reads(), 2, "a new revision reads again");
    }
}

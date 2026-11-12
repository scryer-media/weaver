// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Which torrents a list shows, picked from their records before any view
//! is built: a page, a category or a few hashes cost the views they show,
//! not one view per torrent.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

use weaver_torrent::core::Id20;

use super::{Live, TorrentService, TorrentSettings, TorrentView, hashes_of};
use crate::jobs::JobId;
use crate::torrents::FilePriority;
use crate::torrents::layout::PieceLayout;

/// What a view carries beside its own fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shown {
    pub files: bool,
    pub trackers: bool,
    pub tags: bool,
}

impl Shown {
    /// Its own fields alone.
    pub const BARE: Self = Self {
        files: false,
        trackers: false,
        tags: false,
    };
    /// Its trackers and tags: a torrent as a list shows it.
    pub const LISTED: Self = Self {
        files: false,
        trackers: true,
        tags: true,
    };
    /// Its files as well.
    pub const WHOLE: Self = Self {
        files: true,
        trackers: true,
        tags: true,
    };
}

/// What a list's row reads of a torrent's files, without the files: whether
/// it has any, whether it is one file named as the torrent is, and the
/// bytes of the files it fetches with the bytes of those it still lacks.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FileTotals {
    pub any: bool,
    pub one_file: bool,
    pub selected_bytes: u64,
    pub selected_left_bytes: u64,
}

impl FileTotals {
    /// The totals of a view that carries its files.
    pub fn of(view: &TorrentView) -> Self {
        let mut totals = Self {
            any: !view.files.is_empty(),
            one_file: view.is_one_file(),
            ..Self::default()
        };
        for file in view.files.iter().filter(|file| file.selected()) {
            let have = view
                .file_have
                .get(file.index as usize)
                .copied()
                .unwrap_or(0);
            totals.add(file.length, have);
        }
        totals
    }

    // The totals of a torrent named `name` with these files at these
    // levels, read as `of` reads its view's files.
    pub(super) fn of_layout(
        name: &str,
        layout: Option<&PieceLayout>,
        levels: &[FilePriority],
        file_have: &[u64],
    ) -> Self {
        let mut totals = Self::default();
        let mut count = 0usize;
        for (index, file) in layout.iter().flat_map(|layout| layout.files()).enumerate() {
            count += 1;
            if count == 1 {
                totals.one_file = file.name == name;
            }
            if levels.get(index).copied().unwrap_or_default().wanted() {
                totals.add(file.length, file_have.get(index).copied().unwrap_or(0));
            }
        }
        totals.any = count > 0;
        totals.one_file &= count == 1;
        totals
    }

    fn add(&mut self, length: u64, have: u64) {
        self.selected_bytes += length;
        self.selected_left_bytes += length.saturating_sub(have);
    }
}

/// The category a pick keeps.
#[derive(Debug, Clone, Copy)]
pub enum CategoryPick<'a> {
    /// The torrents in the category of this name, in any case; a torrent
    /// in none is not kept.
    Named(&'a str),
    /// The torrents in the category of this name or one under it,
    /// `name/...`, in any case; a torrent in none is in the category named
    /// "" (qBittorrent `TorrentFilter::matchCategory`).
    WithSubcategories(&'a str),
}

impl CategoryPick<'_> {
    fn keeps(self, category: Option<&str>) -> bool {
        match self {
            Self::Named(name) => {
                category.is_some_and(|category| category.eq_ignore_ascii_case(name))
            }
            Self::WithSubcategories(name) => {
                let category = category.unwrap_or_default();
                category.eq_ignore_ascii_case(name)
                    || category
                        .get(..name.len() + 1)
                        .is_some_and(|parent| parent.eq_ignore_ascii_case(&format!("{name}/")))
            }
        }
    }
}

/// Which torrents a list keeps, read from their records alone.
#[derive(Debug, Clone, Copy, Default)]
pub struct Pick<'a> {
    /// The torrents whose `TorrentView::infohash` is one of these, in any
    /// case; every torrent when absent.
    pub hashes: Option<&'a [&'a str]>,
    pub category: Option<CategoryPick<'a>>,
    pub private: Option<bool>,
}

// The 20-byte hashes `hashes` names. A text that is no 40-digit hex hash
// names no torrent: a view's infohash always is one.
fn parsed(hashes: &[&str]) -> HashSet<Id20> {
    hashes
        .iter()
        .filter_map(|hash| {
            let bytes = hex::decode(hash.trim()).ok()?;
            Id20::from_bytes(&bytes).ok()
        })
        .collect()
}

impl Pick<'_> {
    // The torrents of `live` it keeps, in the order they were added: the
    // order `TorrentService::torrents` lists them in.
    pub(super) fn ids(
        &self,
        settings: &TorrentSettings,
        live: &BTreeMap<JobId, Live>,
    ) -> Vec<JobId> {
        let hashes = self.hashes.map(parsed);
        let mut kept: Vec<(i64, JobId)> = live
            .values()
            .filter(|torrent| {
                let record = &torrent.record;
                hashes
                    .as_ref()
                    .is_none_or(|hashes| hashes.contains(&hashes_of(record).best()))
                    && self
                        .private
                        .is_none_or(|private| torrent.private == private)
                    && self.category.is_none_or(|category| {
                        category.keeps(
                            settings
                                .category(record.category_id)
                                .map(|category| category.name.as_str()),
                        )
                    })
            })
            .map(|torrent| (torrent.record.added_at_ms, torrent.record.job_id))
            .collect();
        kept.sort_unstable();
        kept.into_iter().map(|(_, job_id)| job_id).collect()
    }

    // Of the torrents it keeps, those at the places `page` gives for how
    // many it keeps.
    fn paged(
        &self,
        settings: &TorrentSettings,
        live: &BTreeMap<JobId, Live>,
        page: impl FnOnce(usize) -> Range<usize>,
    ) -> Vec<JobId> {
        let ids = self.ids(settings, live);
        let Range { start, end } = page(ids.len());
        let end = end.min(ids.len());
        ids.get(start.min(end)..end).unwrap_or_default().to_vec()
    }
}

impl TorrentService {
    /// How many torrents there are.
    pub async fn count(&self) -> usize {
        self.inner.live.lock().await.len()
    }

    /// Every torrent's job id, in the order they were added.
    pub async fn job_ids(&self) -> Vec<JobId> {
        let settings = self.settings();
        Pick::default().ids(&settings, &*self.inner.live.lock().await)
    }

    /// The torrents `pick` keeps, in the order they were added, and of
    /// those the ones at the places `page` gives for how many it keeps.
    /// Only their views are built.
    pub async fn picked(
        &self,
        pick: Pick<'_>,
        page: impl FnOnce(usize) -> Range<usize>,
        shown: Shown,
    ) -> Vec<TorrentView> {
        let now = self.inner.clock.now_ms();
        self.listed_by(shown, false, false, now, |settings, live| {
            pick.paged(settings, live, page)
        })
        .await
        .into_iter()
        .map(|(view, ..)| view)
        .collect()
    }

    /// The torrents `picked` gives, each with the totals of its files: what
    /// a list's row reads of the files when it does not show them.
    pub async fn picked_rows(
        &self,
        pick: Pick<'_>,
        page: impl FnOnce(usize) -> Range<usize>,
        shown: Shown,
    ) -> Vec<(TorrentView, FileTotals)> {
        let now = self.inner.clock.now_ms();
        self.listed_by(shown, false, true, now, |settings, live| {
            pick.paged(settings, live, page)
        })
        .await
        .into_iter()
        .map(|(view, _, totals)| (view, totals.expect("asked for")))
        .collect()
    }

    /// The torrents of `ids` there are, as [`TorrentService::torrents`]
    /// lists them and in its order. Only their views are built.
    pub async fn torrents_of(&self, ids: &BTreeSet<JobId>) -> Vec<TorrentView> {
        let now = self.inner.clock.now_ms();
        self.listed_by(Shown::LISTED, false, false, now, |_, live| {
            let mut kept: Vec<(i64, JobId)> = ids
                .iter()
                .filter_map(|job_id| live.get(job_id))
                .map(|torrent| (torrent.record.added_at_ms, torrent.record.job_id))
                .collect();
            kept.sort_unstable();
            kept.into_iter().map(|(_, job_id)| job_id).collect()
        })
        .await
        .into_iter()
        .map(|(view, ..)| view)
        .collect()
    }

    /// The torrent whose `TorrentView::infohash` is `hash`, in any case, as
    /// [`TorrentService::torrent`] gives it.
    pub async fn torrent_by_hash(&self, hash: &str) -> Option<TorrentView> {
        let wanted = *parsed(&[hash]).iter().next()?;
        let job_id = self
            .inner
            .live
            .lock()
            .await
            .values()
            .find(|torrent| hashes_of(&torrent.record).best() == wanted)?
            .record
            .job_id;
        self.torrent(job_id).await
    }
}

/// The places a page of `first` torrents after `offset` takes.
pub fn page(offset: usize, first: usize) -> impl FnOnce(usize) -> Range<usize> {
    move |_| offset..offset.saturating_add(first)
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Which torrents a list shows, picked before any view is built: a page, a
//! category or a few hashes cost the views they show, not one view per
//! torrent. The table picks them, a page at a time, through the index of
//! the order they were added in, of a privacy through the index of that
//! privacy.

use crate::torrents::queries::TorrentsInternalQueries;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;

use weaver_torrent::core::Id20;

use super::{ListError, Live, TorrentService, TorrentSettings, TorrentView, hashes_of};
use crate::jobs::JobId;
use crate::torrents::FilePriority;
use crate::torrents::layout::PieceLayout;
use crate::torrents::queries::{CategoryIds, TorrentListFilter};

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

/// The places a list takes among the torrents a pick keeps, in the order
/// they were added.
pub enum Window {
    /// Every one.
    Every,
    /// At most `first` after the first `offset`.
    Page { offset: usize, first: usize },
    /// The places `places` gives for how many the pick keeps, which are
    /// counted first.
    Counted(Box<dyn FnOnce(usize) -> Range<usize> + Send>),
}

impl Window {
    // Its places among `kept` torrents.
    fn places(self, kept: usize) -> Range<usize> {
        let Range { start, end } = match self {
            Self::Every => 0..kept,
            Self::Page { offset, first } => offset..offset.saturating_add(first),
            Self::Counted(places) => places(kept),
        };
        let end = end.min(kept);
        start.min(end)..end
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
    pub(super) fn keeps(self, category: Option<&str>) -> bool {
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
    // What the table is asked for it: the ids of the categories it keeps,
    // as `CategoryPick::keeps` reads a torrent's category through
    // `TorrentSettings::category`, the hashes it names and its privacy.
    pub fn filter(&self, settings: &TorrentSettings) -> TorrentListFilter {
        TorrentListFilter {
            categories: self.category.map(|pick| {
                let mut seen = HashSet::new();
                let mut categories = CategoryIds {
                    none: pick.keeps(None),
                    ..CategoryIds::default()
                };
                // The first category of an id is the one a torrent of it is in.
                for category in settings.categories.iter() {
                    if seen.insert(category.id) {
                        categories.known.push(category.id);
                        if pick.keeps(Some(&category.name)) {
                            categories.ids.push(category.id);
                        }
                    }
                }
                categories
            }),
            hashes: self.hashes.map(|hashes| {
                let mut parsed: Vec<[u8; 20]> = parsed(hashes).into_iter().map(|id| id.0).collect();
                parsed.sort_unstable();
                parsed
            }),
            private: self.private,
        }
    }

    // The torrents of `live` its hashes and category keep, in the order they
    // were added: the order `TorrentService::torrents` lists them in. Its
    // privacy is kept by the table alone, never here.
    pub(super) fn ids(
        &self,
        settings: &TorrentSettings,
        live: &BTreeMap<JobId, Live>,
    ) -> Vec<JobId> {
        self.ids_noting(settings, live, |_| {})
    }

    // As `ids`, with `note` shown every torrent of `live` in the same walk.
    pub(super) fn ids_noting(
        &self,
        settings: &TorrentSettings,
        live: &BTreeMap<JobId, Live>,
        mut note: impl FnMut(&Live),
    ) -> Vec<JobId> {
        let hashes = self.hashes.map(parsed);
        let mut kept: Vec<(i64, JobId)> = live
            .values()
            .inspect(|torrent| note(torrent))
            .filter(|torrent| {
                let record = &torrent.record;
                hashes
                    .as_ref()
                    .is_none_or(|hashes| hashes.contains(&hashes_of(record).best()))
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

    // Of the torrents it keeps, those at `window`'s places, read from the
    // records of `live`: the reading the table's replaces, kept to prove it
    // the same.
    #[cfg(any(test, feature = "test-support"))]
    pub fn paged(
        &self,
        settings: &TorrentSettings,
        live: &BTreeMap<JobId, Live>,
        window: Window,
    ) -> Vec<JobId> {
        // The privacy the in-memory filter kept before the table did: the
        // one a restore read from each torrent's metainfo.
        let ids: Vec<JobId> = self
            .ids(settings, live)
            .into_iter()
            .filter(|job_id| {
                self.private
                    .is_none_or(|private| live[job_id].record.private == private)
            })
            .collect();
        let places = window.places(ids.len());
        ids[places].to_vec()
    }

    // The ids of the torrents it keeps at `window`'s places, in the order
    // they were added, read from the table with no torrent's lock held: the
    // page itself, its privacy kept in the table too. A pick of every
    // torrent reads no table: `Picked::under` takes them from the live
    // torrents it walks anyway.
    pub fn picked(
        &self,
        settings: &TorrentSettings,
        db: &crate::persistence::Database,
        window: Window,
    ) -> Result<Picked, crate::StateError> {
        if self.hashes.is_none()
            && self.category.is_none()
            && self.private.is_none()
            && matches!(window, Window::Every)
        {
            return Ok(Picked::Live);
        }
        let filter = self.filter(settings);
        let (offset, limit) = match window {
            Window::Every => (0, None),
            Window::Page { offset, first } => (offset, Some(first)),
            counted @ Window::Counted(_) => {
                let places = counted.places(db.count_torrent_list(&filter)?);
                (places.start, Some(places.len()))
            }
        };
        if limit == Some(0) {
            return Ok(Picked::Page(Vec::new()));
        }
        Ok(Picked::Page(db.torrent_list_ids(&filter, offset, limit)?))
    }
}

/// What a pick keeps, read from the views a list's rows hold in place of
/// the torrents' records: the hashes, the category and the privacy the
/// table keeps, for a list the snapshot answers.
pub struct RowPick<'a> {
    hashes: Option<HashSet<String>>,
    category: Option<CategoryPick<'a>>,
    private: Option<bool>,
}

impl<'a> Pick<'a> {
    /// The pick as it reads a row's view.
    pub fn of_rows(&self) -> RowPick<'a> {
        RowPick {
            // A view's infohash is its best hash as text.
            hashes: self.hashes.map(|hashes| {
                parsed(hashes)
                    .into_iter()
                    .map(|hash| hash.as_string())
                    .collect()
            }),
            category: self.category,
            private: self.private,
        }
    }
}

impl RowPick<'_> {
    /// Whether it keeps the torrent `view` shows: as the table keeps its
    /// record, its category read through its name as
    /// `TorrentSettings::category` names it.
    pub fn keeps(&self, view: &TorrentView) -> bool {
        self.hashes
            .as_ref()
            .is_none_or(|hashes| hashes.contains(&view.infohash))
            && self
                .category
                .is_none_or(|category| category.keeps(view.category.as_deref()))
            && self.private.is_none_or(|private| view.private == private)
    }
}

/// The torrents a pick read from the table.
pub enum Picked {
    /// The page.
    Page(Vec<JobId>),
    /// Every live torrent: the table was not read.
    Live,
}

impl Picked {
    // The page, among the torrents of `live`: a torrent the table holds
    // that is not live, being added or removed, is not shown.
    pub fn under(self, live: &BTreeMap<JobId, Live>) -> Vec<JobId> {
        match self {
            Self::Page(page) => page,
            Self::Live => {
                let mut kept: Vec<(i64, JobId)> = live
                    .values()
                    .map(|torrent| (torrent.record.added_at_ms, torrent.record.job_id))
                    .collect();
                kept.sort_unstable();
                kept.into_iter().map(|(_, job_id)| job_id).collect()
            }
        }
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

    // The ids `pick` keeps at `window`'s places, read from the table.
    //
    // While the stored torrents are still being read, a pick that names
    // its torrents by hash, or takes a page of `PAGE_WHILE_LOADING` at
    // most, is answered; any other needs every torrent here, and is told
    // they are not yet, before the table is read for it.
    fn picked_ids(&self, pick: Pick<'_>, window: Window) -> Result<Picked, ListError> {
        let named = pick.hashes.is_some();
        let paged = match &window {
            Window::Every => false,
            Window::Page { first, .. } => *first <= PAGE_WHILE_LOADING,
            Window::Counted(_) => true,
        };
        if !named && !paged {
            self.whole()?;
        }
        let picked = pick.picked(&self.settings(), &self.inner.db, window)?;
        // A counted window's size is known only now.
        if !named
            && let Picked::Page(page) = &picked
            && page.len() > PAGE_WHILE_LOADING
        {
            self.whole()?;
        }
        Ok(picked)
    }

    // `picked_ids`, each torrent of the page here to be shown: one the
    // start's load has not reached is read now, so a page lists what the
    // table holds however far the load is.
    async fn picked_here(&self, pick: Pick<'_>, window: Window) -> Result<Picked, ListError> {
        let picked = self.picked_ids(pick, window)?;
        if let Picked::Page(page) = &picked {
            self.ensure(page).await;
        }
        Ok(picked)
    }

    /// The torrents `pick` keeps, in the order they were added, and of
    /// those the ones at `window`'s places. Only their views are built. An
    /// error when the table cannot be read, never an empty list; and while
    /// the stored torrents are still being read at start, an error for a
    /// pick that needs every one of them: one that names no hash and takes
    /// more than a page of [`PAGE_WHILE_LOADING`].
    pub async fn picked(
        &self,
        pick: Pick<'_>,
        window: Window,
        shown: Shown,
    ) -> Result<Vec<TorrentView>, ListError> {
        let picked = self.picked_here(pick, window).await?;
        let now = self.inner.clock.unix_ms();
        Ok(self
            .listed_by(shown, false, false, now, move |_, live| picked.under(live))
            .await
            .into_iter()
            .map(|(view, ..)| view)
            .collect())
    }

    /// The torrents `picked` gives, each with the totals of its files: what
    /// a list's row reads of the files when it does not show them.
    pub async fn picked_rows(
        &self,
        pick: Pick<'_>,
        window: Window,
        shown: Shown,
    ) -> Result<Vec<(TorrentView, FileTotals)>, ListError> {
        let picked = self.picked_here(pick, window).await?;
        let now = self.inner.clock.unix_ms();
        Ok(self
            .listed_by(shown, false, true, now, move |_, live| picked.under(live))
            .await
            .into_iter()
            .map(|(view, _, totals)| (view, totals.expect("asked for")))
            .collect())
    }

    /// The torrents of `ids` there are, as [`TorrentService::torrents`]
    /// lists them and in its order. Only their views are built.
    pub async fn torrents_of(&self, ids: &BTreeSet<JobId>) -> Vec<TorrentView> {
        let now = self.inner.clock.unix_ms();
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
    /// [`TorrentService::torrent`] gives it: found through the table's
    /// index of hashes, not by a walk of every torrent. An error when the
    /// table cannot be read, never none.
    pub async fn torrent_by_hash(
        &self,
        hash: &str,
    ) -> Result<Option<TorrentView>, crate::StateError> {
        let hashes = [hash];
        let pick = Pick {
            hashes: Some(&hashes),
            ..Pick::default()
        };
        let Picked::Page(found) = pick.picked(&self.settings(), &self.inner.db, page(0, 1))? else {
            unreachable!("a pick of hashes reads its page from the table")
        };
        Ok(match found.first() {
            Some(job_id) => self.torrent(*job_id).await,
            None => None,
        })
    }
}

/// The most torrents a page may take while the stored torrents are still
/// being read at start: the page is read for the request that asks.
pub const PAGE_WHILE_LOADING: usize = 500;

/// The places a page of `first` torrents after `offset` takes.
pub fn page(offset: usize, first: usize) -> Window {
    Window::Page { offset, first }
}

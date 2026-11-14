// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The two collections a queue scan judges by that are derived from state the
// pipeline changes as it works: the recovery indexes a job still waits for,
// and the cursors of the uuencoded files. Each is owned here by a type that
// counts the changes its answer can move on and hands out one shared list
// per count, so an ask made while nothing changed derives nothing and a scan
// memo compares two pointers. Every change goes through these types: their
// maps are private, so a new site that changes one does not compile until it
// uses a method that counts it.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::UuFileAssembly;
use super::graph::job::GraphJob;
use super::graph::metadata::RecoveryFormat;
use crate::jobs::ids::{JobId, NzbFileId};

// The graph runtime of each job. Reading goes straight to the map; every
// mutable borrow of a job's graph moves that graph's counter first, since
// the graph is the only thing that can say what the borrow changed.
#[derive(Default)]
pub(crate) struct GraphJobs {
    jobs: HashMap<JobId, GraphJob>,
}

impl Deref for GraphJobs {
    type Target = HashMap<JobId, GraphJob>;

    fn deref(&self) -> &Self::Target {
        &self.jobs
    }
}

impl GraphJobs {
    pub(crate) fn get_mut(&mut self, job_id: &JobId) -> Option<&mut GraphJob> {
        let job = self.jobs.get_mut(job_id)?;
        job.touched();
        Some(job)
    }

    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut GraphJob> {
        fn touch(job: &mut GraphJob) -> &mut GraphJob {
            job.touched();
            job
        }
        self.jobs.values_mut().map(touch)
    }

    // A graph put in starts its own count and holds no list.
    pub(crate) fn insert(&mut self, job_id: JobId, job: GraphJob) -> Option<GraphJob> {
        self.jobs.insert(job_id, job)
    }

    pub(crate) fn remove(&mut self, job_id: &JobId) -> Option<GraphJob> {
        self.jobs.remove(job_id)
    }

    // The declared recovery indexes the job's root still holds its payload
    // for, as one shared list for as long as the graph is not borrowed
    // mutably. `declared` is read only when the list is made again.
    pub(crate) fn indexes_to_come(
        &mut self,
        job_id: &JobId,
        declared: impl IntoIterator<Item = (weaver_graph::MemberId, RecoveryFormat)>,
    ) -> Option<Arc<[u32]>> {
        self.jobs.get_mut(job_id)?.indexes_to_come_kept(declared)
    }
}

// The next segment ordinal a uuencoded file's cursor will place. It is set
// only through [`UuCursor::set`], which counts the move for the files' owner.
#[derive(Default)]
pub(crate) struct UuCursor {
    next: u32,
    moved: Arc<AtomicU64>,
}

impl UuCursor {
    pub(crate) fn at(next: u32) -> Self {
        Self {
            next,
            moved: Arc::default(),
        }
    }

    pub(crate) fn get(&self) -> u32 {
        self.next
    }

    pub(crate) fn set(&mut self, next: u32) {
        if next != self.next {
            self.next = next;
            self.moved.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// Sequential-assembly state for uuencode files. The cursor ordinals a scan
// respects are every file here with its cursor, so the count moves when a
// file is added or dropped and when a cursor is set.
#[derive(Default)]
pub(crate) struct UuFiles {
    files: HashMap<NzbFileId, UuFileAssembly>,
    moved: Arc<AtomicU64>,
    // The ordinals as of a count, kept until the count moves.
    ordinals: Option<(u64, Arc<HashMap<NzbFileId, u32>>)>,
}

impl Deref for UuFiles {
    type Target = HashMap<NzbFileId, UuFileAssembly>;

    fn deref(&self) -> &Self::Target {
        &self.files
    }
}

impl UuFiles {
    // A file's state, to change anything of it but which files there are.
    // Its cursor counts its own moves.
    pub(crate) fn get_mut(&mut self, file_id: &NzbFileId) -> Option<&mut UuFileAssembly> {
        self.files.get_mut(file_id)
    }

    pub(crate) fn get_or_insert_with(
        &mut self,
        file_id: NzbFileId,
        make: impl FnOnce() -> UuFileAssembly,
    ) -> &mut UuFileAssembly {
        let moved = &self.moved;
        self.files.entry(file_id).or_insert_with(|| {
            moved.fetch_add(1, Ordering::Relaxed);
            Self::adopted(moved, make())
        })
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, file_id: NzbFileId, file: UuFileAssembly) {
        self.moved.fetch_add(1, Ordering::Relaxed);
        self.files.insert(file_id, Self::adopted(&self.moved, file));
    }

    pub(crate) fn remove(&mut self, file_id: &NzbFileId) -> Option<UuFileAssembly> {
        let file = self.files.remove(file_id)?;
        self.moved.fetch_add(1, Ordering::Relaxed);
        Some(file)
    }

    // The cursor of every file, as one shared map for as long as no file
    // came or went and no cursor moved. A count that moved to the same
    // answer keeps the map it had, so its holders still match.
    pub(crate) fn cursor_ordinals(&mut self) -> Arc<HashMap<NzbFileId, u32>> {
        let at = self.moved.load(Ordering::Relaxed);
        if let Some((kept_at, kept)) = &self.ordinals
            && *kept_at == at
        {
            debug_assert_eq!(**kept, self.ordinals_now());
            return Arc::clone(kept);
        }
        let now = self.ordinals_now();
        let kept = match self.ordinals.take() {
            Some((_, kept)) if *kept == now => kept,
            _ => Arc::new(now),
        };
        self.ordinals = Some((at, Arc::clone(&kept)));
        kept
    }

    fn ordinals_now(&self) -> HashMap<NzbFileId, u32> {
        self.files
            .iter()
            .map(|(file_id, uu)| (*file_id, uu.next_index.get()))
            .collect()
    }

    fn adopted(moved: &Arc<AtomicU64>, mut file: UuFileAssembly) -> UuFileAssembly {
        file.next_index.moved = Arc::clone(moved);
        file
    }
}

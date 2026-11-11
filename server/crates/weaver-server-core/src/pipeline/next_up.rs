// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The jobs next up, their article lists read ahead.
//
// A job waiting its turn holds no article list. The few jobs that take the
// next slots, in dispatch order, have theirs read from their stored NZBs off
// the pipeline thread as they come up, so a job that gets a slot builds its
// articles from lists already in hand. A job that falls out of the next few
// without starting drops its lists. The lists read ahead are checked against
// the job's files as any read is, and a job whose lists do not match, or did
// not read, reads them again as it activates.
//
// The ring holds each job's lists packed (see `HeldLists`), and they are
// unpacked as the job activates, on the blocking thread that checks them.

use super::*;
use crate::jobs::model::{JobArticles, MessageIds, SegmentSpec};

// How many jobs past the slots have their lists read ahead.
pub(crate) const NEXT_UP_DEPTH: usize = 3;

pub(crate) struct NextUp {
    pub(crate) depth: usize,
    entries: HashMap<JobId, Entry>,
    next_read: u64,
}

enum Entry {
    // A read is out, told by its number.
    Reading(u64),
    Read(HeldLists),
    // The read failed: the job reads its lists as it activates.
    Unread,
}

impl Default for NextUp {
    fn default() -> Self {
        Self {
            depth: NEXT_UP_DEPTH,
            entries: HashMap::new(),
            next_read: 0,
        }
    }
}

impl NextUp {
    // The lists read ahead for the job, taken.
    pub(crate) fn take(&mut self, job_id: JobId) -> Option<HeldLists> {
        match self.entries.remove(&job_id)? {
            Entry::Read(lists) => Some(lists),
            Entry::Reading(_) | Entry::Unread => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn held(&self) -> Vec<JobId> {
        let mut held: Vec<JobId> = self
            .entries
            .iter()
            .filter(|(_, entry)| matches!(entry, Entry::Read(_)))
            .map(|(id, _)| *id)
            .collect();
        held.sort_unstable();
        held
    }

    // Whether a read ahead is out.
    pub(crate) fn reading(&self) -> bool {
        self.entries
            .values()
            .any(|entry| matches!(entry, Entry::Reading(_)))
    }

    // Every job the ring tracks, read or not.
    #[cfg(test)]
    pub(crate) fn ids(&self) -> Vec<JobId> {
        let mut ids: Vec<JobId> = self.entries.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    // The number of the last read sent out.
    #[cfg(test)]
    pub(crate) fn last_read(&self) -> u64 {
        self.next_read
    }
}

// One job's article lists as the ring holds them: every message-id of the
// job end to end in one block, and each part a fixed record that names where
// its ids end. A part costs its record and its ids' bytes, with no string or
// list of its own. The lists come back, in order and whole, as the job
// activates, off the pipeline thread.
pub(crate) struct HeldLists {
    ids: Box<str>,
    // Where each id ends in `ids`.
    id_ends: Box<[u32]>,
    // Every part of every file, in order.
    parts: Box<[HeldPart]>,
    // Where each file's parts end in `parts`.
    file_ends: Box<[u32]>,
}

struct HeldPart {
    ordinal: u32,
    article_number: u32,
    bytes: u32,
    // Where the part's ids end in `id_ends`.
    ids_end: u32,
}

impl HeldLists {
    // The lists packed, or None when they cannot be held this way: a part
    // whose fetch has moved past its first id, or a job too large for the
    // offsets. Such a job reads its lists as it activates.
    pub(crate) fn pack(lists: &[Vec<SegmentSpec>]) -> Option<Self> {
        let mut id_count = 0usize;
        let mut id_bytes = 0usize;
        let mut part_count = 0usize;
        for list in lists {
            part_count += list.len();
            for part in list {
                let all = part.message_ids.all();
                if !std::ptr::eq(part.message_ids.in_use(), all[0].as_str()) {
                    return None;
                }
                id_count += all.len();
                id_bytes += all.iter().map(String::len).sum::<usize>();
            }
        }
        u32::try_from(id_bytes).ok()?;
        u32::try_from(id_count).ok()?;
        u32::try_from(part_count).ok()?;
        let mut ids = String::with_capacity(id_bytes);
        let mut id_ends = Vec::with_capacity(id_count);
        let mut parts = Vec::with_capacity(part_count);
        let mut file_ends = Vec::with_capacity(lists.len());
        for list in lists {
            for part in list {
                for id in part.message_ids.all() {
                    ids.push_str(id);
                    id_ends.push(ids.len() as u32);
                }
                parts.push(HeldPart {
                    ordinal: part.ordinal,
                    article_number: part.article_number,
                    bytes: part.bytes,
                    ids_end: id_ends.len() as u32,
                });
            }
            file_ends.push(parts.len() as u32);
        }
        Some(Self {
            ids: ids.into_boxed_str(),
            id_ends: id_ends.into_boxed_slice(),
            parts: parts.into_boxed_slice(),
            file_ends: file_ends.into_boxed_slice(),
        })
    }

    // The lists as they were packed.
    pub(crate) fn unpack(&self) -> Vec<Vec<SegmentSpec>> {
        let mut lists = Vec::with_capacity(self.file_ends.len());
        let mut part_start = 0usize;
        let mut id_start = 0usize;
        let mut byte_start = 0usize;
        for &part_end in &self.file_ends {
            let part_end = part_end as usize;
            let mut list = Vec::with_capacity(part_end - part_start);
            for part in &self.parts[part_start..part_end] {
                let id_end = part.ids_end as usize;
                let mut ids = Vec::with_capacity(id_end - id_start);
                for &byte_end in &self.id_ends[id_start..id_end] {
                    let byte_end = byte_end as usize;
                    ids.push(self.ids[byte_start..byte_end].to_string());
                    byte_start = byte_end;
                }
                id_start = id_end;
                list.push(SegmentSpec {
                    ordinal: part.ordinal,
                    article_number: part.article_number,
                    bytes: part.bytes,
                    message_ids: MessageIds::new(ids),
                });
            }
            part_start = part_end;
            lists.push(list);
        }
        lists
    }

    // The bytes the lists hold on the heap.
    #[cfg(test)]
    pub(crate) fn heap_bytes(&self) -> usize {
        self.ids.len()
            + std::mem::size_of_val(&*self.id_ends)
            + std::mem::size_of_val(&*self.parts)
            + std::mem::size_of_val(&*self.file_ends)
    }
}

impl Pipeline {
    // The jobs next up: installed jobs that never started, waiting for a
    // slot, not paused, in dispatch order, as many as the depth.
    pub(in crate::pipeline) fn next_up_jobs(&self) -> Vec<JobId> {
        let depth = self.next_up.depth;
        if depth == 0 {
            return Vec::new();
        }
        self.shared_state.queue.read(|inner| {
            if inner.globals.shutting_down {
                return Vec::new();
            }
            // Paused as the admission pass counts it: a pause asked for is in
            // force at once, and a job resumed while it waits for its slot,
            // still paused until it takes one, is next up.
            let mut waiting = inner.dispatch_keys(|row| {
                row.intents.cancelled.is_none()
                    && !row.report.retiring
                    && !self.nzb_admitted.contains(&row.id)
                    && !self.admission_paused(row)
                    && self.jobs.get(&row.id).is_some_and(|state| {
                        matches!(state.articles, JobArticles::Queued(_))
                            && matches!(state.status, JobStatus::Queued | JobStatus::Paused)
                    })
            });
            crate::pipeline::queue::order::keep_first(&mut waiting, depth);
            waiting.into_iter().map(|key| key.job_id()).collect()
        })
    }

    // Read ahead the lists of the jobs now next up, and drop those of the
    // jobs that are not. A job that took a slot keeps its lists until it
    // activates. Each read waits for the writes queued for its job, then
    // streams its stored NZB on a blocking thread.
    pub(crate) fn refresh_next_up(&mut self) {
        let wanted: HashSet<JobId> = self.next_up_jobs().into_iter().collect();
        let admitted = &self.nzb_admitted;
        let jobs = &self.jobs;
        self.next_up.entries.retain(|id, _| {
            wanted.contains(id)
                || (admitted.contains(id)
                    && jobs
                        .get(id)
                        .is_some_and(|state| matches!(state.articles, JobArticles::Queued(_))))
        });
        for job_id in wanted {
            if self.next_up.entries.contains_key(&job_id) {
                continue;
            }
            self.next_up.next_read += 1;
            let read = self.next_up.next_read;
            self.next_up.entries.insert(job_id, Entry::Reading(read));
            let (written_tx, written_rx) = tokio::sync::oneshot::channel();
            if let Err(error) = self
                .db
                .try_queue_job_write(job_id, "next_up_read", move |_| {
                    let _ = written_tx.send(());
                    Ok(())
                })
            {
                warn!(job_id = job_id.0, error = %error, "could not queue the job's read ahead behind its writes");
            }
            let db = self.db.clone();
            let replies = self.db_reply_tx.clone();
            tokio::spawn(async move {
                let _ = written_rx.await;
                let lists = tokio::task::spawn_blocking(move || {
                    let (_, read) = super::activation::read_stored_lists(&db, job_id)?;
                    HeldLists::pack(&read.lists)
                        .ok_or_else(|| "the lists cannot be held packed".to_string())
                })
                .await
                .unwrap_or_else(|error| Err(format!("read ahead worker failed: {error}")));
                let _ = replies.send(crate::pipeline::db_reply::DbReply::NextUp {
                    job_id,
                    read,
                    lists,
                });
            });
        }
    }

    // A read ahead came back. Only the job's current read is kept.
    pub(crate) fn next_up_read(
        &mut self,
        job_id: JobId,
        read: u64,
        lists: Result<HeldLists, String>,
    ) {
        let Some(entry) = self.next_up.entries.get_mut(&job_id) else {
            return;
        };
        if !matches!(entry, Entry::Reading(current) if *current == read) {
            return;
        }
        *entry = match lists {
            Ok(lists) => Entry::Read(lists),
            Err(error) => {
                debug!(job_id = job_id.0, %error, "could not read the job's lists ahead");
                Entry::Unread
            }
        };
    }
}

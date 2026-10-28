// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's articles exist only while it runs.
//
// A job waiting its turn holds its files' names and identities and, per file,
// how many articles it has and their size: nothing per article, and nothing
// charged to the process ledger. The first time the scheduler reaches it, the
// job becomes activating and its article lists are read back from the NZB the
// job was admitted with, and its graph from its checkpoint when it has one,
// off the pipeline thread. When they arrive the job's assembly, queues, graph
// root and repeated-article index are built in
// [`Pipeline::activate_job_articles`], the one place they are built, and their
// whole size is charged. They are dropped, and the charge released, in
// [`Pipeline::release_job_articles`] when the job leaves the pipeline, or
// once a paused job has nothing left out ([`Pipeline::park_job_articles`]).
// A paused job is waiting its turn again, and is activated afresh when it is
// next reached, as a restart restores it, keeping the parts that arrived and
// wait to be placed.

use super::*;
use crate::jobs::model::{ActiveArticles, FileSummaries, FileSummary, JobArticles, SegmentSpec};
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::download::repeated::RepeatedArticles;
use crate::pipeline::graph::restore::{GraphRestore, GraphRestoreError, RestoreSeat, root_files};
use crate::pipeline::held::HeldPart;

// What the read of one job's stored NZB produced.
pub(crate) struct JobActivated {
    pub(crate) job_id: JobId,
    // The number the job's activation was told by when the read was sent.
    pub(crate) activation: u64,
    pub(crate) result: Result<ReadArticles, String>,
}

pub(crate) struct ReadArticles {
    // Every file's article list, in the spec's file order.
    lists: Vec<Vec<SegmentSpec>>,
    repeated: Option<Arc<RepeatedArticles>>,
    // The job's graph rebuilt from its checkpoint, and the articles that
    // checkpoint has in hand; none for a job never checkpointed.
    restored: Option<GraphRestore>,
}

impl Pipeline {
    // Whether the job is waiting for its articles to be built: their read is
    // out, or its earlier graph still has something out on a thread.
    pub(in crate::pipeline) fn awaits_job_articles(&self, job_id: JobId) -> bool {
        self.retiring_graph_jobs.contains_key(&job_id)
            || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| matches!(state.articles, JobArticles::Activating(..)))
    }

    // Start reading a queued job's articles back. A job already activating
    // or active is left as it is, so the scheduler may ask on every pass. So
    // is a job whose earlier graph still has something out on a thread: its
    // new graph is not built over the old one's runs.
    pub(in crate::pipeline) fn request_job_activation(&mut self, job_id: JobId) {
        if self.retiring_graph_jobs.contains_key(&job_id) {
            return;
        }
        self.next_activation += 1;
        let activation = self.next_activation;
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let summaries = match &mut state.articles {
            JobArticles::Queued(summaries) => std::mem::take(summaries),
            JobArticles::Activating(..) | JobArticles::Active(_) => return,
        };
        let category = state.spec.category.clone();
        let metadata = state.spec.metadata.clone();
        let working_dir = state.working_dir.clone();
        let spec_password = state.spec.password.clone();
        let expected = summaries.clone();
        state.articles = JobArticles::Activating(summaries, activation);
        let seat = self.graph_restore_seat(spec_password, &working_dir);
        // The read waits for every write already queued for the job, so a
        // checkpoint taken as the job paused is the one it reads back.
        let (written_tx, written_rx) = tokio::sync::oneshot::channel();
        if let Err(error) = self
            .db
            .try_queue_job_write(job_id, "job_articles_read", move |_| {
                let _ = written_tx.send(());
                Ok(())
            })
        {
            warn!(job_id = job_id.0, error = %error, "could not queue the job's article read behind its writes");
        }
        let db = self.db.clone();
        let memory = Arc::clone(&self.process_memory_budget);
        let tx = self.job_activated_tx.clone();
        tokio::spawn(async move {
            let _ = written_rx.await;
            let result = tokio::task::spawn_blocking(move || {
                read_job_articles(&db, job_id, category, metadata, &expected, memory, seat)
            })
            .await
            .unwrap_or_else(|error| Err(format!("article read worker failed: {error}")));
            let _ = tx
                .send(JobActivated {
                    job_id,
                    activation,
                    result,
                })
                .await;
        });
    }

    // The read came back: build the job's articles, or fail the job if they
    // could not be read. Only the reply to the job's current activation is
    // heard: a job that left the pipeline, paused, or was activated again
    // meanwhile drops what was read with this message.
    pub(crate) fn handle_job_activated(&mut self, done: JobActivated) {
        let JobActivated {
            job_id,
            activation,
            result,
        } = done;
        if !self.jobs.get(&job_id).is_some_and(|state| {
            matches!(state.articles, JobArticles::Activating(_, current) if current == activation)
        }) {
            return;
        }
        match result {
            Ok(read) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    for (file, list) in state.spec.files.iter_mut().zip(read.lists) {
                        file.segments = list;
                    }
                }
                let (graph, skip, held) = match read.restored {
                    Some(GraphRestore { job, skip, held }) => (Some(job), skip, held),
                    None => (None, HashSet::new(), Vec::new()),
                };
                self.activate_job_articles(job_id, &skip, graph, read.repeated, held);
                self.report_job(job_id);
                self.download_dispatch_wake = true;
            }
            Err(error) => {
                self.fail_job(job_id, format!("job articles could not be read: {error}"));
            }
        }
    }

    // The one place a job's articles are built: its assembly and queues from
    // the article lists its spec now holds, less the articles in `skip`
    // already in hand and the parts that arrived and wait to be placed, its
    // graph root (restored from a checkpoint, or admitted afresh), the held
    // parts its checkpoint names, and its repeated-article index. Everything
    // built is charged, never refused, and held until the job pauses or
    // leaves the pipeline.
    pub(crate) fn activate_job_articles(
        &mut self,
        job_id: JobId,
        skip: &HashSet<SegmentId>,
        graph: Option<crate::pipeline::graph::job::GraphJob>,
        repeated: Option<Arc<RepeatedArticles>>,
        held_parts: Vec<HeldPart>,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let mut held = Self::held_parts(&self.uu_files, &self.unanchored_parked, job_id);
        held.extend(held_parts.iter().map(HeldPart::segment_id));
        let (assembly, download_queue) = Self::build_job_assembly(job_id, &state.spec, skip, &held);
        state.articles = JobArticles::Active(Box::new(ActiveArticles {
            assembly,
            download_queue,
            recovery_queue: DownloadQueue::new(),
        }));
        if let Some(cache) = repeated {
            self.repeated_articles.insert(job_id, cache);
        }
        self.charge_job_article_memory(job_id);
        match graph {
            Some(graph) => self.install_graph_job(job_id, graph, held_parts),
            None => self.admit_graph_job(job_id),
        }
    }

    // The one place a job's articles end, as a paused job parks or a job
    // leaves the pipeline: its article lists, assembly and queues, its graph
    // runtime, its repeated-article index, the retries kept for its
    // articles, and their charge. A job that stays holds its summaries again,
    // counting as still to fetch what its graph root has not settled and no
    // park holds, as its next activation would, and a read of its articles
    // still out is disowned. The caller removes a job that leaves.
    pub(crate) fn release_job_articles(&mut self, job_id: JobId) {
        let graph = self.graph_jobs.get(&job_id);
        let held = Self::held_parts(&self.uu_files, &self.unanchored_parked, job_id);
        let summaries = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| match &mut state.articles {
                JobArticles::Queued(_) => None,
                JobArticles::Activating(summaries, _) => Some(std::mem::take(summaries)),
                JobArticles::Active(_) => {
                    Some(state.spec.drop_article_lists_with_in_hand(|file, ordinal| {
                        graph.is_some_and(|graph| {
                            graph.article_settled(weaver_graph::VolumeIndex(file), ordinal)
                        }) || held.contains(&SegmentId {
                            file_id: NzbFileId {
                                job_id,
                                file_index: file,
                            },
                            segment_number: ordinal,
                        })
                    }))
                }
            });
        self.job_article_memory.remove(&job_id);
        self.repeated_articles.remove(&job_id);
        self.retire_graph_job(job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.transport_failure_streaks
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.cancel_infrastructure_retries_for_job(job_id);
        self.decode_retries
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        if let (Some(summaries), Some(state)) = (summaries, self.jobs.get_mut(&job_id)) {
            state.articles = JobArticles::Queued(summaries);
        }
    }

    // A paused job holds nothing per article, once nothing of it is out: what
    // a connection or the decoder still has for it comes back and is taken
    // as a running job takes it, so no article that arrives is fetched
    // again. Every part that arrived and waits to be placed is kept, so no
    // delivered article is fetched again: those in memory spill to the job's
    // spool first, on the next turn, so the checkpoint names them. Then its
    // checkpoint is taken, so it holds every article in hand, and its
    // articles are released, with no terminal state kept per article: its
    // graph root records the articles given up. With nothing out, no
    // article of it is in flight for the checkpoint. A job resumed before
    // that keeps its articles, and runs on with them.
    pub(crate) fn park_job_articles(&mut self, job_id: JobId) {
        self.parking_jobs.insert(job_id);
        self.park_drained_jobs();
    }

    // Park every paused job with nothing left out, once its held parts are
    // spooled. Each turn asks.
    pub(crate) fn park_drained_jobs(&mut self) {
        if self.parking_jobs.is_empty() {
            return;
        }
        let parking: Vec<JobId> = self.parking_jobs.iter().copied().collect();
        for job_id in parking {
            let paused = self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.run_state == crate::jobs::model::RunState::Paused);
            if paused && self.job_has_results_out(job_id) {
                continue;
            }
            self.parking_jobs.remove(&job_id);
            if !paused {
                continue;
            }
            if self.job_holds_parts_in_memory(job_id) {
                self.spilling_jobs.insert(job_id);
            } else {
                self.park_drained_job(job_id);
            }
        }
    }

    // The spill turn has spooled what of the job's held parts the spool
    // took: the job parks, unless something of it came out meanwhile, when
    // it waits to be drained again. A part the spool could not take stays in
    // memory, kept across the park and left out of the checkpoint.
    pub(crate) fn park_spilled_job(&mut self, job_id: JobId) {
        if self.job_has_results_out(job_id) {
            self.parking_jobs.insert(job_id);
            return;
        }
        self.park_drained_job(job_id);
    }

    fn park_drained_job(&mut self, job_id: JobId) {
        debug_assert!(
            !self.checkpoint_progress_articles.contains_key(&job_id),
            "a drained job has no article in flight for its checkpoint"
        );
        self.checkpoint_graph_job(job_id);
        self.release_job_articles(job_id);
        self.segment_terminal_states
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.report_job(job_id);
    }

    // Whether a part of the job that arrived and waits to be placed is held
    // only in memory.
    fn job_holds_parts_in_memory(&self, job_id: JobId) -> bool {
        let uu = self
            .uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .any(|(_, uu)| uu.parked.values().any(|bytes| !bytes.is_spilled()));
        uu || self
            .unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .any(|(_, parked)| parked.values().any(|part| !part.bytes.is_spilled()))
    }

    // The parts of a job that arrived and wait to be placed: uuencode parts
    // ahead of their file's cursor, and articles that declared no usable
    // start ahead of the ordinal they follow.
    fn held_parts(
        uu_files: &HashMap<NzbFileId, UuFileAssembly>,
        unanchored_parked: &HashMap<NzbFileId, BTreeMap<u32, UnanchoredPart>>,
        job_id: JobId,
    ) -> HashSet<SegmentId> {
        let uu = uu_files
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, uu)| uu.parked.keys().map(move |ordinal| (*file_id, *ordinal)));
        let unanchored = unanchored_parked
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .flat_map(|(file_id, parked)| parked.keys().map(move |ordinal| (*file_id, *ordinal)));
        uu.chain(unanchored)
            .map(|(file_id, segment_number)| SegmentId {
                file_id,
                segment_number,
            })
            .collect()
    }

    // Whether anything of the job is still out: an article a lane holds, a
    // result a lane released and the pipeline has yet to take, or a decode
    // queued or running.
    fn job_has_results_out(&self, job_id: JobId) -> bool {
        let counted = |counts: &HashMap<JobId, usize>| counts.get(&job_id).is_some_and(|n| *n > 0);
        counted(&self.active_downloads_by_job)
            || counted(&self.pending_released_download_results_by_job)
            || counted(&self.active_decodes_by_job)
            || self
                .pending_decode
                .iter()
                .any(|work| work.segment_id.file_id.job_id == job_id)
    }

    // Stop the job's graph runtime. One with nothing out on a thread goes at
    // once; one with a run or a repair out is kept, apart from the graphs
    // that serve jobs, until the last of its mail is heard.
    fn retire_graph_job(&mut self, job_id: JobId) {
        self.forget_graph_repair(job_id);
        let Some(graph) = self.graph_jobs.remove(&job_id) else {
            return;
        };
        graph.request_stop();
        if graph.awaits_mail() {
            self.retiring_graph_jobs.insert(job_id, graph);
        }
    }
}

// Read one job's article lists back from its stored NZB, check they are the
// ones the job was admitted with, and rebuild its graph from its checkpoint
// when it has one.
fn read_job_articles(
    db: &crate::Database,
    job_id: JobId,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    expected: &FileSummaries,
    memory: Arc<ProcessMemoryBudget>,
    seat: RestoreSeat,
) -> Result<ReadArticles, String> {
    let (nzb_path, nzb_zstd) = db
        .load_active_job_persisted_nzb(job_id)
        .map_err(|error| format!("stored NZB could not be loaded: {error}"))?
        .ok_or_else(|| "the job has no stored row".to_string())?;
    let nzb_zstd = nzb_zstd.ok_or_else(|| "the job has no stored NZB".to_string())?;
    let nzb = crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd)
        .map_err(|error| format!("stored NZB could not be parsed: {error}"))?;
    drop(nzb_zstd);
    let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, category, metadata);
    drop(nzb);
    let read: Vec<FileSummary> = spec.files.iter().map(FileSummary::of).collect();
    if read.len() != expected.0.len()
        || !read
            .iter()
            .zip(&expected.0)
            .all(|(read, expected)| read.same_list(expected))
    {
        return Err("stored NZB no longer matches the job's files".to_string());
    }
    let repeated = RepeatedArticles::new(&spec, seat.working_dir(), memory)?;
    let restored = match seat.restore(job_id, root_files(&spec)) {
        Ok(restored) => Some(restored),
        Err(GraphRestoreError::NoCheckpoint(_)) => None,
        Err(error) => return Err(error.to_string()),
    };
    let lists = spec.files.into_iter().map(|file| file.segments).collect();
    Ok(ReadArticles {
        lists,
        repeated,
        restored,
    })
}

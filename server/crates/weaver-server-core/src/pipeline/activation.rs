// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's articles exist only while it runs.
//
// A job waiting its turn holds its files' names and identities and, per file,
// how many articles it has and their size: nothing per article, and nothing
// charged to the process ledger. The first time the scheduler reaches it, the
// job becomes activating and its article lists are read back from the NZB the
// job was admitted with, off the pipeline thread. When they arrive the job's
// assembly, queues, graph root and repeated-article index are built in
// [`Pipeline::activate_job_articles`], the one place they are built, and their
// whole size is charged. They are dropped, and the charge released, in
// [`Pipeline::release_job_articles`] when the job leaves the pipeline.

use super::*;
use crate::jobs::model::{ActiveArticles, FileSummaries, FileSummary, JobArticles, SegmentSpec};
use crate::pipeline::download::queue::DownloadQueue;
use crate::pipeline::download::repeated::RepeatedArticles;

// What the read of one job's stored NZB produced.
pub(crate) struct JobActivated {
    pub(crate) job_id: JobId,
    pub(crate) result: Result<ReadArticles, String>,
}

pub(crate) struct ReadArticles {
    // Every file's article list, in the spec's file order.
    lists: Vec<Vec<SegmentSpec>>,
    repeated: Option<Arc<RepeatedArticles>>,
}

impl Pipeline {
    // Start reading a queued job's articles back. A job already activating
    // or active is left as it is, so the scheduler may ask on every pass.
    pub(in crate::pipeline) fn request_job_activation(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let summaries = match &mut state.articles {
            JobArticles::Queued(summaries) => std::mem::take(summaries),
            JobArticles::Activating(_) | JobArticles::Active(_) => return,
        };
        let category = state.spec.category.clone();
        let metadata = state.spec.metadata.clone();
        let working_dir = state.working_dir.clone();
        let expected = summaries.clone();
        state.articles = JobArticles::Activating(summaries);
        let db = self.db.clone();
        let memory = Arc::clone(&self.process_memory_budget);
        let tx = self.job_activated_tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                read_job_articles(
                    &db,
                    job_id,
                    category,
                    metadata,
                    &expected,
                    &working_dir,
                    memory,
                )
            })
            .await
            .unwrap_or_else(|error| Err(format!("article read worker failed: {error}")));
            let _ = tx.send(JobActivated { job_id, result }).await;
        });
    }

    // The read came back: build the job's articles, or fail the job if they
    // could not be read. A job that left the pipeline meanwhile is gone, and
    // what was read is dropped with this message.
    pub(crate) fn handle_job_activated(&mut self, done: JobActivated) {
        let JobActivated { job_id, result } = done;
        if !self
            .jobs
            .get(&job_id)
            .is_some_and(|state| matches!(state.articles, JobArticles::Activating(_)))
        {
            return;
        }
        match result {
            Ok(read) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    for (file, list) in state.spec.files.iter_mut().zip(read.lists) {
                        file.segments = list;
                    }
                }
                self.activate_job_articles(job_id, &HashSet::new(), None, read.repeated);
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
    // already in hand, its graph root (restored from a checkpoint, or
    // admitted afresh), and its repeated-article index. Everything built is
    // charged, never refused, and held until the job leaves the pipeline.
    pub(crate) fn activate_job_articles(
        &mut self,
        job_id: JobId,
        skip: &HashSet<SegmentId>,
        graph: Option<crate::pipeline::graph::job::GraphJob>,
        repeated: Option<Arc<RepeatedArticles>>,
    ) {
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let (assembly, download_queue) = Self::build_job_assembly(job_id, &state.spec, skip);
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
            Some(graph) => self.install_graph_job(job_id, graph),
            None => self.admit_graph_job(job_id),
        }
    }

    // The one place a job's articles end: the job leaves the pipeline, and
    // its articles, its repeated-article index and their charge go with it.
    pub(crate) fn release_job_articles(&mut self, job_id: JobId) -> Option<JobState> {
        self.job_article_memory.remove(&job_id);
        self.repeated_articles.remove(&job_id);
        self.jobs.remove(&job_id)
    }
}

// Read one job's article lists back from its stored NZB, and check they are
// the ones the job was admitted with.
fn read_job_articles(
    db: &crate::Database,
    job_id: JobId,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    expected: &FileSummaries,
    working_dir: &Path,
    memory: Arc<ProcessMemoryBudget>,
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
    if read != expected.0 {
        return Err("stored NZB no longer matches the job's files".to_string());
    }
    let repeated = RepeatedArticles::new(&spec, working_dir, memory)?;
    let lists = spec.files.into_iter().map(|file| file.segments).collect();
    Ok(ReadArticles { lists, repeated })
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(in crate::pipeline::download) fn status_allows_download_dispatch(
        status: &JobStatus,
    ) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    }

    // Whether the job holds work dispatch can hand out: articles in its
    // download queue, in a phase that dispatches. Recovery a seek round has
    // promoted is moved into the download queue, so it counts; recovery still
    // parked in the recovery queue dispatches nothing until a round promotes
    // it, so it does not. The dispatcher picks its jobs by this, and every
    // stall warning counts by it too.
    pub(in crate::pipeline::download) fn has_dispatchable_work(state: &JobState) -> bool {
        state.articles.has_queued_download_work()
            && Self::status_allows_download_dispatch(&state.status)
    }

    // The articles `has_dispatchable_work` counts, across the jobs holding a
    // slot: only those can dispatch.
    pub(in crate::pipeline::download) fn dispatchable_queue_depth(&self) -> usize {
        self.nzb_admitted
            .iter()
            .filter_map(|job_id| self.jobs.get(job_id))
            .filter(|state| Self::has_dispatchable_work(state))
            .map(|state| state.articles.queued_download_len())
            .sum()
    }

    #[cfg(test)]
    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let uu_capped = !self.uu_files.is_empty() && self.uu_spool_dispatch_capped();
        let checkpoint = self.checkpoint_admission(job_id);
        self.jobs.get(&job_id).is_some_and(|state| {
            if !Self::status_allows_download_dispatch(&state.status) {
                return false;
            }
            // A job whose articles are not built yet has every one to hand out
            // once they are.
            let Some(queue) = state.articles.download_queue() else {
                return true;
            };
            // The checkpoint holds articles one by one, so raw queue occupancy
            // would overstate what this job can hand out right now.
            if checkpoint.enforced {
                return queue
                    .peek_first_matching(|work| {
                        checkpoint.decision(work, &[]).allows()
                            && (!uu_capped
                                || self
                                    .uu_files
                                    .get(&work.segment_id.file_id)
                                    .is_none_or(|uu| {
                                        uu.next_index == work.segment_id.segment_number
                                    }))
                    })
                    .is_some();
            }
            if !uu_capped {
                return !queue.is_empty();
            }
            // A blocked UU head must not hide another encoding deeper in
            // either heap. Reuse per-file counts instead of scanning articles
            // or allocating a cursor map for this availability check.
            let queued_uu: usize = self
                .uu_files
                .keys()
                .filter(|file_id| file_id.job_id == job_id)
                .map(|file_id| queue.queued_count_for_file(*file_id) as usize)
                .sum();
            queue.len() > queued_uu
                || queue
                    .peek_next_matching(|work| {
                        self.uu_files
                            .get(&work.segment_id.file_id)
                            .is_none_or(|uu| uu.next_index == work.segment_id.segment_number)
                    })
                    .is_some()
        })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn job_has_dispatchable_work_for_test(
        &mut self,
        job_id: JobId,
    ) -> bool {
        self.job_has_dispatchable_work(job_id)
    }

    // The gate the dispatcher stops this job at while it holds no lane: no
    // server, byte pressure, a direct-store hold or the restart checkpoint.
    // None while a lane holds the job or none of its queued work is gated, so
    // a job only waiting its turn behind another reads as plain queued.
    pub(in crate::pipeline) fn job_dispatch_hold_reason(
        &self,
        job_id: JobId,
    ) -> Option<&'static str> {
        let state = self.jobs.get(&job_id)?;
        if !Self::has_dispatchable_work(state) || self.job_has_current_download_activity(job_id) {
            return None;
        }
        // In the order the dispatch pass meets them.
        if crate::DownloadPressureState::from_code(
            self.metrics.download_pressure_state.load(Ordering::Relaxed),
        ) == crate::DownloadPressureState::Hard
        {
            return Some("byte_pressure");
        }
        if self.nntp.pool().server_count() == 0 {
            return Some("no_server");
        }
        let checkpoint = self.checkpoint_admission(job_id);
        if !checkpoint.enforced {
            return None;
        }
        let queue = state.articles.download_queue()?;
        // A checkpoint holding every payload article admits only recovery,
        // and the queue counts its recovery articles, so a queue of payload
        // alone is held without reading an article.
        if checkpoint.holds_every_payload_article() && queue.recovery_work_count() == 0 {
            return Some("checkpoint");
        }
        let open = queue.peek_first_matching(|work| {
            #[cfg(test)]
            self.dispatch_hold_items_examined
                .fetch_add(1, Ordering::Relaxed);
            checkpoint.decision(work, &[]).allows()
        });
        if open.is_some() {
            None
        } else {
            Some("checkpoint")
        }
    }

    // The dispatcher's word on a job: the wait it recorded, else the gate
    // holding it.
    pub(in crate::pipeline) fn dispatch_wait_reason(&self, job_id: JobId) -> Option<&'static str> {
        self.download_wait_by_job
            .get(&job_id)
            .map(|wait| wait.reason)
            .or_else(|| self.job_dispatch_hold_reason(job_id))
    }

    // A hold lands or lifts, and a job gains or loses its lanes, with no job
    // event of its own, so each dispatch pass re-reports the jobs whose queue
    // row no longer says what the dispatcher sees. Only a job holding a slot,
    // a recorded wait or download activity can move that way, so the pass
    // looks at those and at the ones the last pass looked at, which catches a
    // job the moment it leaves them. A job without a slot reads its slot
    // wait, which the admission pass restates.
    pub(in crate::pipeline) fn report_changed_dispatch_holds(&mut self) {
        let mut watched: Vec<JobId> = self
            .nzb_admitted
            .iter()
            .chain(self.download_wait_by_job.keys())
            .chain(self.active_download_connections_by_job.keys())
            .chain(self.active_downloads_by_job.keys())
            .chain(self.active_decodes_by_job.jobs())
            .chain(self.pending_released_download_results_by_job.keys())
            .copied()
            .collect();
        watched.sort_unstable();
        watched.dedup();
        let previous = std::mem::take(&mut self.dispatch_hold_watch);
        let mut ids = watched.clone();
        ids.extend(
            previous
                .into_iter()
                .filter(|id| watched.binary_search(id).is_err()),
        );
        self.dispatch_hold_watch = watched;
        let stale: Vec<JobId> = self.shared_state.queue.read(|inner| {
            ids.into_iter()
                .filter(|job_id| {
                    self.jobs.contains_key(job_id)
                        && inner.job(*job_id).is_some_and(|row| {
                            row.report.job.download_wait_reason.as_deref()
                                != self.stored_wait_reason(row)
                                || row.report.download_active
                                    != self.job_has_current_download_activity(*job_id)
                        })
                })
                .collect()
        });
        for job_id in stale {
            self.report_job(job_id);
        }
    }

    pub(in crate::pipeline::download) fn job_dispatch_priority(state: &JobState) -> u8 {
        crate::pipeline::queue::order::metadata_priority(&state.spec.metadata)
    }
}

#[cfg(test)]
mod dispatch_hold_reason_tests {
    use super::*;
    use crate::pipeline::tests::new_direct_pipeline;
    use crate::pipeline::tests::{insert_active_job, segmented_job_spec};

    const JOB: JobId = JobId(42010);
    const ARTICLES: usize = 2_000;

    // The hold reason read by walking the queue for an article the
    // checkpoint admits, counting each article read.
    fn hold_reason_by_scan(
        pipeline: &Pipeline,
        job_id: JobId,
        examined: &mut usize,
    ) -> Option<&'static str> {
        let state = pipeline.jobs.get(&job_id)?;
        if !Pipeline::has_dispatchable_work(state)
            || pipeline.job_has_current_download_activity(job_id)
        {
            return None;
        }
        if crate::DownloadPressureState::from_code(
            pipeline
                .metrics
                .download_pressure_state
                .load(Ordering::Relaxed),
        ) == crate::DownloadPressureState::Hard
        {
            return Some("byte_pressure");
        }
        if pipeline.nntp.pool().server_count() == 0 {
            return Some("no_server");
        }
        let checkpoint = pipeline.checkpoint_admission(job_id);
        if !checkpoint.enforced {
            return None;
        }
        let open = state
            .articles
            .download_queue()?
            .peek_first_matching(|work| {
                *examined += 1;
                checkpoint.decision(work, &[]).allows()
            });
        if open.is_some() {
            None
        } else {
            Some("checkpoint")
        }
    }

    fn client(servers: usize) -> weaver_nntp::client::NntpClient {
        weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
            servers: (0..servers)
                .map(|idx| weaver_nntp::pool::ServerPoolConfig {
                    server: weaver_nntp::ServerConfig {
                        host: format!("hold-{idx}.example.com"),
                        ..Default::default()
                    },
                    max_connections: 2,
                    ..weaver_nntp::pool::ServerPoolConfig::default()
                })
                .collect(),
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        })
    }

    // A hold reason and the queued articles read to reach it.
    type Read = (Option<&'static str>, usize);

    // The answer, and the articles read for it, from the function and from
    // the scan it replaces.
    fn both(pipeline: &Pipeline) -> (Read, Read) {
        pipeline
            .dispatch_hold_items_examined
            .store(0, Ordering::Relaxed);
        let answer = pipeline.job_dispatch_hold_reason(JOB);
        let read = pipeline
            .dispatch_hold_items_examined
            .load(Ordering::Relaxed);
        let mut scanned = 0;
        let reference = hold_reason_by_scan(pipeline, JOB, &mut scanned);
        ((answer, read), (reference, scanned))
    }

    fn set_floor(pipeline: &mut Pipeline, floor: u64, downloaded: u64) {
        let state = pipeline.jobs.get_mut(&JOB).unwrap();
        state.restored_download_floor_bytes = floor;
        state.downloaded_bytes = downloaded;
    }

    fn hold_progress_article(pipeline: &mut Pipeline) {
        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id: JOB,
                file_index: 0,
            },
            segment_number: 0,
        };
        pipeline
            .checkpoint_progress_articles
            .insert(JOB, (0, segment_id));
    }

    // Requeues the job's last article as recovery promoted into the
    // download queue.
    fn queue_recovery(pipeline: &mut Pipeline) {
        let last = (ARTICLES - 1) as u32;
        let item = pipeline
            .jobs
            .get_mut(&JOB)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop_first_matching(|work| work.segment_id.segment_number == last)
            .unwrap();
        let mut work = pipeline.dispatched(item);
        work.is_recovery = true;
        pipeline
            .jobs
            .get_mut(&JOB)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }

    #[tokio::test]
    async fn the_hold_reason_matches_the_queue_scan_and_reads_no_article_when_payload_is_held() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.nntp = Arc::new(client(1));
        insert_active_job(
            &mut pipeline,
            JOB,
            segmented_job_spec("Dispatch hold", "payload.bin", &vec![512u32; ARTICLES]),
        )
        .await;
        let limit = Pipeline::restart_durable_lead_limit_bytes();
        let queued = |pipeline: &Pipeline| {
            pipeline.jobs[&JOB]
                .articles
                .active()
                .unwrap()
                .download_queue
                .len()
        };

        // No checkpoint: neither reads the queue.
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer, (None, 0));

        // A checkpoint with room admits the head.
        set_floor(&mut pipeline, 1, 0);
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer, (None, 1));

        // Past the lead with nothing to advance the floor: the head is the
        // one article sent to close the gap.
        set_floor(&mut pipeline, 1, limit + 4096);
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer, (None, 1));

        // The gap-closing article out: every payload article is held. The
        // scan reads the whole queue to learn it; the function reads none.
        hold_progress_article(&mut pipeline);
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer.0, reference.0);
        assert_eq!(answer.0, Some("checkpoint"));
        assert_eq!(reference.1, queued(&pipeline));
        assert_eq!(answer.1, 0);

        // Recovery queued behind the held payload is admitted, so the job is
        // not held, and both find it the same way.
        queue_recovery(&mut pipeline);
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer.0, None);
        assert!(answer.1 >= 1);

        // The gates ahead of the checkpoint answer first, unchanged.
        pipeline.metrics.download_pressure_state.store(
            crate::DownloadPressureState::Hard.as_code(),
            Ordering::Relaxed,
        );
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer, (Some("byte_pressure"), 0));
        pipeline.metrics.download_pressure_state.store(
            crate::DownloadPressureState::Clear.as_code(),
            Ordering::Relaxed,
        );
        pipeline.nntp = Arc::new(client(0));
        let (answer, reference) = both(&pipeline);
        assert_eq!(answer, reference);
        assert_eq!(answer, (Some("no_server"), 0));
    }
}

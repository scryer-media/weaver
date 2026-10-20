// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Decoded batches off the pipeline task: a batch is handed to its disk owner
// thread and the task moves on; its commit is applied when the owner's
// answer comes back as a message. Nothing in a decode's landing waits on the
// disk, so a slow volume never holds the task that feeds the UI.
//
// A file's tickets land in the order they were submitted, which is the order
// the owner writes them, so a landing sees exactly what the inline write saw.

use super::worker::{SegmentHashMode, SegmentWriteError};
use super::*;
use crate::pipeline::{
    SegmentWriteDone, SegmentWriteKind, SegmentWriteOutcome, SegmentWriteTicket,
};

fn unanswered(message: &str) -> SegmentWriteOutcome {
    Err(SegmentWriteBatchError {
        source: std::io::Error::other(message.to_string()),
        written: Vec::new(),
        unwritten: Vec::new(),
    })
}

impl Pipeline {
    // Whether any batch of this job is still out to the disk owner or
    // waiting for its landing.
    pub(crate) fn has_pending_segment_writes(&self, job_id: JobId) -> bool {
        self.segment_writes
            .iter()
            .any(|(file_id, tickets)| file_id.job_id == job_id && !tickets.is_empty())
    }

    // Hands a prepared batch to the disk owner and keeps the ticket its
    // landing will apply. The bytes stay counted as write backlog until
    // then: they are still in memory.
    pub(super) fn submit_segment_write(
        &mut self,
        file_id: NzbFileId,
        filename: String,
        file_path: std::path::PathBuf,
        segments: Vec<(u64, BufferedDecodedSegment)>,
        kind: SegmentWriteKind,
        context: &'static str,
    ) {
        let id = self.next_segment_write_id;
        self.next_segment_write_id = self.next_segment_write_id.wrapping_add(1);
        let bytes = segments
            .iter()
            .map(|(_, segment)| segment.len_bytes())
            .sum::<usize>();
        let count = segments.len();
        let clean_ranges = segments
            .iter()
            .filter(|(_, segment)| segment.damaged_source.is_none())
            .map(|(offset, segment)| (*offset, offset.saturating_add(segment.len_bytes() as u64)))
            .collect();
        let started = Instant::now();
        let answer = submit_segments_to_disk(&file_path, segments);
        let (outcome_tx, outcome) = oneshot::channel();
        let done_tx = self.segment_write_done_tx.clone();
        // The answer first, so a settle never waits on the done channel,
        // which only the pipeline task drains. A closed channel is the
        // pipeline gone; the ticket goes with it.
        tokio::spawn(async move {
            let outcome = answer
                .await
                .unwrap_or_else(|_| unanswered("the disk write owner did not answer"));
            crate::runtime::perf_probe::record("download.disk_write.await", started.elapsed());
            let _ = outcome_tx.send(outcome);
            let _ = done_tx
                .send(SegmentWriteDone {
                    file_id,
                    ticket: id,
                })
                .await;
        });
        self.write_inflight_bytes = self.write_inflight_bytes.saturating_add(bytes);
        self.segment_writes
            .entry(file_id)
            .or_default()
            .push_back(SegmentWriteTicket {
                id,
                filename,
                file_path,
                kind,
                context,
                bytes,
                count,
                clean_ranges,
                started,
                outcome,
                answered: false,
            });
    }

    // The owner answered a batch: land it, and every answered batch of the
    // file behind it, in submission order.
    pub(in crate::pipeline) async fn handle_segment_write_done(&mut self, done: SegmentWriteDone) {
        let Some(ticket) = self
            .segment_writes
            .get_mut(&done.file_id)
            .and_then(|tickets| tickets.iter_mut().find(|ticket| ticket.id == done.ticket))
        else {
            // Its job let it go first (a removal, a settle), and accounted
            // for it then.
            debug!(
                file_id = %done.file_id,
                ticket = done.ticket,
                "a decoded batch landed after its file let it go"
            );
            return;
        };
        ticket.answered = true;
        loop {
            let Some(ticket) = self
                .segment_writes
                .get_mut(&done.file_id)
                .and_then(|tickets| match tickets.front() {
                    Some(front) if front.answered => tickets.pop_front(),
                    _ => None,
                })
            else {
                break;
            };
            self.land_segment_write(done.file_id, ticket).await;
        }
        self.after_segment_writes_landed(done.file_id.job_id).await;
    }

    // Applies one batch's answer: the commit of every written segment, then
    // what the kind owes, or the job's failure.
    async fn land_segment_write(&mut self, file_id: NzbFileId, ticket: SegmentWriteTicket) {
        // Sent before the done message; this does not wait.
        let outcome = ticket
            .outcome
            .await
            .unwrap_or_else(|_| unanswered("the disk write task did not complete"));
        self.write_inflight_bytes = self.write_inflight_bytes.saturating_sub(ticket.bytes);
        self.release_write_buffered(ticket.bytes, ticket.count);
        // One clock read serves the gauge and the histogram, so they agree.
        let elapsed = ticket.started.elapsed();
        self.metrics
            .disk_write_latency_us
            .store(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.metrics.pipeline_histograms.observe_disk_write(elapsed);
        let (written, write_error) = match outcome {
            Ok(written) => (written, None),
            Err(error) => {
                drop(error.unwritten);
                (error.written, Some(error.source))
            }
        };
        let hash_mode = match ticket.kind {
            SegmentWriteKind::Ready { .. } => SegmentHashMode::UpdateNow,
            SegmentWriteKind::OutOfOrder => SegmentHashMode::DeferRange,
        };
        for (offset, segment) in written {
            crate::e2e_failpoint::maybe_trip("download.after_disk_write_before_commit");
            if matches!(ticket.kind, SegmentWriteKind::OutOfOrder) {
                self.metrics
                    .direct_write_evictions
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.commit_persisted_segment(
                offset,
                segment,
                &ticket.filename,
                &ticket.file_path,
                hash_mode,
            )
            .await;
        }
        if let Some(source) = write_error {
            self.fail_job_for_disk_write(SegmentWriteError::new(file_id, source), ticket.context);
            return;
        }
        // Bytes kept for repair are settled by the booking their retries
        // end in, which the commits above made; the parts held behind them
        // may now be laid.
        self.release_settled_unanchored_runs().await;
        if let SegmentWriteKind::Ready { contiguous_end } = ticket.kind {
            self.note_file_progress_floor(file_id, contiguous_end, false);
            // Bytes `0..contiguous_end` are on disk, so a direct-unpack chase
            // may read them.
            self.direct_unpack_note_commit(file_id, &ticket.filename, contiguous_end, false);
        }
        self.remove_empty_write_buffer(file_id);
    }

    // What a job's last landing owes: the download pass it was holding open,
    // the completion check that deferred to it, and the pressure latch its
    // bytes helped raise.
    async fn after_segment_writes_landed(&mut self, job_id: JobId) {
        self.segment_writes.retain(|_, tickets| !tickets.is_empty());
        if !self.has_pending_segment_writes(job_id) {
            if self.jobs.contains_key(&job_id) && self.job_decode_stage_drained(job_id) {
                self.maybe_finish_download_pass(job_id);
            }
            self.schedule_job_completion_check_if_download_pipeline_drained(
                job_id,
                "segment_writes_landed",
            );
        }
        self.relieve_latched_write_backlog().await;
    }

    // Lands every batch of the job's files now, waiting on the owner's
    // answers. For anything that must not run beside the files' writes: a
    // demotion taking a file's image, the shutdown drain. The last landing's
    // latch relief can send the job's files more, so it runs until none is
    // out.
    pub(crate) async fn settle_segment_writes_for_job(&mut self, job_id: JobId) {
        loop {
            while let Some((file_id, ticket)) = self
                .segment_writes
                .iter_mut()
                .find(|(file_id, tickets)| file_id.job_id == job_id && !tickets.is_empty())
                .and_then(|(file_id, tickets)| Some((*file_id, tickets.pop_front()?)))
            {
                self.land_segment_write(file_id, ticket).await;
            }
            self.after_segment_writes_landed(job_id).await;
            if !self.has_pending_segment_writes(job_id) {
                break;
            }
        }
    }

    // [`Self::settle_segment_writes_for_job`] for every job with a batch
    // out. A landing can send more (the latch relief), so it runs until
    // nothing is out.
    pub(crate) async fn settle_all_segment_writes(&mut self) {
        loop {
            let jobs: HashSet<JobId> = self
                .segment_writes
                .iter()
                .filter(|(_, tickets)| !tickets.is_empty())
                .map(|(file_id, _)| file_id.job_id)
                .collect();
            if jobs.is_empty() {
                break;
            }
            for job_id in jobs {
                self.settle_segment_writes_for_job(job_id).await;
            }
        }
        // The done messages of what landed here have nothing left to apply.
        while self.segment_write_done_rx.try_recv().is_ok() {}
    }

    // Forgets a removed job's batches. Their writes run to the end on their
    // own, and a close of the job's roots queues behind them.
    pub(crate) fn drop_segment_writes_for_job(&mut self, job_id: JobId) {
        let mut bytes = 0usize;
        let mut count = 0usize;
        self.segment_writes.retain(|file_id, tickets| {
            if file_id.job_id != job_id {
                return true;
            }
            for ticket in tickets.drain(..) {
                bytes += ticket.bytes;
                count += ticket.count;
            }
            false
        });
        self.write_inflight_bytes = self.write_inflight_bytes.saturating_sub(bytes);
        self.release_write_buffered(bytes, count);
    }
}

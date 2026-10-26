// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::queue::Change;

pub(crate) const NZB_SLOT_WAIT_REASON: &str = "nzb_slot";

// One queue row as the admission pass sees it, copied out under the read lock.
struct AdmissionRow {
    id: JobId,
    position: usize,
    priority: u8,
    paused: bool,
    // The row still carries an intake, a restore or a resume for the actor.
    pending: bool,
    cancelled_or_retiring: bool,
    // The row carries a fresh intake the actor has not installed yet.
    intake: bool,
}

impl Pipeline {
    // Ownership survives empty queues, retries and all downstream processing.
    // Intake remains in the shared queue until its durable admission commits.
    pub(crate) async fn reconcile_nzb_admission(&mut self) {
        // A crash between accepting pause and draining work must not turn
        // the interrupted pause into an automatic resume.
        self.shared_state.queue.write(|inner| {
            let mut changed = false;
            for row in &mut inner.jobs {
                if row.intents.restore.is_some() && self.nzb_pause_releases.contains(&row.id) {
                    row.intents.paused = Some(true);
                    row.intents.seq = row.intents.seq.wrapping_add(1);
                    row.intents.pause_seq = row.intents.seq;
                    changed = true;
                }
            }
            if changed {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
        let (stopped, rows) = self.shared_state.queue.read(|inner| {
            (
                inner.globals.paused
                    || inner.globals.scheduled_pause
                    || inner.globals.speed_test_paused
                    || inner.globals.shutting_down
                    || inner.globals.schedule_replay_paused,
                inner
                    .jobs
                    .iter()
                    .enumerate()
                    .map(|(position, row)| {
                        let paused = row.intents.paused.unwrap_or_else(|| {
                            row.admission
                                .as_ref()
                                .is_some_and(|job| job.options.initially_paused)
                                || row
                                    .intents
                                    .restore
                                    .as_ref()
                                    .is_some_and(|job| job.status == JobStatus::Paused)
                                || row.report.job.run_state == crate::RunState::Paused
                        });
                        let pending = row.admission.is_some()
                            || row.intents.restore.is_some()
                            || row.intents.paused == Some(false);
                        let info = row.info(&inner.globals);
                        let priority = info
                            .metadata
                            .iter()
                            .find(|(key, _)| key.eq_ignore_ascii_case("priority"))
                            .map_or(1, |(_, value)| {
                                if value.eq_ignore_ascii_case("high") {
                                    0
                                } else if value.eq_ignore_ascii_case("low") {
                                    2
                                } else {
                                    1
                                }
                            });
                        AdmissionRow {
                            id: row.id,
                            position,
                            priority,
                            paused,
                            pending,
                            cancelled_or_retiring: row.intents.cancelled.is_some()
                                || row.report.retiring,
                            intake: row.admission.is_some(),
                        }
                    })
                    .collect::<Vec<_>>(),
            )
        });
        self.nzb_seen.extend(rows.iter().map(|row| row.id));
        let releases: Vec<_> = self
            .nzb_admitted
            .iter()
            .copied()
            .filter(|id| {
                let gone = self.nzb_seen.contains(id) && !rows.iter().any(|row| row.id == *id);
                // History retry/reprocess can reuse an ID before this actor
                // observes the old row's removal. The new run must compete
                // for a slot and commit its own ownership flag.
                let replaced = !self.jobs.contains_key(id)
                    && rows.iter().any(|row| row.id == *id && row.intake);
                let paused = self.nzb_pause_releases.contains(id)
                    && rows.iter().any(|row| row.id == *id && row.paused);
                (gone || replaced || paused) && !self.nzb_work_in_flight(*id)
            })
            .collect();
        for id in releases {
            // A replacement's old record has already been durably archived.
            // Its new record starts unowned and must acquire independently.
            let replaced =
                !self.jobs.contains_key(&id) && rows.iter().any(|row| row.id == id && row.intake);
            if !replaced
                && let Err(error) = self
                    .db_blocking(move |db| db.set_nzb_admitted(id, false))
                    .await
            {
                error!(job_id = id.0, %error, "failed to release NZB admission");
                continue;
            }
            self.nzb_admitted.remove(&id);
            self.nzb_pause_releases.remove(&id);
        }
        let retired: Vec<_> = self
            .nzb_seen
            .iter()
            .copied()
            .filter(|id| {
                !self.nzb_admitted.contains(id)
                    && !rows.iter().any(|row| row.id == *id)
                    && !self.nzb_work_in_flight(*id)
            })
            .collect();
        for id in retired {
            self.nzb_seen.remove(&id);
            self.nzb_pause_releases.remove(&id);
            self.nzb_added_scripts_pending.remove(&id);
            self.nzb_reprocess_pending.remove(&id);
        }
        // A history retry can replace a canceled, unadmitted paused job
        // before this actor sees the old row disappear.
        for row in &rows {
            if row.intake && !self.jobs.contains_key(&row.id) && !self.nzb_work_in_flight(row.id) {
                self.nzb_added_scripts_pending.remove(&row.id);
                self.nzb_reprocess_pending.remove(&row.id);
            }
        }
        if stopped {
            return;
        }
        let mut candidates: Vec<_> = rows
            .iter()
            .filter(|row| {
                row.pending
                    && !row.paused
                    && !row.cancelled_or_retiring
                    && !self.nzb_admitted.contains(&row.id)
            })
            .map(|row| (row.priority, row.position, row.id))
            .collect();
        candidates.sort_unstable();
        let propagating: HashSet<_> = candidates
            .iter()
            .filter_map(|(_, _, id)| self.propagation_hold_until(*id).map(|_| *id))
            .collect();
        for (_, _, id) in candidates {
            // A settings edit or global hold can arrive while the previous
            // acquisition awaits its durable commit. Re-read before admitting
            // the next job rather than using the pass's earlier snapshot.
            let allowed = self.shared_state.queue.read(|inner| {
                let globals = &inner.globals;
                self.nzb_admitted.len()
                    < globals
                        .max_concurrent_nzbs
                        .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                        .get() as usize
                    && !globals.paused
                    && !globals.scheduled_pause
                    && !globals.speed_test_paused
                    && !globals.shutting_down
                    && !globals.schedule_replay_paused
            });
            if !allowed {
                break;
            }
            if propagating.contains(&id) {
                continue;
            }
            if let Err(error) = self
                .db_blocking(move |db| db.set_nzb_admitted(id, true))
                .await
            {
                error!(job_id = id.0, %error, "failed to acquire NZB admission");
                continue;
            }
            self.nzb_admitted.insert(id);
            if self.nzb_added_scripts_pending.remove(&id) {
                self.raise_added_script_event(id);
            }
            self.download_dispatch_wake = true;
        }
        self.shared_state.queue.write(|inner| {
            let mut changed = false;
            for row in &mut inner.jobs {
                let admitted = self.nzb_admitted.contains(&row.id);
                if row.report.nzb_admitted != admitted {
                    row.report.nzb_admitted = admitted;
                    changed = true;
                }
                if propagating.contains(&row.id) {
                    row.report.job.download_wait_reason = None;
                    row.report.job.download_state = crate::DownloadState::Queued;
                    row.report.propagation_until_epoch_ms = self
                        .propagation_ready_at
                        .get(&row.id)
                        .map(|(_, epoch)| *epoch as f64);
                    changed = true;
                    continue;
                }
                let waiting = !self.nzb_admitted.contains(&row.id)
                    && row.intents.cancelled.is_none()
                    && row.intents.paused != Some(true)
                    && (row
                        .admission
                        .as_ref()
                        .is_some_and(|job| !job.options.initially_paused)
                        || row
                            .intents
                            .restore
                            .as_ref()
                            .is_some_and(|job| job.status != JobStatus::Paused)
                        || row.intents.paused == Some(false));
                if waiting
                    && row.report.job.download_wait_reason.as_deref() != Some(NZB_SLOT_WAIT_REASON)
                {
                    row.report.job.download_wait_reason = Some(NZB_SLOT_WAIT_REASON.into());
                    row.report.job.download_state = crate::DownloadState::Queued;
                    changed = true;
                } else if !waiting
                    && row.report.job.download_wait_reason.as_deref() == Some(NZB_SLOT_WAIT_REASON)
                {
                    row.report.job.download_wait_reason = None;
                    changed = true;
                }
            }
            if changed {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
    }

    fn nzb_work_in_flight(&self, id: JobId) -> bool {
        self.download_lane_owners.values().any(|lane| {
            lane.outstanding
                .keys()
                .any(|segment| segment.file_id.job_id == id)
        }) || self
            .active_decodes_by_job
            .get(&id)
            .is_some_and(|count| *count > 0)
            || self
                .pending_decode
                .iter()
                .any(|work| work.segment_id.file_id.job_id == id)
            || self
                .pending_released_download_results_by_job
                .get(&id)
                .is_some_and(|count| *count > 0)
            || self
                .graph_jobs
                .get(&id)
                .is_some_and(|job| job.awaits_mail())
            || self.inflight_terminal_post_processing.contains(&id)
            || self.added_script_holds.contains(&id)
            || self.inflight_moves.contains(&id)
    }
}

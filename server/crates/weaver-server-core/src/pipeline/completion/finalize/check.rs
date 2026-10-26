// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const PAR2_IGNORE_EXTENSIONS_ENV: &str = "WEAVER_PAR2_IGNORE_EXTENSIONS";
// Metadata that travels with a post rather than being part of it. Damage to
// one of these never fails a job in either reference downloader: one clears its
// "has damaged files" flag for them outright, so ignorable-only damage reports
// as repair-not-needed even when the recovery set said repair was impossible;
// the other's quick check passes such a file both on a checksum mismatch and
// when it is missing entirely. This default is the union of the two lists,
// overridable through the env var above (comma- or semicolon-separated; an
// empty value disables the behaviour).
const DEFAULT_PAR2_IGNORE_EXTENSIONS: [&str; 4] = ["nfo", "sfv", "srr", "nzb"];

// The extension list [`par2_damage_ignorable`] matches against, as read from
// the environment once per verdict.
pub(in crate::pipeline) fn configured_par2_ignore_extensions() -> Vec<String> {
    parse_par2_ignore_extensions(std::env::var(PAR2_IGNORE_EXTENSIONS_ENV).ok().as_deref())
}

// Parse the override list. Unset takes the baked defaults; an explicitly empty
// value turns the behaviour off entirely, which is the only way to get the old
// "every described file must be whole" rule back.
//
// Entries are separated by `,` or `;`, may carry a leading dot, and are
// compared case-insensitively.
fn parse_par2_ignore_extensions(raw: Option<&str>) -> Vec<String> {
    let Some(value) = raw else {
        return DEFAULT_PAR2_IGNORE_EXTENSIONS
            .iter()
            .map(|extension| (*extension).to_string())
            .collect();
    };
    let mut extensions: Vec<String> = value
        .split([',', ';'])
        .map(|entry| {
            entry
                .trim()
                .trim_start_matches('.')
                .trim()
                .to_ascii_lowercase()
        })
        .filter(|entry| !entry.is_empty())
        .collect();
    extensions.sort();
    extensions.dedup();
    extensions
}

// Whether damage to this file is the kind both reference downloaders refuse to
// fail a job over.
//
// The rule is by extension, not by size or by role: an `.nfo`, an `.sfv` and
// their relatives are furniture posted alongside the payload, and shipping one
// with a hole in it is strictly better for the user than refusing the payload
// it describes. Matching is on the file's own extension so a payload that
// merely *mentions* one of these names is unaffected.
pub(in crate::pipeline) fn par2_damage_ignorable(
    filename: &str,
    ignore_extensions: &[String],
) -> bool {
    if ignore_extensions.is_empty() {
        return false;
    }
    let Some(extension) = std::path::Path::new(filename)
        .extension()
        .and_then(|extension| extension.to_str())
    else {
        return false;
    };
    let extension = extension.to_ascii_lowercase();
    ignore_extensions.contains(&extension)
}

impl Pipeline {
    // The ignore-extension list in force for this job.
    //
    // Process-global configuration, read where it is used rather than cached,
    // exactly as the repair memory limit is. The test hook exists so the
    // "override disables it" case can be exercised without mutating a
    // process-global environment other tests are reading concurrently.
    pub(in crate::pipeline) fn par2_ignore_extensions(&self) -> Vec<String> {
        #[cfg(test)]
        if let Some(extensions) = self.par2_ignore_extensions_override.as_ref() {
            return extensions.clone();
        }
        configured_par2_ignore_extensions()
    }

    // Whether a job currently owns live download-stage work. It feeds two
    // things: `list_jobs` showing a job whose download state reads Complete as
    // still Downloading while transfers finish, and the download phase rate
    // in the progress samples, which only counts while transfers are live.
    // Queued articles and delayed retries deliberately do not count: they are
    // the scheduler's backlog, not current transfer activity, and a started
    // job with only backlog still lists as Downloading on its own state.
    pub(crate) fn job_has_current_download_activity(&self, job_id: JobId) -> bool {
        self.active_download_connections_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0
            || self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .active_decodes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .pending_released_download_results_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
            || self
                .pending_decode
                .iter()
                .any(|work| work.segment_id.file_id.job_id == job_id)
    }

    // Everything the download stage still owes this job.
    //
    // A health probe is deliberately *not* on this list. A probe is an
    // estimate sampled alongside the download, never work the job owes: it
    // moves no payload byte and cannot change the outcome of a job whose
    // recovery set already covers the damage. Counting it here made the
    // completion checkpoint — and PAR2 recovery promotion behind it — wait out
    // a probe that had nothing left to say, on every job that lost an article
    // early.
    pub(crate) fn job_has_pending_download_pipeline_work(&self, job_id: JobId) -> bool {
        self.job_has_pending_download_work_beyond_health_probe(job_id)
    }

    // The same question, named for the one caller that has to be explicit
    // about excluding the probe:
    // [`Self::retire_health_probe_if_download_pipeline_drained`] asks whether
    // every segment has reached a terminal state, which is the moment the
    // probe's estimate is moot.
    pub(crate) fn job_has_pending_download_work_beyond_health_probe(&self, job_id: JobId) -> bool {
        let has_queued_work = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| !state.download_queue.is_empty());
        let has_inflight_downloads = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_inflight_decodes = self
            .active_decodes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_delayed_retries = self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_released_download_results = self
            .pending_released_download_results_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id.job_id == job_id);

        has_queued_work
            || has_inflight_downloads
            || has_inflight_decodes
            || has_delayed_retries
            || has_released_download_results
            || has_pending_decode
    }

    // The completion gate: an admitted job that is not retiring and whose
    // post-processing is not paused hands its next step to its graph.
    pub(crate) async fn check_job_completion(&mut self, job_id: JobId) {
        if !self.nzb_admitted.contains(&job_id) {
            return;
        }
        // A retiring job does not advance: its archive's done event either
        // removes it or hands it back live, and a handed-back job is checked
        // again then.
        if self.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .any(|row| row.id == job_id && row.report.retiring)
        }) {
            return;
        }
        if self.shared_state.is_post_processing_paused() {
            self.deferred_post_processing.insert(job_id);
            return;
        }
        crate::pipeline::graph::status::advance(self, job_id).await;
    }
}

#[cfg(test)]
mod tests;

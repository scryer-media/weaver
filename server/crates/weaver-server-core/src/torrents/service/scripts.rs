// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The scripts a torrent's job runs: TORRENT_ADDED, TORRENT_DOWNLOADED and
// TORRENT_DELETED at the points an NZB job raises NZB_ADDED, NZB_DOWNLOADED
// and NZB_DELETED, each with its JOB_ twin, and Weaver's own seeding events.
// qBittorrent starts its external program detached
// (`Application::runExternalProgram`), so the torrent never waits on it:
// here each event is queued and the torrent goes on.

use std::path::PathBuf;

use super::{Live, TorrentService, TorrentView};
use crate::post_processing::events::EventContext;
use crate::post_processing::model::{PipelineOutcome, QueueEvent, vars};
use crate::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

/// Where the scripts a torrent's job runs are told weaver keeps its data,
/// its downloads in progress and its finished ones.
#[derive(Debug, Clone, Default)]
pub struct ScriptDirs {
    pub data: Option<PathBuf>,
    pub intermediate: Option<PathBuf>,
    pub complete: Option<PathBuf>,
}

// The event as a torrent's job raises it. The job's directory is the
// torrent's content path, as `content_path` says it (qBittorrent `%F`): its
// file when it is one file named as the torrent, else its folder. The
// script runs in the folder that holds it, or weaver's complete directory
// before the torrent is placed.
pub(crate) fn torrent_event(
    view: &TorrentView,
    event: QueueEvent,
    dirs: &ScriptDirs,
) -> EventContext {
    let (save, content) = view.paths();
    let content = PathBuf::from(content);
    let mut context = EventContext::from_job(
        &JobExecutionContext {
            job_id: view.job_id.0,
            name: view.name.clone(),
            // A torrent has no NZB file, and a magnet no file at all.
            nzb_filename: String::new(),
            category: view.category.clone(),
            group: None,
            source_url: crate::post_processing::scan::source_url(&view.metadata),
            working_directory: content.clone(),
            final_directory: content,
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts {
                parameters: view.metadata.clone(),
                total_bytes: view.wanted_bytes,
                downloaded_bytes: view.wanted_have_bytes,
                data_dir: dirs.data.clone(),
                intermediate_dir: dirs.intermediate.clone(),
                complete_dir: dirs.complete.clone(),
                ..Default::default()
            },
        },
        event,
    );
    if !save.is_empty() {
        context.cwd = PathBuf::from(save);
    } else if let Some(complete) = &dirs.complete {
        context.cwd = complete.clone();
    }
    let tracker = view
        .current_tracker()
        .map(|tracker| tracker.url.clone())
        .unwrap_or_default();
    for (name, value) in [
        (vars::PROTOCOL, "torrent".to_string()),
        (
            vars::INFOHASH_V1,
            view.infohash_v1.clone().unwrap_or_default(),
        ),
        (
            vars::INFOHASH_V2,
            view.infohash_v2.clone().unwrap_or_default(),
        ),
        (vars::TRACKER, tracker),
    ] {
        context.env.insert(name.into(), value);
    }
    context
}

// Why a torrent is removed: asked for, or its seeding goal's action.
#[derive(Debug, Clone, Copy)]
pub(super) enum Ending {
    Asked,
    Goal,
}

// Why a seed stopped seeding before its goals were met, as its scripts
// hear it in `WEAVER_SEEDING_STOPPED_REASON`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoppedSeeding {
    // It failed, or unpacking refused its output.
    Failed,
    Paused,
    Removed,
}

impl StoppedSeeding {
    #[cfg(test)]
    const ALL: [Self; 3] = [Self::Failed, Self::Paused, Self::Removed];

    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Failed => "failed",
            Self::Paused => "paused",
            Self::Removed => "removed",
        }
    }

    #[cfg(test)]
    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|reason| reason.as_str() == value)
    }
}

impl TorrentService {
    // Queue `event` for the scripts of `torrent`'s job, with `env` beside
    // the job's own variables. Nothing waits for it: a script never holds a
    // torrent's download or its seeding.
    pub(super) fn raise_script_event(
        &self,
        torrent: &Live,
        event: QueueEvent,
        env: &[(&str, &str)],
    ) {
        self.queue_script_event(self.script_event(torrent, event, env));
    }

    // `event` as `torrent`'s job raises it now, to be queued once the job
    // is where the queue finds it; none while no script could run.
    pub(super) fn script_event(
        &self,
        torrent: &Live,
        event: QueueEvent,
        env: &[(&str, &str)],
    ) -> Option<EventContext> {
        if !self.inner.db.queue_scripts_possible() {
            return None;
        }
        let mut view = self.view(&self.settings(), torrent, self.inner.clock.unix_ms());
        // Its trackers as its run announces to them, for the one it goes by.
        self.track(
            &mut view,
            torrent.record.pool_member,
            &torrent.trackers,
            &torrent.announces(),
            &mut std::collections::HashMap::new(),
        );
        let mut context = torrent_event(&view, event, &self.inner.env.script_dirs);
        for (name, value) in env {
            context.env.insert((*name).into(), (*value).into());
        }
        Some(context)
    }

    // SEEDING_STOPPED for `torrent`, ended for `reason` before its goals
    // were met; none for a torrent that does not seed now.
    pub(super) fn seeding_stopped(
        &self,
        torrent: &Live,
        reason: StoppedSeeding,
    ) -> Option<EventContext> {
        if !(torrent.seeding() && torrent.resumed()) {
            return None;
        }
        self.script_event(
            torrent,
            QueueEvent::SeedingStopped,
            &[(vars::SEEDING_STOPPED_REASON, reason.as_str())],
        )
    }

    pub(super) fn queue_script_event(&self, context: Option<EventContext>) {
        if let Some(context) = context {
            drop(self.inner.db.admit_queue_script_event(context, false));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::StoppedSeeding;

    // Each reason reads back as itself, and its text stays what scripts
    // have always been handed.
    #[test]
    fn a_seeding_stop_keeps_the_reason_its_scripts_read() {
        for reason in StoppedSeeding::ALL {
            assert_eq!(StoppedSeeding::parse(reason.as_str()), Some(reason));
        }
        assert_eq!(
            StoppedSeeding::ALL.map(StoppedSeeding::as_str),
            ["failed", "paused", "removed"]
        );
    }
}

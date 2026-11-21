// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The composed database's members each domain's queries reach: one
// `XDatabase` implementation per domain crate.

use std::sync::Mutex;

use crate::persistence::Database;

impl weaver_server_history::history::database::HistoryDatabase for Database {
    fn job_history_cache(&self) -> &Mutex<weaver_server_history::history::cache::JobHistoryCache> {
        &self.job_history_cache
    }

    fn history_row_read(&self) {
        crate::torrents::queries::TorrentsInternalQueries::torrent_read(self);
    }

    fn accept_history_archive(&self, job_ids: &[u64]) -> Result<Vec<u64>, crate::StateError> {
        use crate::operations::ArchiveHistoryAcceptOutcome as Outcome;
        use crate::operations::async_ops::OperationsAsyncOpsQueries;
        let acceptance = self.accept_archive_history(job_ids)?;
        Ok(acceptance
            .results
            .iter()
            .filter(|(_, outcome)| matches!(outcome, Outcome::Accepted | Outcome::AlreadyArchived))
            .map(|(id, _)| *id)
            .collect::<Vec<_>>())
    }

    fn wake_history_delete(&self) {
        self.history_delete_wake.notify_one();
    }

    fn forget_job_script_effects(&self, job_ids: &[u64]) {
        crate::post_processing::effects::PostProcessingEffectsInternalQueries::forget_script_effects_for(self, job_ids);
    }

    fn lock_tag_writer(&self) -> std::sync::MutexGuard<'_, ()> {
        self.tags.lock_writer()
    }

    fn job_history_bundles_deleted(&self, job_ids: &[u64]) {
        crate::post_processing::events::PostProcessingEventsInvalidateQueueScriptAdmissionInternalQueries::notify_script_events_changed(self);
        self.tags.jobs_deleted(job_ids);
    }
}

impl weaver_server_jobs::jobs::database::JobsDatabase for Database {
    type ScriptEffects = crate::post_processing::effects::JobScriptEffects;

    fn lock_job_script_effects_writer(&self) -> std::sync::MutexGuard<'_, ()> {
        crate::post_processing::effects::PostProcessingEffectsInternalQueries::lock_script_effects_writer(self)
    }

    fn cache_job_script_effects(&self, job_id: u64, effects: Self::ScriptEffects) {
        crate::post_processing::effects::PostProcessingEffectsInternalQueries::cache_script_effects(
            self, job_id, effects,
        );
    }

    fn graph_checkpoint_write(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.graph_checkpoint_writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl weaver_server_net_runtime::database::NetDatabase for Database {
    fn proxy_route_read(&self) {
        #[cfg(any(test, feature = "test-support"))]
        self.proxy_route_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl From<Database> for weaver_server_net_runtime::database::NetDb {
    fn from(db: Database) -> Self {
        Self::new(db)
    }
}

impl weaver_server_postproc::post_processing::database::PostprocDatabase for Database {
    fn script_runtime(
        &self,
    ) -> &std::sync::Arc<weaver_server_postproc::post_processing::events::ScriptRuntime> {
        &self.script_runtime
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The archive, delete and prune moved to weaver-server-postproc; the ordered
// writer's archive stays with the composed writer.
pub use weaver_server_postproc::post_processing::job_repository::*;

use crate::StateError;
use crate::history;
use crate::persistence::DatabaseWriterExecutor;
use weaver_server_base::jobs::ids::JobId;
#[cfg(test)]
use weaver_server_jobs::jobs::child_rows::ACTIVE_JOB_CHILD_TABLES;

impl DatabaseWriterExecutor {
    #[allow(dead_code)] // retained for callers without typed pipeline provenance.
    pub(crate) fn archive_job(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
    ) -> Result<Option<history::JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(archive_job_sql(
            datastore,
            job_id,
            history.clone(),
            None,
            None,
        ))
    }

    pub(crate) fn archive_job_with_terminal_cause(
        &self,
        job_id: JobId,
        history: &history::JobHistoryRow,
        typed_terminal_cause: Option<crate::jobs::SemanticTerminalCause>,
        failure_kind: Option<history::HistoryFailureKind>,
    ) -> Result<Option<history::JobHistoryRow>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(archive_job_sql(
            datastore,
            job_id,
            history.clone(),
            typed_terminal_cause,
            failure_kind,
        ))
    }
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the history queries need from the database beyond its SQL: the job
// history cache the composed database keeps, and a hook that counts history
// reads for tests. Implemented by the composed database.

use std::borrow::Borrow;
use std::sync::Mutex;

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database as SqlDatabase;

use super::cache::JobHistoryCache;

pub trait HistoryDatabase: Borrow<SqlDatabase> {
    fn job_history_cache(&self) -> &Mutex<JobHistoryCache>;

    // Called once for each history row read that goes to the database, so
    // tests can count the reads a torrent's restore makes.
    fn history_row_read(&self) {}

    // Accept the archive of the history entries of `job_ids` and return the
    // ids the archive took or had already archived, in the order accepted.
    // The archive is an operation above the history crate.
    fn accept_history_archive(&self, job_ids: &[u64]) -> Result<Vec<u64>, StateError>;

    // Wakes the history delete worker once an accepted operation is queued.
    fn wake_history_delete(&self);
}

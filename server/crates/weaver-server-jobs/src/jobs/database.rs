// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the jobs queries need from the database beyond its SQL: the script
// effects a job edit rewrites, held by the composed database's script
// runtime, and a hook that counts graph checkpoint writes for tests.
// Implemented by the composed database.

use std::borrow::Borrow;
use std::collections::BTreeMap;

use weaver_server_db::persistence::connection::Database as SqlDatabase;

// A job's stored script effects, as far as a job edit changes them: the
// script parameters it carries.
pub trait ScriptEffectsParameters:
    serde::Serialize + serde::de::DeserializeOwned + Send + 'static
{
    fn parameters_mut(&mut self) -> &mut BTreeMap<String, String>;
}

pub trait JobsDatabase: Borrow<SqlDatabase> + 'static {
    // The script effects a job row's `script_job_state` holds.
    type ScriptEffects: ScriptEffectsParameters;

    // Called once for each graph checkpoint save, before it is written.
    fn graph_checkpoint_write(&self) {}

    // Serialises writers of stored script effects.
    fn lock_job_script_effects_writer(&self) -> std::sync::MutexGuard<'_, ()>;

    // Replaces the cached script effects of `job_id` after a write.
    fn cache_job_script_effects(&self, job_id: u64, effects: Self::ScriptEffects);
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the post-processing queries need from the database beyond its SQL:
// the history and job hooks, the tag cache, and the script runtime the
// composed database keeps. Implemented by the composed database.

use weaver_server_history::history::database::HistoryDatabase;
use weaver_server_jobs::jobs::database::JobsDatabase;
use weaver_server_settings::tags::TagStore;

use super::effects::JobScriptEffects;
use super::events::ScriptRuntime;

pub trait PostprocDatabase:
    HistoryDatabase
    + JobsDatabase<ScriptEffects = JobScriptEffects>
    + TagStore
    + Clone
    + Send
    + Sync
    + 'static
{
    fn script_runtime(&self) -> &std::sync::Arc<ScriptRuntime>;
}

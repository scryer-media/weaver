// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 89, upgrade step: an install that already has history keeps all
// of it.
//
// History is pruned by age from 89 on, after 90 days unless the operator
// chooses otherwise. An install upgrading with history in it would lose
// every entry older than that on its first maintenance pass, so it upgrades
// with retention off: the setting is written as 0. An install with no
// history writes nothing and reads the default. A setting already saved is
// kept. A restore of an older bundle runs the same step.
//
// The settings key and the value are spelled out here as they stood at
// schema 89, so later changes to the settings type do not change what this
// step does.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::SqlConn;

pub const HOOK_ID: &str = "seed_history_retention_v89";
// The schema that first prunes history by age. A backup taken below it
// restores history that was never pruned.
pub const SCHEMA_VERSION: i64 = 89;

const SEED: &str = "INSERT INTO settings (key, value)
    SELECT 'history_retention_days', '0'
     WHERE EXISTS (SELECT 1 FROM job_history)
    ON CONFLICT(key) DO NOTHING";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn seed_history_retention(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    conn.execute(SEED, &[]).await?;
    Ok(())
}

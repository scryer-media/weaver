// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 58, upgrade step: a saved script concurrency below 32 is raised
// to 32.
//
// One concurrency setting now bounds every script weaver runs, where it used
// to bound only the scripts a download waits for, with a default of 4 and a
// ceiling of 8. A saved value under the new default is raised to it; a value
// at or above it is kept. A database that never saved the settings already
// reads the new default, so it is left alone. The step runs once, on the
// upgrade to 58: an operator who lowers the value afterwards keeps it.
//
// The settings key, the field and the value are spelled out here as they
// stood at schema 58, so later changes to the settings type do not change
// what this step does.

use serde_json::Value;

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};

pub const HOOK_ID: &str = "raise_script_concurrency_v58";
// The schema that first raises the setting. A backup taken below it can
// carry a value chosen under the old meaning.
pub const SCHEMA_VERSION: i64 = 58;

pub const SETTINGS_KEY: &str = "post_processing.settings.v2";
const FIELD: &str = "concurrency";
pub const RAISED_TO: u64 = 32;

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn raise_script_concurrency(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let Some(row) = conn
        .fetch_all(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text(SETTINGS_KEY.into())],
        )
        .await?
        .into_iter()
        .next()
    else {
        return Ok(());
    };
    let raw = row.text("value")?;
    let Some(raised) = raised(&raw) else {
        return Ok(());
    };
    conn.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[SqlArg::Text(raised), SqlArg::Text(SETTINGS_KEY.into())],
    )
    .await?;
    Ok(())
}

// The settings document with the concurrency raised, or `None` when it is
// already at least 32 or cannot be read.
fn raised(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!(
                "the saved post-processing settings could not be read and their script concurrency was not raised"
            );
            return None;
        }
    };
    let object = document.as_object_mut()?;
    let low = match object.get(FIELD) {
        Some(Value::Number(value)) => value.as_u64().is_none_or(|value| value < RAISED_TO),
        _ => false,
    };
    if !low {
        return None;
    }
    object.insert(FIELD.into(), Value::from(RAISED_TO));
    serde_json::to_string(&document).ok()
}

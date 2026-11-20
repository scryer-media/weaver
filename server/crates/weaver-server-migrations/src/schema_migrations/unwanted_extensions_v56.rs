// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 56, upgrade step: an install that never set an unwanted
// extension list gets the default one.
//
// Earlier builds shipped the list empty, which turned the check off, so an
// executable posting was delivered like any other file. A saved settings
// document whose list is empty or absent is given the default list. A
// database that never saved the settings already reads the defaults, so it
// is left alone. The step runs once, on the upgrade to 56: an operator who
// clears the list afterwards keeps it cleared.
//
// The settings key, the field and the list are spelled out here as they
// stood at schema 56, so later changes to the settings type do not change
// what this step does.

use serde_json::Value;

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};

pub const HOOK_ID: &str = "default_unwanted_extensions_v56";
// The schema that first ships a default list. A backup taken below it can
// carry the empty list earlier builds defaulted to.
pub const SCHEMA_VERSION: i64 = 56;

pub const SETTINGS_KEY: &str = "post_processing.settings.v2";
const FIELD: &str = "unacceptableExtensions";
// Stored sorted, as a saved list is normalized.
pub const DEFAULT_LIST: [&str; 10] = [
    "bat", "cmd", "com", "exe", "js", "lnk", "msi", "ps1", "scr", "vbs",
];

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn fill_default_unwanted_extensions(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
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
    let Some(filled) = filled(&raw) else {
        return Ok(());
    };
    conn.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[SqlArg::Text(filled), SqlArg::Text(SETTINGS_KEY.into())],
    )
    .await?;
    Ok(())
}

// The settings document with the default list, or `None` when it already
// names extensions or cannot be read.
fn filled(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!(
                "the saved post-processing settings could not be read and were not given the default unwanted extensions"
            );
            return None;
        }
    };
    let object = document.as_object_mut()?;
    let empty = match object.get(FIELD) {
        None | Some(Value::Null) => true,
        Some(Value::Array(list)) => list.is_empty(),
        Some(_) => false,
    };
    if !empty {
        return None;
    }
    object.insert(
        FIELD.into(),
        Value::Array(DEFAULT_LIST.iter().map(|ext| Value::from(*ext)).collect()),
    );
    serde_json::to_string(&document).ok()
}

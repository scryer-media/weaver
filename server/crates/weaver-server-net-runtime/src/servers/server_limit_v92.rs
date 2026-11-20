// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 92, upgrade step: a server list an older version stored past the
// server limit is brought down to it, once.
//
// Weaver takes at most 50 servers. One pass deletes servers until 50 remain:
// primary servers (the fill servers of the top priority level) and
// backfill-only servers are kept; of the others, the disabled go first, then
// the enabled, each lowest priority first and newest first inside a level.
// Only when every other server is gone and more than 50 are still stored do
// kept servers go: the first 50 in stored order stay.
//
// Each server goes through the rows a user's delete removes, and is named in
// a warning. A list of 50 or fewer writes nothing.

use std::cmp::Reverse;

use crate::servers::persistence::delete_server_rows;
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::SqlConn;

pub const HOOK_ID: &str = "trim_servers_to_limit_v92";
pub const SCHEMA_VERSION: i64 = 92;

struct Stored {
    id: u32,
    host: String,
    port: i64,
    priority: i64,
    backfill: bool,
    active: bool,
}

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn trim_servers_to_limit(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let stored = conn
        .fetch_all(
            "SELECT id, host, port, priority, backfill, active FROM servers ORDER BY priority, id",
            &[],
        )
        .await?
        .iter()
        .map(|row| {
            Ok(Stored {
                id: u32::try_from(row.i64("id")?)
                    .map_err(|error| StateError::Database(error.to_string()))?,
                host: row.text("host")?,
                port: row.i64("port")?,
                priority: row.i64("priority")?,
                backfill: row.bool("backfill")?,
                active: row.bool("active")?,
            })
        })
        .collect::<Result<Vec<_>, StateError>>()?;
    for server in past_limit(&stored, weaver_nntp::MAX_PROVIDERS) {
        delete_server_rows(conn, server.id).await?;
        tracing::warn!(
            server_id = server.id,
            host = %server.host,
            port = server.port,
            priority = server.priority,
            backfill = server.backfill,
            enabled = server.active,
            "server deleted: the server list held more than {} servers",
            weaver_nntp::MAX_PROVIDERS
        );
    }
    Ok(())
}

// The servers a list in stored order gives up so that `max` remain, first to
// go first.
fn past_limit(stored: &[Stored], max: usize) -> Vec<&Stored> {
    let excess = stored.len().saturating_sub(max);
    if excess == 0 {
        return Vec::new();
    }
    let top = stored
        .iter()
        .filter(|server| !server.backfill)
        .map(|server| server.priority)
        .min();
    let (kept, mut gone): (Vec<&Stored>, Vec<&Stored>) = stored
        .iter()
        .partition(|server| server.backfill || Some(server.priority) == top);
    gone.sort_by_key(|server| (server.active, Reverse(server.priority), Reverse(server.id)));
    gone.truncate(excess);
    let still_over = excess - gone.len();
    gone.extend(&kept[kept.len() - still_over..]);
    gone
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 92, upgrade step: the hosts the torrents already in the database
// announce to become known tracker hosts, as each torrent added from 92 on
// makes its own. The host is read by the one function that reads it for
// storage and for matching, so a host stored here is the host matched later.

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::settings::model::tracker_host;

pub(crate) const HOOK_ID: &str = "fill_tracker_hosts_v92";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub(crate) async fn fill_tracker_hosts(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let rows = conn
        .fetch_all("SELECT DISTINCT url FROM torrent_trackers", &[])
        .await?;
    let mut hosts = std::collections::BTreeSet::new();
    for row in rows {
        hosts.extend(tracker_host(&row.text("url")?));
    }
    for host in hosts {
        conn.execute(
            "INSERT INTO tracker_hosts (host) VALUES ({}) ON CONFLICT (host) DO NOTHING",
            &[SqlArg::Text(host)],
        )
        .await?;
    }
    Ok(())
}

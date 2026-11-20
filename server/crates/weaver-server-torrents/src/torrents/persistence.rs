// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_db::persistence::{
    StateError,
    sql_runtime::{SqlArg, SqlTx},
};
use weaver_server_settings::settings::model::tracker_host;

use super::model::{TrackerEntry, named_by_str};

// A tracker row holds where the tracker sits in the announce list and who
// named it; how announcing to it goes is read from the session. Its URL is
// sealed, so its ciphertext differs each time it is written: the URL's keyed
// digest, `url_key`, is what says two rows of a tier are the same tracker,
// and the second is not kept.
const TRACKER_INSERT: &str =
    "INSERT INTO torrent_trackers (job_id, tier, url, url_key, named_by) VALUES ";
const TRACKER_ROW: &str = "({}, {}, {}, {}, {})";
const TRACKER_CONFLICT: &str = " ON CONFLICT (job_id, tier, url_key) DO NOTHING";
// Every host a torrent has announced to stays known after the torrent is
// gone, so a tracker rule can be picked from them. The URL is not kept: a
// private tracker's carries the operator's passkey.
const TRACKER_HOST_INSERT: &str = "INSERT INTO tracker_hosts (host) VALUES ";
const TRACKER_HOST_ROW: &str = "({})";
const TRACKER_HOST_CONFLICT: &str = " ON CONFLICT (host) DO NOTHING";

// A torrent's tracker rows (`insert_tracker_rows`).
pub async fn insert_trackers(
    tx: &mut SqlTx<'_>,
    job_id: i64,
    trackers: &[TrackerEntry],
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
) -> Result<(), StateError> {
    let rows: Vec<(i64, &TrackerEntry)> =
        trackers.iter().map(|tracker| (job_id, tracker)).collect();
    insert_tracker_rows(tx, &rows, key).await
}

// The one place tracker rows are written, of one torrent or of many, each
// row its torrent's id and the tracker: each row, and the host of each,
// recorded as `tracker_host` spells it.
pub async fn insert_tracker_rows(
    tx: &mut SqlTx<'_>,
    rows: &[(i64, &TrackerEntry)],
    key: Option<&weaver_server_db::persistence::encryption::EncryptionKey>,
) -> Result<(), StateError> {
    // As many rows a statement as its binds allow.
    for chunk in rows.chunks(weaver_server_db::persistence::sql_runtime::max_rows_for_tx(
        tx, 5,
    )) {
        let sql = format!(
            "{TRACKER_INSERT}{}{TRACKER_CONFLICT}",
            vec![TRACKER_ROW; chunk.len()].join(", ")
        );
        let mut args = Vec::with_capacity(chunk.len() * 5);
        for &(job_id, tracker) in chunk {
            args.extend([
                SqlArg::I64(job_id),
                SqlArg::I64(i64::from(tracker.tier)),
                // Sealed: a private tracker's URL carries the passkey.
                SqlArg::Text(super::sealed::seal_url(key, &tracker.url)?),
                SqlArg::Bytes(super::sealed::url_key(key, &tracker.url)?),
                SqlArg::Text(named_by_str(tracker.named_by).to_owned()),
            ]);
        }
        tx.execute(&sql, &args).await?;
    }
    let hosts: Vec<String> = rows
        .iter()
        .filter_map(|(_, tracker)| tracker_host(&tracker.url))
        .collect::<std::collections::BTreeSet<String>>()
        .into_iter()
        .collect();
    for chunk in hosts.chunks(weaver_server_db::persistence::sql_runtime::max_rows_for_tx(
        tx, 1,
    )) {
        let sql = format!(
            "{TRACKER_HOST_INSERT}{}{TRACKER_HOST_CONFLICT}",
            vec![TRACKER_HOST_ROW; chunk.len()].join(", ")
        );
        let args: Vec<SqlArg> = chunk.iter().cloned().map(SqlArg::Text).collect();
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

// One column of each torrent `rows` names, each row its id and the value,
// as `UPDATE ... CASE` statements of as many rows as their binds allow.
// A torrent named twice takes the value named last, as one statement per
// row in turn left it.
pub async fn update_column_tx(
    tx: &mut SqlTx<'_>,
    column: &str,
    rows: &[(i64, SqlArg)],
) -> Result<(), StateError> {
    let mut seen = std::collections::HashSet::with_capacity(rows.len());
    let mut last: Vec<&(i64, SqlArg)> = rows
        .iter()
        .rev()
        .filter(|(id, _)| seen.insert(*id))
        .collect();
    last.reverse();
    // Three binds a row: the CASE arm's id and value, and the id in the
    // list.
    for chunk in last.chunks(weaver_server_db::persistence::sql_runtime::max_rows_for_tx(
        tx, 3,
    )) {
        let sql = format!(
            "UPDATE torrents SET {column} = CASE job_id {}END WHERE job_id IN ({})",
            "WHEN {} THEN {} ".repeat(chunk.len()),
            vec!["{}"; chunk.len()].join(", ")
        );
        let mut args = Vec::with_capacity(chunk.len() * 3);
        for (id, value) in chunk {
            args.push(SqlArg::I64(*id));
            args.push(value.clone());
        }
        args.extend(chunk.iter().map(|(id, _)| SqlArg::I64(*id)));
        tx.execute(&sql, &args).await?;
    }
    Ok(())
}

// The mark a removal leaves on a torrent's row: its files kept, or asked
// to go with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetiringMark {
    Kept,
    Deleted,
}

impl RetiringMark {
    pub const ALL: [Self; 2] = [Self::Kept, Self::Deleted];

    pub const fn of(delete_files: bool) -> Self {
        if delete_files {
            Self::Deleted
        } else {
            Self::Kept
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kept => "kept",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mark| mark.as_str() == value)
    }
}

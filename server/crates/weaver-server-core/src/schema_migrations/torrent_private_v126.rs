// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 126, upgrade step: each torrent already kept takes the privacy
// its metainfo gives it.
//
// A torrent list keeps a privacy in the table, through an index, so 126
// stores it beside the metainfo, which SQL cannot read. This step reads it
// from the metainfo of each torrent written before, as a restore reads it,
// a page of rows at a time by job id: a metainfo can be megabytes, so a page
// is small. A torrent with no metainfo, or one that does not read, stays
// public, as its restore holds it.

use crate::StateError;
use crate::persistence::sql_runtime::{SqlArg, SqlConn};
use crate::torrents::service::metainfo_is_private;

pub(crate) const HOOK_ID: &str = "fill_torrent_private_v126";

// The rows a page reads, each with its metainfo, and the most ids one
// update binds.
const PAGE_ROWS: i64 = 64;

// Run the step on a connection that is already inside the transaction it
// belongs to. Only the private torrents are written: the column starts
// false.
pub(crate) async fn fill_torrent_private(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    loop {
        let rows = conn
            .fetch_all(
                "SELECT job_id, metainfo FROM torrents
                  WHERE job_id > {} AND metainfo IS NOT NULL
                  ORDER BY job_id
                  LIMIT {}",
                &[SqlArg::I64(after), SqlArg::I64(PAGE_ROWS)],
            )
            .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = last.i64("job_id")?;
        let page_len = rows.len();
        let mut private = Vec::new();
        for row in &rows {
            if row
                .opt_bytes("metainfo")?
                .is_some_and(|metainfo| metainfo_is_private(&metainfo))
            {
                private.push(SqlArg::I64(row.i64("job_id")?));
            }
        }
        if !private.is_empty() {
            let ids = vec!["{}"; private.len()].join(", ");
            let mut args = vec![SqlArg::Bool(true)];
            args.extend(private);
            conn.execute(
                &format!("UPDATE torrents SET private = {{}} WHERE job_id IN ({ids})"),
                &args,
            )
            .await?;
        }
        if page_len < PAGE_ROWS as usize {
            return Ok(());
        }
    }
}

// A one-file metainfo, private or not, as bencode spells it.
#[cfg(test)]
pub(crate) fn tiny_metainfo(name: &str, private: bool) -> Vec<u8> {
    let mut info = format!(
        "d6:lengthi3e4:name{}:{name}12:piece lengthi16384e6:pieces20:",
        name.len()
    )
    .into_bytes();
    info.extend_from_slice(&[0xab; 20]);
    if private {
        info.extend_from_slice(b"7:privatei1e");
    }
    info.push(b'e');
    let mut bytes = b"d4:info".to_vec();
    bytes.extend(info);
    bytes.push(b'e');
    bytes
}

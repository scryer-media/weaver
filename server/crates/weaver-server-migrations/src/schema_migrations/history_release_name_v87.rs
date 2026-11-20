// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 87, upgrade step: every history row is given its full release
// name.
//
// The History page sorts and searches by the full release name: the renamed
// title, else the original title, else the stored name. It lives in the
// row's metadata, which SQL cannot order by through an index, so 87 stores
// it in `release_name` and this step fills that column for the rows written
// before it. The rows are read and written a page at a time by job id.

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};
use weaver_server_history::history::attributes::history_release_name;

pub const HOOK_ID: &str = "fill_history_release_names_v87";
// A restore of an older bundle fills this column with the search key 88
// stores, through `history_search_key_v88`.

// Three binds a row in the update, well under either engine's bind limit.
pub const PAGE_ROWS: i64 = 1_000;

// Run the step on a connection that is already inside the transaction it
// belongs to. Rows whose stored name is already right are not written.
pub async fn fill_history_release_names(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let mut after = i64::MIN;
    loop {
        let rows = conn
            .fetch_all(
                "SELECT job_id, name, metadata, release_name
                   FROM job_history
                  WHERE job_id > {}
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

        let mut changed = Vec::new();
        for row in rows {
            let release_name =
                history_release_name(&row.text("name")?, row.opt_text("metadata")?.as_deref());
            if row.text("release_name")? != release_name {
                changed.push((row.i64("job_id")?, release_name));
            }
        }
        if !changed.is_empty() {
            let cases = vec!["WHEN {} THEN {}"; changed.len()].join(" ");
            let ids = vec!["{}"; changed.len()].join(", ");
            let sql = format!(
                "UPDATE job_history SET release_name = CASE job_id {cases} END WHERE job_id IN ({ids})"
            );
            let mut args = Vec::with_capacity(changed.len() * 3);
            for (job_id, release_name) in &changed {
                args.push(SqlArg::I64(*job_id));
                args.push(SqlArg::Text(release_name.clone()));
            }
            args.extend(changed.iter().map(|(job_id, _)| SqlArg::I64(*job_id)));
            conn.execute(&sql, &args).await?;
        }
        if page_len < PAGE_ROWS as usize {
            return Ok(());
        }
    }
}

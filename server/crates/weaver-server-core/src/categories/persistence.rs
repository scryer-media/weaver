// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::categories::CategoryConfig;
use crate::categories::record::{CATEGORY_COLUMNS, CategoryRecord};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::proxies::{Consumer, TorrentScope};

impl Database {
    pub fn insert_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = CategoryRecord::from_config(category).into_args()?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                &format!(
                    "INSERT INTO categories ({CATEGORY_COLUMNS}) VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
                ),
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let datastore = self.datastore();
        // The id binds first in `into_args`, last here.
        let mut args = CategoryRecord::from_config(category).into_args()?;
        args.rotate_left(1);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE categories
                    SET name = {}, dest_dir = {}, aliases = {}, seeding = {}, torrent_route = {}, unpack = {}, upload_cap = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    // Returns whether the category existed. Its torrent route goes with it,
    // so a category given its id later starts on the default route.
    pub fn delete_category(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_category", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "DELETE FROM categories WHERE id = {}",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?;
                    tx.execute(
                        "DELETE FROM proxy_routes WHERE consumer = {}",
                        &[SqlArg::Text(
                            Consumer::Torrent(TorrentScope::Category(id)).key(),
                        )],
                    )
                    .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }
}

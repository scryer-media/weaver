// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::categories::CategoryConfig;
use crate::categories::record::{CATEGORY_COLUMNS, CategoryRecord};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

impl Database {
    pub fn insert_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = CategoryRecord::from_config(category).into_args()?;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                &format!(
                    "INSERT INTO categories ({CATEGORY_COLUMNS}) VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
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
                datastore.read_exec(),
                "UPDATE categories
                    SET name = {}, dest_dir = {}, aliases = {}, seeding = {}, torrent_route = {}, unpack = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_category(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM categories WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

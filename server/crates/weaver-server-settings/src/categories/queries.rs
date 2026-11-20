// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::categories::CategoryConfig;
use crate::categories::record::{CATEGORY_COLUMNS, CategoryRecord};
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database;
use weaver_server_db::persistence::sql_runtime::SqlRuntime;

pub trait CategoriesQueries {
    fn list_categories(&self) -> Result<Vec<CategoryConfig>, StateError>;
    fn next_category_id(&self) -> Result<u32, StateError>;
}

impl CategoriesQueries for Database {
    fn list_categories(&self) -> Result<Vec<CategoryConfig>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("SELECT {CATEGORY_COLUMNS} FROM categories ORDER BY name"),
                &[],
            )
            .await?;

            rows.iter()
                .map(|row| Ok(CategoryRecord::from_row(row)?.into_config()))
                .collect()
        })
    }

    fn next_category_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT MAX(id) AS id FROM categories",
                &[],
            )
            .await?;
            let max = row
                .map(|row| row.opt_i64("id"))
                .transpose()?
                .flatten()
                .unwrap_or(0);
            Ok(max as u32 + 1)
        })
    }
}

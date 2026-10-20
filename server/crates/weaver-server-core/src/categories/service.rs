// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::categories::CategoryConfig;
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;

impl Database {
    pub(crate) fn replace_categories(
        &self,
        categories: &[CategoryConfig],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking({
            let categories = categories.to_vec();
            async move {
                SqlRuntime::run_in_transaction(&datastore, "replace_categories", |tx| {
                    let categories = categories.clone();
                    Box::pin(async move {
                        tx.execute("DELETE FROM categories", &[]).await?;
                        for category in categories {
                            let args =
                                crate::categories::record::CategoryRecord::from_config(&category)
                                    .into_args()?;
                            tx.execute(
                                &format!(
                                    "INSERT INTO categories ({}) VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})",
                                    crate::categories::record::CATEGORY_COLUMNS
                                ),
                                &args,
                            )
                            .await?;
                        }
                        Ok(())
                    })
                })
                .await
            }
        })
    }
}

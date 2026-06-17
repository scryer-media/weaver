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
                            let record = crate::categories::record::CategoryRecord::from_config(&category);
                            tx.execute(
                                "INSERT INTO categories (id, name, dest_dir, aliases) VALUES ({}, {}, {}, {})",
                                &[
                                    crate::persistence::sql_runtime::SqlArg::I64(i64::from(record.id)),
                                    crate::persistence::sql_runtime::SqlArg::Text(record.name),
                                    crate::persistence::sql_runtime::SqlArg::OptText(record.dest_dir),
                                    crate::persistence::sql_runtime::SqlArg::Text(record.aliases),
                                ],
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

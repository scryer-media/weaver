use crate::StateError;
use crate::categories::{CategoryConfig, record::CategoryRecord};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

impl Database {
    pub fn insert_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let datastore = self.datastore();
        let record = CategoryRecord::from_config(category);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO categories (id, name, dest_dir, aliases) VALUES ({}, {}, {}, {})",
                &[
                    SqlArg::I64(i64::from(record.id)),
                    SqlArg::Text(record.name),
                    SqlArg::OptText(record.dest_dir),
                    SqlArg::Text(record.aliases),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let datastore = self.datastore();
        let record = CategoryRecord::from_config(category);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE categories SET name = {}, dest_dir = {}, aliases = {} WHERE id = {}",
                &[
                    SqlArg::Text(record.name),
                    SqlArg::OptText(record.dest_dir),
                    SqlArg::Text(record.aliases),
                    SqlArg::I64(i64::from(record.id)),
                ],
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

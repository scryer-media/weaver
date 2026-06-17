use crate::StateError;
use crate::categories::{CategoryConfig, record::CategoryRecord};
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;

impl Database {
    pub fn list_categories(&self) -> Result<Vec<CategoryConfig>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, name, dest_dir, aliases FROM categories ORDER BY name",
                &[],
            )
            .await?;

            rows.into_iter()
                .map(|row| {
                    Ok(CategoryRecord {
                        id: row.i32("id")? as u32,
                        name: row.text("name")?,
                        dest_dir: row.opt_text("dest_dir")?,
                        aliases: row.text("aliases")?,
                    }
                    .into_config())
                })
                .collect()
        })
    }

    pub fn next_category_id(&self) -> Result<u32, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
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

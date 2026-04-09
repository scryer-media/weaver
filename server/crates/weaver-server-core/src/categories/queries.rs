use crate::StateError;
use crate::categories::{CategoryConfig, record::CategoryRecord};
use crate::persistence::Database;

impl Database {
    pub fn list_categories(&self) -> Result<Vec<CategoryConfig>, StateError> {
        let conn = self.conn();
        let mut stmt = conn
            .prepare_cached("SELECT id, name, dest_dir, aliases FROM categories ORDER BY name")
            .map_err(|e| StateError::Database(e.to_string()))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(CategoryRecord {
                    id: row.get::<_, u32>(0)?,
                    name: row.get(1)?,
                    dest_dir: row.get(2)?,
                    aliases: row.get(3)?,
                })
            })
            .map_err(|e| StateError::Database(e.to_string()))?;

        let mut categories = Vec::new();
        for row in rows {
            categories.push(
                row.map_err(|e| StateError::Database(e.to_string()))?
                    .into_config(),
            );
        }
        Ok(categories)
    }

    pub fn next_category_id(&self) -> Result<u32, StateError> {
        let conn = self.conn();
        let max: Option<u32> = conn
            .query_row("SELECT MAX(id) FROM categories", [], |row| row.get(0))
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(max.unwrap_or(0) + 1)
    }
}

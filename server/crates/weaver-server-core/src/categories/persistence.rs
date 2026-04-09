use crate::StateError;
use crate::categories::{CategoryConfig, record::CategoryRecord};
use crate::persistence::Database;

impl Database {
    pub fn insert_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let conn = self.conn();
        let record = CategoryRecord::from_config(category);
        conn.execute(
            "INSERT INTO categories (id, name, dest_dir, aliases) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![record.id, record.name, record.dest_dir, record.aliases],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn update_category(&self, category: &CategoryConfig) -> Result<(), StateError> {
        let conn = self.conn();
        let record = CategoryRecord::from_config(category);
        conn.execute(
            "UPDATE categories SET name=?2, dest_dir=?3, aliases=?4 WHERE id=?1",
            rusqlite::params![record.id, record.name, record.dest_dir, record.aliases],
        )
        .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(())
    }

    pub fn delete_category(&self, id: u32) -> Result<bool, StateError> {
        let conn = self.conn();
        let changed = conn
            .execute("DELETE FROM categories WHERE id = ?1", [id])
            .map_err(|e| StateError::Database(e.to_string()))?;
        Ok(changed > 0)
    }
}

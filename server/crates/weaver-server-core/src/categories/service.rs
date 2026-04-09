use crate::StateError;
use crate::categories::CategoryConfig;
use crate::persistence::Database;

impl Database {
    pub(crate) fn replace_categories(
        &self,
        categories: &[CategoryConfig],
    ) -> Result<(), StateError> {
        {
            let conn = self.conn();
            conn.execute("DELETE FROM categories", [])
                .map_err(|e| StateError::Database(e.to_string()))?;
        }
        for category in categories {
            self.insert_category(category)?;
        }
        Ok(())
    }
}

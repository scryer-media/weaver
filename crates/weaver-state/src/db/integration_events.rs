use super::Database;
use crate::StateError;

/// A persisted public integration event.
#[derive(Debug, Clone)]
pub struct IntegrationEventRow {
    pub id: i64,
    pub timestamp: i64,
    pub kind: String,
    pub item_id: Option<u64>,
    pub payload_json: String,
}

fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

impl Database {
    /// Batch-insert integration events in a single transaction.
    pub fn insert_integration_events(
        &self,
        events: &[IntegrationEventRow],
    ) -> Result<(), StateError> {
        if events.is_empty() {
            return Ok(());
        }

        let conn = self.conn();
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO integration_events (timestamp, kind, item_id, payload_json)
                     VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(db_err)?;
            for event in events {
                stmt.execute(rusqlite::params![
                    event.timestamp,
                    event.kind,
                    event.item_id.map(|value| value as i64),
                    event.payload_json,
                ])
                .map_err(db_err)?;
            }
        }
        tx.commit().map_err(db_err)?;
        Ok(())
    }

    /// Load persisted integration events after an optional cursor, ordered by
    /// insertion ID ascending.
    pub fn list_integration_events_after(
        &self,
        after_id: Option<i64>,
        item_id: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Vec<IntegrationEventRow>, StateError> {
        let conn = self.conn();

        let mut sql = String::from(
            "SELECT id, timestamp, kind, item_id, payload_json
             FROM integration_events
             WHERE 1=1",
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(after_id) = after_id {
            sql.push_str(&format!(" AND id > ?{}", params.len() + 1));
            params.push(Box::new(after_id));
        }

        if let Some(item_id) = item_id {
            sql.push_str(&format!(" AND item_id = ?{}", params.len() + 1));
            params.push(Box::new(item_id as i64));
        }

        sql.push_str(" ORDER BY id ASC");

        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT ?{}", params.len() + 1));
            params.push(Box::new(limit as i64));
        }

        let mut stmt = conn.prepare(&sql).map_err(db_err)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|value| value.as_ref()).collect();

        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(IntegrationEventRow {
                    id: row.get(0)?,
                    timestamp: row.get(1)?,
                    kind: row.get(2)?,
                    item_id: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    payload_json: row.get(4)?,
                })
            })
            .map_err(db_err)?;

        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(db_err)?);
        }
        Ok(out)
    }

    /// Return the latest persisted integration-event cursor, if any.
    pub fn latest_integration_event_id(&self) -> Result<Option<i64>, StateError> {
        let conn = self.conn();
        conn.query_row("SELECT MAX(id) FROM integration_events", [], |row| {
            row.get(0)
        })
        .map_err(db_err)
    }

    /// Delete all integration events.
    pub fn delete_all_integration_events(&self) -> Result<(), StateError> {
        let conn = self.conn();
        conn.execute("DELETE FROM integration_events", [])
            .map_err(db_err)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_list_events_after_cursor() {
        let db = Database::open_in_memory().unwrap();
        let events = vec![
            IntegrationEventRow {
                id: 0,
                timestamp: 1000,
                kind: "ITEM_CREATED".to_string(),
                item_id: Some(1),
                payload_json: "{\"kind\":\"ITEM_CREATED\"}".to_string(),
            },
            IntegrationEventRow {
                id: 0,
                timestamp: 1001,
                kind: "ITEM_PROGRESS".to_string(),
                item_id: Some(1),
                payload_json: "{\"kind\":\"ITEM_PROGRESS\"}".to_string(),
            },
            IntegrationEventRow {
                id: 0,
                timestamp: 1002,
                kind: "GLOBAL_STATE_CHANGED".to_string(),
                item_id: None,
                payload_json: "{\"kind\":\"GLOBAL_STATE_CHANGED\"}".to_string(),
            },
        ];

        db.insert_integration_events(&events).unwrap();

        let listed = db
            .list_integration_events_after(None, Some(1), None)
            .unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].kind, "ITEM_CREATED");
        assert_eq!(listed[1].kind, "ITEM_PROGRESS");

        let latest = db.latest_integration_event_id().unwrap();
        assert!(latest.is_some());
        let after_latest = db
            .list_integration_events_after(latest, None, None)
            .unwrap();
        assert!(after_latest.is_empty());

        let limited = db
            .list_integration_events_after(None, None, Some(1))
            .unwrap();
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].kind, "ITEM_CREATED");
    }
}

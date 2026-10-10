use crate::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY;
use crate::persistence::encryption::{decrypt_value, encrypt_value};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::{Database, StateError};

impl Database {
    pub(crate) fn validated_archive_password_ciphertexts(&self) -> Result<Vec<String>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(datastore.read_exec(),
                "SELECT metadata FROM active_jobs WHERE metadata LIKE '%validated_archive_password%' UNION ALL SELECT metadata FROM job_history WHERE metadata LIKE '%validated_archive_password%'", &[]).await?;
            let mut values = Vec::new();
            for row in rows {
                values.extend(crate::parse_history_metadata(row.opt_text("metadata")?.as_deref()).into_iter()
                    .filter(|(key, _)| key == VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY).map(|(_, value)| value));
            }
            Ok(values)
        })
    }

    pub(crate) fn seal_validated_archive_password(
        &self,
        password: &str,
    ) -> Result<String, StateError> {
        let key = self
            .encryption_key()
            .ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
        encrypt_value(key, password)
            .map_err(|_| StateError::Conflict("could not encrypt archive password".into()))
    }

    pub fn validated_archive_password(&self, job_id: u64) -> Result<Option<String>, StateError> {
        let datastore = self.datastore();
        let job_id =
            i64::try_from(job_id).map_err(|_| StateError::Conflict("invalid job id".into()))?;
        let sealed = self.run_sql_blocking_read(async move {
            for table in ["active_jobs", "job_history"] {
                if let Some(row) = SqlRuntime::fetch_optional(
                    datastore.read_exec(),
                    &format!("SELECT metadata FROM {table} WHERE job_id = {{}}"),
                    &[SqlArg::I64(job_id)],
                )
                .await?
                {
                    let metadata =
                        crate::parse_history_metadata(row.opt_text("metadata")?.as_deref());
                    return Ok(metadata
                        .into_iter()
                        .find(|(key, _)| key == VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY)
                        .map(|(_, value)| value));
                }
            }
            Ok(None)
        })?;
        sealed
            .map(|sealed| {
                let key = self
                    .encryption_key()
                    .ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
                decrypt_value(key, &sealed)
                    .map_err(|_| StateError::Conflict("could not decrypt archive password".into()))
            })
            .transpose()
    }
}

use chrono::{DateTime, TimeZone, Utc};

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};

/// What the save-time TLS probe learned about one server: the suite it
/// negotiated when weaver offered its CPU-preferred family first, and
/// whether reconnecting with the opposite family first changed that answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerTlsDiagnostics {
    pub server_id: u32,
    /// IANA name of the negotiated suite, e.g. `TLS_AES_128_GCM_SHA256`.
    pub cipher_suite: String,
    /// `Some(true)` when the server follows the client's cipher order, so
    /// the CPU-derived preference decides which suite carries traffic;
    /// `Some(false)` when the server imposes its own order; `None` when the
    /// second probe handshake did not complete.
    pub honors_client_cipher_order: Option<bool>,
    pub probed_at: DateTime<Utc>,
}

impl Database {
    pub fn server_tls_diagnostics(
        &self,
        server_id: u32,
    ) -> Result<Option<ServerTlsDiagnostics>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT server_id, cipher_suite, honors_client_cipher_order,
                        probed_at_epoch_seconds
                   FROM server_tls_diagnostics
                  WHERE server_id = {}",
                &[SqlArg::I64(i64::from(server_id))],
            )
            .await?
            .map(server_tls_diagnostics_from_row)
            .transpose()
        })
    }

    pub fn list_server_tls_diagnostics(&self) -> Result<Vec<ServerTlsDiagnostics>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT server_id, cipher_suite, honors_client_cipher_order,
                        probed_at_epoch_seconds
                   FROM server_tls_diagnostics
                  ORDER BY server_id",
                &[],
            )
            .await?
            .into_iter()
            .map(server_tls_diagnostics_from_row)
            .collect()
        })
    }

    pub fn upsert_server_tls_diagnostics(
        &self,
        diagnostics: &ServerTlsDiagnostics,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::I64(i64::from(diagnostics.server_id)),
            SqlArg::Text(diagnostics.cipher_suite.clone()),
            SqlArg::OptBool(diagnostics.honors_client_cipher_order),
            SqlArg::I64(diagnostics.probed_at.timestamp()),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO server_tls_diagnostics
                    (server_id, cipher_suite, honors_client_cipher_order,
                     probed_at_epoch_seconds)
                 VALUES ({}, {}, {}, {})
                 ON CONFLICT(server_id) DO UPDATE SET
                    cipher_suite = excluded.cipher_suite,
                    honors_client_cipher_order = excluded.honors_client_cipher_order,
                    probed_at_epoch_seconds = excluded.probed_at_epoch_seconds",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_server_tls_diagnostics(&self, server_id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM server_tls_diagnostics WHERE server_id = {}",
                &[SqlArg::I64(i64::from(server_id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

fn server_tls_diagnostics_from_row(row: SqlRow) -> Result<ServerTlsDiagnostics, StateError> {
    let server_id = row.i64("server_id")?;
    let server_id = u32::try_from(server_id)
        .map_err(|_| StateError::Database(format!("server id {server_id} is out of range")))?;
    let probed_at = row.i64("probed_at_epoch_seconds")?;
    let probed_at = Utc
        .timestamp_opt(probed_at, 0)
        .single()
        .ok_or_else(|| StateError::Database(format!("invalid probe timestamp {probed_at}")))?;
    Ok(ServerTlsDiagnostics {
        server_id,
        cipher_suite: row.text("cipher_suite")?,
        honors_client_cipher_order: row.opt_bool("honors_client_cipher_order")?,
        probed_at,
    })
}

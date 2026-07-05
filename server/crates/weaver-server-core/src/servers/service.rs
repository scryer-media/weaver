use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::SqlRuntime;
use crate::servers::ServerConfig;

impl Database {
    pub(crate) fn replace_servers(&self, servers: &[ServerConfig]) -> Result<(), StateError> {
        use crate::persistence::encryption::encrypt_secret_for_write;

        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking({
            let servers = servers.to_vec();
            async move {
                SqlRuntime::run_in_transaction(&datastore, "replace_servers", |tx| {
                    let servers = servers.clone();
                    let encryption_key = encryption_key.clone();
                    Box::pin(async move {
                        tx.execute("DELETE FROM servers", &[]).await?;
                        for server in servers {
                            let record = crate::servers::record::ServerRecord::from_config(&server);
                            let encrypted_password =
                                encrypt_secret_for_write(encryption_key.as_ref(), &record.password)
                                    .map_err(StateError::Database)?;
                            tx.execute(
                                "INSERT INTO servers
                                    (id, host, port, tls, username, password, connections, active, supports_pipelining, priority, backfill, retention_days, tls_ca_cert)
                                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                                &crate::servers::persistence::server_args(record, encrypted_password),
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

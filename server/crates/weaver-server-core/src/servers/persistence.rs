use crate::StateError;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::servers::{ServerConfig, record::ServerRecord};

impl Database {
    pub fn insert_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = maybe_encrypt(self.encryption_key(), &record.password);
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO servers
                    (id, host, port, tls, username, password, connections, active, supports_pipelining, priority, tls_ca_cert)
                 VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})",
                &server_args(record, encrypted_password),
            )
            .await?;
            Ok(())
        })
    }

    pub fn update_server(&self, server: &ServerConfig) -> Result<(), StateError> {
        use crate::persistence::encryption::maybe_encrypt;

        let datastore = self.datastore();
        let record = ServerRecord::from_config(server);
        let encrypted_password = maybe_encrypt(self.encryption_key(), &record.password);
        self.run_sql_blocking(async move {
            let mut args = server_args(record, encrypted_password);
            let id = args.remove(0);
            args.push(id);
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE servers
                    SET host = {}, port = {}, tls = {}, username = {}, password = {},
                        connections = {}, active = {}, supports_pipelining = {}, priority = {}, tls_ca_cert = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_server(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM servers WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

pub(crate) fn server_args(record: ServerRecord, password: Option<String>) -> Vec<SqlArg> {
    vec![
        SqlArg::I64(i64::from(record.id)),
        SqlArg::Text(record.host),
        SqlArg::I64(i64::from(record.port)),
        SqlArg::Bool(record.tls),
        SqlArg::OptText(record.username),
        SqlArg::OptText(password),
        SqlArg::I64(i64::from(record.connections)),
        SqlArg::Bool(record.active),
        SqlArg::Bool(record.supports_pipelining),
        SqlArg::I64(i64::from(record.priority)),
        SqlArg::OptText(record.tls_ca_cert),
    ]
}

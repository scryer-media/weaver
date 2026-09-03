use super::*;
use crate::observability::with_timed_config_read;
use crate::servers::types::ServerDetails;

#[derive(Default)]
pub(crate) struct ServersQuery;

#[Object]
impl ServersQuery {
    /// Return whether any NNTP servers are configured.
    async fn has_configured_servers(&self, ctx: &Context<'_>) -> Result<bool> {
        let config = ctx.data::<SharedConfig>()?;
        Ok(
            with_timed_config_read(config, "servers.query.has_configured_servers", |cfg| {
                !cfg.servers.is_empty()
            })
            .await,
        )
    }

    /// List all configured NNTP servers.
    #[graphql(guard = "AdminGuard")]
    async fn servers(&self, ctx: &Context<'_>) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;
        let policy = ctx.data::<std::sync::Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >>()?;
        let snapshots = policy
            .snapshots()
            .into_iter()
            .map(|snapshot| (snapshot.server_id, snapshot))
            .collect::<std::collections::HashMap<_, _>>();
        let tls_diagnostics = load_tls_diagnostics_by_server(db).await?;
        Ok(
            with_timed_config_read(config, "servers.query.servers", |cfg| {
                cfg.servers
                    .iter()
                    .map(|server| {
                        Server::from_config(server, snapshots.get(&server.id))
                            .with_tls_diagnostics(tls_diagnostics.get(&server.id))
                    })
                    .collect()
            })
            .await,
        )
    }

    /// Load one configured NNTP server for editing.
    #[graphql(guard = "AdminGuard")]
    async fn server(&self, ctx: &Context<'_>, id: u32) -> Result<Option<ServerDetails>> {
        let config = ctx.data::<SharedConfig>()?;
        let policy = ctx.data::<std::sync::Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >>()?;
        let snapshot = policy.snapshot(id);
        let tls_diagnostics = {
            let db = ctx.data::<Database>()?.clone();
            crate::observability::spawn_blocking_db(
                "servers.query.server.tls_diagnostics",
                move || db.server_tls_diagnostics(id),
            )
            .await?
        };
        Ok(
            with_timed_config_read(config, "servers.query.server", |cfg| {
                cfg.servers
                    .iter()
                    .find(|server| server.id == id)
                    .map(|server| {
                        ServerDetails::from_config(server, snapshot.as_ref())
                            .with_tls_diagnostics(tls_diagnostics.as_ref())
                    })
            })
            .await,
        )
    }
}

pub(crate) async fn load_tls_diagnostics_by_server(
    db: &Database,
) -> Result<std::collections::HashMap<u32, weaver_server_core::servers::ServerTlsDiagnostics>> {
    let db = db.clone();
    let rows =
        crate::observability::spawn_blocking_db("servers.query.tls_diagnostics", move || {
            db.list_server_tls_diagnostics()
        })
        .await?;
    Ok(rows
        .into_iter()
        .map(|diagnostics| (diagnostics.server_id, diagnostics))
        .collect())
}

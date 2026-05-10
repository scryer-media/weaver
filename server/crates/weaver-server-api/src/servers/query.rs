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
        Ok(
            with_timed_config_read(config, "servers.query.servers", |cfg| {
                cfg.servers.iter().map(Server::from).collect()
            })
            .await,
        )
    }

    /// Load one configured NNTP server for editing.
    #[graphql(guard = "AdminGuard")]
    async fn server(&self, ctx: &Context<'_>, id: u32) -> Result<Option<ServerDetails>> {
        let config = ctx.data::<SharedConfig>()?;
        Ok(
            with_timed_config_read(config, "servers.query.server", |cfg| {
                cfg.servers
                    .iter()
                    .find(|server| server.id == id)
                    .map(ServerDetails::from)
            })
            .await,
        )
    }
}

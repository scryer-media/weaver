use super::*;
use crate::servers::types::ServerDetails;

#[derive(Default)]
pub(crate) struct ServersQuery;

#[Object]
impl ServersQuery {
    /// Return whether any NNTP servers are configured.
    async fn has_configured_servers(&self, ctx: &Context<'_>) -> Result<bool> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(!cfg.servers.is_empty())
    }

    /// List all configured NNTP servers.
    #[graphql(guard = "AdminGuard")]
    async fn servers(&self, ctx: &Context<'_>) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg.servers.iter().map(Server::from).collect())
    }

    /// Load one configured NNTP server for editing.
    #[graphql(guard = "AdminGuard")]
    async fn server(&self, ctx: &Context<'_>, id: u32) -> Result<Option<ServerDetails>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg
            .servers
            .iter()
            .find(|server| server.id == id)
            .map(ServerDetails::from))
    }
}

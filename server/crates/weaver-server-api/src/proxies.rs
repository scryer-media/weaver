use crate::{auth::AdminGuard, observability::spawn_blocking_db};
use async_graphql::{
    ComplexObject, Context, Enum, InputObject, MaybeUndefined, Object, Result, SimpleObject,
};
use std::{sync::Arc, time::Duration};
use weaver_server_core::{
    Database, SchedulerHandle,
    proxies::{self, Consumer, ProxyProfile, ProxyRuntime, RoutingPolicy},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(name = "ProxyKind")]
pub enum ProxyKind {
    HttpConnect,
    #[graphql(name = "SOCKS5")]
    Socks5,
    Ssh,
    WireGuard,
}
impl From<ProxyKind> for proxies::ProxyKind {
    fn from(v: ProxyKind) -> Self {
        match v {
            ProxyKind::HttpConnect => Self::HttpConnect,
            ProxyKind::Socks5 => Self::Socks5,
            ProxyKind::Ssh => Self::Ssh,
            ProxyKind::WireGuard => Self::WireGuard,
        }
    }
}
impl From<proxies::ProxyKind> for ProxyKind {
    fn from(v: proxies::ProxyKind) -> Self {
        match v {
            proxies::ProxyKind::HttpConnect => Self::HttpConnect,
            proxies::ProxyKind::Socks5 => Self::Socks5,
            proxies::ProxyKind::Ssh => Self::Ssh,
            proxies::ProxyKind::WireGuard => Self::WireGuard,
        }
    }
}

#[derive(Clone, Debug, InputObject)]
pub struct RoutingPolicyInput {
    pub proxy_ids: Vec<u32>,
    pub allow_direct: bool,
}
impl From<RoutingPolicyInput> for RoutingPolicy {
    fn from(v: RoutingPolicyInput) -> Self {
        Self {
            proxy_ids: v.proxy_ids,
            allow_direct: v.allow_direct,
        }
    }
}
#[derive(Clone, Debug, SimpleObject)]
#[graphql(name = "RoutingPolicy")]
pub struct RoutingPolicyGql {
    pub proxy_ids: Vec<u32>,
    pub allow_direct: bool,
}
impl From<RoutingPolicy> for RoutingPolicyGql {
    fn from(v: RoutingPolicy) -> Self {
        Self {
            proxy_ids: v.proxy_ids,
            allow_direct: v.allow_direct,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
pub enum RoutingState {
    Idle,
    Proxy,
    Direct,
    Blocked,
}
#[derive(SimpleObject)]
pub struct RoutingFailure {
    pub proxy_id: u32,
    pub message: String,
}
#[derive(SimpleObject)]
pub struct RoutingStatus {
    pub state: RoutingState,
    pub selected_proxy_id: Option<u32>,
    pub failures: Vec<RoutingFailure>,
}

#[derive(SimpleObject)]
#[graphql(name = "ProxyProfile")]
pub struct ProxyProfileGql {
    pub id: u32,
    pub name: String,
    pub kind: ProxyKind,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub dns_servers: Vec<String>,
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    pub tunnel_public_key: Option<String>,
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    pub timeout_seconds: u32,
    pub host_key_fingerprint: Option<String>,
    pub has_username: bool,
    pub has_password: bool,
    pub has_private_key: bool,
    pub has_passphrase: bool,
    pub has_preshared_key: bool,
}
impl From<ProxyProfile> for ProxyProfileGql {
    fn from(p: ProxyProfile) -> Self {
        let tunnel_public_key = if p.kind == proxies::ProxyKind::WireGuard {
            p.wireguard_spec().ok().map(|s| s.public_key())
        } else {
            None
        };
        Self {
            id: p.id,
            name: p.name,
            kind: p.kind.into(),
            enabled: p.enabled,
            host: p.host,
            port: p.port,
            dns_servers: p.dns_servers.iter().map(ToString::to_string).collect(),
            tunnel_addresses: p.tunnel_addresses,
            peer_public_key: p.peer_public_key,
            tunnel_public_key,
            mtu: p.mtu,
            keepalive_seconds: p.keepalive_seconds,
            timeout_seconds: p.timeout_seconds,
            host_key_fingerprint: p.host_key_fingerprint,
            has_username: p.secrets.username.is_some(),
            has_password: p.secrets.password.is_some(),
            has_private_key: p.secrets.private_key.is_some(),
            has_passphrase: p.secrets.passphrase.is_some(),
            has_preshared_key: p.secrets.preshared_key.is_some(),
        }
    }
}

#[derive(InputObject)]
pub struct ProxyProfileInput {
    pub name: String,
    pub kind: ProxyKind,
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    #[graphql(default)]
    pub dns_servers: Vec<String>,
    #[graphql(default)]
    pub tunnel_addresses: Vec<String>,
    pub peer_public_key: Option<String>,
    #[graphql(default = 1280)]
    pub mtu: u16,
    pub keepalive_seconds: Option<u16>,
    #[graphql(default = 30)]
    pub timeout_seconds: u32,
    pub username: MaybeUndefined<String>,
    pub password: MaybeUndefined<String>,
    pub private_key: MaybeUndefined<String>,
    pub passphrase: MaybeUndefined<String>,
    pub preshared_key: MaybeUndefined<String>,
}
fn secret_update(target: &mut Option<String>, value: MaybeUndefined<String>) {
    match value {
        MaybeUndefined::Undefined => {}
        MaybeUndefined::Null => *target = None,
        MaybeUndefined::Value(value) => *target = (!value.is_empty()).then_some(value),
    }
}
impl ProxyProfileInput {
    fn profile(self, id: u32, existing: Option<&ProxyProfile>) -> Result<ProxyProfile> {
        if existing.is_some_and(|p| p.kind != self.kind.into()) {
            return Err("create a new profile to change proxy type".into());
        }
        let mut secrets = existing.map(|p| p.secrets.clone()).unwrap_or_default();
        secret_update(&mut secrets.username, self.username);
        secret_update(&mut secrets.password, self.password);
        secret_update(&mut secrets.private_key, self.private_key);
        secret_update(&mut secrets.passphrase, self.passphrase);
        secret_update(&mut secrets.preshared_key, self.preshared_key);
        let profile = ProxyProfile {
            id,
            name: self.name.trim().into(),
            kind: self.kind.into(),
            enabled: self.enabled,
            host: self.host.trim().trim_matches(['[', ']']).into(),
            port: self.port,
            dns_servers: self
                .dns_servers
                .iter()
                .map(|s| s.trim().parse())
                .collect::<std::result::Result<_, _>>()
                .map_err(|_| async_graphql::Error::new("DNS servers must be IP addresses"))?,
            tunnel_addresses: self.tunnel_addresses,
            peer_public_key: self.peer_public_key,
            mtu: self.mtu,
            keepalive_seconds: self.keepalive_seconds,
            timeout_seconds: self.timeout_seconds,
            host_key_fingerprint: existing.and_then(|p| p.host_key_fingerprint.clone()),
            revision: existing.map_or(Ok(1), |p| {
                p.revision.checked_add(1).ok_or("proxy revision exhausted")
            })?,
            secrets,
        };
        profile
            .validate()
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(profile)
    }
}

pub(crate) fn runtime(ctx: &Context<'_>) -> Result<Arc<ProxyRuntime>> {
    ctx.data::<SchedulerHandle>()?
        .proxy_runtime()
        .ok_or_else(|| "proxy runtime unavailable".into())
}
pub(crate) fn draft_route(
    ctx: &Context<'_>,
    consumer: Consumer,
    input: Option<RoutingPolicyInput>,
) -> Result<Option<Arc<proxies::ConsumerRoute>>> {
    let handle = ctx.data::<SchedulerHandle>()?;
    let Some(runtime) = handle.proxy_runtime() else {
        if input
            .as_ref()
            .is_none_or(|p| p.proxy_ids.is_empty() && p.allow_direct)
        {
            return Ok(None);
        }
        return Err("proxy runtime unavailable".into());
    };
    let policy = input
        .map(Into::into)
        .unwrap_or_else(|| runtime.policy(consumer));
    runtime
        .validate_policy(consumer, &policy)
        .map_err(async_graphql::Error::new)?;
    runtime
        .draft_route(policy, Duration::from_secs(30))
        .map(Some)
        .map_err(async_graphql::Error::new)
}
pub(crate) fn refresh<'a>(
    ctx: &'a Context<'_>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        let handle = ctx.data::<SchedulerHandle>()?;
        if let Some(runtime) = handle.proxy_runtime() {
            runtime.reload().await.map_err(async_graphql::Error::new)?;
        }
        Ok(())
    })
}
fn refresh_nntp<'a>(
    ctx: &'a Context<'_>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        refresh(ctx).await?;
        let handle = ctx.data::<SchedulerHandle>()?;
        if handle.server_transfer_policy().is_some() {
            weaver_server_core::runtime::rebuild_nntp_from_config(ctx.data()?, handle)
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        }
        Ok(())
    })
}

#[derive(Default)]
pub(crate) struct ProxiesQuery;
#[Object]
impl ProxiesQuery {
    #[graphql(guard = "AdminGuard")]
    async fn proxy_profiles(&self, ctx: &Context<'_>) -> Result<Vec<ProxyProfileGql>> {
        let db = ctx.data::<Database>()?.clone();
        Ok(
            spawn_blocking_db("proxies.list", move || db.list_proxy_profiles())
                .await?
                .into_iter()
                .map(Into::into)
                .collect(),
        )
    }
}

#[derive(SimpleObject)]
pub struct ProxyTestResult {
    pub success: bool,
    pub message: String,
}

#[derive(Default)]
pub(crate) struct ProxiesMutation;
#[Object]
impl ProxiesMutation {
    #[graphql(guard = "AdminGuard")]
    async fn save_proxy_profile(
        &self,
        ctx: &Context<'_>,
        id: Option<u32>,
        input: ProxyProfileInput,
    ) -> Result<ProxyProfileGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let read_db = db.clone();
        let profiles =
            spawn_blocking_db("proxies.load", move || read_db.list_proxy_profiles()).await?;
        let existing = id
            .map(|id| {
                profiles
                    .iter()
                    .find(|p| p.id == id)
                    .ok_or_else(|| async_graphql::Error::new("proxy not found"))
            })
            .transpose()?;
        if id.is_none() && profiles.len() >= 128 {
            return Err("at most 128 proxy profiles are supported".into());
        }
        let id = id.unwrap_or(
            profiles
                .iter()
                .map(|p| p.id)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or("proxy IDs exhausted")?,
        );
        let profile = input.profile(id, existing)?;
        // Removing DNS from an RSS-assigned profile would invalidate its policy.
        if profile.dns_servers.is_empty() {
            let read_db = db.clone();
            let routes =
                spawn_blocking_db("proxies.references", move || read_db.list_proxy_routes())
                    .await?;
            if routes
                .iter()
                .any(|(key, policy)| key.starts_with("rss:") && policy.proxy_ids.contains(&id))
            {
                return Err("RSS proxy routes require configured DNS servers".into());
            }
        }
        let saved = profile.clone();
        let profile =
            spawn_blocking_db("proxies.save", move || db.save_proxy_profile(&saved)).await?;
        refresh_nntp(ctx).await?;
        Ok(profile.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn delete_proxy_profile(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_db("proxies.delete", move || db.delete_proxy_profile(id)).await?;
        refresh_nntp(ctx).await?;
        Ok(true)
    }
    #[graphql(guard = "AdminGuard")]
    async fn reset_proxy_host_key(&self, ctx: &Context<'_>, id: u32) -> Result<ProxyProfileGql> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let read_db = db.clone();
        let mut profile = spawn_blocking_db("proxies.load", move || read_db.list_proxy_profiles())
            .await?
            .into_iter()
            .find(|p| p.id == id)
            .ok_or("proxy not found")?;
        if profile.kind != proxies::ProxyKind::Ssh {
            return Err("only SSH profiles have host-key trust".into());
        }
        profile.host_key_fingerprint = None;
        profile.revision = profile
            .revision
            .checked_add(1)
            .ok_or("proxy revision exhausted")?;
        let saved = profile.clone();
        let profile = spawn_blocking_db("proxies.reset_trust", move || {
            db.reset_proxy_host_key(&saved)
        })
        .await?;
        refresh_nntp(ctx).await?;
        Ok(profile.into())
    }
    #[graphql(guard = "AdminGuard")]
    async fn test_proxy_profile(&self, ctx: &Context<'_>, id: u32) -> Result<ProxyTestResult> {
        let runtime = runtime(ctx)?;
        let _guard = runtime.mutations.lock().await;
        let db = ctx.data::<Database>()?.clone();
        let profile = spawn_blocking_db("proxies.load", move || db.list_proxy_profiles())
            .await?
            .into_iter()
            .find(|p| p.id == id)
            .ok_or("proxy not found")?;
        let test: std::pin::Pin<
            Box<dyn std::future::Future<Output = std::result::Result<String, String>> + Send + '_>,
        > = Box::pin(runtime.test_profile(&profile));
        Ok(match test.await {
            Ok(message) => ProxyTestResult {
                success: true,
                message,
            },
            Err(message) => ProxyTestResult {
                success: false,
                message,
            },
        })
    }
}

async fn policy_for(ctx: &Context<'_>, consumer: Consumer) -> Result<RoutingPolicyGql> {
    let db = ctx.data::<Database>()?.clone();
    Ok(
        spawn_blocking_db("proxies.routing", move || db.proxy_routing_policy(consumer))
            .await?
            .into(),
    )
}
fn status_for(ctx: &Context<'_>, consumer: Consumer) -> Result<RoutingStatus> {
    let status = match ctx.data::<SchedulerHandle>()?.proxy_runtime() {
        Some(runtime) => runtime
            .route(consumer, Duration::from_secs(30))
            .map_err(async_graphql::Error::new)?
            .status(),
        None => proxies::RoutingStatus::default(),
    };
    Ok(RoutingStatus {
        state: match status.state {
            proxies::RouteState::Idle => RoutingState::Idle,
            proxies::RouteState::Proxy => RoutingState::Proxy,
            proxies::RouteState::Direct => RoutingState::Direct,
            proxies::RouteState::Blocked => RoutingState::Blocked,
        },
        selected_proxy_id: status.selected_proxy_id,
        failures: status
            .failures
            .into_iter()
            .map(|(proxy_id, message)| RoutingFailure { proxy_id, message })
            .collect(),
    })
}
#[ComplexObject]
impl crate::servers::types::Server {
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Server(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Server(self.id))
    }
}
#[ComplexObject]
impl crate::rss::types::RssFeed {
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Rss(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Rss(self.id))
    }
}

#[ComplexObject]
impl crate::servers::types::ServerDetails {
    async fn routing(&self, ctx: &Context<'_>) -> Result<RoutingPolicyGql> {
        policy_for(ctx, Consumer::Server(self.id)).await
    }
    async fn routing_status(&self, ctx: &Context<'_>) -> Result<RoutingStatus> {
        status_for(ctx, Consumer::Server(self.id))
    }
}

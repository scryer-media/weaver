use super::{RssFeedRow, RssService, RssServiceError, model::apply_basic_auth};
use crate::{
    proxies::{Consumer, ConsumerRoute, ProxyHop},
    security::{ResolvedFetchTarget, is_blocked_egress_ip, resolve_fetch_target},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

enum AttemptError {
    Route,
    Destination(String),
}

#[derive(Clone)]
pub(super) struct RoutedBodyContext {
    route: std::sync::Weak<ConsumerRoute>,
    id: u32,
}
impl RoutedBodyContext {
    pub fn failed(&self) {
        if let Some(route) = self.route.upgrade().filter(|r| !r.is_revoked()) {
            route.fail(self.id, "RSS response transport failed");
        }
    }
}

impl RssService {
    pub(super) fn send_routed_request<'a>(
        &'a self,
        feed: &'a RssFeedRow,
        url: &'a reqwest::Url,
        feed_url: &'a reqwest::Url,
        conditional: bool,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<reqwest::Response, RssServiceError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let route = self
                .inner
                .handle
                .proxy_runtime()
                .map(|runtime| runtime.route(Consumer::Rss(feed.id), Duration::from_secs(30)))
                .transpose()
                .map_err(RssServiceError::Http)?;
            if let Some(route) = &route {
                for (id, hop) in route.policy.proxy_ids.iter().zip(&route.hops) {
                    let Some(attempt) = route.begin(*id) else {
                        continue;
                    };
                    let Some(hop) = hop.as_ref().filter(|h| h.profile.enabled) else {
                        attempt.fail("proxy is disabled or unavailable");
                        continue;
                    };
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        self.request_on_route(
                            feed,
                            url,
                            feed_url,
                            conditional,
                            Some(route),
                            Some(hop),
                        ),
                    )
                    .await
                    {
                        Ok(Ok(mut response)) => {
                            attempt.success();
                            response.extensions_mut().insert(RoutedBodyContext {
                                route: Arc::downgrade(route),
                                id: *id,
                            });
                            return Ok(response);
                        }
                        Ok(Err(AttemptError::Destination(message))) => {
                            return Err(RssServiceError::Http(message));
                        }
                        _ => attempt.fail("RSS proxy transport or routed DNS failed"),
                    }
                }
                if !route.policy.allow_direct || route.is_revoked() {
                    route.blocked();
                    return Err(RssServiceError::Http(
                        "RSS routing ladder exhausted; direct access is blocked".into(),
                    ));
                }
            }
            match tokio::time::timeout(
                Duration::from_secs(30),
                self.request_on_route(feed, url, feed_url, conditional, route.as_ref(), None),
            )
            .await
            {
                Ok(Ok(response)) => {
                    if let Some(route) = route {
                        route.success(None);
                    }
                    Ok(response)
                }
                Ok(Err(AttemptError::Destination(message))) => Err(RssServiceError::Http(message)),
                _ => {
                    if let Some(route) = route {
                        route.blocked();
                    }
                    Err(RssServiceError::Http(
                        "RSS request failed on all permitted routes".into(),
                    ))
                }
            }
        })
    }

    async fn request_on_route(
        &self,
        feed: &RssFeedRow,
        url: &reqwest::Url,
        feed_url: &reqwest::Url,
        conditional: bool,
        route: Option<&Arc<ConsumerRoute>>,
        hop: Option<&Arc<ProxyHop>>,
    ) -> Result<reqwest::Response, AttemptError> {
        let request = self.request_on_route_inner(feed, url, feed_url, conditional, route, hop);
        if let Some(route) = route {
            tokio::select! { biased; _ = route.cancelled() => Err(AttemptError::Route), result = request => result }
        } else {
            request.await
        }
    }

    async fn request_on_route_inner(
        &self,
        feed: &RssFeedRow,
        url: &reqwest::Url,
        feed_url: &reqwest::Url,
        conditional: bool,
        route: Option<&Arc<ConsumerRoute>>,
        hop: Option<&Arc<ProxyHop>>,
    ) -> Result<reqwest::Response, AttemptError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(AttemptError::Destination(
                "URL must use http or https".into(),
            ));
        }
        let target = if let Some(hop) = hop {
            let host = url
                .host_str()
                .ok_or_else(|| AttemptError::Destination("URL must include a host".into()))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| AttemptError::Destination("invalid URL port".into()))?;
            let addresses = hop
                .resolve(host.trim_matches(['[', ']']))
                .await
                .map_err(|_| AttemptError::Route)?;
            if addresses.is_empty() {
                return Err(AttemptError::Route);
            }
            if !self.inner.security.rss_allow_private_network
                && addresses.iter().any(|ip| is_blocked_egress_ip(*ip))
            {
                return Err(AttemptError::Destination(
                    "RSS destination address is not allowed".into(),
                ));
            }
            ResolvedFetchTarget {
                url: url.clone(),
                host: host.to_owned(),
                addrs: addresses
                    .into_iter()
                    .map(|ip| SocketAddr::new(ip, port))
                    .collect(),
            }
        } else {
            resolve_fetch_target(url, self.inner.security.rss_allow_private_network)
                .await
                .map_err(AttemptError::Destination)?
        };
        // Explicit policy disables environment proxies and all implicit bypasses.
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .user_agent("weaver-rss/0.1")
            .redirect(reqwest::redirect::Policy::none())
            .gzip(true);
        if let Some(route) = route {
            let bridge = match hop {
                Some(hop) => route.hop_bridge(hop),
                None => route.direct_bridge(),
            }
            .map_err(|_| AttemptError::Route)?;
            let addr = bridge.addr().map_err(|_| AttemptError::Route)?;
            // socks5 (not socks5h) uses exactly the checked addresses below.
            builder = builder.proxy(
                reqwest::Proxy::all(format!("socks5://{addr}"))
                    .map_err(|_| AttemptError::Route)?
                    .basic_auth(bridge.credentials().0, bridge.credentials().1),
            );
        }
        let client = target
            .apply_dns_override(builder)
            .build()
            .map_err(|_| AttemptError::Route)?;
        let mut request = client.get(target.url);
        if super::service::same_rss_origin(url, feed_url) {
            request = apply_basic_auth(request, feed);
        }
        if conditional {
            if let Some(etag) = &feed.etag {
                request = request.header(reqwest::header::IF_NONE_MATCH, etag);
            }
            if let Some(modified) = &feed.last_modified {
                request = request.header(reqwest::header::IF_MODIFIED_SINCE, modified);
            }
        }
        request.send().await.map_err(|error| {
            if weaver_nntp::tls::is_tls_error(&error) {
                AttemptError::Destination("RSS TLS verification failed".into())
            } else {
                AttemptError::Route
            }
        })
    }
}

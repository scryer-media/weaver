use super::{RssFeedRow, RssService, RssServiceError, model::apply_basic_auth};
use crate::{
    proxies::FeedAttempt,
    security::{ResolvedFetchTarget, is_blocked_egress_ip, resolve_fetch_target},
};
use std::{net::SocketAddr, sync::Arc, time::Duration};

enum AttemptError {
    Route,
    Routed(Arc<weaver_tunnel::pipe::DialError>),
    Destination(String),
}

#[derive(Clone)]
pub(super) struct RoutedBodyContext {
    attempt: Arc<FeedAttempt>,
    _bridge: Arc<weaver_tunnel::bridge::Bridge>,
}
impl RoutedBodyContext {
    pub fn failed(&self) {
        self.attempt
            .report(Some(&weaver_tunnel::pipe::DialError::Destination(
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "RSS response transport failed",
                ),
            )));
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
            if let Some(runtime) = self.inner.handle.proxy_runtime() {
                let status = runtime
                    .route(
                        crate::proxies::Consumer::Rss(feed.id),
                        Duration::from_secs(30),
                    )
                    .map_err(RssServiceError::Http)?;
                let attempts = runtime
                    .network
                    .feed_attempts(feed.id, status)
                    .map_err(RssServiceError::Http)?;
                for attempt in attempts {
                    let request = self.request_on_route_inner(
                        feed,
                        url,
                        feed_url,
                        conditional,
                        Some(&attempt),
                    );
                    let result = tokio::select! {
                        biased;
                        _ = attempt.cancelled() => continue,
                        result = tokio::time::timeout(Duration::from_secs(30), request) => result,
                    };
                    if let Some(error) = attempt.take_transport_error() {
                        attempt.report(Some(&error));
                        if matches!(error.as_ref(), weaver_tunnel::pipe::DialError::Fatal(_)) {
                            return Err(RssServiceError::Http(error.to_string()));
                        }
                        continue;
                    }
                    match result {
                        Ok(Ok(response)) => {
                            attempt.report(None);
                            return Ok(response);
                        }
                        Ok(Err(AttemptError::Destination(message))) => {
                            return Err(RssServiceError::Http(message));
                        }
                        Ok(Err(AttemptError::Routed(error))) => {
                            attempt.report(Some(&error));
                            if matches!(error.as_ref(), weaver_tunnel::pipe::DialError::Fatal(_)) {
                                return Err(RssServiceError::Http(error.to_string()));
                            }
                        }
                        _ => attempt.report(Some(&weaver_tunnel::pipe::DialError::Destination(
                            std::io::Error::new(
                                std::io::ErrorKind::ConnectionReset,
                                "RSS transport or routed DNS failed",
                            ),
                        ))),
                    }
                }
                return Err(RssServiceError::Http(
                    "RSS request failed on all permitted routes".into(),
                ));
            }
            match tokio::time::timeout(
                Duration::from_secs(30),
                self.request_on_route_inner(feed, url, feed_url, conditional, None),
            )
            .await
            {
                Ok(Ok(response)) => Ok(response),
                Ok(Err(AttemptError::Destination(message))) => Err(RssServiceError::Http(message)),
                _ => Err(RssServiceError::Http("RSS request failed".into())),
            }
        })
    }

    async fn request_on_route_inner(
        &self,
        feed: &RssFeedRow,
        url: &reqwest::Url,
        feed_url: &reqwest::Url,
        conditional: bool,
        attempt: Option<&Arc<FeedAttempt>>,
    ) -> Result<reqwest::Response, AttemptError> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(AttemptError::Destination(
                "URL must use http or https".into(),
            ));
        }
        let target = if let Some(attempt) = attempt {
            let host = url
                .host_str()
                .ok_or_else(|| AttemptError::Destination("URL must include a host".into()))?;
            let port = url
                .port_or_known_default()
                .ok_or_else(|| AttemptError::Destination("invalid URL port".into()))?;
            let addresses = attempt
                .resolve(host.trim_matches(['[', ']']))
                .await
                .map_err(|error| AttemptError::Routed(Arc::new(error)))?;
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
            attempt.pin_addresses(host.trim_matches(['[', ']']), addresses.clone());
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
        let bridge = attempt
            .map(|attempt| attempt.bridge())
            .transpose()
            .map_err(|_| AttemptError::Route)?;
        if let Some(bridge) = &bridge {
            let addr = bridge.addr().map_err(|_| AttemptError::Route)?;
            // socks5h hands the bridge the hostname: a proxy hop forwards it
            // as NNTP does, and a direct dial uses exactly the checked
            // addresses pinned on the attempt, trying each in turn.
            builder = builder.proxy(
                reqwest::Proxy::all(format!("socks5h://{addr}"))
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
        let mut response = request.send().await.map_err(|error| {
            if weaver_nntp::tls::is_tls_error(&error) {
                AttemptError::Destination("RSS TLS verification failed".into())
            } else {
                AttemptError::Route
            }
        })?;
        if let (Some(attempt), Some(bridge)) = (attempt, bridge) {
            response.extensions_mut().insert(RoutedBodyContext {
                attempt: attempt.clone(),
                _bridge: bridge,
            });
        }
        Ok(response)
    }
}

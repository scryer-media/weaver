// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent fetched from a URL, on the route the torrent itself will take.
//! The fetch is the one every routed fetch makes; what is this fetch's own is
//! the cookie a client hands over for the site, sent to that site alone, and
//! the client it asks as: the one the torrent will name itself as.

use std::time::Duration;

use crate::proxies::{Consumer, FetchOptions, ProxyRuntime, TorrentScope, routed_fetch};
use weaver_server_settings::torrents::identity::ClientIdentity;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REDIRECTS: usize = 10;

/// The bytes at `url`, fetched on the torrent route of `scope` as `client`,
/// with `cookie` sent to the host named, and only to it, when one is given;
/// a fetch begun over https never sends it over http.
/// A body is bounded as a .torrent file is
/// ([`weaver_torrent::MAX_TORRENT_FILE_BYTES`]).
pub async fn fetch_torrent_url(
    runtime: &ProxyRuntime,
    scope: TorrentScope,
    url: &reqwest::Url,
    cookie: Option<&str>,
    client: ClientIdentity,
) -> Result<Vec<u8>, String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("URL must use http or https".into());
    }
    let options = FetchOptions {
        label: "torrent",
        timeout: FETCH_TIMEOUT,
        redirects: MAX_REDIRECTS,
        user_agent: client.user_agent(),
        https_only: false,
        allow_private_network: runtime.allow_private_network(),
    };
    let cookie = cookie.map(str::trim).filter(|cookie| !cookie.is_empty());
    let response = routed_fetch(
        runtime,
        Consumer::Torrent(scope),
        url,
        &options,
        |request, current, _| match cookie {
            Some(cookie) if sends_cookie(url, current) => {
                request.header(reqwest::header::COOKIE, cookie)
            }
            _ => request,
        },
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let limit = weaver_torrent::MAX_TORRENT_FILE_BYTES as u64;
    let bytes = crate::proxies::read_body(response, limit).await?;
    if bytes.is_empty() {
        return Err("empty response body".into());
    }
    Ok(bytes)
}

// Whether a fetch that began at `first` sends the cookie on to `current`:
// to the host it named, on any port, as a browser or Qt scopes a cookie,
// and never over http once it began over https.
pub fn sends_cookie(first: &reqwest::Url, current: &reqwest::Url) -> bool {
    current.host_str() == first.host_str()
        && !(first.scheme() == "https" && current.scheme() == "http")
}

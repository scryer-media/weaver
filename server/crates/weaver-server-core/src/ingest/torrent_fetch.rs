// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent fetched from a URL, on the route the torrent itself will take.
//! The fetch is the one every routed fetch makes; what is this fetch's own is
//! the cookie a client hands over for the site, sent to that site alone.

use std::time::Duration;

use crate::proxies::{Consumer, FetchOptions, ProxyRuntime, TorrentScope, routed_fetch};
use crate::security::RuntimeSecurityConfig;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REDIRECTS: usize = 10;

/// The bytes at `url`, fetched on the torrent route of `scope` with `cookie`
/// sent to the host named, and only to it, when one is given. A body is
/// bounded like an NZB's.
pub async fn fetch_torrent_url(
    runtime: &ProxyRuntime,
    scope: TorrentScope,
    url: &reqwest::Url,
    cookie: Option<&str>,
) -> Result<Vec<u8>, String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err("URL must use http or https".into());
    }
    let options = FetchOptions {
        label: "torrent",
        timeout: FETCH_TIMEOUT,
        redirects: MAX_REDIRECTS,
        user_agent: crate::torrents::identity::IDENTITY.user_agent(),
        https_only: false,
        allow_private_network: runtime.allow_private_network(),
    };
    let cookie = cookie.map(str::trim).filter(|cookie| !cookie.is_empty());
    let site = url.host_str();
    let response = routed_fetch(
        runtime,
        Consumer::Torrent(scope),
        url,
        &options,
        |request, current, _| match cookie {
            Some(cookie) if current.host_str() == site => {
                request.header(reqwest::header::COOKIE, cookie)
            }
            _ => request,
        },
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!("HTTP {}", response.status()));
    }
    let limit = RuntimeSecurityConfig::from_env_or_default_for_tests().nzb_decompressed_limit_bytes;
    let bytes = crate::proxies::read_body(response, limit).await?;
    if bytes.is_empty() {
        return Err("empty response body".into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    async fn read_request(stream: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut buf = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    #[tokio::test]
    async fn the_cookie_goes_with_the_request_and_the_body_comes_back() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = b"d4:infod4:name1:aee";
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/x-bittorrent\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            request
        });
        let url = reqwest::Url::parse(&format!("http://{addr}/a.torrent?passkey=k")).unwrap();

        let bytes = fetch_torrent_url(
            &crate::proxies::direct_runtime(),
            TorrentScope::Default,
            &url,
            Some("uid=1; pass=2"),
        )
        .await
        .unwrap();

        assert_eq!(bytes, body);
        let request = server.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /a.torrent?passkey=k http/1.1"));
        assert!(request.contains("cookie: uid=1; pass=2"), "{request}");
    }

    #[tokio::test]
    async fn a_private_destination_is_refused_before_connecting() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = reqwest::Url::parse(&format!(
            "http://{}/a.torrent",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let runtime = crate::proxies::direct_runtime();
        runtime.set_allow_private_network(false);

        let error = fetch_torrent_url(&runtime, TorrentScope::Default, &url, None)
            .await
            .unwrap_err();

        assert!(error.contains("not allowed"), "{error}");
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn a_body_past_the_cap_is_refused() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let limit =
            RuntimeSecurityConfig::from_env_or_default_for_tests().nzb_decompressed_limit_bytes;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                limit + 1
            );
            stream.write_all(head.as_bytes()).await.unwrap();
        });
        let url = reqwest::Url::parse(&format!("http://{addr}/big.torrent")).unwrap();

        let error = fetch_torrent_url(
            &crate::proxies::direct_runtime(),
            TorrentScope::Default,
            &url,
            None,
        )
        .await
        .unwrap_err();

        server.await.unwrap();
        assert!(error.contains("exceeds"), "{error}");
    }
}

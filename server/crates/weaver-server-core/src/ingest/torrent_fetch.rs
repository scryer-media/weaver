// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent fetched from a URL, on the route the torrent itself will take.
//! The fetch is the one every routed fetch makes; what is this fetch's own is
//! the cookie a client hands over for the site, sent to that site alone, and
//! the client it asks as: the one the torrent will name itself as.

use std::time::Duration;

use crate::proxies::{Consumer, FetchOptions, ProxyRuntime, TorrentScope, routed_fetch};
use crate::security::RuntimeSecurityConfig;
use crate::torrents::identity::ClientIdentity;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_REDIRECTS: usize = 10;

/// The bytes at `url`, fetched on the torrent route of `scope` as `client`,
/// with `cookie` sent to the host named, and only to it, when one is given;
/// a fetch begun over https never sends it over http.
/// A body is bounded like an NZB's.
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
    let limit = RuntimeSecurityConfig::from_env_or_default_for_tests().nzb_decompressed_limit_bytes;
    let bytes = crate::proxies::read_body(response, limit).await?;
    if bytes.is_empty() {
        return Err("empty response body".into());
    }
    Ok(bytes)
}

// Whether a fetch that began at `first` sends the cookie on to `current`:
// to the host it named, on any port, as a browser or Qt scopes a cookie,
// and never over http once it began over https.
fn sends_cookie(first: &reqwest::Url, current: &reqwest::Url) -> bool {
    current.host_str() == first.host_str()
        && !(first.scheme() == "https" && current.scheme() == "http")
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use crate::torrents::identity::IDENTITY;

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
            IDENTITY,
        )
        .await
        .unwrap();

        assert_eq!(bytes, body);
        let request = server.await.unwrap().to_ascii_lowercase();
        assert!(request.starts_with("get /a.torrent?passkey=k http/1.1"));
        assert!(request.contains("cookie: uid=1; pass=2"), "{request}");
        assert!(
            request.contains(&format!(
                "user-agent: {}",
                IDENTITY.user_agent().to_ascii_lowercase()
            )),
            "{request}"
        );
    }

    #[test]
    fn the_cookie_goes_to_the_host_named_and_never_down_to_http() {
        let url = |url: &str| reqwest::Url::parse(url).unwrap();
        let first = url("https://site.invalid/a.torrent");
        for (current, sent) in [
            ("https://site.invalid/b.torrent", true),
            ("https://site.invalid:8443/b.torrent", true),
            ("http://site.invalid/b.torrent", false),
            ("http://site.invalid:8080/b.torrent", false),
            ("https://other.invalid/b.torrent", false),
            ("https://cdn.site.invalid/b.torrent", false),
        ] {
            assert_eq!(sends_cookie(&first, &url(current)), sent, "{current}");
        }
        // A fetch that began over http may go up to https.
        let first = url("http://site.invalid/a.torrent");
        for (current, sent) in [
            ("http://site.invalid:8080/b.torrent", true),
            ("https://site.invalid/b.torrent", true),
            ("http://other.invalid/b.torrent", false),
        ] {
            assert_eq!(sends_cookie(&first, &url(current)), sent, "{current}");
        }
    }

    // A redirect to the same host on another port carries the cookie on.
    #[tokio::test]
    async fn the_cookie_follows_a_redirect_to_another_port_of_the_host() {
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = reqwest::Url::parse(&format!("http://{}/a.torrent", first.local_addr().unwrap()))
            .unwrap();
        let next = format!("http://{}/b.torrent", second.local_addr().unwrap());
        let body = b"d4:infod4:name1:aee";
        let redirect = tokio::spawn(async move {
            let (mut stream, _) = first.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            let head = format!(
                "HTTP/1.1 302 Found\r\nLocation: {next}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            request
        });
        let answer = tokio::spawn(async move {
            let (mut stream, _) = second.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            request
        });

        let bytes = fetch_torrent_url(
            &crate::proxies::direct_runtime(),
            TorrentScope::Default,
            &url,
            Some("uid=1; pass=2"),
            IDENTITY,
        )
        .await
        .unwrap();

        assert_eq!(bytes, body);
        for request in [redirect.await.unwrap(), answer.await.unwrap()] {
            let request = request.to_ascii_lowercase();
            assert!(request.contains("cookie: uid=1; pass=2"), "{request}");
        }
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

        let error = fetch_torrent_url(&runtime, TorrentScope::Default, &url, None, IDENTITY)
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
            IDENTITY,
        )
        .await
        .unwrap_err();

        server.await.unwrap();
        assert!(error.contains("exceeds"), "{error}");
    }
}

use super::proxy_routing::{Fixture, service};
use super::*;
use crate::proxies::{Consumer, ProxyKind, RoutingPolicy};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn partial_response(stall: bool) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(socket.read_u8().await.unwrap());
            assert!(headers.len() < 8192);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\npartial")
            .await
            .unwrap();
        if stall {
            std::future::pending::<()>().await;
        }
    });
    (addr, task)
}

#[tokio::test]
async fn truncated_nzb_body_is_not_submitted_or_spliced_and_cools_the_route() {
    let temp = TempDir::new().unwrap();
    let (addr, task) = partial_response(false).await;
    let fixture = Fixture::start(ProxyKind::Socks5, addr).await;
    let (service, runtime, feed, submissions) =
        service(&temp, &fixture, "http://feed.invalid/download.nzb", false);
    let response = service
        .send_rss_request(&feed, &feed.url, false)
        .await
        .unwrap();
    assert!(read_response_with_limit(response, 2000).await.is_err());
    let route = runtime
        .route(Consumer::Rss(1), Duration::from_secs(30))
        .unwrap();
    assert!(route.begin(1).is_none());
    assert!(submissions.lock().unwrap().is_empty());
    task.await.unwrap();
    runtime.stop_all().await;
}

#[tokio::test]
async fn changing_feed_policy_closes_an_active_response_before_mutation_completes() {
    let temp = TempDir::new().unwrap();
    let (addr, task) = partial_response(true).await;
    let fixture = Fixture::start(ProxyKind::HttpConnect, addr).await;
    let (service, runtime, feed, _) = service(&temp, &fixture, "http://feed.invalid/feed", false);
    let response = service
        .send_rss_request(&feed, &feed.url, false)
        .await
        .unwrap();
    service
        .inner
        .db
        .save_proxy_routing_policy(
            Consumer::Rss(feed.id),
            &RoutingPolicy {
                proxy_ids: vec![],
                allow_direct: false,
            },
        )
        .unwrap();
    runtime.reload().await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            read_response_with_limit(response, 2000)
        )
        .await
        .unwrap()
        .is_err()
    );
    assert!(
        service
            .send_rss_request(&feed, &feed.url, false)
            .await
            .is_err()
    );
    runtime.stop_all().await;
    task.abort();
    let _ = task.await;
}

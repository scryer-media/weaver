use super::*;

fn proxy(kind: TransportKind) -> TransportProxy {
    TransportProxy {
        kind,
        host: "unresolved.invalid".into(),
        port: 1,
        username: None,
        password: None,
    }
}

#[tokio::test]
async fn socks_negotiates_over_an_inner_stream_and_preserves_payload() {
    let (mut inner, mut peer) = tokio::io::duplex(256);
    let server = async {
        let mut hello = [0; 3];
        peer.read_exact(&mut hello).await.unwrap();
        assert_eq!(hello, [5, 1, 0]);
        peer.write_all(&[5, 0]).await.unwrap();
        let expected = socks_request("news.example", 563).unwrap();
        let mut request = vec![0; expected.len()];
        peer.read_exact(&mut request).await.unwrap();
        assert_eq!(request, expected);
        peer.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 2, 51])
            .await
            .unwrap();
        peer.write_all(b"200 greeting\r\n").await.unwrap();
    };
    let client = async {
        proxy(TransportKind::Socks5)
            .negotiate(&mut inner, "news.example", 563)
            .await
            .unwrap();
        let mut greeting = [0; 14];
        inner.read_exact(&mut greeting).await.unwrap();
        assert_eq!(&greeting, b"200 greeting\r\n");
    };
    tokio::join!(server, client);
}

#[tokio::test]
async fn connect_negotiates_over_an_inner_stream_without_consuming_greeting() {
    let (mut inner, mut peer) = tokio::io::duplex(256);
    let server = async {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(peer.read_u8().await.unwrap());
        }
        assert_eq!(
            request,
            b"CONNECT [2001:db8::1]:563 HTTP/1.1\r\nHost: [2001:db8::1]:563\r\n\r\n"
        );
        peer.write_all(b"HTTP/1.1 200 Connected\r\n\r\n200 greeting\r\n")
            .await
            .unwrap();
    };
    let client = async {
        proxy(TransportKind::HttpConnect)
            .negotiate(&mut inner, "2001:db8::1", 563)
            .await
            .unwrap();
        let mut greeting = [0; 14];
        inner.read_exact(&mut greeting).await.unwrap();
        assert_eq!(&greeting, b"200 greeting\r\n");
    };
    tokio::join!(server, client);
}

#[tokio::test]
async fn failed_inner_negotiation_is_reported_without_a_second_dial() {
    let (mut inner, mut peer) = tokio::io::duplex(256);
    let server = async {
        let mut hello = [0; 3];
        peer.read_exact(&mut hello).await.unwrap();
        peer.write_all(&[5, 255]).await.unwrap();
    };
    let client = async {
        let error = proxy(TransportKind::Socks5)
            .negotiate(&mut inner, "news.example", 563)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("authentication method rejected"));
    };
    tokio::join!(server, client);
}

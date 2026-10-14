use super::*;
use crate::{
    NoopTunnelObserver, SshTunnelProvider, TunnelProvider, parse_key, public_key_of,
    test_support::{CLIENT_ED25519_PEM_WITH_PASSPHRASE, ECDSA_P256_PEM, spec_for},
    transport::{TransportKind, TransportProxy, hop_before},
    validate_private_key, validate_wireguard_keys,
};
use base64::Engine;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

const WAIT: Duration = Duration::from_secs(30);

fn proxy(kind: TransportKind, credentials: Option<(&str, &str)>) -> TransportProxy {
    TransportProxy {
        kind,
        host: "unresolved.invalid".into(),
        port: 1,
        username: credentials.map(|(user, _)| user.into()),
        password: credentials.map(|(_, pass)| pass.into()),
    }
}

// What a proxy that answers `script` to everything it is sent comes to,
// asked for news.example:563.
async fn answered(proxy: &TransportProxy, script: &[u8]) -> ProxyFault {
    answered_for(proxy, "news.example", script).await
}

async fn answered_for(proxy: &TransportProxy, host: &str, script: &[u8]) -> ProxyFault {
    let (mut client, mut server) = tokio::io::duplex(4096);
    server.write_all(script).await.unwrap();
    server.shutdown().await.unwrap();
    let error = proxy.negotiate(&mut client, host, 563).await.unwrap_err();
    drop(server);
    classify(&error, WAIT)
}

fn socks() -> TransportProxy {
    proxy(TransportKind::Socks5, None)
}

fn http() -> TransportProxy {
    proxy(TransportKind::HttpConnect, None)
}

fn target(target: &str) -> String {
    target.to_owned()
}

#[tokio::test]
async fn a_socks_proxy_that_wants_credentials_says_so() {
    assert_eq!(
        answered(&socks(), &[5, 255]).await,
        ProxyFault::AuthRequired
    );
}

#[tokio::test]
async fn a_socks_proxy_that_takes_no_credentials_says_to_remove_them() {
    let proxy = proxy(TransportKind::Socks5, Some(("user", "pass")));
    assert_eq!(answered(&proxy, &[5, 0]).await, ProxyFault::AuthUnsupported);
}

#[tokio::test]
async fn a_socks_proxy_that_rejects_credentials_says_so() {
    let proxy = proxy(TransportKind::Socks5, Some(("user", "pass")));
    assert_eq!(
        answered(&proxy, &[5, 2, 1, 1]).await,
        ProxyFault::CredentialsRejected
    );
}

#[tokio::test]
async fn socks_credentials_too_long_to_send_are_named() {
    let long = "u".repeat(256);
    let proxy = proxy(TransportKind::Socks5, Some((&long, "pass")));
    assert_eq!(
        answered(&proxy, &[5, 2]).await,
        ProxyFault::CredentialsInvalid
    );
}

#[tokio::test]
async fn a_socks_proxy_that_hangs_up_during_sign_in_says_so() {
    let proxy = proxy(TransportKind::Socks5, Some(("user", "pass")));
    assert_eq!(
        answered(&proxy, &[5, 2]).await,
        ProxyFault::ClosedDuringSignIn
    );
}

#[tokio::test]
async fn a_server_that_is_not_a_socks_proxy_is_named() {
    assert_eq!(
        answered(&socks(), b"HTTP/1.1 400 Bad Request\r\n\r\n").await,
        ProxyFault::NotAProxy { protocol: "SOCKS5" }
    );
    assert_eq!(
        answered(&socks(), &[5, 0, 9, 9, 9, 9]).await,
        ProxyFault::NotAProxy { protocol: "SOCKS5" }
    );
}

#[tokio::test]
async fn a_proxy_that_closes_before_answering_says_so() {
    assert_eq!(answered(&socks(), &[]).await, ProxyFault::ConnectionClosed);
    assert_eq!(answered(&http(), &[]).await, ProxyFault::ConnectionClosed);
}

#[tokio::test]
async fn a_socks_reply_names_what_became_of_the_destination() {
    let reply = |code: u8| [5, 0, 5, code, 0, 1];
    assert_eq!(
        answered(&socks(), &reply(5)).await,
        ProxyFault::DestinationRefused {
            target: target("news.example:563")
        }
    );
    assert_eq!(
        answered(&socks(), &reply(2)).await,
        ProxyFault::DestinationForbidden {
            target: target("news.example:563")
        }
    );
    assert_eq!(
        answered(&socks(), &reply(4)).await,
        ProxyFault::DestinationUnreachable {
            target: target("news.example:563"),
            reply: "SOCKS reply 4".into()
        }
    );
    assert_eq!(
        answered(&socks(), &reply(1)).await,
        ProxyFault::DestinationUnreachable {
            target: target("news.example:563"),
            reply: "SOCKS reply 1".into()
        }
    );
}

#[tokio::test]
async fn an_ipv6_destination_is_named_in_brackets() {
    assert_eq!(
        answered_for(&socks(), "2001:db8::1", &[5, 0, 5, 5, 0, 1]).await,
        ProxyFault::DestinationRefused {
            target: target("[2001:db8::1]:563")
        }
    );
    assert_eq!(
        answered_for(&http(), "2001:db8::1", b"HTTP/1.1 403 Forbidden\r\n\r\n").await,
        ProxyFault::DestinationForbidden {
            target: target("[2001:db8::1]:563")
        }
    );
}

#[tokio::test]
async fn an_http_proxy_that_wants_credentials_says_so() {
    let required = b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n";
    assert_eq!(answered(&http(), required).await, ProxyFault::AuthRequired);
    let proxy = proxy(TransportKind::HttpConnect, Some(("user", "pass")));
    assert_eq!(
        answered(&proxy, required).await,
        ProxyFault::CredentialsRejected
    );
}

#[tokio::test]
async fn an_http_status_names_what_the_proxy_did() {
    assert_eq!(
        answered(&http(), b"HTTP/1.1 403 Forbidden\r\n\r\n").await,
        ProxyFault::DestinationForbidden {
            target: target("news.example:563")
        }
    );
    assert_eq!(
        answered(&http(), b"HTTP/1.1 405 Method Not Allowed\r\n\r\n").await,
        ProxyFault::ConnectRefused { status: 405 }
    );
    assert_eq!(
        answered(&http(), b"HTTP/1.1 502 Bad Gateway\r\n\r\n").await,
        ProxyFault::DestinationUnreachable {
            target: target("news.example:563"),
            reply: "HTTP 502".into()
        }
    );
    assert_eq!(
        answered(&http(), b"HTTP/1.1 504 Gateway Timeout\r\n\r\n").await,
        ProxyFault::DestinationTimedOut {
            target: target("news.example:563")
        }
    );
}

#[tokio::test]
async fn a_server_that_is_not_an_http_proxy_is_named() {
    assert_eq!(
        answered(&http(), b"SSH-2.0-OpenSSH_9.9\r\n\r\n").await,
        ProxyFault::NotAProxy { protocol: "HTTP" }
    );
}

#[test]
fn the_hop_before_a_proxy_is_read_back() {
    let error = |reply| hop_before("192.0.2.2", 8080, reply);
    assert_eq!(
        classify(&error(Reply::Socks(5)), WAIT),
        ProxyFault::DestinationRefused {
            target: target("192.0.2.2:8080")
        }
    );
    assert_eq!(
        classify(&error(Reply::Silent), WAIT),
        ProxyFault::DestinationTimedOut {
            target: target("192.0.2.2:8080")
        }
    );
    assert_eq!(
        classify(&error(Reply::Http(503)), WAIT),
        ProxyFault::DestinationUnreachable {
            target: target("192.0.2.2:8080"),
            reply: "HTTP 503".into()
        }
    );
}

#[test]
fn a_reply_reads_back_as_written() {
    for reply in [Reply::Socks(4), Reply::Http(502), Reply::Silent] {
        assert_eq!(Reply::read(&reply.to_string()), Some(reply));
    }
    assert_eq!(Reply::read("HTTP"), None);
}

#[test]
fn an_operating_system_error_is_read_by_its_number() {
    let cases = [
        (libc::ECONNREFUSED, ProxyFault::ConnectionRefused),
        (libc::EHOSTUNREACH, ProxyFault::HostUnreachable),
        (libc::ENETUNREACH, ProxyFault::NetworkUnreachable),
        (libc::ENETDOWN, ProxyFault::NetworkUnreachable),
        (libc::ETIMEDOUT, ProxyFault::TimedOut { seconds: 30 }),
        (libc::ECONNRESET, ProxyFault::ConnectionClosed),
        (libc::EPIPE, ProxyFault::ConnectionClosed),
    ];
    for (number, expected) in cases {
        let error = io::Error::from_raw_os_error(number);
        assert_eq!(from_io(&error, WAIT), expected, "{error}");
        // However many times it was wrapped in words.
        let wrapped = TunnelError::Engine(format!("proxy endpoint is unreachable: {error}"));
        assert_eq!(classify(&wrapped, WAIT), expected, "{wrapped}");
        let ssh = TunnelError::Connect {
            host: "ssh.example".into(),
            port: 22,
            detail: error.to_string(),
        };
        assert_eq!(classify(&ssh, WAIT), expected, "{ssh}");
    }
}

#[test]
fn the_error_number_is_the_last_one_written() {
    assert_eq!(os_error("Connection refused (os error 61)"), Some(61));
    assert_eq!(os_error("a: (os error 1) b: x (os error 113)"), Some(113));
    assert_eq!(os_error("connection refused"), None);
    assert_eq!(os_error("(os error x)"), None);
}

#[test]
fn a_name_lookup_that_found_nothing_is_named() {
    let texts = [
        // The system's resolver, as std writes it.
        "failed to lookup address information: nodename nor servname provided, or not known",
        // Windows sockets, by number.
        "No such host is known. (os error 11001)",
        "proxy endpoint is unreachable: could not resolve to any addresses",
    ];
    for text in texts {
        assert_eq!(
            classify(&TunnelError::Engine(text.into()), WAIT),
            ProxyFault::NameNotResolved,
            "{text}"
        );
    }
    let error = io::Error::new(io::ErrorKind::NotFound, ResolutionFailed);
    assert_eq!(from_io(&error, WAIT), ProxyFault::NameNotResolved);
}

#[test]
fn a_wait_that_ran_out_is_named_with_its_seconds() {
    let ssh = TunnelError::Connect {
        host: "ssh.example".into(),
        port: 22,
        detail: "the server did not complete the handshake in time".into(),
    };
    assert_eq!(
        classify(&ssh, Duration::from_millis(7500)),
        ProxyFault::TimedOut { seconds: 8 }
    );
    let timeout = DialError::Timeout {
        stage: "proxy 7".into(),
    };
    assert_eq!(timeout.fault(WAIT), ProxyFault::TimedOut { seconds: 30 });
}

#[test]
fn seconds_round_up_and_are_never_zero() {
    assert_eq!(seconds(Duration::ZERO), 1);
    assert_eq!(seconds(Duration::from_millis(1)), 1);
    assert_eq!(seconds(Duration::from_secs(30)), 30);
    assert_eq!(seconds(Duration::from_millis(30_001)), 31);
}

#[test]
fn an_authority_brackets_ipv6() {
    assert_eq!(authority("192.0.2.1", 80), "192.0.2.1:80");
    assert_eq!(authority("2001:db8::1", 80), "[2001:db8::1]:80");
    assert_eq!(authority("[2001:db8::1]", 80), "[2001:db8::1]:80");
    assert_eq!(authority("news.example", 563), "news.example:563");
}

#[test]
fn ssh_key_faults_come_from_the_parser_that_refused_them() {
    let fault = |pem: &str| classify(&validate_private_key(pem, None).unwrap_err(), WAIT);
    assert_eq!(
        fault(CLIENT_ED25519_PEM_WITH_PASSPHRASE),
        ProxyFault::SshKeyPassphrase
    );
    assert!(
        matches!(fault(ECDSA_P256_PEM), ProxyFault::SshKeyNotEd25519 { .. }),
        "{}",
        fault(ECDSA_P256_PEM)
    );
    assert!(
        matches!(
            fault(
                "-----BEGIN OPENSSH PRIVATE KEY-----\nbm90IGEga2V5\n-----END OPENSSH PRIVATE KEY-----"
            ),
            ProxyFault::SshKeyUnreadable { .. }
        ),
        "{}",
        fault(
            "-----BEGIN OPENSSH PRIVATE KEY-----\nbm90IGEga2V5\n-----END OPENSSH PRIVATE KEY-----"
        )
    );
}

#[test]
fn a_key_of_another_algorithm_is_named() {
    let error = TunnelError::Configuration(format!(
        "{ED25519_ONLY_PRIVATE_KEY_MESSAGE} (this key is ssh-rsa)"
    ));
    assert_eq!(
        classify(&error, WAIT),
        ProxyFault::SshKeyNotEd25519 {
            algorithm: Some("ssh-rsa".into())
        }
    );
    assert_eq!(
        classify(
            &TunnelError::Configuration(ED25519_ONLY_PRIVATE_KEY_MESSAGE.into()),
            WAIT
        ),
        ProxyFault::SshKeyNotEd25519 { algorithm: None }
    );
}

#[tokio::test]
async fn an_ssh_tunnel_without_a_key_says_it_needs_one() {
    let mut spec = spec_for("proxy-nokey", "192.0.2.1", 22);
    spec.private_key_pem.clear();
    let error = SshTunnelProvider::new(spec, Arc::new(NoopTunnelObserver))
        .prepare()
        .await
        .unwrap_err();
    assert_eq!(classify(&error, WAIT), ProxyFault::SshKeyMissing);
}

#[test]
fn ssh_session_failures_are_named() {
    let mismatch = TunnelError::HostKeyMismatch {
        host: "ssh.example".into(),
        port: 22,
        expected: "SHA256:old".into(),
        actual: "SHA256:new".into(),
    };
    assert_eq!(
        classify(&mismatch, WAIT),
        ProxyFault::SshHostKeyChanged {
            expected: "SHA256:old".into(),
            actual: "SHA256:new".into()
        }
    );
    let auth = TunnelError::Auth {
        username: "operator".into(),
        host: "ssh.example".into(),
        port: 22,
        detail: "publickey".into(),
    };
    assert_eq!(
        classify(&auth, WAIT),
        ProxyFault::SshKeyRejected {
            username: "operator".into()
        }
    );
    let refused = TunnelError::Dial {
        host: "news.example".into(),
        port: 563,
        detail: "AdministrativelyProhibited".into(),
    };
    assert_eq!(
        classify(&refused, WAIT),
        ProxyFault::SshForwardingRefused {
            target: target("news.example:563")
        }
    );
    let other = TunnelError::Connect {
        host: "ssh.example".into(),
        port: 22,
        detail: "Kex error".into(),
    };
    assert_eq!(
        classify(&other, WAIT),
        ProxyFault::SshHandshakeFailed {
            detail: "Kex error".into()
        }
    );
}

#[test]
fn a_tunnel_that_cannot_reach_a_destination_says_why() {
    let dial = |detail: &str| TunnelError::Dial {
        host: "news.example".into(),
        port: 563,
        detail: detail.into(),
    };
    assert_eq!(
        classify(&dial("the far side refused the connection"), WAIT),
        ProxyFault::DestinationRefused {
            target: target("news.example:563")
        }
    );
    assert_eq!(
        classify(&dial("the tunnel did not open a channel in time"), WAIT),
        ProxyFault::DestinationTimedOut {
            target: target("news.example:563")
        }
    );
    assert_eq!(
        classify(
            &dial("the tunnel's DNS server does not know this name"),
            WAIT
        ),
        ProxyFault::NameNotResolved
    );
    assert_eq!(
        classify(&dial("ConnectFailed"), WAIT),
        ProxyFault::DestinationUnreachable {
            target: target("news.example:563"),
            reply: "ConnectFailed".into()
        }
    );
}

#[test]
fn wireguard_key_faults_come_from_the_validator_that_refused_them() {
    let private = base64::prelude::BASE64_STANDARD.encode([7u8; 32]);
    let own = public_key_of(&[7u8; 32]);
    let peer = public_key_of(&[9u8; 32]);
    let fault = |private: &str, peer: &str| {
        classify(
            &validate_wireguard_keys(private, peer, None).unwrap_err(),
            WAIT,
        )
    };
    assert_eq!(fault("", &peer), ProxyFault::WireGuardKeyInvalid);
    assert_eq!(
        fault(&private, "not base64!"),
        ProxyFault::WireGuardKeyInvalid
    );
    assert_eq!(fault(&private, &own), ProxyFault::WireGuardPeerIsSelf);
    assert_eq!(
        classify(&parse_key("AAAA").unwrap_err(), WAIT),
        ProxyFault::WireGuardKeyInvalid
    );
}

#[test]
fn wireguard_tunnel_failures_are_named() {
    let connect = |detail: String| TunnelError::WireGuardConnect {
        host: "wg.example".into(),
        port: 51820,
        detail,
    };
    let silent = connect(format!(
        "{WIREGUARD_NO_HANDSHAKE}12s. WireGuard answers nothing it cannot authenticate, so \
         this is what a wrong private key and an unreachable endpoint look like. Check that \
         the server's `[Peer]` section lists this tunnel's public key (KEY+/abc=)."
    ));
    assert_eq!(
        classify(&silent, WAIT),
        ProxyFault::WireGuardNoHandshake {
            seconds: 12,
            public_key: "KEY+/abc=".into()
        }
    );
    assert_eq!(
        classify(
            &connect("the endpoint name did not resolve in time".into()),
            WAIT
        ),
        ProxyFault::NameNotResolved
    );
    let refused = io::Error::from_raw_os_error(libc::ECONNREFUSED);
    assert_eq!(
        classify(&connect(format!("{WIREGUARD_CARRIER}{refused}")), WAIT),
        ProxyFault::ConnectionRefused
    );
    assert_eq!(
        classify(
            &connect("the tunnel's network stack stopped before the handshake completed".into()),
            WAIT
        ),
        ProxyFault::WireGuardFailed {
            detail: "the tunnel's network stack stopped before the handshake completed".into()
        }
    );
    let no_dns = TunnelError::Configuration(format!(
        "{WIREGUARD_NO_DNS}, so names cannot be resolved on the far side"
    ));
    assert_eq!(classify(&no_dns, WAIT), ProxyFault::WireGuardNoDns);
}

#[test]
fn http3_failures_are_named() {
    let connect = |detail: &str| TunnelError::Http3Connect {
        host: "h3.example".into(),
        port: 443,
        detail: detail.into(),
    };
    assert_eq!(
        classify(&connect("proxy endpoint DNS lookup failed"), WAIT),
        ProxyFault::NameNotResolved
    );
    assert_eq!(
        classify(&connect("HTTP/3 dial deadline exceeded"), WAIT),
        ProxyFault::UdpTimedOut { seconds: 30 }
    );
    assert_eq!(
        classify(&connect("QUIC handshake failed"), WAIT),
        ProxyFault::Http3HandshakeFailed
    );
    assert_eq!(
        classify(&connect("invalid or interrupted CONNECT response"), WAIT),
        ProxyFault::NotAProxy { protocol: "HTTP/3" }
    );
    assert_eq!(
        classify(&connect("stream reset"), WAIT),
        ProxyFault::Http3Failed {
            detail: "stream reset".into()
        }
    );
    let tls = TunnelError::Http3Tls {
        host: "h3.example".into(),
        port: 443,
    };
    assert_eq!(classify(&tls, WAIT), ProxyFault::Http3Tls);
    assert_eq!(
        classify(&TunnelError::Http3ProxyStatus { status: 407 }, WAIT),
        ProxyFault::CredentialsRejected
    );
    assert_eq!(
        classify(&TunnelError::Http3ProxyStatus { status: 403 }, WAIT),
        ProxyFault::ConnectRefused { status: 403 }
    );
}

#[test]
fn an_egress_that_cannot_route_names_its_interface() {
    let unreachable = io::Error::from_raw_os_error(libc::ENETUNREACH);
    let error = io::Error::other(format!("{}eth1: {unreachable}", pipe::NO_ROUTE_VIA));
    assert_eq!(
        from_io(&error, WAIT),
        ProxyFault::NoRouteVia { via: "eth1".into() }
    );
    let bind = DialError::Bind(io::Error::from_raw_os_error(libc::EADDRNOTAVAIL));
    assert!(matches!(
        bind.fault(WAIT),
        ProxyFault::EgressUnusable { .. }
    ));
}

#[test]
fn a_hop_error_is_read_through_its_source() {
    let error = DialError::Hop {
        proxy: 7,
        source: hop_before("192.0.2.2", 8080, Reply::Socks(5)),
    };
    assert_eq!(
        error.fault(WAIT),
        ProxyFault::DestinationRefused {
            target: target("192.0.2.2:8080")
        }
    );
    let carried = io::Error::other(TunnelError::Engine(Said::HttpAuthRequired.text().into()));
    assert_eq!(from_io(&carried, WAIT), ProxyFault::AuthRequired);
}

#[test]
fn anything_else_keeps_its_own_words() {
    let error = TunnelError::Engine("tunnel provider stopped".into());
    assert_eq!(
        classify(&error, WAIT),
        ProxyFault::Other {
            detail: "tunnel provider stopped".into()
        }
    );
    assert_eq!(
        classify(&error, WAIT).to_string(),
        "tunnel provider stopped"
    );
    let error = io::Error::other("something new");
    assert_eq!(
        from_io(&error, WAIT),
        ProxyFault::Other {
            detail: "something new".into()
        }
    );
}

#[test]
fn params_carry_the_names_the_sentences_use() {
    let fault = ProxyFault::WireGuardNoHandshake {
        seconds: 12,
        public_key: "KEY".into(),
    };
    assert_eq!(fault.code(), "WIREGUARD_NO_HANDSHAKE");
    assert_eq!(
        fault.params(),
        vec![
            ("seconds", Param::Number(12)),
            ("publicKey", Param::Text("KEY".into()))
        ]
    );
    assert_eq!(
        ProxyFault::SshKeyNotEd25519 { algorithm: None }.params(),
        Vec::new()
    );
    assert_eq!(
        ProxyFault::ConnectRefused { status: 405 }.params(),
        vec![("status", Param::Number(405))]
    );
}

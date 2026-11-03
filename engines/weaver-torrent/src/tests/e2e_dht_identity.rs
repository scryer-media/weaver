// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use tempfile::TempDir;

use crate::core::peer_id::generate_azereus_style;
use crate::{
    DhtSessionConfig, Session, SessionOptions,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, SocketFactory,
        StreamListener,
    },
};

/// Binds every datagram socket on loopback, whatever address is asked for, and
/// opens no stream sockets: the session's DHT stays on 127.0.0.1.
struct LoopbackUdpOnly;

impl SocketFactory for LoopbackUdpOnly {
    fn connect_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async { Err(io::ErrorKind::Unsupported.into()) })
    }

    fn bind_udp<'a>(
        &'a self,
        _addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async {
            crate::test_support::sockets()
                .bind_udp((Ipv4Addr::LOCALHOST, 0).into())
                .await
        })
    }
}

#[tokio::test]
async fn test_session_peer_id_fingerprint_is_the_dht_node_id_prefix() -> anyhow::Result<()> {
    // The DHT's one bootstrap node: a loopback socket nobody answers on.
    let bootstrap = crate::test_support::loopback_udp().await;
    let dir = TempDir::with_prefix("test_session_dht_identity")?;
    let peer_id = generate_azereus_style(*b"XY", (1, 2, 3, 4));

    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            identity: Some(crate::Identity {
                peer_id,
                ..Default::default()
            }),
            dht: Some(DhtSessionConfig {
                bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
                port: None,
                persistence: None,
            }),
            persistence: None,
            disable_local_service_discovery: true,
            socket_factory: Arc::new(LoopbackUdpOnly),
            ..crate::test_support::session_options()
        },
    )
    .await?;

    let dht = session.get_dht().expect("the session runs a DHT");
    let node_id = dht.stats().id;
    assert_eq!(&node_id.0[..8], b"-XY1234-");
    assert_ne!(node_id, peer_id, "only the fingerprint is shared");

    // Its first query, to the bootstrap node, carries the client version (BEP 5 "v").
    let mut buf = [0u8; 2048];
    let query = loop {
        let (len, from) = bootstrap.recv_from(&mut buf).await?;
        if from == dht.listen_addr() {
            break &buf[..len];
        }
    };
    let v = b"1:v4:XY\x01\x02";
    assert!(
        query.windows(v.len()).any(|w| w == v),
        "no client version in {query:?}"
    );
    session.cancellation_token().cancel();
    Ok(())
}

// A session named as another client, or as this build, carries that client's
// fingerprint as its DHT node id prefix and its letters and version as "v".
#[tokio::test]
async fn test_a_session_names_its_client_to_the_dht() -> anyhow::Result<()> {
    let weaver = crate::Identity::default();
    let (major, minor, _, _) = crate::crate_version!();
    let mut weaver_prefix = [0u8; 8];
    weaver_prefix.copy_from_slice(&weaver.peer_id.0[..8]);
    for (identity, prefix, v) in [
        (
            crate::Identity {
                peer_id: generate_azereus_style(*b"qB", (5, 2, 3, 0)),
                tracker_key: 1,
                version: "qBittorrent/5.2.3".into(),
                user_agent: "qBittorrent/5.2.3".into(),
                tracker_ids: Default::default(),
            },
            *b"-qB5230-",
            b"1:v4:qB\x05\x02".to_vec(),
        ),
        (
            weaver,
            weaver_prefix,
            [b"1:v4:WV".as_slice(), &[major, minor]].concat(),
        ),
    ] {
        let bootstrap = crate::test_support::loopback_udp().await;
        let dir = TempDir::with_prefix("test_session_dht_client")?;
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                identity: Some(identity),
                dht: Some(DhtSessionConfig {
                    bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
                    port: None,
                    persistence: None,
                }),
                persistence: None,
                disable_local_service_discovery: true,
                socket_factory: Arc::new(LoopbackUdpOnly),
                ..crate::test_support::session_options()
            },
        )
        .await?;
        let dht = session.get_dht().expect("the session runs a DHT");
        assert_eq!(&dht.stats().id.0[..8], &prefix);
        let mut buf = [0u8; 2048];
        let query = loop {
            let (len, from) = bootstrap.recv_from(&mut buf).await?;
            if from == dht.listen_addr() {
                break buf[..len].to_vec();
            }
        };
        assert!(
            query.windows(v.len()).any(|w| w == v.as_slice()),
            "no {v:?} in {query:?}"
        );
        session.cancellation_token().cancel();
    }
    Ok(())
}

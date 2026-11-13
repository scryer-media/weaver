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

// Whichever client a session goes as, its DHT is Weaver's own: a node id
// that shares nothing with the peer id, and Weaver's letters and version as
// "v".
#[tokio::test]
async fn test_a_sessions_dht_is_weavers_whatever_client_it_goes_as() -> anyhow::Result<()> {
    let (major, minor, _, _) = crate::crate_version!();
    let v = [b"1:v4:WV".as_slice(), &[major, minor]].concat();
    for identity in [
        crate::Identity {
            peer_id: generate_azereus_style(*b"qB", (5, 3, 0, 0)),
            tracker_key: 1,
            version: crate::tracker::qbittorrent::VERSION.into(),
            user_agent: crate::tracker::qbittorrent::USER_AGENT.into(),
            tracker_ids: Default::default(),
            completion: Default::default(),
            shape: &crate::tracker::qbittorrent::SHAPE,
        },
        crate::Identity::default(),
    ] {
        let peer_id = identity.peer_id;
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
        assert!(!dht.stats().id.0.starts_with(&peer_id.0[..8]));
        // Its first query, to the bootstrap node, carries the client version
        // (BEP 5 "v").
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

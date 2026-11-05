// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io::Cursor,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};

use super::{Harness, MemoryStorageFactory, PeerSourceFlag, isolated_session_opts};
use crate::{
    IpFilter, IpRanges, SessionOptions, peer_info_reader::tests::Dialed, storage::StorageFactory,
};

const MAPPED_LIST: &[u8] = b"mapped:::ffff:127.0.0.1-::ffff:127.0.0.1\n";

fn peers() -> [SocketAddr; 2] {
    [
        (Ipv4Addr::LOCALHOST, 6881).into(),
        (Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 6881).into(),
    ]
}

#[tokio::test(start_paused = true)]
async fn explicit_mapped_block_ranges_refuse_both_peer_representations_before_admission() {
    for parsed in [false, true] {
        let blocked = if parsed {
            IpRanges::create_from_stream(&mut Cursor::new(MAPPED_LIST))
                .await
                .unwrap()
        } else {
            IpRanges::new(
                [],
                std::iter::once(
                    Ipv4Addr::LOCALHOST.to_ipv6_mapped()
                        ..Ipv4Addr::new(127, 0, 0, 2).to_ipv6_mapped(),
                ),
            )
        };
        let (sent, mut dialed) = tokio::sync::mpsc::unbounded_channel();
        let harness = Harness::build_on(
            "mapped_block_entries",
            0,
            SessionOptions {
                socket_factory: Arc::new(Dialed(sent)),
                ip_filter: IpFilter {
                    blocked: Arc::new(blocked),
                    ..Default::default()
                },
                ..isolated_session_opts(false)
            },
            MemoryStorageFactory::default().clone_box(),
        )
        .await;
        for peer in peers() {
            harness.add_peer(peer, PeerSourceFlag::Tracker);
            assert!(
                harness.peers.with_peer(peer, |_| ()).is_none(),
                "{peer}, parsed={parsed}"
            );
        }
        assert!(dialed.try_recv().is_err());
        let allowed = (Ipv4Addr::new(127, 0, 0, 2), 6881).into();
        harness.add_peer(allowed, PeerSourceFlag::Tracker);
        assert!(harness.peers.with_peer(allowed, |_| ()).is_some());
        harness._session.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_parsed_mapped_allowlist_admits_both_representations_and_refuses_other_ipv4_peers() {
    let directory = tempfile::TempDir::new().unwrap();
    let path = directory.path().join("allow.txt");
    std::fs::write(&path, MAPPED_LIST).unwrap();
    let (sent, _dialed) = tokio::sync::mpsc::unbounded_channel();
    let harness = Harness::build_on(
        "mapped_allow_entries",
        0,
        SessionOptions {
            socket_factory: Arc::new(Dialed(sent)),
            allowlist_url: Some(url::Url::from_file_path(path).unwrap().to_string()),
            ..isolated_session_opts(false)
        },
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let refused = (Ipv4Addr::new(127, 0, 0, 2), 6881).into();
    harness.add_peer(refused, PeerSourceFlag::Tracker);
    assert!(harness.peers.with_peer(refused, |_| ()).is_none());
    for peer in peers() {
        harness.add_peer(peer, PeerSourceFlag::Tracker);
        assert!(harness.peers.with_peer(peer, |_| ()).is_some(), "{peer}");
    }
    harness._session.stop().await;
}

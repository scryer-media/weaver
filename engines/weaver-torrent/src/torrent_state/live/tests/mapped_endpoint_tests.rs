// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{net::Ipv4Addr, sync::Arc};

use super::{Harness, MemoryStorageFactory, PeerSourceFlag, isolated_session_opts};
use crate::{
    IpFilter, IpRanges, SessionOptions, peer_info_reader::tests::Dialed, storage::StorageFactory,
};

#[tokio::test(start_paused = true)]
async fn an_ipv4_blocked_peer_is_refused_in_both_address_representations_before_dialing() {
    let (sent, mut dialed) = tokio::sync::mpsc::unbounded_channel();
    let options = SessionOptions {
        socket_factory: Arc::new(Dialed(sent)),
        ip_filter: IpFilter {
            blocked: Arc::new(IpRanges::new(
                std::iter::once(Ipv4Addr::LOCALHOST..Ipv4Addr::new(127, 0, 0, 2)),
                [],
            )),
            ..Default::default()
        },
        ..isolated_session_opts(false)
    };
    let harness = Harness::build_on(
        "mapped_peer_filter",
        0,
        options,
        MemoryStorageFactory::default().clone_box(),
    )
    .await;
    let ipv4 = (Ipv4Addr::LOCALHOST, 6881).into();
    let mapped = (Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 6881).into();
    for address in [ipv4, mapped] {
        harness.add_peer(address, PeerSourceFlag::Tracker);
        assert!(harness.peers.with_peer(address, |_| ()).is_none());
    }
    assert!(dialed.try_recv().is_err());

    harness._session.set_ip_filter(IpFilter::default());
    harness.add_peer(mapped, PeerSourceFlag::Tracker);
    assert!(harness.peers.with_peer(mapped, |_| ()).is_some());
    harness._session.stop().await;
}

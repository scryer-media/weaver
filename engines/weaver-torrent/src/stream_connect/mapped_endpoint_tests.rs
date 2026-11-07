// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV6},
    sync::Arc,
};

use super::{EncryptionPolicy, StreamConnector, StreamConnectorArgs, Transports};
use crate::peer_info_reader::tests::Dialed;

#[tokio::test(start_paused = true)]
async fn an_ipv4_only_connector_dials_a_mapped_peer_as_ipv4() -> anyhow::Result<()> {
    let (sent, mut dialed) = tokio::sync::mpsc::unbounded_channel();
    let connector = StreamConnector::new(StreamConnectorArgs {
        enable_tcp: true,
        utp_socket: None,
        utp_rebind: None,
        ipv4_only: true,
        socket_factory: Arc::new(Dialed(sent)),
        encryption: EncryptionPolicy::Disabled,
    })
    .await?;
    let ipv4 = SocketAddr::from((Ipv4Addr::LOCALHOST, 6881));
    let mapped = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), 6881));
    let _connection = connector.connect(mapped, Transports::Any).await?;
    assert_eq!(dialed.try_recv()?.0, ipv4);
    let stats = connector.stats().tcp.snapshot();
    assert_eq!(stats.v4.successes, 1);
    assert_eq!(stats.v6.attempts, 0);
    assert!(
        connector
            .connect("[2001:db8::1]:6881".parse()?, Transports::Any)
            .await
            .is_err()
    );
    assert!(dialed.try_recv().is_err());
    assert_eq!(connector.stats().tcp.snapshot().v6.attempts, 0);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn a_true_ipv6_dial_preserves_its_scope_and_flow_information() -> anyhow::Result<()> {
    let (sent, mut dialed) = tokio::sync::mpsc::unbounded_channel();
    let connector = StreamConnector::new(StreamConnectorArgs {
        enable_tcp: true,
        utp_socket: None,
        utp_rebind: None,
        ipv4_only: false,
        socket_factory: Arc::new(Dialed(sent)),
        encryption: EncryptionPolicy::Disabled,
    })
    .await?;
    let address = SocketAddr::V6(SocketAddrV6::new("fe80::1234".parse()?, 6881, 17, 9));
    let _connection = connector.connect(address, Transports::Any).await?;
    assert_eq!(dialed.try_recv()?.0, address);
    assert_eq!(connector.stats().tcp.snapshot().v6.successes, 1);
    Ok(())
}

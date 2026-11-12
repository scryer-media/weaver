// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use weaver_utp::{DefaultUtpEnvironment, SocketOpts, UtpSocket};

use super::{DatagramSocket, DatagramTransport, KernelSockets, PairedUdp, bind_pair};

#[tokio::test]
async fn a_mapped_peer_transfers_both_ways_through_a_same_port_datagram_pair() -> anyhow::Result<()>
{
    let factory = KernelSockets::new(None)?;
    let (v6, v4) = bind_pair(
        (Ipv6Addr::LOCALHOST, 0).into(),
        |address, reuse| factory.kernel_udp(address, reuse),
        |socket| socket.bind_addr().port(),
    )?;
    assert_eq!(v6.bind_addr().port(), v4.bind_addr().port());
    let local = SocketAddr::from((Ipv4Addr::LOCALHOST, v4.bind_addr().port()));
    let paired = Arc::new(PairedUdp { v6, v4 });
    let peer_transport = crate::test_support::udp((Ipv4Addr::LOCALHOST, 0)).await;
    let peer_address = peer_transport.bind_addr();
    let mapped_peer = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), peer_address.port()));
    let options = || SocketOpts {
        dont_wait_for_lastack: true,
        ..Default::default()
    };
    let ours = UtpSocket::new_with_opts(
        DatagramTransport::alone(paired),
        DefaultUtpEnvironment::default(),
        options(),
    )?;
    let theirs = UtpSocket::new_with_opts(
        DatagramTransport::alone(peer_transport),
        DefaultUtpEnvironment::default(),
        options(),
    )?;
    let (connected, accepted) = tokio::try_join!(ours.connect(mapped_peer), theirs.accept())?;
    assert_eq!(connected.remote_addr(), peer_address);
    assert_eq!(accepted.remote_addr(), local);

    let outgoing: Vec<_> = (0usize..65_673)
        .map(|n| (n * 37 + n / 251).to_le_bytes()[0])
        .collect();
    let returning: Vec<_> = (0usize..32_891)
        .map(|n| (n * 19 + n / 127).to_le_bytes()[0])
        .collect();
    let (mut our_read, mut our_write) = connected.split();
    let (mut their_read, mut their_write) = accepted.split();
    let send = async {
        our_write.write_all(&outgoing).await?;
        our_write.flush().await?;
        our_write.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let reply = async {
        their_write.write_all(&returning).await?;
        their_write.flush().await?;
        their_write.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let receive = async {
        let mut bytes = Vec::new();
        our_read.read_to_end(&mut bytes).await?;
        anyhow::ensure!(bytes == returning, "return payload changed");
        Ok::<_, anyhow::Error>(())
    };
    let receive_peer = async {
        let mut bytes = Vec::new();
        their_read.read_to_end(&mut bytes).await?;
        anyhow::ensure!(bytes == outgoing, "outgoing payload changed");
        Ok::<_, anyhow::Error>(())
    };
    tokio::try_join!(send, reply, receive, receive_peer)?;
    let (ours, theirs) = tokio::join!(ours.shutdown(), theirs.shutdown());
    ours?;
    theirs?;
    Ok(())
}

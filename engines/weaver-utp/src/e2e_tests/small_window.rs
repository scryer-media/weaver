// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    num::NonZeroUsize,
    time::Duration,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::link;
use crate::{DefaultUtpEnvironment, SocketOpts, UtpSocket};

async fn transfer(addresses: [SocketAddr; 2], window: usize) -> anyhow::Result<()> {
    let (left, right) = link(addresses[0], addresses[1], 0, Duration::ZERO);
    let options = || SocketOpts {
        vsock_rx_bufsize_bytes: NonZeroUsize::new(window),
        dont_wait_for_lastack: true,
        ..Default::default()
    };
    let client = UtpSocket::new_with_opts(left, DefaultUtpEnvironment::default(), options())?;
    let server = UtpSocket::new_with_opts(right, DefaultUtpEnvironment::default(), options())?;
    let (connected, accepted) = tokio::try_join!(client.connect(addresses[1]), server.accept())?;
    let payload: Vec<_> = (0usize..4097)
        .map(|n| (n * 17 + n / 31).to_le_bytes()[0])
        .collect();
    let reply: Vec<_> = payload.iter().rev().copied().collect();
    let (mut client_read, mut client_write) = connected.split();
    let (mut server_read, mut server_write) = accepted.split();
    let send = async {
        client_write.write_all(&payload).await?;
        client_write.flush().await?;
        client_write.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let reply_sent = async {
        server_write.write_all(&reply).await?;
        server_write.flush().await?;
        server_write.shutdown().await?;
        Ok::<_, anyhow::Error>(())
    };
    let receive = async {
        let mut bytes = Vec::new();
        server_read.read_to_end(&mut bytes).await?;
        anyhow::ensure!(bytes == payload, "payload changed");
        Ok::<_, anyhow::Error>(())
    };
    let reply_received = async {
        let mut bytes = Vec::new();
        client_read.read_to_end(&mut bytes).await?;
        anyhow::ensure!(bytes == reply, "reply changed");
        Ok::<_, anyhow::Error>(())
    };
    tokio::try_join!(send, reply_sent, receive, reply_received)?;
    let (client, server) = tokio::join!(client.shutdown(), server.shutdown());
    client?;
    server?;
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn ipv4_streams_with_128_byte_receive_buffers_transfer_and_flush() -> anyhow::Result<()> {
    transfer(
        [
            (Ipv4Addr::LOCALHOST, 1).into(),
            (Ipv4Addr::LOCALHOST, 2).into(),
        ],
        128,
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn ipv6_streams_with_1024_byte_receive_buffers_transfer_and_flush() -> anyhow::Result<()> {
    transfer(
        [
            (Ipv6Addr::LOCALHOST, 1).into(),
            (Ipv6Addr::LOCALHOST, 2).into(),
        ],
        1024,
    )
    .await
}

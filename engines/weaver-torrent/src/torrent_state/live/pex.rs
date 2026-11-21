// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use crate::buffers::ByteBufOwned;
use crate::peer_protocol::extended::ut_pex::{PexPeerInfo, UtPex};
use tracing::trace;

use crate::peer_connection::WriterRequest;

use super::{peer::PeerTx, peers::holepunch::is_local};

use super::{TorrentStateLive, pex_interval};

impl TorrentStateLive {
    /// The next PEX message to the peer at `to`, which our earlier ones
    /// told of the peers in `told`, and `told` brought up to it; None when
    /// nothing changed (libtorrent `ut_pex_plugin::tick`,
    /// `ut_pex_peer_plugin::send_ut_peer_diff`). It adds the peers we list
    /// that `to` was not told of, and drops those told of that we no longer
    /// list. A peer on a local network is not told to one that is not: it
    /// drops them (`ut_pex_peer_plugin::on_extended`).
    pub(super) fn next_pex_message(
        &self,
        to: SocketAddr,
        told: &mut HashSet<SocketAddr>,
    ) -> Option<UtPex<ByteBufOwned>> {
        let (added, dropped) = pex_diff(&self.peers.pex_listed(), to, told)?;
        Some(UtPex::from_peers(added.into_iter(), dropped.into_iter()))
    }

    pub(super) async fn task_send_pex_to_peer(
        self: Arc<Self>,
        this_peer_addr: SocketAddr,
        tx: PeerTx,
    ) -> anyhow::Result<()> {
        // As per BEP 11 recommended interval is min 60 seconds
        const PEX_MESSAGE_INTERVAL: Duration = Duration::from_secs(60);

        let mut told = HashSet::new();

        // Wait 10 seconds before sending the first message to assure that peer will stay with us
        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut interval = pex_interval(PEX_MESSAGE_INTERVAL);

        loop {
            interval.tick().await;

            // This task should die with the cancellation token, but check defensively just in case.
            if tx.is_closed() {
                return Ok(());
            }

            if let Some(pex_msg) = self.next_pex_message(this_peer_addr, &mut told) {
                trace!(?pex_msg, "sending PEX");
                if tx.send(WriterRequest::UtPex(pex_msg)).is_err() {
                    return Ok(()); // Peer disconnected
                }
            }
        }
    }
}

// BEP 11: at most 50 added and 50 dropped peers in one message.
pub(crate) const MAX_SENT_PEX_PEERS: usize = 50;

// The peers the next PEX message to `to` adds and drops, given the peers we
// list and those our earlier messages to `to` told of in `told`, and `told`
// brought up to it; None when nothing changed. See
// [`TorrentStateLive::next_pex_message`].
pub(crate) fn pex_diff(
    listed: &HashMap<SocketAddr, u8>,
    to: SocketAddr,
    told: &mut HashSet<SocketAddr>,
) -> Option<(Vec<PexPeerInfo>, Vec<SocketAddr>)> {
    let shown = |addr: &SocketAddr| is_local(to.ip()) || !is_local(addr.ip());
    let added: Vec<PexPeerInfo> = listed
        .iter()
        .filter(|(addr, _)| shown(addr) && !told.contains(*addr))
        .take(MAX_SENT_PEX_PEERS)
        .map(|(&addr, &flags)| PexPeerInfo { flags, addr })
        .collect();
    let dropped: Vec<SocketAddr> = told
        .iter()
        .filter(|addr| !listed.contains_key(*addr))
        .take(MAX_SENT_PEX_PEERS)
        .copied()
        .collect();
    if added.is_empty() && dropped.is_empty() {
        return None;
    }
    for addr in &dropped {
        told.remove(addr);
    }
    told.extend(added.iter().map(|p| p.addr));
    Some((added, dropped))
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::HashSet, future::Future, net::SocketAddr, sync::Arc};

use crate::buffers::ByteBufOwned;
use crate::core::torrent_metainfo::TorrentMetaV1Info;
use anyhow::Context;
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use tokio::time::Instant;
use tracing::{Instrument, debug, debug_span};

use crate::core::hash_id::{Id20, InfoHashes};
use crate::{
    peer_connection::PeerConnectionOptions,
    peer_info_reader,
    spawn_utils::BlockingSpawner,
    stream_connect::StreamConnector,
    torrent_state::live::peers::{NextCandidate, PeerStates},
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadMetainfoResult<Rx> {
    Found {
        info: TorrentMetaV1Info<ByteBufOwned>,
        info_bytes: ByteBufOwned,
        rx: Rx,
        seen: HashSet<SocketAddr>,
    },
    ChannelClosed {
        #[allow(dead_code)]
        seen: HashSet<SocketAddr>,
    },
}

pub async fn read_metainfo_from_peer_receiver<A: Stream<Item = SocketAddr> + Unpin>(
    peer_id: Id20,
    info_hashes: InfoHashes,
    initial_addrs: Vec<SocketAddr>,
    addrs_stream: A,
    peer_connection_options: Option<PeerConnectionOptions>,
    connector: Arc<StreamConnector>,
    client_name_and_version: String,
) -> ReadMetainfoResult<A> {
    let semaphore = tokio::sync::Semaphore::new(128);

    let read_info_guarded = |addr| {
        let semaphore = &semaphore;
        let connector = connector.clone();
        let client_name_and_version = client_name_and_version.clone();
        async move {
            let token = semaphore.acquire().await?;
            let ret = peer_info_reader::read_metainfo_from_peer(
                addr,
                peer_id,
                info_hashes,
                peer_connection_options,
                // This shouldn't be called anyway as we aren't reading/writing to disk, so it's
                // ok not to use a shared one.
                BlockingSpawner::new(1),
                connector,
                client_name_and_version,
            )
            .instrument(debug_span!("read_metainfo_from_peer", ?addr))
            .await
            .with_context(|| format!("error reading metainfo from {addr}"));
            drop(token);
            ret
        }
    };

    match first_answer(initial_addrs, addrs_stream, read_info_guarded).await {
        (Some((info, info_bytes)), rx, seen) => ReadMetainfoResult::Found {
            info,
            info_bytes,
            rx,
            seen,
        },
        (None, _, seen) => ReadMetainfoResult::ChannelClosed { seen },
    }
}

/// The first answer `ask` gets from a peer of `initial_addrs` or
/// `addrs_stream`, with the stream and every address seen. A peer that does
/// not answer is a connection that failed: it is asked again when the peer
/// list's reconnect time for it comes, until it has failed too often, and
/// sooner when someone names it again (libtorrent `peer_list`; a magnet's
/// metadata comes from the torrent's ordinary connections). None once the
/// stream has ended and no peer is left to ask.
async fn first_answer<A, T, F, Fut>(
    initial_addrs: Vec<SocketAddr>,
    mut addrs: A,
    ask: F,
) -> (Option<T>, A, HashSet<SocketAddr>)
where
    A: Stream<Item = SocketAddr> + Unpin,
    F: Fn(SocketAddr) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let peers = PeerStates::new(Default::default());
    let mut seen = HashSet::<SocketAddr>::new();
    let mut name = |addr| {
        seen.insert(addr);
        peers.add_peer(addr);
    };
    for addr in initial_addrs {
        name(addr);
    }

    let mut asking = FuturesUnordered::new();
    let mut addrs_completed = false;
    loop {
        while let Some((addr, _, _)) = peers.connect_next(Instant::now()) {
            let answer = ask(addr);
            asking.push(async move { (addr, answer.await) });
        }
        if addrs_completed
            && asking.is_empty()
            && peers.next_candidate(Instant::now()) == NextCandidate::None
        {
            return (None, addrs, seen);
        }

        tokio::select! {
            Some((addr, answer)) = asking.next(), if !asking.is_empty() => match answer {
                Ok(found) => return (Some(found), addrs, seen),
                Err(e) => {
                    debug!("{e:#}");
                    peers.with_peer_mut(addr, "metadata_failed", |peer| {
                        peers.connection_closed(peer, Some(Instant::now()), true)
                    });
                }
            },
            next_addr = addrs.next(), if !addrs_completed => match next_addr {
                Some(addr) => name(addr),
                None => addrs_completed = true,
            },
            _ = peers.wait_for_candidate() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::peer_id::generate_peer_id;
    use crate::dht::{DhtBuilder, Id20};

    use super::*;
    use std::{
        str::FromStr,
        sync::{Arc, Once},
    };

    static LOG_INIT: Once = Once::new();

    fn init_logging() {
        #[allow(unused_must_use)]
        LOG_INIT.call_once(|| {
            // pretty_env_logger::try_init();
        })
    }

    use crate::torrent_state::live::peer::{MAX_FAILCOUNT, MIN_RECONNECT_TIME};
    use std::sync::Mutex;

    /// Asks a peer for metadata: records when, and answers once it has been
    /// asked `fails + 1` times.
    fn asker(
        fails: usize,
    ) -> (
        Arc<Mutex<Vec<(SocketAddr, Instant)>>>,
        impl Fn(SocketAddr) -> std::future::Ready<anyhow::Result<SocketAddr>>,
    ) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let log = asked.clone();
        let ask = move |addr| {
            let mut log = log.lock().unwrap();
            log.push((addr, Instant::now()));
            let times = log.iter().filter(|(a, _)| *a == addr).count();
            std::future::ready(if times > fails {
                Ok(addr)
            } else {
                Err(anyhow::anyhow!("{addr} sent no metadata"))
            })
        };
        (asked, ask)
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_sent_no_metadata_is_asked_again_once_its_reconnect_time_comes() {
        let (asked, ask) = asker(1);
        let (found, _, seen) = first_answer(vec![addr(1)], futures::stream::pending(), ask).await;

        assert_eq!(found, Some(addr(1)));
        assert_eq!(seen, HashSet::from([addr(1)]));
        let asked = asked.lock().unwrap();
        assert_eq!(asked.len(), 2);
        // libtorrent `peer_list`: (failcount + 1) reconnect times after the failure.
        assert!(asked[1].1 >= asked[0].1 + MIN_RECONNECT_TIME * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_named_while_another_failed_is_asked_at_once() {
        let (asked, ask) = asker(1);
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tx.unbounded_send(addr(2)).unwrap();
        let (found, _, seen) = first_answer(vec![addr(1)], rx, ask).await;

        assert_eq!(seen, HashSet::from([addr(1), addr(2)]));
        let asked = asked.lock().unwrap();
        // The second peer is asked when it is named, before the first one's
        // reconnect time; both are asked again when theirs comes.
        assert_eq!(asked[0], (addr(1), asked[0].1));
        assert_eq!(asked[1], (addr(2), asked[0].1));
        assert!(
            asked[2..]
                .iter()
                .all(|(_, at)| *at >= asked[0].1 + MIN_RECONNECT_TIME * 2)
        );
        assert!(found.is_some_and(|found| asked[2..].iter().any(|(a, _)| *a == found)));
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn no_metadata_once_every_peer_failed_too_often_and_no_more_are_named() {
        let (asked, ask) = asker(usize::MAX);
        let (found, _, _) =
            first_answer(vec![addr(1), addr(2)], futures::stream::empty(), ask).await;

        assert_eq!(found, None);
        let asked = asked.lock().unwrap();
        for peer in [addr(1), addr(2)] {
            let times = asked.iter().filter(|(a, _)| *a == peer).count();
            assert_eq!(times, MAX_FAILCOUNT as usize, "{peer}: {asked:?}");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn read_metainfo_from_dht() {
        init_logging();

        let info_hash = Id20::from_str("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        let dht = DhtBuilder::new().await.unwrap();

        let peer_rx = dht.get_peers(info_hash, None);
        let peer_id = generate_peer_id(b"-xx1234-");
        match read_metainfo_from_peer_receiver(
            peer_id,
            InfoHashes {
                v1: Some(info_hash),
                v2: None,
            },
            Vec::new(),
            peer_rx,
            None,
            Arc::new(StreamConnector::new(Default::default()).await.unwrap()),
            crate::client_name_and_version().to_owned(),
        )
        .await
        {
            ReadMetainfoResult::Found { info, .. } => dbg!(info),
            ReadMetainfoResult::ChannelClosed { .. } => todo!("should not have happened"),
        };
    }
}

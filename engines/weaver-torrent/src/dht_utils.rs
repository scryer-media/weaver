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
    peer_info_reader::{self, MetadataFetch},
    spawn_utils::BlockingSpawner,
    stream_connect::StreamConnector,
    torrent_state::live::peers::{NextCandidate, PeerList, PeerStates},
    type_aliases::PeerSourceFlag,
};

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ReadMetainfoResult<Rx> {
    Found {
        info: TorrentMetaV1Info<ByteBufOwned>,
        info_bytes: ByteBufOwned,
        rx: Rx,
        seen: HashSet<SocketAddr>,
        /// The torrent's peer list, as the metadata connections left it.
        peers: PeerList,
    },
    ChannelClosed {
        #[allow(dead_code)]
        seen: HashSet<SocketAddr>,
    },
}

pub async fn read_metainfo_from_peer_receiver<
    A: Stream<Item = (SocketAddr, PeerSourceFlag)> + Unpin,
>(
    peer_id: Id20,
    info_hashes: InfoHashes,
    peers: PeerList,
    addrs_stream: A,
    unable: &tokio::sync::watch::Sender<HashSet<SocketAddr>>,
    peer_connection_options: Option<PeerConnectionOptions>,
    connector: Arc<StreamConnector>,
    client_name_and_version: String,
) -> ReadMetainfoResult<A> {
    let semaphore = tokio::sync::Semaphore::new(128);
    // The peers asked fill one buffer between them.
    let fetch = MetadataFetch::new(info_hashes);

    let read_info_guarded = |addr, peers: &Arc<PeerStates>| {
        let semaphore = &semaphore;
        let peers = peers.clone();
        let fetch = fetch.clone();
        let connector = connector.clone();
        let client_name_and_version = client_name_and_version.clone();
        async move {
            let token = semaphore.acquire().await?;
            let ret = peer_info_reader::read_metainfo_from_peer(
                addr,
                peers,
                peer_id,
                fetch,
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

    match first_answer(peers, addrs_stream, unable, read_info_guarded).await {
        (Some((info, info_bytes)), rx, seen, peers) => ReadMetainfoResult::Found {
            info,
            info_bytes,
            rx,
            seen,
            peers,
        },
        (None, _, seen, _) => ReadMetainfoResult::ChannelClosed { seen },
    }
}

/// The first answer `ask` gets from a peer on `list` or named by
/// `addrs_stream`, with the stream, every address it named, and the list as
/// the connections left it. The list is the torrent's: its peers' memory
/// carries on into the torrent's own connections (libtorrent
/// `torrent::set_metadata` keeps `m_peer_list`; a magnet's metadata comes
/// from the torrent's ordinary connections). A peer that does not answer is
/// a connection that failed: it is asked again when the list's reconnect
/// time for it comes, until it has failed too often, and sooner when a
/// tracker names it again (`peer_list`). `ask` is handed the list, so the
/// connection keeps the peer's memory there. Each peer that did not answer,
/// or that the list did not take, joins `unable`. None once the stream has
/// ended and no peer is left to ask.
async fn first_answer<A, T, F, Fut>(
    list: PeerList,
    mut addrs: A,
    unable: &tokio::sync::watch::Sender<HashSet<SocketAddr>>,
    ask: F,
) -> (Option<T>, A, HashSet<SocketAddr>, PeerList)
where
    A: Stream<Item = (SocketAddr, PeerSourceFlag)> + Unpin,
    F: Fn(SocketAddr, &Arc<PeerStates>) -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    // No metadata yet, so no pieces to count availability over. Each answer
    // comes over a connection of its own, wherever its peer is.
    let peers = Arc::new(
        PeerStates::founded_on(list, Default::default(), 0).allowing_multiple_connections_per_ip(
            Arc::new(std::sync::atomic::AtomicBool::new(true)),
        ),
    );
    let mut seen = HashSet::<SocketAddr>::new();
    let found_unable = |addr| unable.send_if_modified(|unable| unable.insert(addr));
    let mut name = |addr, source| {
        seen.insert(addr);
        peers.add_peer(addr, source);
        if peers.with_peer(addr, |_| ()).is_none() {
            found_unable(addr);
        }
    };

    let mut asking = FuturesUnordered::new();
    let mut addrs_completed = false;
    let found = loop {
        while let Some((addr, _, _)) = peers.connect_next(Instant::now()) {
            let answer = ask(addr, &peers);
            asking.push(async move { (addr, answer.await) });
        }
        if addrs_completed
            && asking.is_empty()
            && peers.next_candidate(Instant::now()) == NextCandidate::None
        {
            break None;
        }

        tokio::select! {
            Some((addr, answer)) = asking.next(), if !asking.is_empty() => match answer {
                Ok(found) => break Some(found),
                Err(e) => {
                    debug!("{e:#}");
                    peers.with_peer_mut(addr, "metadata_failed", |peer| {
                        peers.connection_closed(peer, Instant::now(), true, false)
                    });
                    found_unable(addr);
                }
            },
            next_addr = addrs.next(), if !addrs_completed => match next_addr {
                Some((addr, source)) => name(addr, source),
                None => addrs_completed = true,
            },
            _ = peers.wait_for_candidate() => {}
        }
    };
    // The connections still asking end here, and their peers go to the
    // torrent with no wait to reconnect: libtorrent's metadata connections
    // are the torrent's own and carry on past `torrent::set_metadata`, so
    // the torrent connects to them again at once.
    drop(asking);
    (found, addrs, seen, peers.idle_list())
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
        impl Fn(SocketAddr, &Arc<PeerStates>) -> std::future::Ready<anyhow::Result<SocketAddr>>,
    ) {
        let asked = Arc::new(Mutex::new(Vec::new()));
        let log = asked.clone();
        let ask = move |addr, _: &Arc<PeerStates>| {
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

    /// Where a fetch records the peers it found unable.
    fn unable() -> tokio::sync::watch::Sender<HashSet<SocketAddr>> {
        tokio::sync::watch::Sender::new(HashSet::new())
    }

    /// A list with `addrs` on it, as a torrent is added with.
    fn listing(addrs: impl IntoIterator<Item = SocketAddr>) -> PeerList {
        PeerList::resumed(addrs, [])
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_sent_no_metadata_is_asked_again_once_its_reconnect_time_comes() {
        let (asked, ask) = asker(1);
        let (found, _, _, _) = first_answer(
            listing([addr(1)]),
            futures::stream::pending(),
            &unable(),
            ask,
        )
        .await;

        assert_eq!(found, Some(addr(1)));
        let asked = asked.lock().unwrap();
        assert_eq!(asked.len(), 2);
        // libtorrent `peer_list`: (failcount + 1) reconnect times after the failure.
        assert!(asked[1].1 >= asked[0].1 + MIN_RECONNECT_TIME * 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_sent_no_metadata_or_was_refused_by_the_list_is_found_unable() {
        let (_, ask) = asker(1);
        let banned = SocketAddr::from(([10, 0, 0, 9], 1));
        let unable = unable();
        let mut found_unable = unable.subscribe();
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tx.unbounded_send((banned, PeerSourceFlag::Tracker))
            .unwrap();
        let list = PeerList::resumed([addr(1)], [banned.ip()]);
        let (found, _, _, _) = first_answer(list, rx, &unable, ask).await;

        assert_eq!(found, Some(addr(1)));
        assert_eq!(
            *found_unable.borrow_and_update(),
            HashSet::from([banned, addr(1)])
        );
        drop(tx);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_named_while_another_failed_is_asked_at_once() {
        let (asked, ask) = asker(1);
        let (tx, rx) = futures::channel::mpsc::unbounded();
        tx.unbounded_send((addr(2), PeerSourceFlag::Dht)).unwrap();
        let (found, _, seen, _) = first_answer(listing([addr(1)]), rx, &unable(), ask).await;

        assert_eq!(seen, HashSet::from([addr(2)]));
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
        let (found, _, _, _) = first_answer(
            listing([addr(1), addr(2)]),
            futures::stream::empty(),
            &unable(),
            ask,
        )
        .await;

        assert_eq!(found, None);
        let asked = asked.lock().unwrap();
        for peer in [addr(1), addr(2)] {
            let times = asked.iter().filter(|(a, _)| *a == peer).count();
            assert_eq!(times, MAX_FAILCOUNT as usize, "{peer}: {asked:?}");
        }
    }

    // libtorrent `torrent::set_metadata`: the connections that brought the
    // metadata are the torrent's own and carry on, so the torrent has the
    // peers they reached at once, the one that answered and one still
    // asking alike.
    #[tokio::test(start_paused = true)]
    async fn the_peers_still_connected_when_the_metadata_comes_are_due_at_once() {
        // At two hosts: the torrent's list keeps one connection per address.
        let (answers, asking) = (
            SocketAddr::from(([10, 0, 0, 1], 1)),
            SocketAddr::from(([10, 0, 0, 2], 1)),
        );
        let ask = |peer, _: &Arc<PeerStates>| async move {
            if peer == answers {
                Ok(peer)
            } else {
                std::future::pending().await
            }
        };
        let (found, _, _, list) = first_answer(
            listing([answers, asking]),
            futures::stream::pending(),
            &unable(),
            ask,
        )
        .await;
        assert_eq!(found, Some(answers));

        let next = PeerStates::founded_on(list, Default::default(), 0);
        let now = Instant::now();
        let due = HashSet::from_iter(
            std::iter::from_fn(|| next.connect_next(now)).map(|(peer, ..)| peer),
        );
        assert_eq!(due, HashSet::from([answers, asking]));
    }

    #[tokio::test]
    #[ignore]
    async fn read_metainfo_from_dht() {
        init_logging();

        let info_hash = Id20::from_str("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        let dht = DhtBuilder::new().await.unwrap();

        let peer_rx = dht
            .get_peers(info_hash, None)
            .map(|addr| (addr, PeerSourceFlag::Dht));
        let peer_id = generate_peer_id(b"-xx1234-");
        match read_metainfo_from_peer_receiver(
            peer_id,
            InfoHashes {
                v1: Some(info_hash),
                v2: None,
            },
            PeerList::default(),
            peer_rx,
            &unable(),
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

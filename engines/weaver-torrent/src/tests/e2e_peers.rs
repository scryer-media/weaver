// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A torrent's peer listing, and banning one of its peers, over loopback.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Condvar, Mutex},
};

use anyhow::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
    AddTorrent, AddTorrentOptions, ListenerMode, ListenerOptions, ManagedTorrentShared,
    ManagedTorrentState, Session, SessionOptions, TorrentMetadata,
    core::{hash_id::Id20, lengths::ValidPieceIndex, peer_id::generate_azereus_style},
    peer_protocol::Handshake,
    storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
    tests::{
        embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        test_util::setup_test_logging,
    },
    torrent_state::{
        ManagedTorrentHandle,
        peer::stats::snapshot::{PeerStatsFilter, PeerStatsFilterState},
    },
    type_aliases::{PeerSourceFlag, PeerSources},
};

const PIECE_LENGTH: u32 = 16384;
const FILE_SIZE: usize = 8 * PIECE_LENGTH as usize;

// A session listening on `ip` under a peer id of `client`'s, and with no
// `v` in its extended handshake, so its peers name it by its peer id.
fn opts(ip: IpAddr, client: [u8; 2]) -> SessionOptions {
    SessionOptions {
        identity: Some(crate::Identity {
            peer_id: generate_azereus_style(client, (4, 6, 5, 0)),
            version: String::new(),
            ..Default::default()
        }),
        listen: Some(ListenerOptions {
            mode: ListenerMode::TcpOnly,
            listen_addr: (ip, 0).into(),
            ..Default::default()
        }),
        ..isolated_session_opts(true)
    }
}

// A session with the torrent and none of its data, so it stays connected to
// peers that have none either; it connects to `peers`.
async fn empty(
    t: &TestTorrent,
    name: &str,
    opts: SessionOptions,
    peers: Vec<SocketAddr>,
) -> anyhow::Result<(Arc<Session>, ManagedTorrentHandle, SocketAddr)> {
    let out = t.dir.path().join(name);
    let session = Session::new_with_opts(out, opts).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                initial_peers: Some(peers),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    let addr = session.listen_addr().context("not listening")?;
    Ok((session, handle, addr))
}

// libtorrent `torrent::get_peer_info` lists every connection, and
// `torrent::ban_peer` closes the connection to the banned address now and
// refuses it from then on.
#[tokio::test(flavor = "multi_thread")]
async fn test_peers_listing_shows_the_peer_and_a_ban_refuses_it() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_peers_listing", FILE_SIZE, PIECE_LENGTH).await?;
    let (_a, _, a) = empty(&t, "a", opts(Ipv4Addr::LOCALHOST.into(), *b"qB"), vec![]).await?;
    let (_us, handle, ours) =
        empty(&t, "us", opts(Ipv4Addr::LOCALHOST.into(), *b"DE"), vec![a]).await?;

    let peer = loop {
        if let [peer] = handle.peers().as_slice() {
            break peer.clone();
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(peer.addr, a);
    assert_eq!(peer.client, "qBittorrent 4.6.5");
    assert!(peer.outgoing, "{peer:?}");
    assert!(peer.flags.choked && peer.flags.remote_choked, "{peer:?}");
    assert!(!peer.flags.seed && !peer.flags.holepunched, "{peer:?}");
    assert_eq!((peer.pieces, peer.progress), (0, 0.0), "{peer:?}");
    assert_eq!(peer.sources, PeerSources::from(PeerSourceFlag::ResumeData));

    assert!(handle.ban_peers(&[a.ip()]).is_some());
    while !handle.peers().is_empty() {
        tokio::task::yield_now().await;
    }

    // Named again, the banned peer is no connect candidate.
    let live = handle.live().context("not live")?;
    live.add_peer(a, PeerSourceFlag::Tracker);
    let all = PeerStatsFilter {
        state: PeerStatsFilterState::All,
    };
    assert_eq!(
        live.per_peer_stats_snapshot(all).peers[&a.to_string()].state,
        "dead"
    );
    assert_eq!(
        handle.resume_peers().context("no peer list")?.banned,
        [a.ip()]
    );

    // Connecting to us from the banned address, it is turned away: the
    // connection closes, where a welcome one would stay open.
    let (mut incoming_read, mut incoming) = crate::test_support::tcp(ours).await?;
    let mut hs = vec![0u8; 68];
    let len =
        Handshake::new(handle.info_hash(), Id20::new([9; 20])).serialize_unchecked_len(&mut hs);
    incoming.write_all(&hs[..len]).await?;
    let mut rest = Vec::new();
    incoming_read.read_to_end(&mut rest).await.ok();
    assert!(!handle.peers().iter().any(|p| p.addr.ip() == a.ip()));
    Ok(())
}

/// A memory storage whose reads wait while the gate is held, and whose
/// writes fail when `writes_fail` holds.
#[derive(Clone, Default)]
struct GatedStorageFactory {
    inner: MemoryStorageFactory,
    gate: Arc<Gate>,
    writes_fail: bool,
}

#[derive(Default)]
struct Gate {
    held: Mutex<bool>,
    opened: Condvar,
    // A read came to the gate.
    reached: tokio::sync::Notify,
}

impl Gate {
    fn hold(&self) {
        *self.held.lock().unwrap() = true;
    }

    fn open(&self) {
        *self.held.lock().unwrap() = false;
        self.opened.notify_all();
    }

    fn pass(&self) {
        self.reached.notify_one();
        let held = self.held.lock().unwrap();
        drop(self.opened.wait_while(held, |held| *held).unwrap());
    }
}

struct GatedStorage {
    inner: Box<dyn TorrentStorage>,
    gate: Arc<Gate>,
    writes_fail: bool,
}

impl StorageFactory for GatedStorageFactory {
    type Storage = GatedStorage;

    fn create(
        &self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<GatedStorage> {
        Ok(GatedStorage {
            inner: Box::new(self.inner.create(shared, metadata)?),
            gate: self.gate.clone(),
            writes_fail: self.writes_fail,
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

impl TorrentStorage for GatedStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.gate.pass();
        self.inner.pread_exact(file_id, offset, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        anyhow::ensure!(!self.writes_fail, "the disk failed");
        self.inner.pwrite_all(file_id, offset, buf)
    }

    fn remove_file(&self, file_id: usize, filename: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, filename)
    }

    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(GatedStorage {
            inner: self.inner.take()?,
            gate: self.gate.clone(),
            writes_fail: self.writes_fail,
        }))
    }

    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.on_piece_completed(piece)
    }
}

// `storage` added to a session of its own, connecting to `peers`.
async fn added(
    t: &TestTorrent,
    name: &str,
    storage: &GatedStorageFactory,
    peers: Vec<SocketAddr>,
) -> anyhow::Result<(Arc<Session>, ManagedTorrentHandle)> {
    let session =
        Session::new_with_opts(t.dir.path().join(name), isolated_session_opts(false)).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_peers: Some(peers),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    Ok((session, handle))
}

// libtorrent `torrent::ban_peer` bans on the torrent's peer list, which it
// holds while it checks its files: the ban is on the list its first run
// is founded on.
#[tokio::test(flavor = "multi_thread")]
async fn test_ban_while_initializing_holds_in_the_run() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_ban_initializing", FILE_SIZE, PIECE_LENGTH).await?;
    let storage = GatedStorageFactory::default();
    storage.gate.hold();
    let (_session, handle) = added(&t, "us", &storage, vec![]).await?;

    // The check reads the files, and waits at the gate.
    storage.gate.reached.notified().await;
    assert!(handle.with_state(|s| matches!(s, ManagedTorrentState::Initializing(_))));
    let banned: IpAddr = Ipv4Addr::new(203, 0, 113, 1).into();
    let saved = handle.ban_peers(&[banned]).context("no peer list")?;
    assert_eq!(saved.banned, [banned]);

    storage.gate.open();
    handle.wait_until_initialized().await?;
    assert!(handle.live().is_some());
    assert_eq!(
        handle.resume_peers().context("no peer list")?.banned,
        [banned]
    );
    Ok(())
}

// A torrent stopped by an error holds no peer list here, where libtorrent's
// `torrent` keeps its `peer_list` through one: the engine has nothing to
// ban on, and says so, for the embedder to ban on the list it saved.
#[tokio::test(flavor = "multi_thread")]
async fn test_ban_in_error_finds_no_peer_list() -> anyhow::Result<()> {
    setup_test_logging();
    let t = TestTorrent::new("test_ban_error", FILE_SIZE, PIECE_LENGTH).await?;
    let (_seed, seed) = t.seed().await?;
    let storage = GatedStorageFactory {
        writes_fail: true,
        ..Default::default()
    };
    let (_session, handle) = added(&t, "us", &storage, vec![seed]).await?;

    // The first block from the seed fails to write, which stops the torrent.
    assert!(handle.wait_until_completed().await.is_err());
    assert!(handle.with_state(|s| matches!(s, ManagedTorrentState::Error(_))));
    assert!(handle.ban_peers(&[seed.ip()]).is_none());
    assert!(handle.resume_peers().is_none());
    Ok(())
}

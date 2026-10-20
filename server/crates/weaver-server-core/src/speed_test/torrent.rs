// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The test torrent: a public torrent fetched through the path under test,
// announced to as a leecher, pieces downloaded and verified in memory, and
// stopped. Nothing is kept on disk. The probe takes one piece, and each step
// is a rung that passes or fails on what the path did, in the words of
// whatever stopped it; a torrent speed test pulls for as long as it measures
// and then serves what it held.

use std::collections::HashSet;

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use librqbit::{
    AddTorrent, AddTorrentOptions, ConnectionOptions, ListenerMode, ListenerOptions,
    ManagedTorrent, PiecePriority, Session, SessionOptions, TrackerAnnounce,
};
use librqbit_core::lengths::Lengths;
use tokio::sync::watch;
use weaver_tunnel::pipe::Dialer;

use super::natpmp;
use super::progress::StreamProvider;
use super::torrent_net::{PieceStore, ProbeSockets, Watched};
use crate::proxies::{FetchPath, start_bridge, through_bridge};
use crate::torrents::identity::IDENTITY;
use crate::torrents::net::LegKind;

// Where the test torrent is found: the directory of the current Debian
// netinst torrents.
pub const DEBIAN_TORRENTS: &str = "https://cdimage.debian.org/debian-cd/current/amd64/bt-cd/";
// The directory listing and the torrent may be no larger than this.
const FETCH_LIMIT: usize = 1024 * 1024;
// The most peers the test talks to at once.
const PEER_LIMIT: usize = 8;
// How long the test waits for its piece.
const PIECE_WAIT: Duration = Duration::from_secs(60);
// How long the test serves its piece.
const UPLOAD_WAIT: Duration = Duration::from_secs(30);

// Where the test torrent comes from: a directory listing the torrent
// among others.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestTorrentSource {
    pub directory: reqwest::Url,
}

impl TestTorrentSource {
    pub fn debian() -> Self {
        Self {
            directory: reqwest::Url::parse(DEBIAN_TORRENTS).expect("the Debian URL parses"),
        }
    }

    // The host and port the source is fetched from.
    pub fn target(&self) -> (String, u16) {
        (
            self.directory.host_str().unwrap_or_default().to_owned(),
            self.directory.port_or_known_default().unwrap_or(443),
        )
    }
}

impl Default for TestTorrentSource {
    fn default() -> Self {
        Self::debian()
    }
}

// One step of the test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum P2pStep {
    // The test torrent, fetched through the path.
    Torrent,
    // An announce over the path.
    Announce,
    // Peers dialled over the path and answering the handshake.
    Handshake,
    // One piece downloaded and verified.
    Piece,
    // Peers able to connect in.
    Inbound,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum P2pOutcome {
    Passed(String),
    // What stopped it, in the words of whatever did.
    Failed(String),
    // An earlier step failed.
    NotReached,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pRung {
    pub step: P2pStep,
    pub outcome: P2pOutcome,
}

// Pieces served to the peers the tracker sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PeerUpload {
    // No peer asked for anything.
    NoDemand,
    Measured {
        bytes: u64,
        elapsed: Duration,
        peers: u32,
    },
    // The piece to serve never arrived.
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct P2pReport {
    pub rungs: Vec<P2pRung>,
    // The port a tunnel's gateway mapped to the path.
    pub mapped_port: Option<u16>,
    pub upload: Option<PeerUpload>,
}

impl P2pReport {
    pub fn passed(&self, step: P2pStep) -> bool {
        self.rungs
            .iter()
            .any(|rung| rung.step == step && matches!(rung.outcome, P2pOutcome::Passed(_)))
    }
}

// The path a test torrent runs over.
pub(crate) struct P2pPath {
    pub stage: Arc<dyn Dialer>,
    pub kind: LegKind,
    // The gateway asked for a port mapping, when the path ends in a
    // WireGuard tunnel.
    pub gateway: Option<IpAddr>,
    // The port the mapping is asked for.
    pub internal_port: u16,
}

pub(crate) struct P2pPlan {
    pub source: TestTorrentSource,
    // Serve the piece until this many bytes have gone to peers.
    pub upload_budget: Option<u64>,
}

// Why the wait for pieces ended.
enum Ended {
    Verified,
    AnnounceFailed,
    PeersExhausted,
    Stopped,
    TimedOut,
}

// Run the test torrent over `path`. `stop` ends it early.
pub(crate) async fn probe(
    path: P2pPath,
    plan: &P2pPlan,
    mut stop: Pin<&mut (dyn Future<Output = ()> + Send)>,
) -> P2pReport {
    let fetched = tokio::select! {
        biased;
        () = stop.as_mut() => return failed_at(P2pStep::Torrent, "stopped".into()),
        fetched = TestTorrent::fetch(path.stage.clone(), &plan.source) => fetched,
    };
    let torrent = match fetched {
        Ok(torrent) => torrent,
        Err(error) => return failed_at(P2pStep::Torrent, error),
    };
    let report = run(&path, plan, &torrent, stop).await;
    torrent.close().await;
    report
}

fn failed_at(step: P2pStep, error: String) -> P2pReport {
    let steps = [
        P2pStep::Torrent,
        P2pStep::Announce,
        P2pStep::Handshake,
        P2pStep::Piece,
        P2pStep::Inbound,
    ];
    P2pReport {
        rungs: steps
            .into_iter()
            .map(|each| P2pRung {
                step: each,
                outcome: if each == step {
                    P2pOutcome::Failed(error.clone())
                } else {
                    P2pOutcome::NotReached
                },
            })
            .collect(),
        mapped_port: None,
        upload: None,
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "the test torrent's client rides the path's bridge"
)]
fn http_client(bridge: &weaver_tunnel::bridge::Bridge) -> Result<reqwest::Client, String> {
    through_bridge(
        reqwest::Client::builder()
            .user_agent(IDENTITY.user_agent())
            .read_timeout(Duration::from_secs(30)),
        bridge,
    )?
    .build()
    .map_err(|error| error.to_string())
}

// The netinst torrent a directory listing names.
pub(crate) fn pick_torrent(listing: &str) -> Option<String> {
    listing
        .split("href=\"")
        .skip(1)
        .filter_map(|rest| rest.split('"').next())
        .find(|name| {
            name.strip_prefix("debian-")
                .and_then(|rest| rest.strip_suffix("-amd64-netinst.iso.torrent"))
                .is_some_and(|version| {
                    !version.is_empty() && version.chars().all(|c| c.is_ascii_digit() || c == '.')
                })
        })
        .map(str::to_owned)
}

async fn get_capped(client: &reqwest::Client, url: &reqwest::Url) -> Result<Vec<u8>, String> {
    let mut response = client
        .get(url.clone())
        .send()
        .await
        .map_err(|error| format!("GET {url} failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!("GET {url} answered {}", response.status()));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("reading {url} failed: {error}"))?
    {
        if body.len() + chunk.len() > FETCH_LIMIT {
            return Err(format!("{url} is larger than {FETCH_LIMIT} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

// The test torrent, fetched through a path: its name and metainfo, and the
// client its trackers are asked through, which rides the path's bridge. The
// bridge is revoked as the torrent closes or drops.
pub(crate) struct TestTorrent {
    name: String,
    bytes: Vec<u8>,
    lengths: Lengths,
    client: reqwest::Client,
    bridge: Arc<weaver_tunnel::bridge::Bridge>,
}

impl TestTorrent {
    pub(crate) async fn fetch(
        stage: Arc<dyn Dialer>,
        source: &TestTorrentSource,
    ) -> Result<Self, String> {
        let provider = StreamProvider::new(stage, None);
        let bridge = start_bridge(provider as Arc<dyn FetchPath>)?;
        let client = http_client(&bridge)?;
        let listing = get_capped(&client, &source.directory).await?;
        let name = pick_torrent(&String::from_utf8_lossy(&listing))
            .ok_or_else(|| format!("{} lists no netinst torrent", source.directory))?;
        let url = source
            .directory
            .join(&name)
            .map_err(|error| error.to_string())?;
        let bytes = get_capped(&client, &url).await?;
        let lengths = librqbit::torrent_from_bytes(&bytes)
            .map_err(|error| error.to_string())
            .and_then(|torrent| {
                torrent
                    .info
                    .data
                    .validate()
                    .map(|info| *info.lengths())
                    .map_err(|error| error.to_string())
            })
            .map_err(|error| format!("{name} is not a torrent: {error}"))?;
        Ok(Self {
            name,
            bytes,
            lengths,
            client,
            bridge,
        })
    }

    pub(crate) async fn close(self) {
        self.bridge.revoke().await;
    }

    fn pieces(&self) -> u32 {
        self.lengths.total_pieces()
    }

    fn piece_len(&self, piece: u32) -> u32 {
        self.lengths
            .validate_piece_index(piece)
            .map_or(0, |valid| self.lengths.piece_length(valid))
    }
}

// A session in the test torrent's swarm over a path. It has the pieces it
// was given, fetches the rest of what its store wants, and skips the others.
struct Joined {
    session: Arc<Session>,
    handle: Arc<ManagedTorrent>,
    _dir: tempfile::TempDir,
}

impl Joined {
    async fn open(
        torrent: &TestTorrent,
        path: &P2pPath,
        store: &PieceStore,
        have: &[u32],
        watched: Arc<Watched>,
    ) -> Result<Self, String> {
        let dir = tempfile::TempDir::new().map_err(|error| error.to_string())?;
        let listens = path.kind != LegKind::Ladder;
        let options = SessionOptions {
            dht: None,
            disable_trackers: false,
            fastresume: false,
            persistence: None,
            peer_id: Some(IDENTITY.peer_id()),
            listen: listens.then(|| ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
                enable_upnp_port_forwarding: false,
                ..ListenerOptions::default()
            }),
            connect: Some(ConnectionOptions {
                proxy_url: None,
                enable_tcp: true,
                peer_opts: None,
            }),
            peer_limit: Some(PEER_LIMIT),
            disable_local_service_discovery: true,
            client_name_and_version: Some(IDENTITY.user_agent().to_owned()),
            socket_factory: Some(Arc::new(ProbeSockets::new(
                path.stage.clone(),
                path.kind.clone(),
                watched,
            ))),
            http_client: Some(torrent.client.clone()),
            ..SessionOptions::default()
        };
        let session = Session::new_with_opts(dir.path().join("output"), options)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let pieces = torrent.pieces();
        let wanted: HashSet<u32> = store.wanted().iter().copied().collect();
        let mut initial_have: BitBox<u8, Msb0> =
            BitBox::from_boxed_slice(vec![0u8; (pieces as usize).div_ceil(8)].into_boxed_slice());
        for piece in have {
            initial_have.set(*piece as usize, true);
        }
        let added = session
            .add_torrent(
                AddTorrent::from_bytes(torrent.bytes.clone()),
                Some(AddTorrentOptions {
                    output_folder: Some(dir.path().join("output").display().to_string()),
                    overwrite: true,
                    peer_limit: Some(PEER_LIMIT),
                    storage_factory: Some(store.factory()),
                    initial_have: Some(initial_have),
                    piece_priorities: Some(
                        (0..pieces)
                            .filter(|index| !wanted.contains(index))
                            .map(|index| (index, PiecePriority::Skip))
                            .collect(),
                    ),
                    ..AddTorrentOptions::default()
                }),
            )
            .await;
        match added.map(|added| added.into_handle()) {
            Ok(Some(handle)) => Ok(Self {
                session,
                handle,
                _dir: dir,
            }),
            Ok(None) => {
                session.stop().await;
                Err("the torrent was not added".into())
            }
            Err(error) => {
                session.stop().await;
                Err(format!("{error:#}"))
            }
        }
    }

    // Stopping sends `stopped` to every tracker that took `started`.
    async fn leave(self) {
        self.session.stop().await;
    }
}

async fn run(
    path: &P2pPath,
    plan: &P2pPlan,
    torrent: &TestTorrent,
    mut stop: Pin<&mut (dyn Future<Output = ()> + Send)>,
) -> P2pReport {
    let piece = 0u32;
    let watched = Arc::new(Watched::new());
    let store = PieceStore::new(vec![piece]);
    let joined = match Joined::open(torrent, path, &store, &[], watched.clone()).await {
        Ok(joined) => joined,
        Err(error) => return failed_at(P2pStep::Announce, error),
    };
    let handle = &joined.handle;

    let mut verified = store.verified();
    let deadline = tokio::time::Instant::now() + PIECE_WAIT;
    let ended = tokio::select! {
        biased;
        () = stop.as_mut() => Ended::Stopped,
        () = tokio::time::sleep_until(deadline) => Ended::TimedOut,
        ended = wait_pieces(handle, &watched, &mut verified, 1) => ended,
    };
    let announces = handle.tracker_announces();
    let upload = match (&ended, plan.upload_budget) {
        (Ended::Verified, Some(budget)) => {
            let started = store
                .verified_at()
                .unwrap_or_else(tokio::time::Instant::now);
            Some(serve(handle, &watched, budget, started, stop.as_mut()).await)
        }
        (_, Some(_)) => Some(PeerUpload::Failed("no piece arrived to serve".into())),
        (_, None) => None,
    };
    let (inbound, mapped_port) = inbound(path, &watched).await;
    let report = rungs(
        torrent.name.clone(),
        &announces,
        &watched,
        &ended,
        piece,
        torrent.piece_len(piece),
        inbound,
        mapped_port,
        upload,
    );
    joined.leave().await;
    report
}

// What a pull held at its end, to serve, and why it moved nothing when it
// did not.
pub(crate) struct Pulled {
    store: PieceStore,
    pub failure: Option<String>,
}

// Pull the test torrent from its swarm over `path` as a leecher until every
// piece is verified, nothing more can come, or `stop`. Each block's bytes go
// to `received` as they are written; at most `keep` bytes of verified pieces
// are held, and nothing else.
pub(crate) async fn pull(
    path: &P2pPath,
    torrent: &TestTorrent,
    keep: u64,
    received: impl Fn(u64) + Send + Sync + 'static,
    mut stop: Pin<&mut (dyn Future<Output = ()> + Send)>,
) -> Pulled {
    let pieces = torrent.pieces();
    let watched = Arc::new(Watched::new());
    let store = PieceStore::new((0..pieces).collect())
        .keeping(keep)
        .counting(received);
    let joined = match Joined::open(torrent, path, &store, &[], watched.clone()).await {
        Ok(joined) => joined,
        Err(error) => {
            return Pulled {
                store,
                failure: Some(error),
            };
        }
    };
    let mut verified = store.verified();
    let ended = tokio::select! {
        biased;
        () = stop.as_mut() => Ended::Stopped,
        ended = wait_pieces(&joined.handle, &watched, &mut verified, pieces as usize) => ended,
    };
    let announces = joined.handle.tracker_announces();
    joined.leave().await;
    let report = rungs(
        torrent.name.clone(),
        &announces,
        &watched,
        &ended,
        0,
        torrent.piece_len(0),
        P2pOutcome::NotReached,
        None,
        None,
    );
    let failure = report
        .rungs
        .into_iter()
        .find_map(|rung| match rung.outcome {
            P2pOutcome::Failed(error) => Some(error),
            _ => None,
        });
    Pulled { store, failure }
}

// Serve the pieces a pull held to the leechers the trackers send over
// `path`, until `budget` bytes went to them or no peer wants them.
pub(crate) async fn seed(
    path: &P2pPath,
    torrent: &TestTorrent,
    pulled: &Pulled,
    budget: u64,
    stop: Pin<&mut (dyn Future<Output = ()> + Send)>,
) -> PeerUpload {
    let store = pulled.store.holding();
    let held = store.held();
    if held.is_empty() {
        return PeerUpload::Failed("no piece arrived to serve".into());
    }
    let watched = Arc::new(Watched::new());
    let joined = match Joined::open(torrent, path, &store, &held, watched.clone()).await {
        Ok(joined) => joined,
        Err(error) => return PeerUpload::Failed(error),
    };
    let upload = match joined.handle.wait_until_initialized().await {
        Ok(()) => {
            serve(
                &joined.handle,
                &watched,
                budget,
                tokio::time::Instant::now(),
                stop,
            )
            .await
        }
        Err(error) => PeerUpload::Failed(format!("{error:#}")),
    };
    joined.leave().await;
    upload
}

// Wait until `wanted` pieces are verified or nothing more can bring them.
async fn wait_pieces(
    handle: &Arc<ManagedTorrent>,
    watched: &Watched,
    verified: &mut watch::Receiver<usize>,
    wanted: usize,
) -> Ended {
    let mut wake = watched.subscribe();
    if handle.wait_until_initialized().await.is_err() {
        return Ended::AnnounceFailed;
    }
    let mut announces = handle.tracker_status().map(|status| status.subscribe());
    loop {
        if *verified.borrow_and_update() >= wanted {
            return Ended::Verified;
        }
        let table = handle.tracker_announces();
        if handle.tracker_status().is_none() || announce_failed(&table) {
            return Ended::AnnounceFailed;
        }
        if announced(&table).is_some() {
            let seen = handle
                .live()
                .map_or(0, |live| live.stats_snapshot().peer_stats.seen as usize);
            let dials = watched.dials();
            if seen > 0
                && dials > 0
                && watched.answered() == 0
                && watched.settled() >= dials.max(seen)
            {
                return Ended::PeersExhausted;
            }
        }
        tokio::select! {
            _ = wake.changed() => {}
            _ = verified.changed() => {}
            _ = async {
                match announces.as_mut() {
                    Some(announces) => announces.changed().await.ok(),
                    None => std::future::pending().await,
                }
            } => {}
        }
    }
}

fn announced(table: &[TrackerAnnounce]) -> Option<&librqbit::AnnounceOk> {
    table
        .iter()
        .filter_map(|tracker| tracker.last.as_ref())
        .find_map(|last| last.result.as_ref().ok())
}

fn announce_failed(table: &[TrackerAnnounce]) -> bool {
    !table.is_empty()
        && table.iter().all(|tracker| {
            !tracker.in_flight
                && tracker
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_err())
        })
}

// Serve the verified piece until `budget` bytes went to peers, or until no
// peer wants it.
async fn serve(
    handle: &Arc<ManagedTorrent>,
    watched: &Watched,
    budget: u64,
    // When the piece was there to serve.
    started: tokio::time::Instant,
    mut stop: Pin<&mut (dyn Future<Output = ()> + Send)>,
) -> PeerUpload {
    let mut wake = watched.subscribe();
    let mut announces = handle.tracker_status().map(|status| status.subscribe());
    let deadline = tokio::time::Instant::now() + UPLOAD_WAIT;
    loop {
        let sent = watched.sent();
        if sent >= budget {
            break;
        }
        let table = handle.tracker_announces();
        if sent == 0 && announced(&table).is_some_and(|ok| ok.leechers == Some(0)) {
            return PeerUpload::NoDemand;
        }
        tokio::select! {
            biased;
            () = stop.as_mut() => break,
            () = tokio::time::sleep_until(deadline) => break,
            _ = wake.changed() => {}
            _ = async {
                match announces.as_mut() {
                    Some(announces) => announces.changed().await.ok(),
                    None => std::future::pending().await,
                }
            } => {}
        }
    }
    let bytes = watched.sent();
    if bytes == 0 {
        return PeerUpload::NoDemand;
    }
    // Timed to the last byte sent, not to the end of the wait.
    PeerUpload::Measured {
        bytes,
        elapsed: watched.last_sent().map_or(Duration::ZERO, |last| {
            last.saturating_duration_since(started)
        }),
        peers: u32::try_from(watched.takers()).unwrap_or(u32::MAX),
    }
}

// Whether peers can connect in: the port a tunnel's gateway maps, else the
// peers that did.
async fn inbound(path: &P2pPath, watched: &Watched) -> (P2pOutcome, Option<u16>) {
    if let Some(gateway) = path.gateway {
        let Some(datagrams) = path.stage.clone().datagrams() else {
            return (
                P2pOutcome::Failed("the tunnel carries no datagrams".into()),
                None,
            );
        };
        let gateway = SocketAddr::new(gateway, natpmp::PORT);
        let socket = match datagrams.bind(gateway).await {
            Ok(socket) => socket,
            Err(error) => return (P2pOutcome::Failed(error.to_string()), None),
        };
        return match natpmp::map_tcp(socket.as_ref(), gateway, path.internal_port).await {
            Ok(port) => (
                P2pOutcome::Passed(format!("{} mapped port {port}", gateway.ip())),
                Some(port),
            ),
            Err(error) => (P2pOutcome::Failed(error), None),
        };
    }
    if path.kind == LegKind::Ladder {
        return (
            P2pOutcome::Failed("this path cannot take peer connections".into()),
            None,
        );
    }
    match watched.inbound() {
        0 => (
            P2pOutcome::Failed("no peer connected in: not connectable".into()),
            None,
        ),
        peers => (P2pOutcome::Passed(format!("{peers} connected in")), None),
    }
}

#[expect(clippy::too_many_arguments, reason = "every rung's evidence")]
fn rungs(
    name: String,
    announces: &[TrackerAnnounce],
    watched: &Watched,
    ended: &Ended,
    piece: u32,
    piece_len: u32,
    inbound: P2pOutcome,
    mapped_port: Option<u16>,
    upload: Option<PeerUpload>,
) -> P2pReport {
    let announce = match announced(announces) {
        Some(ok) => P2pOutcome::Passed(match (ok.seeders, ok.leechers) {
            (Some(seeders), Some(leechers)) => {
                format!("{seeders} seeders, {leechers} leechers")
            }
            _ => "announced".into(),
        }),
        None => {
            let errors: Vec<String> = announces
                .iter()
                .filter_map(|tracker| {
                    let last = tracker.last.as_ref()?;
                    last.result
                        .as_ref()
                        .err()
                        .map(|error| format!("{}: {error}", tracker.url))
                })
                .collect();
            match ended {
                Ended::Stopped => P2pOutcome::NotReached,
                _ if errors.is_empty() && announces.is_empty() => {
                    P2pOutcome::Failed("the torrent names no tracker".into())
                }
                _ if errors.is_empty() => P2pOutcome::Failed("no tracker answered".into()),
                _ => P2pOutcome::Failed(errors.join("; ")),
            }
        }
    };
    let announced_ok = matches!(announce, P2pOutcome::Passed(_));
    let handshake = if watched.answered() > 0 {
        P2pOutcome::Passed(format!(
            "{} of {} peers answered",
            watched.answered(),
            watched.dials()
        ))
    } else if !announced_ok {
        P2pOutcome::NotReached
    } else {
        let errors = watched.errors();
        if errors.is_empty() {
            P2pOutcome::Failed("the tracker returned no peer to dial".into())
        } else {
            P2pOutcome::Failed(errors.join("; "))
        }
    };
    let piece_outcome = match ended {
        Ended::Verified => {
            P2pOutcome::Passed(format!("piece {piece} ({piece_len} bytes) verified"))
        }
        _ if matches!(handshake, P2pOutcome::Passed(_)) => {
            P2pOutcome::Failed("no peer sent the piece".into())
        }
        _ => P2pOutcome::NotReached,
    };
    P2pReport {
        rungs: vec![
            P2pRung {
                step: P2pStep::Torrent,
                outcome: P2pOutcome::Passed(name),
            },
            P2pRung {
                step: P2pStep::Announce,
                outcome: announce,
            },
            P2pRung {
                step: P2pStep::Handshake,
                outcome: handshake,
            },
            P2pRung {
                step: P2pStep::Piece,
                outcome: piece_outcome,
            },
            P2pRung {
                step: P2pStep::Inbound,
                outcome: inbound,
            },
        ],
        mapped_port,
        upload,
    }
}

#[cfg(test)]
mod tests;

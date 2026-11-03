// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Web seeds: HTTP servers that hold a torrent's data, by its files' paths
//! (BEP 19, `url-list`) or by piece (BEP 17, `httpseeds`). A connection to
//! one is a peer that has the pieces of every file the server serves and
//! never chokes: the request path asks it for blocks as it asks any peer,
//! and it answers each request with a PIECE (libtorrent
//! `web_peer_connection`, `torrent::maybe_connect_web_seeds`).

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, Ipv6Addr, SocketAddr},
    num::IntErrorKind,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, anyhow};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream::FuturesUnordered};
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use reqwest::{StatusCode, Url, header};
use tokio::{
    sync::{Notify, mpsc},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    buffers::ByteBuf,
    core::{
        hash_id::Id20,
        lengths::{Lengths, ValidPieceIndex},
        magnet::Escaped,
        net::{HttpClients, HttpTarget, NamedBy, Resolved, Resolver, is_public},
        torrent_metainfo::{WebSeed, WebSeedKind, escape_path},
    },
    peer_connection::{PeerConnectionHandler, WriterRequest},
    peer_protocol::{Handshake, Message, Piece, Request},
    stream_connect::ConnectionKind,
    torrent_state::TorrentMetadata,
    type_aliases::{BF, PeerHandle},
};

use super::peer::{PeerRx, empty_bitfield};

/// How many web seeds a torrent connects to at once (libtorrent
/// `settings_pack::max_web_seed_connections`).
pub(crate) const MAX_WEB_SEED_CONNECTIONS: usize = 3;
/// How long a web seed waits after a failed connection, and after an error
/// status without `Retry-After` (libtorrent `urlseed_wait_retry`).
pub(crate) const URLSEED_WAIT_RETRY: Duration = Duration::from_secs(30);
/// How long a web seed may take to answer a request or send the next part
/// of a body (libtorrent `urlseed_timeout`).
const URLSEED_TIMEOUT: Duration = Duration::from_secs(20);
/// The most bytes one HTTP request asks for (libtorrent
/// `urlseed_max_request_bytes`).
const URLSEED_MAX_REQUEST_BYTES: u64 = 16 * 1024 * 1024;
/// The most HTTP requests outstanding to one web seed (libtorrent
/// `urlseed_pipeline_size`).
const URLSEED_PIPELINE_SIZE: usize = 5;
/// How long a BEP 17 seed that answered 503 with no usable wait is left.
const HTTPSEED_DEFAULT_RETRY: Duration = Duration::from_secs(60);
/// The longest wait a seed may ask for, in seconds: libtorrent holds it in
/// a `seconds32` (`http_parser::header_duration`).
const MAX_RETRY_WAIT_SECS: u64 = i32::MAX as u64;
/// The most of a BEP 17 503 body read. The body is a count of seconds, so
/// anything longer is not one; the cap keeps a seed from making us hold
/// whatever it sends.
const MAX_UNAVAILABLE_BODY: usize = 64 * 1024;
/// Blocks read and not yet handed to the torrent, per connection.
const BLOCKS_BUFFERED: usize = 64;
/// How long a seed whose host name did not resolve is left (libtorrent
/// `settings_pack::web_seed_name_lookup_retry`).
const WEB_SEED_NAME_LOOKUP_RETRY: Duration = Duration::from_secs(1800);
/// The most redirects one request follows (libtorrent `http_connection`
/// `m_redirects`).
const MAX_REDIRECTS: usize = 5;

/// How often the torrent tries its web seeds (libtorrent
/// `torrent::second_tick` calls `maybe_connect_web_seeds`).
pub(crate) const WEB_SEED_TICK: Duration = Duration::from_secs(1);

/// The peer table address of web seed `id`: in the IPv6 discard prefix
/// (RFC 6666), at port 0, which no peer we may connect to has
/// (`TorrentStateLive::may_connect_to`). Its own address, so a ban for bad
/// data bans the one seed.
fn handle_of(id: u32) -> PeerHandle {
    #[allow(clippy::cast_possible_truncation)]
    let ip = Ipv6Addr::new(0x100, 0, 0, 0, 0, 0, (id >> 16) as u16, id as u16);
    SocketAddr::new(IpAddr::V6(ip), 0)
}

/// Whether `handle` is a web seed's.
pub(crate) fn is_web_seed(handle: PeerHandle) -> bool {
    handle.port() == 0
        && matches!(handle.ip(), IpAddr::V6(ip) if ip.segments()[..4] == [0x100, 0, 0, 0])
}

/// A torrent's web seeds, kept for its whole life (libtorrent
/// `torrent::m_web_seeds`).
#[derive(Default)]
pub(crate) struct WebSeeds {
    inner: Mutex<Inner>,
    // Wakes the connect loop: a seed was added, or a connection ended.
    changed: Notify,
}

#[derive(Default)]
struct Inner {
    seeds: Vec<Entry>,
    next_id: u32,
}

struct Entry {
    id: u32,
    seed: WebSeed,
    // Not connected to before this (libtorrent `web_seed_t::retry`).
    retry: Option<Instant>,
    // Never connected to again (libtorrent `web_seed_t::disabled`).
    disabled: bool,
    // Banned for sending bad data; not listed (`peer_info.banned`).
    banned: bool,
    // What the seed does not serve.
    lacks: Lacks,
    // Where the server moved a file (libtorrent `web_seed_t::redirects`).
    redirects: HashMap<usize, Redirect>,
    // Ends the open connection to the seed.
    connection: Option<CancellationToken>,
}

/// What a seed does not serve, kept across its connections: the files the
/// server answered an error status for or sent a bad copy of (the bits
/// cleared in libtorrent `web_seed_t::have_files`), and the pieces spanning
/// several files it sent bad data for (libtorrent
/// `web_peer_connection::received_invalid_data`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Lacks {
    files: HashSet<usize>,
    pieces: HashSet<u32>,
}

/// Where a server moved a file.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Redirect {
    url: String,
    // Who it is asked for as: a server at a public address moved it, or the
    // seed came with the torrent, and it is asked for at public addresses
    // only.
    named_by: NamedBy,
}

/// A web seed the connect loop may connect to now.
#[derive(Debug)]
pub(crate) struct Due {
    pub id: u32,
    /// Where the seed is, when its host is an IP address.
    pub endpoint: Option<SocketAddr>,
}

impl WebSeeds {
    /// The seeds of a torrent, in random order (libtorrent `torrent::torrent`
    /// shuffles them), each once.
    pub fn new(seeds: impl IntoIterator<Item = WebSeed>) -> Self {
        let mut seeds: Vec<WebSeed> = seeds.into_iter().collect();
        seeds.shuffle(&mut rand::rng());
        let registry = Self::default();
        for seed in seeds {
            registry.add(seed);
        }
        registry
    }

    /// The seeds of a torrent: those its metainfo names, then those it was
    /// added with, which get the trailing slash of a multi-file torrent's
    /// URL as `url-list` ones do (libtorrent `torrent::torrent`).
    pub fn of_torrent(metadata: &TorrentMetadata, added: &[WebSeed]) -> Self {
        let multi_file = metadata.file_infos.len() > 1;
        let added = added.iter().cloned().map(|mut seed| {
            if seed.kind == WebSeedKind::Url && multi_file && !seed.url.ends_with('/') {
                seed.url.push('/');
            }
            seed
        });
        Self::new(metadata.web_seeds.iter().cloned().chain(added))
    }

    /// The seeds, but those banned for bad data (libtorrent
    /// `torrent::web_seeds`).
    pub fn list(&self) -> Vec<WebSeed> {
        let g = self.inner.lock();
        g.seeds
            .iter()
            .filter(|e| !e.banned)
            .map(|e| e.seed.clone())
            .collect()
    }

    /// Adds `seed`, unless one of its URL and kind is there, whoever named
    /// it; returns whether it was added.
    pub fn add(&self, seed: WebSeed) -> bool {
        let mut g = self.inner.lock();
        if g.seeds.iter().any(|e| same_seed(&e.seed, &seed)) {
            return false;
        }
        let id = g.next_id;
        g.next_id += 1;
        g.seeds.push(Entry {
            id,
            seed,
            retry: None,
            disabled: false,
            banned: false,
            lacks: Lacks::default(),
            redirects: HashMap::new(),
            connection: None,
        });
        drop(g);
        self.changed.notify_one();
        true
    }

    /// Removes the seed of `seed`'s URL and kind and ends its connection;
    /// returns whether it was there.
    pub fn remove(&self, seed: &WebSeed) -> bool {
        let mut g = self.inner.lock();
        let Some(at) = g.seeds.iter().position(|e| same_seed(&e.seed, seed)) else {
            return false;
        };
        remove_at(&mut g, at);
        true
    }

    /// Ends every connection to a seed (libtorrent `torrent::finished`
    /// disconnects the upload-only peers, which every web seed is).
    pub fn disconnect_all(&self) {
        for entry in self.inner.lock().seeds.iter() {
            if let Some(c) = &entry.connection {
                c.cancel();
            }
        }
    }

    /// Whether the torrent has no web seed.
    pub fn is_empty(&self) -> bool {
        self.inner.lock().seeds.is_empty()
    }

    /// Returns once a seed may have become due.
    pub async fn changed(&self) {
        self.changed.notified().await
    }

    /// The seeds to connect to at `now`. At most MAX_WEB_SEED_CONNECTIONS
    /// seeds count, connected ones among them: those not disabled, not
    /// waiting to retry and with a piece to give, which `interesting` tells
    /// from what a seed lacks. Of those, a seed whose URL can never be
    /// fetched is removed or disabled, as is one `banned` names (libtorrent
    /// `torrent::maybe_connect_web_seeds`, `connect_to_url_seed`,
    /// `connect_web_seed`).
    pub fn due(
        &self,
        now: Instant,
        banned: impl Fn(PeerHandle) -> bool,
        interesting: impl Fn(&Lacks) -> bool,
    ) -> Vec<Due> {
        let mut g = self.inner.lock();
        let mut limit = MAX_WEB_SEED_CONNECTIONS;
        let mut due = Vec::new();
        let mut at = 0;
        while at < g.seeds.len() && limit > 0 {
            let entry = &mut g.seeds[at];
            if entry.disabled || entry.retry.is_some_and(|r| r > now) || !interesting(&entry.lacks)
            {
                at += 1;
                continue;
            }
            limit -= 1;
            if entry.connection.is_some() {
                at += 1;
                continue;
            }
            if banned(handle_of(entry.id)) {
                entry.banned = true;
            }
            match check_url(&entry.seed.url, entry.banned) {
                Ok(endpoint) => {
                    due.push(Due {
                        id: entry.id,
                        endpoint,
                    });
                    at += 1;
                }
                Err(Refused::Disabled(why)) => {
                    debug!(url = entry.seed.url, why, "web seed disabled");
                    entry.disabled = true;
                    at += 1;
                }
                Err(Refused::Removed(why)) => {
                    debug!(url = entry.seed.url, why, "web seed removed");
                    remove_at(&mut g, at);
                }
            }
        }
        due
    }

    /// Opens the connection to seed `id`; None when it is gone or connected.
    pub fn link(self: &Arc<Self>, id: u32) -> Option<SeedLink> {
        let mut g = self.inner.lock();
        let entry = g.seeds.iter_mut().find(|e| e.id == id)?;
        if entry.connection.is_some() {
            return None;
        }
        let cancel = CancellationToken::new();
        entry.connection = Some(cancel.clone());
        Some(SeedLink {
            seeds: self.clone(),
            id,
            seed: entry.seed.clone(),
            cancel,
        })
    }

    /// Seed `handle` sent the whole of `piece`, which failed its hash
    /// check: it serves no more of what the piece lies in (see
    /// [`Layout::failed`]). The pieces it serves now; None when `handle`
    /// is no seed's.
    pub fn piece_failed(
        &self,
        handle: PeerHandle,
        piece: ValidPieceIndex,
        layout: &Layout,
    ) -> Option<BF> {
        let mut g = self.inner.lock();
        let entry = g.seeds.iter_mut().find(|e| handle_of(e.id) == handle)?;
        layout.failed(piece, &mut entry.lacks);
        debug!(
            url = entry.seed.url,
            piece = piece.get(),
            "web seed sent a bad piece"
        );
        Some(layout.bitfield(&entry.lacks))
    }

    fn with_entry<R>(&self, id: u32, f: impl FnOnce(&mut Entry) -> R) -> Option<R> {
        self.inner
            .lock()
            .seeds
            .iter_mut()
            .find(|e| e.id == id)
            .map(f)
    }
}

// A seed is its URL and kind, whoever named it.
fn same_seed(a: &WebSeed, b: &WebSeed) -> bool {
    (&a.url, a.kind) == (&b.url, b.kind)
}

fn remove_at(g: &mut Inner, at: usize) {
    let entry = g.seeds.remove(at);
    if let Some(c) = entry.connection {
        c.cancel();
    }
}

enum Refused {
    Disabled(&'static str),
    Removed(&'static str),
}

/// Whether a seed at `url` may be connected to, and where it is when its
/// host is an IP address (libtorrent `torrent::connect_to_url_seed`,
/// `connect_web_seed`).
fn check_url(url: &str, banned: bool) -> Result<Option<SocketAddr>, Refused> {
    let Ok(parsed) = Url::parse(url) else {
        return Err(Refused::Removed("the URL does not parse"));
    };
    let host = parsed.host_str().unwrap_or_default();
    // `allow_idna` is off.
    if host.split('.').any(|label| label.starts_with("xn--")) {
        return Err(Refused::Disabled("an internationalized host name"));
    }
    if banned {
        return Err(Refused::Disabled("banned"));
    }
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(Refused::Disabled("not http or https"));
    }
    if host.is_empty() {
        return Err(Refused::Removed("no host"));
    }
    let port = parsed.port_or_known_default().unwrap_or(0);
    if port == 0 {
        return Err(Refused::Removed("port 0"));
    }
    let ip = match parsed.host() {
        Some(url::Host::Ipv4(ip)) => IpAddr::V4(ip),
        Some(url::Host::Ipv6(ip)) => IpAddr::V6(ip),
        _ => return Ok(None),
    };
    Ok(Some(SocketAddr::new(ip, port)))
}

/// Why a request may not go to where its URL's host resolved.
#[derive(Debug, PartialEq, Eq)]
enum Unreachable {
    /// No address is one the target may be asked at (see
    /// [`NamedBy::may_ask`]).
    Refused,
    /// The session's IP filter blocks the address.
    Blocked(IpAddr),
}

/// The one address a request for `target`, whose host resolved to `addrs`
/// at `port`, goes to: the first its naming lets it be asked at, which the
/// IP filter `allowed` must let through.
fn endpoint(
    addrs: &[IpAddr],
    port: u16,
    target: &Target,
    allowed: &dyn Fn(SocketAddr) -> bool,
) -> Result<IpAddr, Unreachable> {
    let Some(&addr) = addrs.iter().find(|a| target.named_by.may_ask(**a)) else {
        return Err(Unreachable::Refused);
    };
    if !allowed(SocketAddr::new(addr, port)) {
        return Err(Unreachable::Blocked(addr));
    }
    Ok(addr)
}

/// The open connection to one web seed. Dropping it lets the seed be
/// connected to again.
pub(crate) struct SeedLink {
    seeds: Arc<WebSeeds>,
    id: u32,
    seed: WebSeed,
    cancel: CancellationToken,
}

impl Drop for SeedLink {
    fn drop(&mut self) {
        self.seeds.with_entry(self.id, |e| e.connection = None);
        self.seeds.changed.notify_one();
    }
}

impl SeedLink {
    /// The seed's address in the peer table.
    pub fn handle(&self) -> PeerHandle {
        handle_of(self.id)
    }

    fn lacks(&self) -> Lacks {
        self.seeds
            .with_entry(self.id, |e| e.lacks.clone())
            .unwrap_or_default()
    }

    fn redirect(&self, file: usize) -> Option<Redirect> {
        self.seeds
            .with_entry(self.id, |e| e.redirects.get(&file).cloned())
            .flatten()
    }

    /// The server asked to be left for `retry_after`, or the default wait
    /// when it did not say (libtorrent `torrent::retry_web_seed`).
    fn retry_after(&self, retry_after: Option<Duration>) {
        let at = Instant::now() + retry_after.unwrap_or(URLSEED_WAIT_RETRY);
        self.seeds.with_entry(self.id, |e| {
            if !e.disabled {
                e.retry = Some(at);
            }
        });
    }

    /// The connection failed: the seed waits at least URLSEED_WAIT_RETRY
    /// (libtorrent `web_peer_connection::disconnect`).
    pub fn failed(&self) {
        let at = Instant::now() + URLSEED_WAIT_RETRY;
        self.seeds.with_entry(self.id, |e| {
            e.retry = Some(e.retry.map_or(at, |r| r.max(at)));
        });
    }

    fn disable(&self) {
        self.seeds.with_entry(self.id, |e| e.disabled = true);
    }

    fn remove(&self) {
        let mut g = self.seeds.inner.lock();
        if let Some(at) = g.seeds.iter().position(|e| e.id == self.id) {
            remove_at(&mut g, at);
        }
    }

    fn file_missing(&self, file: usize) {
        self.seeds
            .with_entry(self.id, |e| e.lacks.files.insert(file));
    }

    fn redirected(&self, file: usize, to: Redirect) {
        self.seeds
            .with_entry(self.id, |e| e.redirects.insert(file, to));
    }
}

/// One file of the torrent, as a web seed serves it.
#[derive(Debug, Clone)]
pub(crate) struct LayoutFile {
    offset: u64,
    len: u64,
    pad: bool,
    // The file's path in the torrent, escaped for a URL: the torrent's name,
    // then the file's path in a multi-file torrent (libtorrent
    // `escape_file_path`, `file_storage::file_path`).
    path: String,
}

/// What a web seed's requests are built from.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    info_hash: Id20,
    lengths: Lengths,
    files: Vec<LayoutFile>,
}

impl Layout {
    pub fn new(metadata: &TorrentMetadata, info_hash: Id20) -> Self {
        let info = &metadata.info;
        let name = info.name().unwrap_or_default().into_owned();
        let files = info
            .iter_file_details()
            .zip(metadata.file_infos.iter())
            .map(|(details, fi)| {
                let mut path: Vec<String> = details.filename.to_vec();
                if !info.is_single_file() {
                    path.insert(0, name.clone());
                }
                LayoutFile {
                    offset: fi.offset_in_torrent,
                    len: fi.len,
                    pad: fi.attrs.padding,
                    path: escape_path(path.join("/").as_bytes()),
                }
            })
            .collect();
        Self {
            info_hash,
            lengths: *info.lengths(),
            files,
        }
    }

    /// The pieces a seed that lacks `lacks` has: every piece but those of a
    /// file it lacks that is no padding, and those it lacks itself
    /// (libtorrent `web_peer_connection::on_connected`,
    /// `received_invalid_data`).
    pub fn bitfield(&self, lacks: &Lacks) -> BF {
        let piece_len = u64::from(self.lengths.default_piece_length());
        let mut have = empty_bitfield(self.lengths.total_pieces());
        have[..self.lengths.total_pieces() as usize].fill(true);
        for (i, f) in self.files.iter().enumerate() {
            if f.pad || f.len == 0 || !lacks.files.contains(&i) {
                continue;
            }
            let first = f.offset / piece_len;
            let last = (f.offset + f.len - 1) / piece_len;
            #[allow(clippy::cast_possible_truncation)]
            have[first as usize..=last as usize].fill(false);
        }
        for &piece in &lacks.pieces {
            have.set(piece as usize, false);
        }
        have
    }

    /// A seed that sent the whole of `piece`, which failed its hash check,
    /// holds another copy of the piece's file than the torrent's when the
    /// piece lies in one file, and lacks the file from then on; a piece
    /// spanning several, it lacks alone (libtorrent
    /// `web_peer_connection::received_invalid_data`). A pad file counts for
    /// none: its zeros are not asked of the seed.
    fn failed(&self, piece: ValidPieceIndex, lacks: &mut Lacks) {
        let slices = self.slices(
            self.lengths.piece_offset(piece),
            u64::from(self.lengths.piece_length(piece)),
        );
        let mut files = slices.iter().filter(|s| !self.files[s.file].pad);
        match (files.next(), files.next()) {
            (Some(only), None) => lacks.files.insert(only.file),
            _ => lacks.pieces.insert(piece.get()),
        };
    }

    fn single_file(&self) -> bool {
        self.files.len() == 1
    }

    // Where request `r` starts in the torrent.
    fn offset_of(&self, r: &Request) -> u64 {
        u64::from(r.index) * u64::from(self.lengths.default_piece_length()) + u64::from(r.begin)
    }

    // The parts of `len` bytes at `offset` of the torrent in each file, in
    // order (libtorrent `file_storage::map_block`).
    fn slices(&self, offset: u64, len: u64) -> Vec<Slice> {
        let end = offset + len;
        self.files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.len > 0 && f.offset < end && f.offset + f.len > offset)
            .map(|(file, f)| {
                let start = offset.max(f.offset);
                let stop = end.min(f.offset + f.len);
                Slice {
                    file,
                    offset: start - f.offset,
                    len: stop - start,
                }
            })
            .collect()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Slice {
    file: usize,
    offset: u64,
    len: u64,
}

/// Requests for contiguous bytes of the torrent, fetched with one HTTP
/// request per file they span.
#[derive(Debug)]
struct Run {
    offset: u64,
    len: u64,
    requests: VecDeque<Request>,
}

/// Where one request goes, and what the hosts it may be asked of are held
/// to.
struct Target {
    url: Url,
    // Who it is asked for as: the seed's naming, or held to public
    // addresses once a server at one moved it here.
    named_by: NamedBy,
    // The file moved here, when it did.
    moved: Option<usize>,
}

/// Where a request goes: the address its host was checked at, none when the
/// network resolves the name where it exits, and a client that reaches the
/// host there and nowhere else.
struct Reached {
    addr: Option<IpAddr>,
    client: reqwest::Client,
}

/// A connection to one web seed.
pub(crate) struct WebSeedConnection<'a> {
    // Builds a client for each address a request is checked at.
    clients: Arc<dyn HttpClients>,
    // Resolves the seeds' host names, so their addresses are known before
    // they are asked.
    resolver: Arc<dyn Resolver>,
    // The session's IP filter: whether a connection may go to an address.
    allowed: &'a (dyn Fn(SocketAddr) -> bool + Send + Sync),
    layout: &'a Layout,
    link: &'a SeedLink,
    // The client the torrent names itself as: every request's User-Agent.
    user_agent: &'a str,
    // The clients built for this connection, by host, port and address, so
    // its requests to one address share their connections.
    pinned: Mutex<PinnedClients>,
}

type PinnedClients = HashMap<(String, u16, Option<IpAddr>), reqwest::Client>;

impl<'a> WebSeedConnection<'a> {
    pub fn new(
        clients: Arc<dyn HttpClients>,
        resolver: Arc<dyn Resolver>,
        allowed: &'a (dyn Fn(SocketAddr) -> bool + Send + Sync),
        layout: &'a Layout,
        link: &'a SeedLink,
        user_agent: &'a str,
    ) -> Self {
        Self {
            clients,
            resolver,
            allowed,
            layout,
            link,
            user_agent,
            pinned: Mutex::default(),
        }
    }

    /// Runs the connection: tells `handler` the seed has the pieces of the
    /// files it serves and does not choke, then fetches what `rx` asks for
    /// and hands each block back as a PIECE, until the torrent closes the
    /// connection or the seed fails.
    pub async fn run<H: PeerConnectionHandler>(
        &self,
        handler: H,
        mut rx: PeerRx,
    ) -> crate::Result<()> {
        let anyhow = crate::Error::Anyhow;
        handler.on_connected(Duration::ZERO);
        handler
            .on_handshake(
                Handshake::new(self.layout.info_hash, Id20::new(rand::random())),
                ConnectionKind::Tcp,
                false,
            )
            .map_err(anyhow)?;
        let have = self.layout.bitfield(&self.link.lacks());
        handler
            .on_received_message(Message::Bitfield(ByteBuf(have.as_raw_slice())))
            .await
            .map_err(anyhow)?;
        handler
            .on_received_message(Message::Unchoke)
            .await
            .map_err(anyhow)?;

        let (blocks_tx, mut blocks_rx) = mpsc::channel::<(Request, Bytes)>(BLOCKS_BUFFERED);
        let mut queue: VecDeque<Request> = VecDeque::new();
        let mut fetches = FuturesUnordered::new();
        let deliver = async |(r, data): (Request, Bytes)| {
            handler
                .on_received_message(Message::Piece(Piece::from_data(r.index, r.begin, &data)))
                .await
                .map_err(anyhow)
        };
        loop {
            while fetches.len() < URLSEED_PIPELINE_SIZE {
                let Some(run) = self.take_run(&mut queue) else {
                    break;
                };
                fetches.push(self.fetch(run, blocks_tx.clone()));
            }
            tokio::select! {
                biased;
                _ = self.link.cancel.cancelled() => return Ok(()),
                Some(block) = blocks_rx.recv() => deliver(block).await?,
                msg = rx.recv() => {
                    // Every message queued, so the requests that follow
                    // each other are fetched together.
                    let mut msg = Some(msg.ok_or(crate::Error::PeerTaskDead)?);
                    while let Some(m) = msg {
                        match m {
                            WriterRequest::Message(Message::Request(r)) => queue.push_back(r),
                            WriterRequest::Message(Message::Cancel(c)) => queue.retain(|r| {
                                (r.index, r.begin, r.length) != (c.index, c.begin, c.length)
                            }),
                            WriterRequest::Disconnect(res) => return res.map_err(anyhow),
                            // A web seed hears nothing else: it downloads
                            // nothing, and speaks no extension.
                            _ => {}
                        }
                        msg = rx.try_recv().ok();
                    }
                }
                Some(fetched) = fetches.next(), if !fetches.is_empty() => {
                    if let Err(e) = fetched {
                        // The blocks read before the failure are good.
                        while let Ok(block) = blocks_rx.try_recv() {
                            deliver(block).await?;
                        }
                        return Err(anyhow(e));
                    }
                }
            }
        }
    }

    // The next requests to fetch at once: those at the front of the queue
    // that follow each other in the torrent, up to URLSEED_MAX_REQUEST_BYTES,
    // in one piece for a BEP 17 seed, which is asked by piece (libtorrent
    // `request_large_blocks`, `merge_block_requests`).
    fn take_run(&self, queue: &mut VecDeque<Request>) -> Option<Run> {
        let first = queue.pop_front()?;
        let mut run = Run {
            offset: self.layout.offset_of(&first),
            len: u64::from(first.length),
            requests: VecDeque::from([first]),
        };
        while let Some(next) = queue.front() {
            let follows = self.layout.offset_of(next) == run.offset + run.len
                && run.len + u64::from(next.length) <= URLSEED_MAX_REQUEST_BYTES
                && (self.link.seed.kind == WebSeedKind::Url || next.index == first.index);
            if !follows {
                break;
            }
            run.len += u64::from(next.length);
            run.requests.push_back(queue.pop_front()?);
        }
        Some(run)
    }

    async fn fetch(&self, run: Run, out: mpsc::Sender<(Request, Bytes)>) -> anyhow::Result<()> {
        let first = *run.requests.front().context("bug: an empty run")?;
        let mut blocks = Blocks {
            requests: run.requests,
            buf: BytesMut::new(),
            out,
        };
        match self.link.seed.kind {
            WebSeedKind::Url => {
                for slice in self.layout.slices(run.offset, run.len) {
                    if self.layout.files[slice.file].pad {
                        #[allow(clippy::cast_possible_truncation)]
                        blocks.push(&vec![0; slice.len as usize]).await?;
                    } else {
                        self.fetch_slice(&slice, &mut blocks).await?;
                    }
                }
            }
            WebSeedKind::Http => self.fetch_piece(first, run.len, &mut blocks).await?,
        }
        Ok(())
    }

    // The URL of `file` (libtorrent `web_peer_connection::write_request`):
    // where the server moved it, the seed's URL itself in a torrent of one
    // file whose URL names it, the file's path under the URL otherwise.
    fn file_target(&self, file: usize) -> anyhow::Result<Target> {
        if let Some(to) = self.link.redirect(file) {
            return Ok(Target {
                url: Url::parse(&to.url).context("bad web seed URL")?,
                named_by: to.named_by,
                moved: Some(file),
            });
        }
        let url = &self.link.seed.url;
        let url = if self.layout.single_file() && !url.ends_with('/') {
            url.clone()
        } else {
            let slash = if url.ends_with('/') { "" } else { "/" };
            format!("{url}{slash}{}", self.layout.files[file].path)
        };
        self.seed_target(url)
    }

    // `url`, built on the seed's own URL.
    fn seed_target(&self, url: String) -> anyhow::Result<Target> {
        Ok(Target {
            url: Url::parse(&url).context("bad web seed URL")?,
            named_by: self.link.seed.named_by,
            moved: None,
        })
    }

    // Where `target` is asked: the one address of its host its URL may be
    // asked at, and a client held to it. A seed that may not be asked there
    // is disabled; a file moved where it may not be asked is lost to the
    // seed. A name that does not resolve leaves the seed for
    // WEB_SEED_NAME_LOOKUP_RETRY. A name the session's network resolves
    // where it exits has no address here to check, and its client hands the
    // name on.
    async fn reach(&self, target: &Target) -> anyhow::Result<Reached> {
        let url = &target.url;
        let port = url.port_or_known_default().unwrap_or(0);
        let (host, addrs) = match url.host() {
            Some(url::Host::Ipv4(ip)) => (ip.to_string(), Some(vec![IpAddr::V4(ip)])),
            Some(url::Host::Ipv6(ip)) => (ip.to_string(), Some(vec![IpAddr::V6(ip)])),
            Some(url::Host::Domain(name)) => {
                let found =
                    tokio::time::timeout(URLSEED_TIMEOUT, self.resolver.resolve(name, port))
                        .await
                        .map_err(|_| anyhow!("{url}: the name lookup timed out"))
                        .and_then(|r| r.with_context(|| format!("{url}: the name lookup failed")))
                        .and_then(|resolved| match resolved {
                            Resolved::Addresses(found) => {
                                anyhow::ensure!(
                                    !found.is_empty(),
                                    "{url}: the name has no address"
                                );
                                Ok(Some(found.iter().map(SocketAddr::ip).collect()))
                            }
                            Resolved::AtExit => Ok(None),
                        });
                match found {
                    Ok(found) => (name.to_owned(), found),
                    Err(e) => {
                        self.link.retry_after(Some(WEB_SEED_NAME_LOOKUP_RETRY));
                        return Err(e);
                    }
                }
            }
            None => return Err(anyhow!("{url}: no host")),
        };
        let addr = match addrs.map(|addrs| endpoint(&addrs, port, target, self.allowed)) {
            None => None,
            Some(Ok(addr)) => Some(addr),
            Some(Err(Unreachable::Blocked(addr))) => {
                return Err(anyhow!("{url}: the IP filter blocks {addr}"));
            }
            Some(Err(Unreachable::Refused)) => {
                match target.moved {
                    Some(file) => self.link.file_missing(file),
                    None => self.link.disable(),
                }
                return Err(anyhow!("{url}: no address it may be asked at"));
            }
        };
        let key = (host, port, addr);
        if let Some(client) = self.pinned.lock().get(&key) {
            return Ok(Reached {
                addr,
                client: client.clone(),
            });
        }
        let client = self
            .clients
            .client(Some(HttpTarget {
                host: &key.0,
                port,
                addrs: addr.as_slice(),
            }))
            .with_context(|| format!("{url}: building its client"))?;
        self.pinned.lock().insert(key, client.clone());
        Ok(Reached { addr, client })
    }

    // Asks for `target`, its bytes in `range` when set, and follows where
    // it moves: each redirect is a target of its own, held to public
    // addresses when a server at one moved it, and reached as the first was,
    // at most MAX_REDIRECTS of them. `file` is what moves; with none, the
    // seed itself. The answer, and the target that gave it.
    async fn get(
        &self,
        mut target: Target,
        range: Option<(u64, u64)>,
        file: Option<usize>,
    ) -> anyhow::Result<(reqwest::Response, Target)> {
        for _ in 0..=MAX_REDIRECTS {
            let reached = self.reach(&target).await?;
            let mut request = reached.client.get(target.url.clone());
            if let Some((first, last)) = range {
                request = request.header(header::RANGE, format!("bytes={first}-{last}"));
            }
            let response = self.send(request).await?;
            let status = response.status();
            if !status.is_redirection() {
                return Ok((response, target));
            }
            let Some(to) = response
                .headers()
                .get(header::LOCATION)
                .and_then(|l| l.to_str().ok())
                .and_then(|l| target.url.join(l).ok())
            else {
                self.link.disable();
                return Err(anyhow!("{}: {status} without a location", target.url));
            };
            target = Target {
                url: to,
                named_by: match reached.addr {
                    Some(addr) if is_public(addr) => NamedBy::Torrent,
                    _ => target.named_by,
                },
                moved: file,
            };
        }
        Err(anyhow!(
            "{}: more than {MAX_REDIRECTS} redirects",
            target.url
        ))
    }

    // BEP 19: the slice's bytes of its file, by range (libtorrent
    // `web_peer_connection::write_request`, `on_receive`).
    async fn fetch_slice(&self, slice: &Slice, blocks: &mut Blocks) -> anyhow::Result<()> {
        let asked = self.file_target(slice.file)?;
        let first_url = asked.url.clone();
        let last = slice.offset + slice.len - 1;
        let (response, target) = self
            .get(asked, Some((slice.offset, last)), Some(slice.file))
            .await?;
        let url = target.url;
        let status = response.status();
        if !is_ok_status(status) {
            // The server lacks the file, or cannot serve it now.
            self.link.file_missing(slice.file);
            self.link.retry_after(retry_after(&response));
            return Err(anyhow!("{url}: {status}"));
        }
        if url != first_url {
            // The file moved: it is asked for where it went from now on.
            self.link.redirected(
                slice.file,
                Redirect {
                    url: url.to_string(),
                    named_by: target.named_by,
                },
            );
        }
        let range = if status == StatusCode::PARTIAL_CONTENT {
            content_range(&response)
        } else {
            content_length(&response).map(|len| (0, len))
        };
        let Some((start, end)) = range else {
            self.link.disable();
            return Err(anyhow!("{url}: {status} names no range"));
        };
        if (start, end) != (slice.offset, slice.offset + slice.len) {
            return Err(anyhow!(
                "{url}: range {start}..{end} answers {}..{}",
                slice.offset,
                slice.offset + slice.len
            ));
        }
        self.read_body(response, slice.len, blocks).await
    }

    // BEP 17: the run's bytes of its piece, by `piece` and `ranges`. A
    // moved seed is held to what a moved file is; the seed itself is what
    // is lost.
    async fn fetch_piece(
        &self,
        first: Request,
        len: u64,
        blocks: &mut Blocks,
    ) -> anyhow::Result<()> {
        let piece = self
            .layout
            .lengths
            .validate_piece_index(first.index)
            .context("request past the last piece")?;
        let url = &self.link.seed.url;
        let sep = if url.contains('?') { '&' } else { '?' };
        let mut url = format!(
            "{url}{sep}info_hash={}&piece={}",
            Escaped(&self.layout.info_hash.0),
            first.index
        );
        if first.begin > 0 || len != u64::from(self.layout.lengths.piece_length(piece)) {
            let begin = u64::from(first.begin);
            url += &format!("&ranges={begin}-{}", begin + len - 1);
        }
        let (response, target) = self.get(self.seed_target(url)?, None, None).await?;
        let url = target.url;
        let status = response.status();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            // The body is how many seconds to wait.
            let body =
                tokio::time::timeout(URLSEED_TIMEOUT, read_capped(response, MAX_UNAVAILABLE_BODY))
                    .await
                    .context("timed out")?
                    .unwrap_or_default();
            let wait = std::str::from_utf8(&body)
                .ok()
                .and_then(wait_seconds)
                .unwrap_or(HTTPSEED_DEFAULT_RETRY);
            self.link.retry_after(Some(wait));
            return Err(anyhow!("{url}: {status}"));
        }
        if status != StatusCode::OK {
            self.link.remove();
            return Err(anyhow!("{url}: {status}"));
        }
        self.read_body(response, len, blocks).await
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        // Ranges of the bytes as stored, not of a compressed body.
        let request = request
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::USER_AGENT, self.user_agent);
        tokio::time::timeout(URLSEED_TIMEOUT, request.send())
            .await
            .context("timed out")?
            .context("request failed")
    }

    // Reads exactly `len` bytes of body into `blocks`.
    async fn read_body(
        &self,
        mut response: reqwest::Response,
        len: u64,
        blocks: &mut Blocks,
    ) -> anyhow::Result<()> {
        let url = response.url().clone();
        let mut left = len;
        loop {
            let chunk = tokio::time::timeout(URLSEED_TIMEOUT, response.chunk())
                .await
                .context("timed out")?
                .with_context(|| format!("{url}: reading the body"))?;
            let Some(chunk) = chunk else {
                break;
            };
            if chunk.len() as u64 > left {
                return Err(anyhow!("{url}: the body is longer than the range"));
            }
            left -= chunk.len() as u64;
            blocks.push(&chunk).await?;
        }
        if left > 0 {
            return Err(anyhow!("{url}: the body ended {left} bytes short"));
        }
        Ok(())
    }
}

// libtorrent `aux::is_ok_status`.
fn is_ok_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 200 | 206 | 300..=399)
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    wait_seconds(response.headers().get(header::RETRY_AFTER)?.to_str().ok()?)
}

// libtorrent `http_parser::header_duration`: seconds, none unless above 0,
// at most what a `seconds32` holds.
fn wait_seconds(text: &str) -> Option<Duration> {
    let secs = match text.trim().parse::<u64>() {
        Ok(secs) => secs,
        Err(e) if *e.kind() == IntErrorKind::PosOverflow => u64::MAX,
        Err(_) => return None,
    };
    (secs > 0).then(|| Duration::from_secs(secs.min(MAX_RETRY_WAIT_SECS)))
}

/// The body, refused once it passes `cap` bytes.
async fn read_capped(mut response: reqwest::Response, cap: usize) -> anyhow::Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > cap - body.len() {
            anyhow::bail!("the body is longer than {cap} bytes");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

// The range a 206 answers, end exclusive: `bytes first-last/length`.
fn content_range(response: &reqwest::Response) -> Option<(u64, u64)> {
    let value = response
        .headers()
        .get(header::CONTENT_RANGE)?
        .to_str()
        .ok()?;
    let (range, _) = value.strip_prefix("bytes ")?.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    let (first, last) = (
        first.trim().parse::<u64>().ok()?,
        last.trim().parse::<u64>().ok()?,
    );
    (last >= first).then_some((first, last + 1))
}

fn content_length(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

/// Cuts the bytes of a run into its requests' blocks as they arrive.
struct Blocks {
    requests: VecDeque<Request>,
    buf: BytesMut,
    out: mpsc::Sender<(Request, Bytes)>,
}

impl Blocks {
    async fn push(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.buf.extend_from_slice(data);
        while let Some(r) = self.requests.front()
            && self.buf.len() >= r.length as usize
        {
            let block = self.buf.split_to(r.length as usize).freeze();
            let r = self.requests.pop_front().context("bug: no request")?;
            self.out
                .send((r, block))
                .await
                .map_err(|_| anyhow!("the connection closed"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{Ipv4Addr, TcpListener, TcpStream},
        sync::atomic::{AtomicBool, Ordering},
    };

    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::{core::lengths::ChunkInfo, peer_protocol::extended::handshake::ExtendedHandshake};

    // What the server was asked.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Head {
        target: String,
        range: Option<(u64, u64)>,
        user_agent: Option<String>,
    }

    struct Reply {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
        // How much of the body is sent before the connection closes.
        sent: Option<usize>,
        content_length: bool,
    }

    impl Reply {
        fn status(status: u16) -> Self {
            Reply {
                status,
                headers: Vec::new(),
                body: Vec::new(),
                sent: None,
                content_length: true,
            }
        }

        fn header(mut self, name: &'static str, value: impl Into<String>) -> Self {
            self.headers.push((name, value.into()));
            self
        }

        fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
            self.body = body.into();
            self
        }

        // `content` as a server that honours ranges serves it.
        fn ranged(content: &[u8], head: &Head) -> Self {
            match head.range {
                Some((first, last)) => Reply::status(206)
                    .header(
                        "Content-Range",
                        format!("bytes {first}-{last}/{}", content.len()),
                    )
                    .body(&content[first as usize..=last as usize]),
                None => Reply::status(200).body(content),
            }
        }
    }

    // An HTTP server on loopback, one request per connection. Its accept
    // loop runs on the blocking pool, which holds the paused clock still
    // (tokio does not advance it while a blocking task runs): time moves
    // only when a test moves it.
    struct Server {
        addr: SocketAddr,
        seen: Arc<Mutex<Vec<Head>>>,
        stop: Arc<AtomicBool>,
    }

    type Respond = dyn Fn(&Head) -> Reply + Send + Sync;

    impl Server {
        fn start(respond: impl Fn(&Head) -> Reply + Send + Sync + 'static) -> Self {
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let addr = listener.local_addr().unwrap();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let respond: Arc<Respond> = Arc::new(respond);
            {
                let (seen, stop) = (seen.clone(), stop.clone());
                tokio::task::spawn_blocking(move || {
                    for conn in listener.incoming() {
                        if stop.load(Ordering::SeqCst) {
                            return;
                        }
                        let Ok(conn) = conn else { continue };
                        let (respond, seen) = (respond.clone(), seen.clone());
                        std::thread::spawn(move || serve(conn, &*respond, &seen));
                    }
                });
            }
            Self { addr, seen, stop }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{path}", self.addr)
        }

        fn seen(&self) -> Vec<Head> {
            self.seen.lock().clone()
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);
        }
    }

    fn serve(mut conn: TcpStream, respond: &Respond, seen: &Mutex<Vec<Head>>) {
        let mut buf = Vec::new();
        let mut read = [0u8; 4096];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            match conn.read(&mut read) {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&read[..n]),
            }
        }
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        req.parse(&buf).unwrap();
        let range = req
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("range"))
            .map(|h| {
                let v = std::str::from_utf8(h.value).unwrap();
                let (a, b) = v.strip_prefix("bytes=").unwrap().split_once('-').unwrap();
                (a.parse().unwrap(), b.parse().unwrap())
            });
        let user_agent = req
            .headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case("user-agent"))
            .map(|h| String::from_utf8(h.value.to_vec()).unwrap());
        let head = Head {
            target: req.path.unwrap().to_owned(),
            range,
            user_agent,
        };
        let reply = respond(&head);
        seen.lock().push(head);
        let mut out = format!("HTTP/1.1 {} X\r\nConnection: close\r\n", reply.status);
        if reply.content_length {
            out += &format!("Content-Length: {}\r\n", reply.body.len());
        }
        for (name, value) in &reply.headers {
            out += &format!("{name}: {value}\r\n");
        }
        out += "\r\n";
        let sent = reply.sent.unwrap_or(reply.body.len());
        let _ = conn.write_all(out.as_bytes());
        let _ = conn.write_all(&reply.body[..sent]);
    }

    // A peer handler that records what the web seed tells it.
    #[derive(Default)]
    struct Fake {
        bitfield: Mutex<Option<Vec<u8>>>,
        unchoked: AtomicBool,
        pieces: Mutex<Vec<(u32, u32, Vec<u8>)>>,
        arrived: Notify,
    }

    impl Fake {
        async fn wait_for(&self, n: usize) {
            loop {
                let arrived = self.arrived.notified();
                if self.pieces.lock().len() >= n {
                    return;
                }
                arrived.await;
            }
        }

        // The bytes delivered, in torrent order.
        fn bytes(&self, layout: &Layout) -> Vec<u8> {
            let mut pieces = self.pieces.lock().clone();
            pieces.sort_by_key(|(i, b, _)| (*i, *b));
            let mut out = Vec::new();
            for (i, b, data) in pieces {
                assert_eq!(
                    layout.offset_of(&Request {
                        index: i,
                        begin: b,
                        length: 0
                    }),
                    out.len() as u64
                );
                out.extend(data);
            }
            out
        }
    }

    impl PeerConnectionHandler for &Fake {
        fn should_send_bitfield(&self) -> bool {
            false
        }
        fn serialize_bitfield_message_to_buf(&self, _: &mut [u8]) -> anyhow::Result<usize> {
            Ok(0)
        }
        fn on_handshake(&self, _: Handshake, _: ConnectionKind, _: bool) -> anyhow::Result<()> {
            Ok(())
        }
        fn on_extended_handshake(&self, _: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
            Ok(())
        }
        async fn on_received_message(&self, msg: Message<'_>) -> anyhow::Result<()> {
            match msg {
                Message::Bitfield(b) => *self.bitfield.lock() = Some(b.as_ref().to_vec()),
                Message::Unchoke => self.unchoked.store(true, Ordering::SeqCst),
                Message::Piece(p) => {
                    let (b0, b1) = p.data();
                    let data = [b0, b1].concat();
                    self.pieces.lock().push((p.index, p.begin, data));
                    self.arrived.notify_waiters();
                }
                other => panic!("unexpected {other:?}"),
            }
            Ok(())
        }
        fn on_uploaded_bytes(&self, _: u32) {}
        fn read_chunk(&self, _: &ChunkInfo, _: &mut [u8]) -> anyhow::Result<()> {
            anyhow::bail!("a web seed uploads nothing")
        }
    }

    const PIECE: u32 = 32768;

    fn content(len: usize, salt: u8) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8 ^ salt).collect()
    }

    // Files `(name, len, pad)` of a torrent named `t`, multi-file unless
    // there is one.
    fn layout(files: &[(&str, u64, bool)]) -> Layout {
        let total = files.iter().map(|f| f.1).sum();
        let mut offset = 0;
        let single = files.len() == 1;
        let files = files
            .iter()
            .map(|&(name, len, pad)| {
                let path = if single {
                    name.to_owned()
                } else {
                    format!("t/{name}")
                };
                let f = LayoutFile {
                    offset,
                    len,
                    pad,
                    path: escape_path(path.as_bytes()),
                };
                offset += len;
                f
            })
            .collect();
        Layout {
            info_hash: Id20::new([7; 20]),
            lengths: Lengths::new(total, PIECE).unwrap(),
            files,
        }
    }

    // Every block of `pieces`, in order.
    fn blocks(layout: &Layout, pieces: std::ops::Range<u32>) -> Vec<Request> {
        pieces
            .flat_map(|p| {
                let p = layout.lengths.validate_piece_index(p).unwrap();
                layout.lengths.iter_chunk_infos(p).map(move |c| Request {
                    index: p.get(),
                    begin: c.offset,
                    length: c.size,
                })
            })
            .collect()
    }

    // The client the tests' torrent names itself as.
    const UA: &str = "Client/1.0";

    // Every address may be connected to.
    const ALL: &(dyn Fn(SocketAddr) -> bool + Send + Sync) = &|_| true;

    // A client looks no name up: it reaches a host only at the addresses it
    // was built for.
    struct NoNames;

    impl reqwest::dns::Resolve for NoNames {
        fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let name = name.as_str().to_owned();
            Box::pin(async move { Err(format!("{name}: no lookups in tests").into()) })
        }
    }

    fn builder() -> reqwest::ClientBuilder {
        reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(Arc::new(NoNames))
    }

    // The session's own clients.
    fn direct() -> Arc<dyn HttpClients> {
        Arc::new(builder)
    }

    // The clients of a network where every address is the server at `at`:
    // a client built for an address reaches the server there. Records the
    // addresses clients were built for.
    struct Wired {
        at: SocketAddr,
        built: Mutex<Vec<IpAddr>>,
    }

    impl Wired {
        fn new(at: SocketAddr) -> Arc<Self> {
            Arc::new(Self {
                at,
                built: Mutex::default(),
            })
        }

        fn built(&self) -> Vec<IpAddr> {
            self.built.lock().clone()
        }
    }

    impl HttpClients for Wired {
        fn builder(
            &self,
            target: Option<HttpTarget<'_>>,
        ) -> anyhow::Result<reqwest::ClientBuilder> {
            let at = [self.at.ip()];
            let target = target.map(|t| {
                self.built.lock().extend(t.addrs);
                HttpTarget { addrs: &at, ..t }
            });
            Ok(crate::core::net::for_target(builder(), target))
        }
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    // A resolver that knows `names` and no other.
    struct Names(HashMap<String, Vec<IpAddr>>);

    impl Resolver for Names {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            let found = self.0.get(host).map(|ips| {
                ips.iter()
                    .map(|ip| SocketAddr::new(*ip, port))
                    .collect::<Vec<_>>()
            });
            Box::pin(async move {
                found
                    .map(crate::core::net::Resolved::Addresses)
                    .ok_or_else(|| std::io::ErrorKind::NotFound.into())
            })
        }
    }

    fn names(names: &[(&str, &str)]) -> Arc<dyn Resolver> {
        let mut map: HashMap<String, Vec<IpAddr>> = HashMap::new();
        for (name, ip) in names {
            map.entry((*name).into())
                .or_default()
                .push(ip.parse().unwrap());
        }
        Arc::new(Names(map))
    }

    // A seed the operator named: the test servers listen on loopback.
    fn one_seed(url: String, kind: WebSeedKind) -> (Arc<WebSeeds>, SeedLink) {
        seed_named(url, kind, NamedBy::Operator)
    }

    fn seed_named(url: String, kind: WebSeedKind, named_by: NamedBy) -> (Arc<WebSeeds>, SeedLink) {
        let seeds = Arc::new(WebSeeds::new([WebSeed {
            url,
            kind,
            named_by,
        }]));
        let link = seeds.link(0).unwrap();
        (seeds, link)
    }

    fn entry<R>(seeds: &WebSeeds, f: impl FnOnce(&Entry) -> R) -> R {
        seeds.with_entry(0, |e| f(e)).unwrap()
    }

    fn lacking(files: &[usize], pieces: &[u32]) -> Lacks {
        Lacks {
            files: files.iter().copied().collect(),
            pieces: pieces.iter().copied().collect(),
        }
    }

    // The pieces a seed that lacks `lacks` has, one bool each.
    fn has(layout: &Layout, lacks: &Lacks) -> Vec<bool> {
        let total = layout.lengths.total_pieces() as usize;
        layout.bitfield(lacks)[..total].iter().by_vals().collect()
    }

    // Runs the connection on `requests` until `until` blocks arrived, or
    // it ends.
    async fn drive(
        conn: &WebSeedConnection<'_>,
        fake: &Fake,
        requests: &[Request],
        until: usize,
    ) -> crate::Result<()> {
        let (tx, rx) = unbounded_channel();
        for r in requests {
            tx.send(WriterRequest::Message(Message::Request(*r)))
                .unwrap();
        }
        tokio::select! {
            r = conn.run(fake, rx) => r,
            () = fake.wait_for(until) => Ok(()),
        }
    }

    // The seed has every piece and never chokes; requests that follow each
    // other are fetched with one range request per file they span, a pad
    // file's bytes are zeros asked of no one, and the files are under the
    // torrent's name below the seed's URL (BEP 19).
    #[tokio::test(start_paused = true)]
    async fn a_web_seed_fetches_following_requests_with_a_range_per_file() {
        let (a, b) = (content(40000, 1), content(30000, 2));
        let (sa, sb) = (a.clone(), b.clone());
        let server = Server::start(move |head| match head.target.as_str() {
            "/seed/t/a%20file" => Reply::ranged(&sa, head),
            "/seed/t/b" => Reply::ranged(&sb, head),
            _ => Reply::status(404),
        });
        let layout = layout(&[
            ("a file", 40000, false),
            ("pad", 25536, true),
            ("b", 30000, false),
        ]);
        let (_seeds, link) = one_seed(server.url("/seed"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();

        let requests = blocks(&layout, 0..3);
        drive(&conn, &fake, &requests, requests.len())
            .await
            .unwrap();

        assert_eq!(fake.bitfield.lock().clone(), Some(vec![0b1110_0000]));
        assert!(fake.unchoked.load(Ordering::SeqCst));
        let mut torrent = a;
        torrent.extend(vec![0; 25536]);
        torrent.extend(b);
        assert_eq!(fake.bytes(&layout), torrent);
        assert_eq!(
            server.seen(),
            vec![
                Head {
                    target: "/seed/t/a%20file".into(),
                    range: Some((0, 39999)),
                    user_agent: Some(UA.into()),
                },
                Head {
                    target: "/seed/t/b".into(),
                    range: Some((0, 29999)),
                    user_agent: Some(UA.into()),
                },
            ]
        );
    }

    // An error status: the server lacks the file, so the seed is had
    // without its pieces from then on, and is left for what its
    // Retry-After says, URLSEED_WAIT_RETRY at least once the connection
    // fails (libtorrent `web_peer_connection::on_receive`, `handle_error`,
    // `disconnect`).
    #[tokio::test(start_paused = true)]
    async fn an_error_status_loses_the_file_and_the_seed_waits_retry_after() {
        let a = content(40000, 1);
        let server = Server::start(move |head| match head.target.as_str() {
            "/seed/t/a" => Reply::ranged(&a, head),
            "/seed/t/b" => Reply::status(503).header("Retry-After", "120"),
            _ => Reply::status(404),
        });
        let layout = layout(&[
            ("a", 40000, false),
            ("pad", 25536, true),
            ("b", 30000, false),
        ]);
        let (seeds, link) = one_seed(server.url("/seed/"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();

        let failed = drive(&conn, &fake, &blocks(&layout, 2..3), 1).await;
        assert!(failed.is_err(), "{failed:?}");
        link.failed();
        let now = Instant::now();
        assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([2]));
        assert_eq!(
            entry(&seeds, |e| e.retry),
            Some(now + Duration::from_secs(120))
        );
        drop(link);

        let interesting = |l: &Lacks| layout.bitfield(l).any();
        assert_eq!(has(&layout, &lacking(&[2], &[])), [true, true, false]);
        let due = |at| seeds.due(at, |_| false, interesting).len();
        assert_eq!(due(now + Duration::from_secs(119)), 0);
        assert_eq!(due(now + Duration::from_secs(120)), 1);

        // Without Retry-After, and with a wait shorter than the failure's,
        // the seed waits URLSEED_WAIT_RETRY.
        for reply in [
            Reply::status(404),
            Reply::status(500).header("Retry-After", "5"),
            Reply::status(500).header("Retry-After", "-5"),
        ] {
            let reply = Mutex::new(Some(reply));
            let server = Server::start(move |_| reply.lock().take().unwrap());
            let (seeds, link) = one_seed(server.url("/f"), WebSeedKind::Url);
            let layout = super::tests::layout(&[("f", 100, false)]);
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            assert!(
                drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
                    .await
                    .is_err()
            );
            link.failed();
            assert_eq!(
                entry(&seeds, |e| e.retry),
                Some(Instant::now() + URLSEED_WAIT_RETRY)
            );
            // The only file is lost: the seed has nothing to give.
            let lacks = entry(&seeds, |e| e.lacks.clone());
            assert!(!layout.bitfield(&lacks).any());
        }
    }

    // A Retry-After past what libtorrent's `seconds32` holds is held to it,
    // rather than overflowing the instant it is added to.
    #[tokio::test(start_paused = true)]
    async fn a_retry_after_past_seconds32_waits_the_longest_seconds32() {
        for value in [
            "2147483648",
            "18446744073709551615",
            "99999999999999999999999",
        ] {
            let server = Server::start(move |_| Reply::status(503).header("Retry-After", value));
            let (seeds, link) = one_seed(server.url("/f"), WebSeedKind::Url);
            let layout = super::tests::layout(&[("f", 100, false)]);
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            assert!(
                drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
                    .await
                    .is_err()
            );
            link.failed();
            assert_eq!(
                entry(&seeds, |e| e.retry),
                Some(Instant::now() + Duration::from_secs(MAX_RETRY_WAIT_SECS)),
                "{value}"
            );
        }
    }

    // A redirect is followed, and the file is asked for where it went from
    // then on.
    #[tokio::test(start_paused = true)]
    async fn a_moved_file_is_asked_for_where_it_went() {
        let (a, b) = (content(40000, 3), content(10, 4));
        let (sa, sb) = (a.clone(), b.clone());
        let server = Server::start(move |head| match head.target.as_str() {
            "/seed/t/a" => Reply::status(302).header("Location", "/moved/a"),
            "/moved/a" => Reply::ranged(&sa, head),
            "/seed/t/b" => Reply::ranged(&sb, head),
            _ => Reply::status(404),
        });
        let layout = layout(&[("a", 40000, false), ("b", 10, false)]);
        let (seeds, link) = one_seed(server.url("/seed/"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();

        drive(&conn, &fake, &blocks(&layout, 0..1), 2)
            .await
            .unwrap();
        // The move is asked for under the torrent's client, as the seed was.
        let agents: Vec<_> = server.seen().into_iter().map(|h| h.user_agent).collect();
        assert_eq!(agents, [Some(UA.to_owned()), Some(UA.to_owned())]);
        assert_eq!(
            entry(&seeds, |e| e.redirects.clone()),
            HashMap::from([(
                0,
                Redirect {
                    url: server.url("/moved/a"),
                    // The seed is on loopback, which is no public address:
                    // the move is asked for as the seed was named.
                    named_by: NamedBy::Operator,
                }
            )])
        );
        let fake = Fake::default();
        let requests = blocks(&layout, 1..2);
        drive(&conn, &fake, &requests, requests.len())
            .await
            .unwrap();
        let targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
        assert_eq!(targets, ["/seed/t/a", "/moved/a", "/moved/a", "/seed/t/b"]);
        assert_eq!(fake.pieces.lock()[0].2, [&a[32768..], &b[..]].concat());
    }

    // The address a seed is asked at is known before it is asked, and is
    // the first its host resolves to that the seed's naming lets it be asked
    // at. A seed that came with the torrent is not asked at a loopback,
    // private, link-local or unique-local address, by IP or by a name, with
    // a query or without, and is disabled; of a name that also resolves to
    // a public address it is asked there only. A seed the operator named is
    // asked at a private address.
    #[tokio::test(start_paused = true)]
    async fn a_seed_the_torrent_named_is_asked_at_public_addresses_only() {
        let f = content(100, 5);
        let server = Server::start(move |head| Reply::ranged(&f, head));
        let port = server.addr.port();
        let layout = layout(&[("f", 100, false)]);
        let clients = Wired::new(server.addr);
        for query in ["", "?x=1"] {
            let mut refused = vec![(server.url(&format!("/f{query}")), names(&[]))];
            for private in ["10.0.0.1", "192.168.1.1", "169.254.1.1", "fd00::1"] {
                refused.push((
                    format!("http://seed.test:{port}/f{query}"),
                    names(&[("seed.test", private)]),
                ));
            }
            for (url, resolver) in refused {
                let (seeds, link) = seed_named(url.clone(), WebSeedKind::Url, NamedBy::Torrent);
                let conn =
                    WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
                let failed = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1).await;
                assert!(failed.is_err(), "{url}");
                assert!(entry(&seeds, |e| e.disabled), "{url}");
            }
        }
        assert!(server.seen().is_empty());
        assert!(clients.built().is_empty());

        let (seeds, link) = seed_named(
            format!("http://seed.test:{port}/f"),
            WebSeedKind::Url,
            NamedBy::Torrent,
        );
        let resolver = names(&[("seed.test", "10.0.0.1"), ("seed.test", "8.8.8.8")]);
        let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
        drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
            .await
            .unwrap();
        assert!(!entry(&seeds, |e| e.disabled));
        assert_eq!(server.seen().len(), 1);
        assert_eq!(clients.built(), [ip("8.8.8.8")]);

        let (seeds, link) = one_seed(format!("http://seed.test:{port}/f?x=1"), WebSeedKind::Url);
        let resolver = names(&[("seed.test", "10.0.0.1")]);
        let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
        drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
            .await
            .unwrap();
        assert!(!entry(&seeds, |e| e.disabled));
        assert_eq!(server.seen().len(), 2);
        assert_eq!(clients.built(), [ip("8.8.8.8"), ip("10.0.0.1")]);
    }

    // A seed at a public address may not move a file to a private one,
    // whoever named the seed: the engine follows the redirect itself, and
    // the private address is never asked, nor a client built for it; the
    // file is lost to the seed. Moved to a public address, the file is
    // fetched there and asked for there, at public addresses only, from
    // then on.
    #[tokio::test(start_paused = true)]
    async fn a_public_seed_may_not_move_a_file_to_a_private_address() {
        let a = content(100, 6);
        let port = Arc::new(Mutex::new(0u16));
        let location = port.clone();
        let server = Server::start(move |head| match head.target.as_str() {
            "/seed/f" => Reply::status(302).header(
                "Location",
                format!("http://moved.test:{}/moved/f", location.lock()),
            ),
            _ => Reply::ranged(&a, head),
        });
        *port.lock() = server.addr.port();
        let url = format!("http://seed.test:{}/seed/", server.addr.port());
        let layout = layout(&[("f", 100, false)]);
        let requests = blocks(&layout, 0..1);
        let targets = || {
            server
                .seen()
                .into_iter()
                .map(|h| h.target)
                .collect::<Vec<_>>()
        };

        for (n, named_by) in [NamedBy::Torrent, NamedBy::Operator]
            .into_iter()
            .enumerate()
        {
            let clients = Wired::new(server.addr);
            let resolver = names(&[("seed.test", "8.8.8.8"), ("moved.test", "192.168.1.2")]);
            let (seeds, link) = seed_named(url.clone(), WebSeedKind::Url, named_by);
            let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
            assert!(drive(&conn, &Fake::default(), &requests, 1).await.is_err());
            assert_eq!(entry(&seeds, |e| e.lacks.files.clone()), HashSet::from([0]));
            assert!(!entry(&seeds, |e| e.disabled));
            assert!(entry(&seeds, |e| e.redirects.is_empty()));
            assert_eq!(targets(), vec!["/seed/f"; n + 1]);
            assert_eq!(clients.built(), [ip("8.8.8.8")]);
        }

        let clients = Wired::new(server.addr);
        let resolver = names(&[("seed.test", "8.8.8.8"), ("moved.test", "9.9.9.9")]);
        let (seeds, link) = one_seed(url, WebSeedKind::Url);
        let conn = WebSeedConnection::new(clients.clone(), resolver, ALL, &layout, &link, UA);
        drive(&conn, &Fake::default(), &requests, 1).await.unwrap();
        assert_eq!(
            entry(&seeds, |e| e.redirects.clone()),
            HashMap::from([(
                0,
                Redirect {
                    url: format!("http://moved.test:{}/moved/f", server.addr.port()),
                    named_by: NamedBy::Torrent,
                }
            )])
        );
        assert_eq!(targets(), ["/seed/f", "/seed/f", "/seed/f", "/moved/f"]);
        assert_eq!(clients.built(), [ip("8.8.8.8"), ip("9.9.9.9")]);
    }

    // A request goes to the address its host was checked at, and only
    // there: the client looks no name up of its own, so a host it was not
    // built for is reached nowhere; a redirect's host is checked and reached
    // the same way. The connection's requests to one address share a client.
    #[tokio::test(start_paused = true)]
    async fn a_request_goes_to_the_address_its_host_was_checked_at() {
        let (a, b) = (content(40000, 7), content(10, 8));
        let (sa, sb) = (a.clone(), b.clone());
        let port = Arc::new(Mutex::new(0u16));
        let location = port.clone();
        let server = Server::start(move |head| match head.target.as_str() {
            "/seed/t/a" => Reply::ranged(&sa, head),
            "/seed/t/b" => Reply::status(307).header(
                "Location",
                format!("http://moved.test:{}/b", location.lock()),
            ),
            "/b" => Reply::ranged(&sb, head),
            _ => Reply::status(404),
        });
        *port.lock() = server.addr.port();
        let url = format!("http://seed.test:{}/seed/", server.addr.port());
        let layout = layout(&[("a", 40000, false), ("b", 10, false)]);
        let resolver = names(&[("seed.test", "127.0.0.1"), ("moved.test", "127.0.0.1")]);
        let (seeds, link) = one_seed(url.clone(), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), resolver, ALL, &layout, &link, UA);
        let fake = Fake::default();
        let requests = blocks(&layout, 0..2);
        drive(&conn, &fake, &requests, requests.len())
            .await
            .unwrap();

        let mut torrent = a;
        torrent.extend(b);
        assert_eq!(fake.bytes(&layout), torrent);
        let mut targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
        targets.sort();
        assert_eq!(targets, ["/b", "/seed/t/a", "/seed/t/b"]);
        assert_eq!(
            entry(&seeds, |e| e.redirects.clone()),
            HashMap::from([(
                1,
                Redirect {
                    url: format!("http://moved.test:{}/b", server.addr.port()),
                    named_by: NamedBy::Operator,
                }
            )])
        );
        let mut pinned: Vec<_> = conn.pinned.lock().keys().cloned().collect();
        pinned.sort();
        let at = ip("127.0.0.1");
        let port = server.addr.port();
        assert_eq!(
            pinned,
            [
                ("moved.test".into(), port, Some(at)),
                ("seed.test".into(), port, Some(at))
            ]
        );
        // The same factory's client for no target reaches no such host.
        let unpinned = direct().client(None).unwrap();
        assert!(unpinned.get(url).send().await.is_err());
    }

    // A request follows at most MAX_REDIRECTS redirects; one more fails the
    // connection, and the seed and its file are kept for a later one.
    #[tokio::test(start_paused = true)]
    async fn a_request_follows_at_most_five_redirects() {
        let f = content(100, 9);
        for (served_at, ok) in [(MAX_REDIRECTS, true), (MAX_REDIRECTS + 1, false)] {
            let sf = f.clone();
            let server = Server::start(move |head| {
                let hop: usize = head.target.strip_prefix("/r").unwrap().parse().unwrap();
                if hop == served_at {
                    Reply::ranged(&sf, head)
                } else {
                    Reply::status(302).header("Location", format!("/r{}", hop + 1))
                }
            });
            let layout = layout(&[("f", 100, false)]);
            let (seeds, link) = one_seed(server.url("/r0"), WebSeedKind::Url);
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            let fake = Fake::default();
            let fetched = drive(&conn, &fake, &blocks(&layout, 0..1), 1).await;
            assert_eq!(fetched.is_ok(), ok, "{fetched:?}");
            let hops: Vec<_> = (0..=MAX_REDIRECTS).map(|i| format!("/r{i}")).collect();
            let targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
            assert_eq!(targets, hops);
            assert!(!entry(&seeds, |e| e.disabled));
            assert!(entry(&seeds, |e| e.lacks.files.is_empty()));
            let moved = entry(&seeds, |e| e.redirects.get(&0).map(|r| r.url.clone()));
            assert_eq!(moved, ok.then(|| server.url(&format!("/r{MAX_REDIRECTS}"))));
        }
    }

    // The session's IP filter holds for the address a seed's name resolves
    // to: a blocked address is not asked, and the seed is tried again later.
    #[tokio::test(start_paused = true)]
    async fn the_ip_filter_holds_for_the_address_a_name_resolves_to() {
        let f = content(100, 10);
        let server = Server::start(move |head| Reply::ranged(&f, head));
        let url = format!("http://seed.test:{}/f", server.addr.port());
        let layout = layout(&[("f", 100, false)]);
        let resolver = || names(&[("seed.test", "127.0.0.1")]);
        let (seeds, link) = one_seed(url, WebSeedKind::Url);

        let blocked = |a: SocketAddr| a.ip() != ip("127.0.0.1");
        let conn = WebSeedConnection::new(direct(), resolver(), &blocked, &layout, &link, UA);
        let failed = drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1).await;
        assert!(failed.is_err());
        assert!(server.seen().is_empty());
        assert!(!entry(&seeds, |e| e.disabled));
        assert!(entry(&seeds, |e| e.lacks.files.is_empty()));

        let conn = WebSeedConnection::new(direct(), resolver(), ALL, &layout, &link, UA);
        drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
            .await
            .unwrap();
        assert_eq!(server.seen().len(), 1);
    }

    // A seed whose name the network resolves at its exit is asked by name:
    // its client is built for no address, and no address of it is checked
    // here, so neither the IP filter nor the seed's naming has anything to
    // hold, even for a seed that came with the torrent.
    #[tokio::test(start_paused = true)]
    async fn a_name_resolved_at_the_exit_is_asked_by_name() {
        struct AtExit;
        impl Resolver for AtExit {
            fn resolve<'a>(
                &'a self,
                _: &'a str,
                _: u16,
            ) -> crate::core::net::BoxFuture<'a, std::io::Result<Resolved>> {
                Box::pin(async { Ok(Resolved::AtExit) })
            }
        }
        let f = content(100, 10);
        let server = Server::start(move |head| Reply::ranged(&f, head));
        let url = format!("http://seed.test:{}/f", server.addr.port());
        let layout = layout(&[("f", 100, false)]);
        let (_seeds, link) = seed_named(url, WebSeedKind::Url, NamedBy::Torrent);
        let clients = Wired::new(server.addr);
        let nothing = |_: SocketAddr| false;
        let conn = WebSeedConnection::new(
            clients.clone(),
            Arc::new(AtExit),
            &nothing,
            &layout,
            &link,
            UA,
        );
        drive(&conn, &Fake::default(), &blocks(&layout, 0..1), 1)
            .await
            .unwrap();
        assert_eq!(server.seen().len(), 1);
        assert!(clients.built().is_empty(), "{:?}", clients.built());
        let pinned: Vec<_> = conn.pinned.lock().keys().cloned().collect();
        assert_eq!(pinned, [("seed.test".to_owned(), server.addr.port(), None)]);
    }

    // A body cut short fails the connection; the whole blocks it carried
    // are delivered first.
    #[tokio::test(start_paused = true)]
    async fn a_truncated_body_fails_after_its_whole_blocks() {
        let a = content(PIECE as usize, 4);
        let server = Server::start(move |head| Reply {
            sent: Some(20000),
            ..Reply::ranged(&a, head)
        });
        let layout = layout(&[("a", u64::from(PIECE), false)]);
        let (seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();

        let failed = drive(&conn, &fake, &blocks(&layout, 0..1), 2).await;
        assert!(failed.is_err(), "{failed:?}");
        assert_eq!(fake.pieces.lock().len(), 1);
        assert!(entry(&seeds, |e| e.lacks.files.is_empty() && !e.disabled));
    }

    // A response must answer the range asked for: one naming none disables
    // the seed, one naming another fails the connection (libtorrent
    // `get_range`, `web_peer_connection::on_receive`).
    #[tokio::test(start_paused = true)]
    async fn a_response_must_answer_the_range_asked_for() {
        let a = content(PIECE as usize, 5);
        let layout = layout(&[("a", u64::from(PIECE), false)]);
        for (reply, disabled) in [
            (
                Reply {
                    content_length: false,
                    ..Reply::status(200).body(a.clone())
                },
                true,
            ),
            (
                Reply::status(206).header("Content-Range", "bytes x-y/z"),
                true,
            ),
            (
                Reply::status(206).header("Content-Range", "bytes 1-32768/32769"),
                false,
            ),
            (Reply::status(200).body(&a[1..]), false),
        ] {
            let reply = Mutex::new(Some(reply));
            let server = Server::start(move |_| reply.lock().take().unwrap());
            let (seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            let fake = Fake::default();
            assert!(
                drive(&conn, &fake, &blocks(&layout, 0..1), 1)
                    .await
                    .is_err()
            );
            assert_eq!(entry(&seeds, |e| e.disabled), disabled);
            assert!(fake.pieces.lock().is_empty());
        }
        // A whole file answered with 200 is the range from its start.
        let sa = a.clone();
        let server = Server::start(move |_| Reply::status(200).body(sa.clone()));
        let (_seeds, link) = one_seed(server.url("/a"), WebSeedKind::Url);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
        let fake = Fake::default();
        drive(&conn, &fake, &blocks(&layout, 0..1), 2)
            .await
            .unwrap();
        assert_eq!(fake.bytes(&layout), a);
    }

    // BEP 17: a seed is asked by piece, with the byte ranges of a part of
    // one; a 503's body says how long to wait, and any other error removes
    // the seed.
    #[tokio::test(start_paused = true)]
    async fn an_http_seed_is_asked_by_piece() {
        let f = content(40000, 6);
        let sf = f.clone();
        let hash = "%07".repeat(20);
        let server = Server::start(move |head| {
            let query = head.target.strip_prefix("/s.php?").unwrap();
            let param = |k: &str| {
                query
                    .split('&')
                    .find_map(|kv| kv.strip_prefix(k)?.strip_prefix('='))
                    .map(str::to_owned)
            };
            assert_eq!(param("info_hash").unwrap(), hash);
            let piece: usize = param("piece").unwrap().parse().unwrap();
            let start = piece * PIECE as usize;
            let piece_bytes = &sf[start..sf.len().min(start + PIECE as usize)];
            match param("ranges") {
                Some(r) => {
                    let (a, b) = r.split_once('-').unwrap();
                    let (a, b): (usize, usize) = (a.parse().unwrap(), b.parse().unwrap());
                    Reply::status(200).body(&piece_bytes[a..=b])
                }
                None => Reply::status(200).body(piece_bytes),
            }
        });
        let layout = layout(&[("f", 40000, false)]);
        let (seeds, link) = one_seed(server.url("/s.php"), WebSeedKind::Http);
        let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);

        let fake = Fake::default();
        let requests = blocks(&layout, 0..2);
        drive(&conn, &fake, &requests, requests.len())
            .await
            .unwrap();
        assert_eq!(fake.bytes(&layout), f);
        let fake = Fake::default();
        drive(&conn, &fake, &requests[1..2], 1).await.unwrap();
        assert_eq!(fake.pieces.lock()[0].2, f[16384..32768]);
        let mut targets: Vec<_> = server.seen().into_iter().map(|h| h.target).collect();
        targets[..2].sort();
        let hash = "%07".repeat(20);
        assert_eq!(
            targets,
            [
                format!("/s.php?info_hash={hash}&piece=0"),
                format!("/s.php?info_hash={hash}&piece=1"),
                format!("/s.php?info_hash={hash}&piece=0&ranges=16384-32767"),
            ]
        );
        drop(link);
        assert_eq!(seeds.list().len(), 1);

        for (body, wait) in [
            ("90".to_owned(), 90),
            ("0".to_owned(), 60),
            ("soon".to_owned(), 60),
            // A wait past what a seconds32 holds is held to it.
            ("2147483648".to_owned(), MAX_RETRY_WAIT_SECS),
            ("99999999999999999999999".to_owned(), MAX_RETRY_WAIT_SECS),
            // The body is read to MAX_UNAVAILABLE_BODY and no further.
            (" ".repeat(MAX_UNAVAILABLE_BODY - 2) + "90", 90),
            (" ".repeat(MAX_UNAVAILABLE_BODY - 1) + "90", 60),
        ] {
            let server = Server::start(move |_| Reply::status(503).body(body.clone()));
            let (seeds, link) = one_seed(server.url("/s.php"), WebSeedKind::Http);
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            assert!(
                drive(&conn, &Fake::default(), &requests[..1], 1)
                    .await
                    .is_err()
            );
            assert_eq!(
                entry(&seeds, |e| e.retry),
                Some(Instant::now() + Duration::from_secs(wait))
            );
        }
        // A seed whose URL has a query is asked at a global address.
        let server = Server::start(|_| Reply::status(404));
        let url = format!("http://seed.test:{}/s.php?k=v", server.addr.port());
        let (seeds, link) = one_seed(url, WebSeedKind::Http);
        let conn = WebSeedConnection::new(
            Wired::new(server.addr),
            names(&[("seed.test", "8.8.8.8")]),
            ALL,
            &layout,
            &link,
            UA,
        );
        assert!(
            drive(&conn, &Fake::default(), &requests[..1], 1)
                .await
                .is_err()
        );
        assert!(seeds.list().is_empty());
        assert!(server.seen()[0].target.starts_with("/s.php?k=v&info_hash="));
    }

    // Requests that follow each other in the torrent go in one HTTP
    // request, up to URLSEED_MAX_REQUEST_BYTES, within one piece for a BEP
    // 17 seed.
    #[test]
    fn following_requests_are_fetched_together() {
        let piece = 4 * 1024 * 1024;
        let layout = Layout {
            info_hash: Id20::new([0; 20]),
            lengths: Lengths::new(10 * u64::from(piece), piece).unwrap(),
            files: vec![LayoutFile {
                offset: 0,
                len: 10 * u64::from(piece),
                pad: false,
                path: "f".into(),
            }],
        };
        let runs = |kind, requests: Vec<Request>| {
            let seeds = Arc::new(WebSeeds::new([WebSeed {
                url: "http://h/".into(),
                kind,
                named_by: NamedBy::Torrent,
            }]));
            let link = seeds.link(0).unwrap();
            let conn = WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA);
            let mut queue = VecDeque::from(requests);
            std::iter::from_fn(|| conn.take_run(&mut queue))
                .map(|r| (r.offset, r.len))
                .collect::<Vec<_>>()
        };
        let mib = 1024 * 1024;
        let all = blocks(&layout, 0..5);
        assert_eq!(
            runs(WebSeedKind::Url, all.clone()),
            [(0, 16 * mib), (16 * mib, 4 * mib)]
        );
        let mut gap = all.clone();
        gap.remove(3);
        assert_eq!(
            runs(WebSeedKind::Url, gap),
            [
                (0, 3 * 16384),
                (4 * 16384, 16 * mib),
                (16 * mib + 4 * 16384, 4 * mib - 4 * 16384)
            ]
        );
        assert_eq!(
            runs(WebSeedKind::Http, all)
                .into_iter()
                .map(|r| r.1)
                .collect::<Vec<_>>(),
            [4 * mib; 5]
        );
    }

    // A file's URL: where it moved, the seed's own URL in a torrent of one
    // file unless it ends in a slash, its escaped path under the seed's
    // URL otherwise (libtorrent `web_peer_connection::web_peer_connection`,
    // `write_request`).
    #[test]
    fn a_files_url() {
        let url_of = |seed: &str, files: &[(&str, u64, bool)], file| {
            let layout = layout(files);
            let seeds = Arc::new(WebSeeds::new([WebSeed {
                url: seed.into(),
                kind: WebSeedKind::Url,
                named_by: NamedBy::Torrent,
            }]));
            let link = seeds.link(0).unwrap();
            WebSeedConnection::new(direct(), names(&[]), ALL, &layout, &link, UA)
                .file_target(file)
                .unwrap()
                .url
                .to_string()
        };
        let one = [("f é.iso", 1, false)];
        assert_eq!(url_of("http://h/d/", &one, 0), "http://h/d/f%20%c3%a9.iso");
        assert_eq!(url_of("http://h/d.iso", &one, 0), "http://h/d.iso");
        let two = [("a", 1, false), ("sub/b c", 1, false)];
        assert_eq!(url_of("http://h/d", &two, 1), "http://h/d/t/sub/b%20c");
        assert_eq!(url_of("http://h/d/", &two, 0), "http://h/d/t/a");
    }

    // The connect policy's view of the seeds (libtorrent
    // `torrent::maybe_connect_web_seeds`, `connect_to_url_seed`,
    // `connect_web_seed`).
    // A seed that sent the whole of a bad piece lying in one file holds
    // another copy of that file: it lacks the file from then on, a pad file
    // counting for none. One spanning several files it lacks alone. What it
    // lacks is kept for its next connections, and a seed left nothing to
    // serve is not due (libtorrent
    // `web_peer_connection::received_invalid_data`, `on_connected`).
    #[test]
    fn a_bad_piece_loses_the_seed_its_file_or_itself() {
        let bits = |have: BF, n: usize| have[..n].iter().by_vals().collect::<Vec<_>>();
        let url = || WebSeed {
            url: "http://h/".into(),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Torrent,
        };

        // "a" fills piece 0 and starts piece 1, which a pad file ends.
        let padded = layout(&[
            ("a", 40000, false),
            ("pad", 25536, true),
            ("b", 30000, false),
        ]);
        let piece = |i| padded.lengths.validate_piece_index(i).unwrap();
        let seeds = WebSeeds::new([url()]);
        let have = seeds.piece_failed(handle_of(0), piece(1), &padded).unwrap();
        assert_eq!(bits(have, 3), [false, false, true]);
        assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[0], &[]));
        assert_eq!(has(&padded, &lacking(&[0], &[])), [false, false, true]);
        assert_eq!(
            seeds
                .due(Instant::now(), |_| false, |l| padded.bitfield(l).any())
                .len(),
            1
        );
        // A handle no seed has.
        assert!(
            seeds
                .piece_failed(handle_of(1), piece(2), &padded)
                .is_none()
        );

        // Piece 0 spans "x" and "y", piece 1 "y" and "z"; piece 2 is in "z".
        let spans = layout(&[
            ("x", 20000, false),
            ("y", 20000, false),
            ("z", 30000, false),
        ]);
        let piece = |i| spans.lengths.validate_piece_index(i).unwrap();
        let seeds = WebSeeds::new([url()]);
        let have = seeds.piece_failed(handle_of(0), piece(0), &spans).unwrap();
        assert_eq!(bits(have, 3), [false, true, true]);
        assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[], &[0]));
        let have = seeds.piece_failed(handle_of(0), piece(2), &spans).unwrap();
        assert_eq!(bits(have, 3), [false, false, false]);
        assert_eq!(entry(&seeds, |e| e.lacks.clone()), lacking(&[2], &[0]));
        assert!(
            seeds
                .due(Instant::now(), |_| false, |l| spans.bitfield(l).any())
                .is_empty()
        );

        // The only file of a torrent of one.
        let single = layout(&[("f", 70000, false)]);
        let seeds = WebSeeds::new([url()]);
        let piece = single.lengths.validate_piece_index(1).unwrap();
        let have = seeds.piece_failed(handle_of(0), piece, &single).unwrap();
        assert!(!have.any());
    }

    #[test]
    fn which_seeds_are_due() {
        let now = Instant::now();
        let any = |_: &Lacks| true;
        let seed = |url: &str| WebSeed {
            url: url.into(),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Torrent,
        };

        // At most MAX_WEB_SEED_CONNECTIONS, a connected seed among them.
        let seeds = Arc::new(WebSeeds::new(
            (0..5).map(|i| seed(&format!("http://h{i}.example/"))),
        ));
        let due = seeds.due(now, |_| false, any);
        assert_eq!(due.len(), MAX_WEB_SEED_CONNECTIONS);
        assert!(due.iter().all(|d| d.endpoint.is_none()));
        let _link = seeds.link(due[0].id).unwrap();
        assert!(seeds.link(due[0].id).is_none());
        assert_eq!(
            seeds.due(now, |_| false, any).len(),
            MAX_WEB_SEED_CONNECTIONS - 1
        );
        // A seed with no piece to give is not counted.
        assert_eq!(seeds.due(now, |_| false, |_| false).len(), 0);

        // (url, banned, listed afterwards, disabled afterwards, endpoint)
        let cases = [
            (
                "http://127.0.0.1:8080/f",
                false,
                true,
                false,
                Some("127.0.0.1:8080"),
            ),
            ("https://[::1]/f", false, true, false, Some("[::1]:443")),
            // A query to a local address is refused when the seed is
            // asked, by the address its host resolves to.
            (
                "http://127.0.0.1/f?x=1",
                false,
                true,
                false,
                Some("127.0.0.1:80"),
            ),
            (
                "http://1.2.3.4/f?x=1",
                false,
                true,
                false,
                Some("1.2.3.4:80"),
            ),
            ("http://xn--bcher-kva.example/f", false, true, true, None),
            ("http://bücher.example/f", false, true, true, None),
            ("ftp://h.example/f", false, true, true, None),
            ("http://h.example/f", true, false, true, None),
            ("http://h.example:0/f", false, false, false, None),
            ("not a url", false, false, false, None),
        ];
        for (url, banned, listed, disabled, endpoint) in cases {
            let seeds = WebSeeds::new([seed(url)]);
            let due = seeds.due(now, |h| banned && h == handle_of(0), any);
            assert_eq!(seeds.list().len(), usize::from(listed), "{url}");
            let present = seeds.with_entry(0, |e| e.disabled);
            assert_eq!(present.unwrap_or(false), disabled, "{url}");
            let endpoint = endpoint.map(|e| e.parse::<SocketAddr>().unwrap());
            assert_eq!(due.first().and_then(|d| d.endpoint), endpoint, "{url}");
            assert_eq!(due.len(), usize::from(listed && !disabled), "{url}");
        }

        // A seed is listed once; removing it ends its connection.
        let seeds = Arc::new(WebSeeds::new([seed("http://h/"), seed("http://h/")]));
        assert_eq!(seeds.list(), [seed("http://h/")]);
        assert!(!seeds.add(seed("http://h/")));
        let link = seeds.link(0).unwrap();
        assert!(seeds.remove(&seed("http://h/")));
        assert!(link.cancel.is_cancelled());
        assert!(seeds.list().is_empty());
    }

    // A torrent whose only source is a web seed downloads from it, the
    // seed's bytes counted as any peer's; the seed is listed, added and
    // removed through the torrent, and a magnet link names it as `ws`.
    #[tokio::test(start_paused = true)]
    async fn a_torrent_downloads_from_its_web_seed_alone() -> anyhow::Result<()> {
        use crate::{
            AddTorrent, AddTorrentOptions, Session,
            storage::StorageFactory,
            tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
        };

        let t = TestTorrent::new("test_web_seed", 3 * 16384 + 100, 16384).await?;
        let content = t.content.clone();
        let server = Server::start(move |head| {
            if head.target.ends_with("/0.data") {
                Reply::ranged(&content, head)
            } else {
                Reply::status(404)
            }
        });
        let seed = WebSeed {
            url: server.url("/seed/"),
            kind: WebSeedKind::Url,
            named_by: NamedBy::Operator,
        };
        let out = tempfile::TempDir::with_prefix("test_web_seed_out")?;
        let session =
            Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
        let storage = MemoryStorageFactory::default();
        let handle = session
            .add_torrent(
                AddTorrent::from_bytes(t.torrent.clone()),
                Some(AddTorrentOptions {
                    storage_factory: Some(storage.clone_box()),
                    web_seeds: vec![seed.clone()],
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("expected a handle")?;
        handle.wait_until_completed().await?;

        assert!(storage.log.file_contents(0) == t.content, "content differs");
        let total = t.content.len() as u64;
        let live = handle.stats().live.context("expected a live torrent")?;
        assert_eq!(live.snapshot.fetched_bytes, total);
        assert_eq!(
            session
                .stats
                .counters
                .fetched_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            total
        );
        assert!(!server.seen().is_empty());
        assert!(server.seen().iter().all(|h| h.target.ends_with("/0.data")));

        assert_eq!(handle.web_seeds(), std::slice::from_ref(&seed));
        assert!(!handle.add_web_seed(seed.clone()));
        let http_seed = WebSeed {
            url: "http://h.example/s.php".into(),
            kind: WebSeedKind::Http,
            named_by: NamedBy::Operator,
        };
        assert!(handle.add_web_seed(http_seed.clone()));
        assert_eq!(handle.magnet().web_seeds, std::slice::from_ref(&seed.url));
        assert!(handle.remove_web_seed(&seed));
        assert!(!handle.remove_web_seed(&seed));
        assert_eq!(handle.web_seeds(), [http_seed]);
        assert!(handle.magnet().web_seeds.is_empty());
        Ok(())
    }

    #[test]
    fn a_web_seed_handle_is_no_peers() {
        assert!(is_web_seed(handle_of(0)));
        assert!(is_web_seed(handle_of(u32::MAX)));
        assert_ne!(handle_of(1), handle_of(65536));
        assert!(!is_web_seed("[100::1]:6881".parse().unwrap()));
        assert!(!is_web_seed("1.2.3.4:0".parse().unwrap()));
    }
}

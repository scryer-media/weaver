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
    net::{IpAddr, SocketAddr},
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
        net::{
            ClientShape, HttpClients, HttpTarget, NamedBy, Resolved, Resolver, is_idna, is_public,
        },
        torrent_metainfo::{WebSeed, WebSeedKind, escape_path},
    },
    peer_connection::{PeerConnectionHandler, WriterRequest},
    peer_protocol::{Handshake, Message, Piece, Request},
    session_stats::web_seed::{WebSeedResult, WebSeedStats},
    stream_connect::ConnectionKind,
    torrent_state::TorrentMetadata,
    torrent_state::live::peers::holepunch::is_local,
    tracker::redacted_tracker_url,
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
    /// fetched is removed or disabled, as is one banned for bad data
    /// ([`Self::ban`]) (libtorrent
    /// `torrent::maybe_connect_web_seeds`, `connect_to_url_seed`,
    /// `connect_web_seed`).
    pub fn due(
        &self,
        now: Instant,
        interesting: impl Fn(&Lacks) -> bool,
        allow_idna: bool,
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
            // A seed's URL can carry a passkey: a log names only its
            // scheme, host and port.
            match check_url(&entry.seed.url, entry.banned, allow_idna) {
                Ok(endpoint) => {
                    due.push(Due {
                        id: entry.id,
                        endpoint,
                    });
                    at += 1;
                }
                Err(Refused::Disabled(why)) => {
                    debug!(
                        url = redacted_tracker_url(&entry.seed.url),
                        why, "web seed disabled"
                    );
                    entry.disabled = true;
                    at += 1;
                }
                Err(Refused::Removed(why)) => {
                    debug!(
                        url = redacted_tracker_url(&entry.seed.url),
                        why, "web seed removed"
                    );
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
        let entry = g
            .seeds
            .iter_mut()
            .find(|e| PeerHandle::WebSeed(e.id) == handle)?;
        layout.failed(piece, &mut entry.lacks);
        debug!(
            url = redacted_tracker_url(&entry.seed.url),
            piece = piece.get(),
            "web seed sent a bad piece"
        );
        Some(layout.bitfield(&entry.lacks))
    }

    /// Seed `handle` is banned for sending bad data: it is not listed and
    /// not connected to again (libtorrent `peer_info.banned`, which
    /// `torrent::maybe_connect_web_seeds` reads). Returns whether `handle` is
    /// a seed's that was not banned before.
    pub fn ban(&self, handle: PeerHandle) -> bool {
        let mut g = self.inner.lock();
        let Some(entry) = g
            .seeds
            .iter_mut()
            .find(|e| PeerHandle::WebSeed(e.id) == handle)
        else {
            return false;
        };
        !std::mem::replace(&mut entry.banned, true)
    }

    /// Seed `handle` is banned for sending bad data.
    #[cfg(test)]
    pub fn is_banned(&self, handle: PeerHandle) -> bool {
        self.inner
            .lock()
            .seeds
            .iter()
            .any(|e| e.banned && PeerHandle::WebSeed(e.id) == handle)
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
/// `connect_web_seed`). A host with an internationalized label is refused
/// unless `allow_idna`.
fn check_url(url: &str, banned: bool, allow_idna: bool) -> Result<Option<SocketAddr>, Refused> {
    let Ok(parsed) = Url::parse(url) else {
        return Err(Refused::Removed("the URL does not parse"));
    };
    let host = parsed.host_str().unwrap_or_default();
    if !allow_idna && is_idna(host) {
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
    /// The seed's entry in the peer table.
    pub fn handle(&self) -> PeerHandle {
        PeerHandle::WebSeed(self.id)
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
    // Whether the URL carries a query of its own: the seed's, or the one a
    // move pointed at, not the one added to ask a BEP 17 seed.
    query: bool,
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
    // Whether a host with an internationalized label may be asked.
    allow_idna: bool,
    // The session's web seed counts.
    counts: Arc<WebSeedStats>,
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
            allow_idna: false,
            counts: Arc::default(),
            pinned: Mutex::default(),
        }
    }

    /// Counts the connection's requests in `counts`.
    pub fn counting(mut self, counts: Arc<WebSeedStats>) -> Self {
        self.counts = counts;
        self
    }

    /// Lets the connection ask hosts with an internationalized label, on
    /// the first hop and on every redirect.
    pub fn allowing_idna(mut self, allow: bool) -> Self {
        self.allow_idna = allow;
        self
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
            let url = Url::parse(&to.url).context("bad web seed URL")?;
            return Ok(Target {
                query: url.query().is_some(),
                url,
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
            query: self.link.seed.url.contains('?'),
        })
    }

    // Where `target` is asked: the one address of its host its URL may be
    // asked at, and a client held to it. A seed that may not be asked there
    // is disabled; a file moved where it may not be asked is lost to the
    // seed. A name that does not resolve leaves the seed for
    // WEB_SEED_NAME_LOOKUP_RETRY. A name the session's network resolves
    // where it exits has no address here to check: only the name is
    // judged (see NamedBy::may_ask_by_name), as an address is, and its
    // client hands the name on.
    async fn reach(&self, target: &Target) -> anyhow::Result<Reached> {
        let url = &target.url;
        // Checked on every hop, before the name is looked up: a seed's
        // first URL is checked when it is due, a redirect only here.
        if !self.allow_idna && url.host_str().is_some_and(is_idna) {
            match target.moved {
                Some(file) => self.link.file_missing(file),
                None => self.link.disable(),
            }
            return Err(anyhow!("{url}: an internationalized host name"));
        }
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
                        self.counts.waits();
                        self.link.retry_after(Some(WEB_SEED_NAME_LOOKUP_RETRY));
                        return Err(e);
                    }
                }
            }
            None => return Err(anyhow!("{url}: no host")),
        };
        let addr = match addrs.map(|addrs| endpoint(&addrs, port, target, self.allowed)) {
            None if !target.named_by.may_ask_by_name(&host) => {
                match target.moved {
                    Some(file) => self.link.file_missing(file),
                    None => self.link.disable(),
                }
                return Err(anyhow!("{url}: a local name it may not be asked by"));
            }
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
        // A server on the local network is not asked with a query string of
        // the URL's own, on any hop, whoever named the seed; an address the
        // network resolves at its exit is not known, and not judged
        // (libtorrent `ssrf_mitigation`, src/torrent.cpp:6860-6886 at
        // v2.0.15).
        if target.query && addr.is_some_and(is_local) {
            match target.moved {
                Some(file) => self.link.file_missing(file),
                None => self.link.disable(),
            }
            return Err(anyhow!(
                "SSRF mitigation: {url}: a query to a local address"
            ));
        }
        let key = (host, port, addr);
        if let Some(client) = self.pinned.lock().get(&key) {
            return Ok(Reached {
                addr,
                client: client.clone(),
            });
        }
        let client = self
            .clients
            .client(
                Some(HttpTarget {
                    host: &key.0,
                    port,
                    addrs: addr.as_slice(),
                }),
                &ClientShape::FETCH.for_torrent(self.layout.info_hash),
            )
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
            let response = self
                .send(request)
                .await
                .inspect_err(|e| self.counts.requested(unanswered(e)))?;
            let status = response.status();
            self.counts.requested(answered(status));
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
                query: to.query().is_some(),
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
            self.counts.waits();
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
            self.counts.waits();
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
            self.counts.received(chunk.len());
            blocks.push(&chunk).await?;
        }
        if left > 0 {
            return Err(anyhow!("{url}: the body ended {left} bytes short"));
        }
        Ok(())
    }
}

// How a request that got a status is counted.
fn answered(status: StatusCode) -> WebSeedResult {
    match status.as_u16() {
        200 | 206 => WebSeedResult::Ok,
        300..=399 => WebSeedResult::Redirect,
        503 => WebSeedResult::Unavailable,
        _ => WebSeedResult::Rejected,
    }
}

// How a request that got none is counted.
fn unanswered(error: &anyhow::Error) -> WebSeedResult {
    if error.is::<tokio::time::error::Elapsed>() {
        WebSeedResult::Timeout
    } else {
        WebSeedResult::Failed
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
mod tests;

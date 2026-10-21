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
        lengths::Lengths,
        magnet::Escaped,
        torrent_metainfo::{WebSeed, WebSeedKind, escape_path},
    },
    peer_connection::{PeerConnectionHandler, WriterRequest},
    peer_protocol::{Handshake, Message, Piece, Request},
    stream_connect::ConnectionKind,
    torrent_state::TorrentMetadata,
    type_aliases::PeerHandle,
};

use super::peer::PeerRx;

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
/// Blocks read and not yet handed to the torrent, per connection.
const BLOCKS_BUFFERED: usize = 64;

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
    // The files the server answered an error status for (the bits cleared
    // in libtorrent `web_seed_t::have_files`).
    missing_files: HashSet<usize>,
    // Where the server moved a file (libtorrent `web_seed_t::redirects`).
    redirects: HashMap<usize, String>,
    // Ends the open connection to the seed.
    connection: Option<CancellationToken>,
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

    /// Adds `seed`, unless it is there; returns whether it was added
    /// (libtorrent `torrent::add_web_seed`).
    pub fn add(&self, seed: WebSeed) -> bool {
        let mut g = self.inner.lock();
        if g.seeds.iter().any(|e| e.seed == seed) {
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
            missing_files: HashSet::new(),
            redirects: HashMap::new(),
            connection: None,
        });
        drop(g);
        self.changed.notify_one();
        true
    }

    /// Removes `seed` and ends its connection; returns whether it was there
    /// (libtorrent `torrent::remove_web_seed`).
    pub fn remove(&self, seed: &WebSeed) -> bool {
        let mut g = self.inner.lock();
        let Some(at) = g.seeds.iter().position(|e| &e.seed == seed) else {
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

    /// Returns once a seed may have become due.
    pub async fn changed(&self) {
        self.changed.notified().await
    }

    /// The seeds to connect to at `now`. At most MAX_WEB_SEED_CONNECTIONS
    /// seeds count, connected ones among them: those not disabled, not
    /// waiting to retry and with a piece to give, which `interesting` tells
    /// from the files a seed misses. Of those, a seed whose URL can never be
    /// fetched is removed or disabled, as is one `banned` names (libtorrent
    /// `torrent::maybe_connect_web_seeds`, `connect_to_url_seed`,
    /// `connect_web_seed`).
    pub fn due(
        &self,
        now: Instant,
        banned: impl Fn(PeerHandle) -> bool,
        interesting: impl Fn(&HashSet<usize>) -> bool,
    ) -> Vec<Due> {
        let mut g = self.inner.lock();
        let mut limit = MAX_WEB_SEED_CONNECTIONS;
        let mut due = Vec::new();
        let mut at = 0;
        while at < g.seeds.len() && limit > 0 {
            let entry = &mut g.seeds[at];
            if entry.disabled
                || entry.retry.is_some_and(|r| r > now)
                || !interesting(&entry.missing_files)
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

    fn with_entry<R>(&self, id: u32, f: impl FnOnce(&mut Entry) -> R) -> Option<R> {
        self.inner
            .lock()
            .seeds
            .iter_mut()
            .find(|e| e.id == id)
            .map(f)
    }
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
    // `ssrf_mitigation`: a query to a local service is not a web seed.
    if is_local(ip) && parsed.query().is_some() {
        return Err(Refused::Disabled("a query to a local address"));
    }
    Ok(Some(SocketAddr::new(ip, port)))
}

// libtorrent `aux::is_local`.
fn is_local(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private() || ip.is_link_local(),
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unicast_link_local()
                || ip.is_unique_local()
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_local(IpAddr::V4(v4)))
        }
    }
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

    fn missing_files(&self) -> HashSet<usize> {
        self.seeds
            .with_entry(self.id, |e| e.missing_files.clone())
            .unwrap_or_default()
    }

    fn redirect(&self, file: usize) -> Option<String> {
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
            .with_entry(self.id, |e| e.missing_files.insert(file));
    }

    fn redirected(&self, file: usize, to: String) {
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

    /// The pieces a seed missing `missing` files has: every piece but those
    /// of a missing file that is no padding (libtorrent
    /// `web_peer_connection::on_connected`).
    pub fn bitfield(&self, missing: &HashSet<usize>) -> Vec<bool> {
        let piece_len = u64::from(self.lengths.default_piece_length());
        let mut have = vec![true; self.lengths.total_pieces() as usize];
        for (i, f) in self.files.iter().enumerate() {
            if f.pad || f.len == 0 || !missing.contains(&i) {
                continue;
            }
            let first = f.offset / piece_len;
            let last = (f.offset + f.len - 1) / piece_len;
            #[allow(clippy::cast_possible_truncation)]
            have[first as usize..=last as usize].fill(false);
        }
        have
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

/// How a connection ends.
enum End {
    /// We closed it, or the server moved what we asked for.
    Closed,
    Failed(anyhow::Error),
}

impl From<anyhow::Error> for End {
    fn from(e: anyhow::Error) -> Self {
        End::Failed(e)
    }
}

/// A connection to one web seed.
pub(crate) struct WebSeedConnection<'a> {
    client: reqwest::Client,
    layout: &'a Layout,
    link: &'a SeedLink,
}

impl<'a> WebSeedConnection<'a> {
    pub fn new(client: reqwest::Client, layout: &'a Layout, link: &'a SeedLink) -> Self {
        Self {
            client,
            layout,
            link,
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
        let have = self.layout.bitfield(&self.link.missing_files());
        let mut bitfield = vec![0u8; have.len().div_ceil(8)];
        for (i, _) in have.iter().enumerate().filter(|(_, h)| **h) {
            bitfield[i / 8] |= 0x80 >> (i % 8);
        }
        handler
            .on_received_message(Message::Bitfield(ByteBuf(&bitfield)))
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
                Some(end) = fetches.next(), if !fetches.is_empty() => {
                    if let Err(end) = end {
                        // The blocks read before the end are good.
                        while let Ok(block) = blocks_rx.try_recv() {
                            deliver(block).await?;
                        }
                        return match end {
                            End::Closed => Ok(()),
                            End::Failed(e) => Err(anyhow(e)),
                        };
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

    async fn fetch(&self, run: Run, out: mpsc::Sender<(Request, Bytes)>) -> Result<(), End> {
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
    fn file_url(&self, file: usize) -> String {
        if let Some(to) = self.link.redirect(file) {
            return to;
        }
        let url = &self.link.seed.url;
        if self.layout.single_file() && !url.ends_with('/') {
            return url.clone();
        }
        let slash = if url.ends_with('/') { "" } else { "/" };
        format!("{url}{slash}{}", self.layout.files[file].path)
    }

    // BEP 19: the slice's bytes of its file, by range (libtorrent
    // `web_peer_connection::write_request`, `on_receive`).
    async fn fetch_slice(&self, slice: &Slice, blocks: &mut Blocks) -> Result<(), End> {
        let url = Url::parse(&self.file_url(slice.file)).context("bad web seed URL")?;
        let last = slice.offset + slice.len - 1;
        let response = self
            .send(
                self.client
                    .get(url.clone())
                    .header(header::RANGE, format!("bytes={}-{last}", slice.offset)),
            )
            .await?;
        let status = response.status();
        if !is_ok_status(status) {
            // The server lacks the file, or cannot serve it now.
            self.link.file_missing(slice.file);
            self.link.retry_after(retry_after(&response));
            return Err(anyhow!("{url}: {status}").into());
        }
        if status.is_redirection() {
            let Some(to) = response
                .headers()
                .get(header::LOCATION)
                .and_then(|l| l.to_str().ok())
                .and_then(|l| url.join(l).ok())
            else {
                self.link.disable();
                return Err(anyhow!("{url}: {status} without a location").into());
            };
            self.link.redirected(slice.file, to.into());
            return Err(End::Closed);
        }
        if response.url() != &url {
            // The client followed a redirect: the file is asked for there
            // from now on.
            self.link.redirected(slice.file, response.url().to_string());
        }
        let range = if status == StatusCode::PARTIAL_CONTENT {
            content_range(&response)
        } else {
            content_length(&response).map(|len| (0, len))
        };
        let Some((start, end)) = range else {
            self.link.disable();
            return Err(anyhow!("{url}: {status} names no range").into());
        };
        if (start, end) != (slice.offset, slice.offset + slice.len) {
            return Err(anyhow!(
                "{url}: range {start}..{end} answers {}..{}",
                slice.offset,
                slice.offset + slice.len
            )
            .into());
        }
        self.read_body(response, slice.len, blocks).await
    }

    // BEP 17: the run's bytes of its piece, by `piece` and `ranges`.
    async fn fetch_piece(&self, first: Request, len: u64, blocks: &mut Blocks) -> Result<(), End> {
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
        let url = Url::parse(&url).context("bad web seed URL")?;
        let response = self.send(self.client.get(url.clone())).await?;
        let status = response.status();
        if status == StatusCode::SERVICE_UNAVAILABLE {
            // The body is how many seconds to wait.
            let body = tokio::time::timeout(URLSEED_TIMEOUT, response.text())
                .await
                .context("timed out")?
                .unwrap_or_default();
            let wait = body
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|s| *s > 0)
                .map_or(HTTPSEED_DEFAULT_RETRY, Duration::from_secs);
            self.link.retry_after(Some(wait));
            return Err(anyhow!("{url}: {status}").into());
        }
        if status != StatusCode::OK {
            self.link.remove();
            return Err(anyhow!("{url}: {status}").into());
        }
        self.read_body(response, len, blocks).await
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        // Ranges of the bytes as stored, not of a compressed body.
        let request = request.header(header::ACCEPT_ENCODING, "identity");
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
    ) -> Result<(), End> {
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
                return Err(anyhow!("{url}: the body is longer than the range").into());
            }
            left -= chunk.len() as u64;
            blocks.push(&chunk).await?;
        }
        if left > 0 {
            return Err(anyhow!("{url}: the body ended {left} bytes short").into());
        }
        Ok(())
    }
}

// libtorrent `aux::is_ok_status`.
fn is_ok_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 200 | 206 | 300..=399)
}

// libtorrent `http_parser::header_duration`: seconds, none unless above 0.
fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    response
        .headers()
        .get(header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|s| *s > 0)
        .map(Duration::from_secs)
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
        let head = Head {
            target: req.path.unwrap().to_owned(),
            range,
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

    fn client() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    fn one_seed(url: String, kind: WebSeedKind) -> (Arc<WebSeeds>, SeedLink) {
        let seeds = Arc::new(WebSeeds::new([WebSeed { url, kind }]));
        let link = seeds.link(0).unwrap();
        (seeds, link)
    }

    fn entry<R>(seeds: &WebSeeds, f: impl FnOnce(&Entry) -> R) -> R {
        seeds.with_entry(0, |e| f(e)).unwrap()
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
        let conn = WebSeedConnection::new(client(), &layout, &link);
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
                    range: Some((0, 39999))
                },
                Head {
                    target: "/seed/t/b".into(),
                    range: Some((0, 29999))
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
        let conn = WebSeedConnection::new(client(), &layout, &link);
        let fake = Fake::default();

        let failed = drive(&conn, &fake, &blocks(&layout, 2..3), 1).await;
        assert!(failed.is_err(), "{failed:?}");
        link.failed();
        let now = Instant::now();
        assert_eq!(
            entry(&seeds, |e| e.missing_files.clone()),
            HashSet::from([2])
        );
        assert_eq!(
            entry(&seeds, |e| e.retry),
            Some(now + Duration::from_secs(120))
        );
        drop(link);

        let interesting = |m: &HashSet<usize>| layout.bitfield(m).contains(&true);
        assert_eq!(layout.bitfield(&HashSet::from([2])), [true, true, false]);
        let due = |at| seeds.due(at, |_| false, interesting).len();
        assert_eq!(due(now + Duration::from_secs(119)), 0);
        assert_eq!(due(now + Duration::from_secs(120)), 1);

        // Without Retry-After, and with a wait shorter than the failure's,
        // the seed waits URLSEED_WAIT_RETRY.
        for reply in [
            Reply::status(404),
            Reply::status(500).header("Retry-After", "5"),
        ] {
            let reply = Mutex::new(Some(reply));
            let server = Server::start(move |_| reply.lock().take().unwrap());
            let (seeds, link) = one_seed(server.url("/f"), WebSeedKind::Url);
            let layout = super::tests::layout(&[("f", 100, false)]);
            let conn = WebSeedConnection::new(client(), &layout, &link);
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
            let missing = entry(&seeds, |e| e.missing_files.clone());
            assert!(!layout.bitfield(&missing).contains(&true));
        }
    }

    // A redirect is followed, and the file is asked for where it went from
    // then on (libtorrent `web_peer_connection::handle_redirect` keeps it
    // in `web_seed_t::redirects`).
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
        let conn = WebSeedConnection::new(client(), &layout, &link);
        let fake = Fake::default();

        drive(&conn, &fake, &blocks(&layout, 0..1), 2)
            .await
            .unwrap();
        assert_eq!(
            entry(&seeds, |e| e.redirects.clone()),
            HashMap::from([(0, server.url("/moved/a"))])
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
        let conn = WebSeedConnection::new(client(), &layout, &link);
        let fake = Fake::default();

        let failed = drive(&conn, &fake, &blocks(&layout, 0..1), 2).await;
        assert!(failed.is_err(), "{failed:?}");
        assert_eq!(fake.pieces.lock().len(), 1);
        assert!(entry(&seeds, |e| e.missing_files.is_empty() && !e.disabled));
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
            let conn = WebSeedConnection::new(client(), &layout, &link);
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
        let conn = WebSeedConnection::new(client(), &layout, &link);
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
        let conn = WebSeedConnection::new(client(), &layout, &link);

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

        for (body, wait) in [("90", 90), ("0", 60), ("soon", 60)] {
            let server = Server::start(move |_| Reply::status(503).body(body));
            let (seeds, link) = one_seed(server.url("/s.php"), WebSeedKind::Http);
            let conn = WebSeedConnection::new(client(), &layout, &link);
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
        let server = Server::start(|_| Reply::status(404));
        let (seeds, link) = one_seed(server.url("/s.php?k=v"), WebSeedKind::Http);
        let conn = WebSeedConnection::new(client(), &layout, &link);
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
            }]));
            let link = seeds.link(0).unwrap();
            let conn = WebSeedConnection::new(reqwest::Client::new(), &layout, &link);
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
            }]));
            let link = seeds.link(0).unwrap();
            WebSeedConnection::new(reqwest::Client::new(), &layout, &link).file_url(file)
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
    #[test]
    fn which_seeds_are_due() {
        let now = Instant::now();
        let any = |_: &HashSet<usize>| true;
        let seed = |url: &str| WebSeed {
            url: url.into(),
            kind: WebSeedKind::Url,
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
            ("http://127.0.0.1/f?x=1", false, true, true, None),
            ("http://10.0.0.1/f?x=1", false, true, true, None),
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

        assert_eq!(handle.web_seeds(), [seed.clone()]);
        assert!(!handle.add_web_seed(seed.clone()));
        let http_seed = WebSeed {
            url: "http://h.example/s.php".into(),
            kind: WebSeedKind::Http,
        };
        assert!(handle.add_web_seed(http_seed.clone()));
        assert_eq!(handle.magnet().web_seeds, [seed.url.clone()]);
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

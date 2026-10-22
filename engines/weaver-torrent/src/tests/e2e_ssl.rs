// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! SSL torrents between sessions on loopback: their peer connections are TLS
//! under the torrent's CA, and their HTTPS announces present our identity.

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::Bytes;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::rustls;

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions, Session,
    SessionOptions, SslIdentity, TorrentStatsState,
    core::hash_id::{Id20, InfoHashes},
    create_torrent,
    peer_protocol::Handshake,
    spawn_utils::BlockingSpawner,
    ssl::{Incoming, SslTorrent},
    storage::StorageFactory,
    tests::{
        embedder_util::{MemoryStorageFactory, isolated_session_opts},
        test_util::{create_default_random_dir_with_torrents, setup_test_logging},
    },
    torrent_state::ManagedTorrentHandle,
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
    vectored_traits::AsyncReadVectoredIntoCompat,
};

const PIECE_LENGTH: u32 = 16384;
const FILE_SIZE: usize = 4 * PIECE_LENGTH as usize + 1000;

/// Issued from a day ago for a month: short enough for any platform's
/// verifier to take a server certificate under it.
fn valid_now(params: &mut CertificateParams) -> anyhow::Result<()> {
    let now = rcgen::date_time_ymd(1970, 1, 1) + SystemTime::now().duration_since(UNIX_EPOCH)?;
    const DAY: Duration = Duration::from_secs(24 * 60 * 60);
    params.not_before = now - DAY;
    params.not_after = now + 30 * DAY;
    Ok(())
}

/// A CA and the identities it issues.
struct Ca {
    pem: String,
    issuer: Issuer<'static, KeyPair>,
}

impl Ca {
    fn new(name: &str) -> anyhow::Result<Self> {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        valid_now(&mut params)?;
        let key = KeyPair::generate()?;
        let cert = params.self_signed(&key)?;
        Ok(Self {
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        })
    }

    /// A peer's identity under the CA.
    fn peer(&self, name: &str) -> anyhow::Result<SslIdentity> {
        let (cert, key) = self.issue(name, Vec::new(), Vec::new())?;
        SslIdentity::from_pem(cert.as_bytes(), key.as_bytes())
    }

    /// A certificate under the CA and its key, both PEM.
    fn issue(
        &self,
        name: &str,
        names: Vec<String>,
        usages: Vec<ExtendedKeyUsagePurpose>,
    ) -> anyhow::Result<(String, String)> {
        let mut params = CertificateParams::new(names)?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = usages;
        valid_now(&mut params)?;
        let key = KeyPair::generate()?;
        let cert = params.signed_by(&key, &self.issuer)?;
        Ok((cert.pem(), key.serialize_pem()))
    }
}

/// One file of random bytes, as an SSL torrent under `ca`.
struct SslTestTorrent {
    dir: TempDir,
    torrent: Bytes,
    info_hashes: InfoHashes,
    content: Vec<u8>,
}

impl SslTestTorrent {
    async fn new(name: &str, ca: &Ca) -> anyhow::Result<Self> {
        let dir = create_default_random_dir_with_torrents(1, FILE_SIZE, Some(name));
        let created = create_torrent(
            dir.path(),
            CreateTorrentOptions {
                piece_length: Some(PIECE_LENGTH),
                ssl_cert: Some(ca.pem.as_bytes()),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let info_hashes = InfoHashes {
            v1: Some(created.info_hash()),
            v2: None,
        };
        let torrent = created.as_bytes()?;
        let content = std::fs::read(dir.path().join("0.data"))?;
        Ok(Self {
            dir,
            torrent,
            info_hashes,
            content,
        })
    }

    fn info_hash(&self) -> Id20 {
        self.info_hashes.best()
    }
}

/// Listening on loopback over `mode`, the SSL listener on a port of its own.
fn listening_opts(mode: ListenerMode) -> SessionOptions {
    SessionOptions {
        listen: Some(ListenerOptions {
            mode,
            listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
            ssl_listen_port: Some(0),
            ..Default::default()
        }),
        ..isolated_session_opts(false)
    }
}

/// The torrent seeded from its directory under `identity`, its session
/// listening for SSL peers over TCP.
async fn seed(t: &SslTestTorrent, identity: SslIdentity) -> anyhow::Result<Arc<Session>> {
    seed_over(t, identity, ListenerMode::TcpOnly).await
}

async fn seed_over(
    t: &SslTestTorrent,
    identity: SslIdentity,
    mode: ListenerMode,
) -> anyhow::Result<Arc<Session>> {
    let session = Session::new_with_opts(t.dir.path().into(), listening_opts(mode)).await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(t.dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ssl_identity: Some(identity),
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?
        .wait_until_completed()
        .await?;
    Ok(session)
}

struct Downloader {
    session: Arc<Session>,
    handle: ManagedTorrentHandle,
    storage: MemoryStorageFactory,
    _out: TempDir,
}

/// The torrent added to download from `peer` under `identity`.
async fn download(
    t: &SslTestTorrent,
    peer: SocketAddr,
    identity: Option<SslIdentity>,
) -> anyhow::Result<Downloader> {
    download_with(t, peer, identity, isolated_session_opts(false)).await
}

async fn download_with(
    t: &SslTestTorrent,
    peer: SocketAddr,
    identity: Option<SslIdentity>,
    opts: SessionOptions,
) -> anyhow::Result<Downloader> {
    let out = TempDir::with_prefix("test_ssl_out")?;
    let session = Session::new_with_opts(out.path().into(), opts).await?;
    let storage = MemoryStorageFactory::default();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(storage.clone_box()),
                initial_peers: Some(vec![peer]),
                ssl_identity: identity,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("expected a handle")?;
    Ok(Downloader {
        session,
        handle,
        storage,
        _out: out,
    })
}

fn handshake(info_hash: Id20) -> Vec<u8> {
    let mut hs = vec![0u8; 68];
    let len = Handshake::new(info_hash, Id20::new([9; 20])).serialize_unchecked_len(&mut hs);
    hs.truncate(len);
    hs
}

fn halves(stream: TcpStream) -> (BoxAsyncReadVectored, BoxAsyncWrite) {
    let (r, w) = stream.into_split();
    (Box::new(r.into_vectored_compat()), Box::new(w))
}

/// Everything the other end sends until it closes; a reset ends it too.
async fn read_until_closed(reader: &mut BoxAsyncReadVectored) -> Vec<u8> {
    let mut got = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) | Err(_) => return got,
            Ok(n) => got.extend_from_slice(&buf[..n]),
        }
    }
}

/// TLS to `addr` for `t` under `ca_pem`, presenting `identity`, then the
/// BitTorrent handshake: what comes back before the connection closes, or
/// the 68 bytes of the answering handshake.
async fn tls_handshake(
    t: &SslTestTorrent,
    addr: SocketAddr,
    ca_pem: &str,
    identity: SslIdentity,
) -> anyhow::Result<Vec<u8>> {
    let ssl = SslTorrent::new(ca_pem.as_bytes(), t.info_hashes)?;
    ssl.set_identity(Some(identity))?;
    let (r, w) = halves(TcpStream::connect(addr).await?);
    let (mut r, mut w) = match ssl.connect(r, w).await {
        Ok(halves) => halves,
        Err(_) => return Ok(Vec::new()),
    };
    if w.write_all(&handshake(t.info_hash())).await.is_err() || w.flush().await.is_err() {
        return Ok(Vec::new());
    }
    let mut answer = vec![0u8; 68];
    match r.read_exact(&mut answer).await {
        Ok(_) => Ok(answer),
        Err(_) => Ok(read_until_closed(&mut r).await),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_torrent_transfers_over_tls() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let t = SslTestTorrent::new("test_ssl_transfer", &ca).await?;
    let seeder = seed(&t, ca.peer("seeder")?).await?;
    let ssl_addr = seeder
        .ssl_listen_addr()
        .context("seeder has no SSL listener")?;

    let d = download(&t, ssl_addr, Some(ca.peer("downloader")?)).await?;
    d.handle.wait_until_completed().await?;
    assert!(
        d.storage.log.file_contents(0) == t.content,
        "content differs"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_torrent_transfers_over_tls_on_utp() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let t = SslTestTorrent::new("test_ssl_transfer_utp", &ca).await?;
    let seeder = seed_over(&t, ca.peer("seeder")?, ListenerMode::UtpOnly).await?;
    let ssl_addr = seeder
        .ssl_listen_addr()
        .context("seeder has no SSL listener")?;

    let d = download_with(
        &t,
        ssl_addr,
        Some(ca.peer("downloader")?),
        listening_opts(ListenerMode::UtpOnly),
    )
    .await?;
    d.handle.wait_until_completed().await?;
    assert!(
        d.storage.log.file_contents(0) == t.content,
        "content differs"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_torrent_without_identity_needs_certificate_and_dials_tls() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let t = SslTestTorrent::new("test_ssl_needs_certificate", &ca).await?;
    // A peer on a plain port: what the torrent sends it is seen here.
    let peer = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;

    let d = download(&t, peer.local_addr()?, None).await?;
    d.handle.wait_until_initialized().await?;
    assert!(matches!(
        d.handle.stats().state,
        TorrentStatsState::NeedsCertificate
    ));
    assert!(d.handle.needs_certificate());
    // Not running: no peer is connected to, no tracker announced to.
    assert!(d.handle.live().is_none());

    // An identity starts it, and its peer is reached through TLS naming the
    // torrent, never with a plain handshake.
    d.session
        .set_ssl_identity(&d.handle, Some(ca.peer("downloader")?))?;
    assert!(d.handle.live().is_some());
    assert!(!matches!(
        d.handle.stats().state,
        TorrentStatsState::NeedsCertificate
    ));
    let (stream, _) = peer.accept().await?;
    let (r, w) = halves(stream);
    let incoming = Incoming::start(r, w)
        .await
        .context("the torrent's connection does not open with TLS")?;
    assert_eq!(
        incoming.server_name().as_deref(),
        Some(t.info_hash().as_string().as_str())
    );

    // Its identity taken away, it stops and waits again.
    d.session.set_ssl_identity(&d.handle, None)?;
    assert!(d.handle.live().is_none());
    assert!(matches!(
        d.handle.stats().state,
        TorrentStatsState::NeedsCertificate
    ));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_listener_refuses_a_peer_under_another_ca() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let other = Ca::new("test_ssl_other_ca")?;
    let t = SslTestTorrent::new("test_ssl_foreign_ca", &ca).await?;
    let seeder = seed(&t, ca.peer("seeder")?).await?;
    let ssl_addr = seeder
        .ssl_listen_addr()
        .context("seeder has no SSL listener")?;

    // Under the torrent's CA the seeder answers with its handshake.
    let answer = tls_handshake(&t, ssl_addr, &ca.pem, ca.peer("peer")?).await?;
    assert_eq!(answer.len(), 68, "{answer:?}");
    assert_eq!(&answer[28..48], &t.info_hash().0[..]);

    // The same, presenting an identity under another CA: refused.
    let answer = tls_handshake(&t, ssl_addr, &ca.pem, other.peer("stranger")?).await?;
    assert!(answer.is_empty(), "{answer:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_torrent_refuses_plain_connections() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let t = SslTestTorrent::new("test_ssl_plain_refused", &ca).await?;
    let seeder = seed(&t, ca.peer("seeder")?).await?;

    // A plain handshake on the SSL listener: not TLS, refused.
    let ssl_addr = seeder
        .ssl_listen_addr()
        .context("seeder has no SSL listener")?;
    let (mut r, mut w) = halves(TcpStream::connect(ssl_addr).await?);
    w.write_all(&handshake(t.info_hash())).await?;
    let got = read_until_closed(&mut r).await;
    assert!(got.first() != Some(&19), "{got:?}");

    // A plain handshake for the torrent on the plain listener: refused.
    let plain_addr = seeder.listen_addr().context("seeder is not listening")?;
    let (mut r, mut w) = halves(TcpStream::connect(plain_addr).await?);
    w.write_all(&handshake(t.info_hash())).await?;
    let got = read_until_closed(&mut r).await;
    assert!(got.first() != Some(&19), "{got:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ssl_torrent_https_tracker_sees_identity_and_ssl_port() -> anyhow::Result<()> {
    setup_test_logging();
    let ca = Ca::new("test_ssl_ca")?;
    let t = SslTestTorrent::new("test_ssl_tracker", &ca).await?;
    let (client_cert, client_key) = ca.issue("downloader", Vec::new(), Vec::new())?;
    let identity = SslIdentity::from_pem(client_cert.as_bytes(), client_key.as_bytes())?;
    let presented = identity.certificate_chain[0].clone();

    // The tracker: HTTPS under the torrent's CA, asking for a client
    // certificate under it.
    let (tracker_cert, tracker_key) = ca.issue(
        "tracker",
        vec!["127.0.0.1".to_owned()],
        vec![ExtendedKeyUsagePurpose::ServerAuth],
    )?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(ca.pem.as_bytes())?)?;
    let tracker_identity = SslIdentity::from_pem(tracker_cert.as_bytes(), tracker_key.as_bytes())?;
    let config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                .build()?,
        )
        .with_single_cert(
            tracker_identity.certificate_chain,
            tracker_identity.private_key,
        )?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let tracker = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let url = format!("https://{}/announce", tracker.local_addr()?);

    let out = TempDir::with_prefix("test_ssl_tracker_out")?;
    let mut opts = listening_opts(ListenerMode::TcpOnly);
    if let Some(listen) = opts.listen.as_mut() {
        // Announced at all: the SSL torrent announces its own port.
        listen.announce_port = Some(1);
    }
    let session = Session::new_with_opts(
        out.path().into(),
        SessionOptions {
            disable_trackers: false,
            http_client: Some(Arc::new(|| reqwest::Client::builder().no_proxy())),
            ..opts
        },
    )
    .await?;
    let ssl_port = session.ssl_listen_addr().context("no SSL listener")?.port();
    let _handle = session
        .add_torrent(
            AddTorrent::from_bytes(t.torrent.clone()),
            Some(AddTorrentOptions {
                storage_factory: Some(MemoryStorageFactory::default().clone_box()),
                trackers: Some(vec![url]),
                ssl_identity: Some(identity),
                ..Default::default()
            }),
        )
        .await?;

    let (stream, _) = tracker.accept().await?;
    let mut stream = acceptor.accept(stream).await?;
    let certificates = stream
        .get_ref()
        .1
        .peer_certificates()
        .context("no client certificate")?
        .to_vec();
    assert_eq!(certificates.first(), Some(&presented));

    let mut request = Vec::new();
    let mut chunk = [0u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let n = stream.read(&mut chunk).await?;
        anyhow::ensure!(n > 0, "connection closed before the request was read");
        request.extend_from_slice(&chunk[..n]);
    }
    let request = String::from_utf8(request)?;
    let path = request.split(' ').nth(1).context("no request path")?;
    let query = path.split_once('?').context("no query")?.1;
    let port = query
        .split('&')
        .find_map(|kv| kv.strip_prefix("port="))
        .context("no port")?;
    assert_eq!(port, ssl_port.to_string());
    Ok(())
}

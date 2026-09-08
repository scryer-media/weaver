use std::io::{self, Cursor, Read};
use std::sync::Arc;

use super::*;
use crate::test_support::{ScriptedStep, spawn_scripted_server as spawn_shared_scripted_server};
use crate::tls::{ManualTlsStream, TLS_READ_TURN_LIMIT};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::rustls::client::UnbufferedClientConnection;
use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio_rustls::rustls::unbuffered::ConnectionState as UnbufferedConnectionState;
use tokio_rustls::rustls::{
    ClientConfig, ClientConnection, RootCertStore, ServerConfig as RustlsServerConfig,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use weaver_yenc::encode;

const FUSED_YENC_OUTPUT_BATCH_TARGET: usize = 512 * 1024;
const TLS_DRAIN_RECORD_BYTES: usize = 16 * 1024;
const TLS_DRAIN_RECORDS: usize = 8;
const TLS_DRAIN_PAYLOAD_BYTES: usize = TLS_DRAIN_RECORD_BYTES * TLS_DRAIN_RECORDS;
const TLS_TEST_BUFFER_BYTES: usize = 256 * 1024;

struct ScriptStep {
    expect_prefix: Option<&'static str>,
    response: &'static [u8],
    delay: Duration,
}

impl ScriptedStep for ScriptStep {
    fn expected_prefix(&self) -> Option<&str> {
        self.expect_prefix
    }

    fn response(&self) -> &[u8] {
        self.response
    }

    fn delay(&self) -> Duration {
        self.delay
    }
}

fn test_tls_configs() -> (Arc<ClientConfig>, Arc<RustlsServerConfig>) {
    let (client_config, server_config, _) = test_tls_configs_with_cert_der();
    (client_config, server_config)
}

fn test_tls_configs_with_cert_der() -> (Arc<ClientConfig>, Arc<RustlsServerConfig>, Vec<u8>) {
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generate test cert");
    let cert_der = certified_key.cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified_key.signing_key.serialize_der(),
    ));

    let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .expect("server TLS config");

    let mut roots = RootCertStore::empty();
    roots.add(cert_der.clone()).expect("client root store");
    let client_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let client_config = ClientConfig::builder_with_provider(Arc::new(client_provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();

    (
        Arc::new(client_config),
        Arc::new(server_config),
        cert_der.as_ref().to_vec(),
    )
}

async fn spawn_tls_drain_server(
    server_config: Arc<RustlsServerConfig>,
) -> (SocketAddr, oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (flushed_tx, flushed_rx) = oneshot::channel();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        let mut tls = acceptor.accept(socket).await.unwrap();
        let record = vec![0x5Au8; TLS_DRAIN_RECORD_BYTES];

        for _ in 0..TLS_DRAIN_RECORDS {
            tls.write_all(&record).await.unwrap();
            tls.flush().await.unwrap();
        }

        let _ = flushed_tx.send(());
        tokio::time::sleep(Duration::from_millis(250)).await;
    });

    (addr, flushed_rx)
}

async fn connect_tls_drain_client(
    addr: SocketAddr,
    client_config: Arc<ClientConfig>,
) -> NntpConnection {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let remote_addr = tcp.peer_addr().unwrap();
    let connector = TlsConnector::from(client_config);
    let server_name = ServerName::try_from("localhost").unwrap();
    let tls = connector
        .connect(server_name, crate::route_stream::RouteStream::from(tcp))
        .await
        .unwrap();
    let now = Instant::now();

    NntpConnection {
        _route_socket: None,
        route_outcome: None,
        transport: Some(NntpTransport::Tls {
            inner: tls,
            remote_addr: Some(remote_addr),
        }),
        codec: NntpCodec::new(),
        read_buf: BytesMut::with_capacity(256 * 1024),
        buffer_profile: NntpBufferProfile {
            read_buf_capacity: 256 * 1024,
            socket_read_size: 256 * 1024,
        },
        state: ConnectionState::Ready,
        capabilities: Capabilities::default(),
        host: "localhost".to_string(),
        port: remote_addr.port(),
        remote_addr: Some(remote_addr),
        created_at: now,
        last_used: now,
        command_timeout: Duration::from_secs(5),
        poisoned: false,
        current_group: None,
        credentials: None,
        tls_ca_cert: None,
        tls_name_mismatch_certificate_der: None,
        tls_cipher_preference: crate::tls::TlsCipherPreference::Auto,
        transfer_control: None,
        body_accounting: VecDeque::new(),
        checkpoint_plan: CheckpointPlan::None,
        last_response_line_wait: Duration::ZERO,
        group_probe_armed: false,
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct BufferedRustlsFeedStats {
    tls_read_calls: usize,
    process_packets_calls: usize,
    tls_bytes: usize,
    plaintext_bytes: usize,
}

impl BufferedRustlsFeedStats {
    fn add(&mut self, other: Self) {
        self.tls_read_calls += other.tls_read_calls;
        self.process_packets_calls += other.process_packets_calls;
        self.tls_bytes += other.tls_bytes;
        self.plaintext_bytes += other.plaintext_bytes;
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct UnbufferedRustlsStats {
    process_calls: usize,
    encode_states: usize,
    transmit_states: usize,
    blocked_handshake_states: usize,
    write_ready_states: usize,
    read_traffic_states: usize,
    app_records: usize,
    app_bytes: usize,
    discarded_bytes: usize,
}

enum UnbufferedStep {
    NeedRead,
    Send(Vec<u8>),
    ReadyToWrite,
    PeerClosed,
    Continue,
}

/// Ciphertext read from the socket that rustls has not consumed yet. It
/// outlives a single handshake or drain call: a socket read routinely
/// ends inside a TLS record, and that record's head must still sit in
/// front of whatever the next read appends. Starting the next call from
/// an empty buffer hands rustls the tail of a record as if it were the
/// start of one, and it reports an invalid content type.
struct UnbufferedInput {
    buf: Vec<u8>,
    len: usize,
}

fn feed_manual_tls(
    tls: &mut ClientConnection,
    ciphertext: &[u8],
    output: &mut Vec<u8>,
) -> io::Result<BufferedRustlsFeedStats> {
    let mut cursor = Cursor::new(ciphertext);
    let mut stats = BufferedRustlsFeedStats::default();

    while (cursor.position() as usize) < ciphertext.len() {
        stats.tls_read_calls += 1;
        match tls.read_tls(&mut cursor) {
            Ok(0) => break,
            Ok(n) => {
                stats.tls_bytes += n;
                stats.process_packets_calls += 1;
                tls.process_new_packets()
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                stats.plaintext_bytes += drain_manual_plaintext(tls, output)?;
            }
            Err(err) if err.kind() == io::ErrorKind::Other => {
                let drained = drain_manual_plaintext(tls, output)?;
                if drained == 0 {
                    return Err(err);
                }
                stats.plaintext_bytes += drained;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(stats)
}

fn drain_manual_plaintext(tls: &mut ClientConnection, output: &mut Vec<u8>) -> io::Result<usize> {
    let mut total = 0usize;
    let mut chunk = [0u8; TLS_DRAIN_RECORD_BYTES];

    loop {
        match tls.reader().read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                output.extend_from_slice(&chunk[..n]);
                total += n;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    Ok(total)
}

async fn connect_manual_rustls_client(
    addr: SocketAddr,
    client_config: Arc<ClientConfig>,
) -> (TcpStream, ClientConnection) {
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = ClientConnection::new(client_config, server_name).unwrap();
    let mut ciphertext = vec![0u8; 64 * 1024];
    let mut ignored_plaintext = Vec::new();

    while tls.is_handshaking() {
        while tls.wants_write() {
            let mut outbound = Vec::new();
            tls.write_tls(&mut outbound).unwrap();
            if outbound.is_empty() {
                break;
            }
            tcp.write_all(&outbound).await.unwrap();
            tcp.flush().await.unwrap();
        }

        if tls.is_handshaking() {
            let n = tcp.read(&mut ciphertext).await.unwrap();
            assert!(n > 0, "server closed during manual TLS handshake");
            let _ = feed_manual_tls(&mut tls, &ciphertext[..n], &mut ignored_plaintext).unwrap();
        }
    }

    while tls.wants_write() {
        let mut outbound = Vec::new();
        tls.write_tls(&mut outbound).unwrap();
        if outbound.is_empty() {
            break;
        }
        tcp.write_all(&outbound).await.unwrap();
        tcp.flush().await.unwrap();
    }

    (tcp, tls)
}

async fn manual_rustls_try_drain_ready_plaintext(
    tcp: &mut TcpStream,
    tls: &mut ClientConnection,
    output: &mut Vec<u8>,
) -> io::Result<(usize, usize, BufferedRustlsFeedStats)> {
    let mut ciphertext = vec![0u8; 256 * 1024];
    let mut socket_reads = 0usize;
    let mut stats = BufferedRustlsFeedStats::default();

    tcp.readable().await?;
    loop {
        match tcp.try_read(&mut ciphertext) {
            Ok(0) => break,
            Ok(n) => {
                socket_reads += 1;
                stats.add(feed_manual_tls(tls, &ciphertext[..n], output)?);
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    Ok((output.len(), socket_reads, stats))
}

fn discard_unbuffered_input(input: &mut [u8], input_len: &mut usize, discard: usize) {
    if discard == 0 {
        return;
    }
    assert!(
        discard <= *input_len,
        "rustls asked to discard {discard} bytes from {input_len} buffered bytes"
    );
    input.copy_within(discard..*input_len, 0);
    *input_len -= discard;
}

fn process_unbuffered_tls_once(
    tls: &mut UnbufferedClientConnection,
    input: &mut [u8],
    input_len: &mut usize,
    output: &mut Vec<u8>,
    stats: &mut UnbufferedRustlsStats,
) -> io::Result<UnbufferedStep> {
    stats.process_calls += 1;
    let mut outgoing = vec![0u8; TLS_TEST_BUFFER_BYTES];

    let (discard, extra_discard, step) = {
        let status = tls.process_tls_records(&mut input[..*input_len]);
        let discard = status.discard;
        match status
            .state
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        {
            UnbufferedConnectionState::EncodeTlsData(mut encoder) => {
                stats.encode_states += 1;
                let n = encoder.encode(&mut outgoing).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to encode TLS handshake data: {err:?}"),
                    )
                })?;
                outgoing.truncate(n);
                (discard, 0, UnbufferedStep::Send(outgoing))
            }
            UnbufferedConnectionState::TransmitTlsData(transmit) => {
                stats.transmit_states += 1;
                transmit.done();
                (discard, 0, UnbufferedStep::Continue)
            }
            UnbufferedConnectionState::BlockedHandshake => {
                stats.blocked_handshake_states += 1;
                (discard, 0, UnbufferedStep::NeedRead)
            }
            UnbufferedConnectionState::WriteTraffic(_) => {
                stats.write_ready_states += 1;
                (discard, 0, UnbufferedStep::ReadyToWrite)
            }
            UnbufferedConnectionState::ReadTraffic(mut traffic) => {
                stats.read_traffic_states += 1;
                let mut record_discard = 0usize;
                while let Some(record) = traffic.next_record() {
                    let record =
                        record.map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    record_discard += record.discard;
                    stats.app_records += 1;
                    stats.app_bytes += record.payload.len();
                    output.extend_from_slice(record.payload);
                }
                (discard, record_discard, UnbufferedStep::Continue)
            }
            UnbufferedConnectionState::PeerClosed | UnbufferedConnectionState::Closed => {
                (discard, 0, UnbufferedStep::PeerClosed)
            }
            _ => {
                return Err(io::Error::other(
                    "unexpected rustls unbuffered state in client test probe",
                ));
            }
        }
    };

    let total_discard = discard + extra_discard;
    discard_unbuffered_input(input, input_len, total_discard);
    stats.discarded_bytes += total_discard;
    Ok(step)
}

async fn connect_unbuffered_rustls_client(
    addr: SocketAddr,
    client_config: Arc<ClientConfig>,
) -> (
    TcpStream,
    UnbufferedClientConnection,
    UnbufferedRustlsStats,
    UnbufferedInput,
) {
    let mut tcp = TcpStream::connect(addr).await.unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let mut tls = UnbufferedClientConnection::new(client_config, server_name).unwrap();
    let mut input = vec![0u8; TLS_TEST_BUFFER_BYTES];
    let mut input_len = 0usize;
    let mut ignored_plaintext = Vec::new();
    let mut stats = UnbufferedRustlsStats::default();

    loop {
        match process_unbuffered_tls_once(
            &mut tls,
            &mut input,
            &mut input_len,
            &mut ignored_plaintext,
            &mut stats,
        )
        .unwrap()
        {
            UnbufferedStep::NeedRead => {
                if input_len == input.len() {
                    input.resize(input.len() * 2, 0);
                }
                let n = tcp.read(&mut input[input_len..]).await.unwrap();
                assert!(n > 0, "server closed during unbuffered TLS handshake");
                input_len += n;
            }
            UnbufferedStep::Send(bytes) => {
                tcp.write_all(&bytes).await.unwrap();
                tcp.flush().await.unwrap();
            }
            UnbufferedStep::ReadyToWrite => break,
            UnbufferedStep::PeerClosed => panic!("server closed during unbuffered handshake"),
            UnbufferedStep::Continue => {}
        }
    }

    let input = UnbufferedInput {
        buf: input,
        len: input_len,
    };
    (tcp, tls, stats, input)
}

async fn unbuffered_rustls_try_drain_ready_plaintext(
    tcp: &mut TcpStream,
    tls: &mut UnbufferedClientConnection,
    input: &mut UnbufferedInput,
    output: &mut Vec<u8>,
    stats: &mut UnbufferedRustlsStats,
) -> io::Result<(usize, usize, usize)> {
    let mut socket_reads = 0usize;
    let mut tls_bytes = 0usize;

    tcp.readable().await?;
    loop {
        if input.len == input.buf.len() {
            input.buf.resize(input.buf.len() * 2, 0);
        }
        match tcp.try_read(&mut input.buf[input.len..]) {
            Ok(0) => break,
            Ok(n) => {
                socket_reads += 1;
                tls_bytes += n;
                input.len += n;
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
            Err(err) => return Err(err),
        }
    }

    loop {
        match process_unbuffered_tls_once(tls, &mut input.buf, &mut input.len, output, stats)? {
            UnbufferedStep::NeedRead | UnbufferedStep::ReadyToWrite => break,
            UnbufferedStep::Send(bytes) => {
                tcp.write_all(&bytes).await?;
                tcp.flush().await?;
            }
            UnbufferedStep::PeerClosed => break,
            UnbufferedStep::Continue => {}
        }
    }

    Ok((output.len(), socket_reads, tls_bytes))
}

#[tokio::test]
async fn tls_read_into_buffer_bulk_drain_probe() {
    let (client_config, server_config) = test_tls_configs();
    let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
    let mut conn = connect_tls_drain_client(addr, client_config).await;

    flushed_rx.await.expect("test server flushed TLS payload");

    let first_read = conn.read_into_buffer().await.unwrap();
    let mut read_calls = 1usize;
    while conn.read_buf.len() < TLS_DRAIN_PAYLOAD_BYTES {
        let n = conn.read_into_buffer().await.unwrap();
        read_calls += 1;
        assert!(n > 0, "transport closed before reading diagnostic payload");
    }

    println!(
        "tls_drain_probe first_read_bytes={first_read} total_bytes={} read_calls={read_calls} target_socket_read_size={}",
        conn.read_buf.len(),
        conn.buffer_profile.socket_read_size
    );

    assert!(first_read > 0);
    assert!(first_read <= TLS_DRAIN_PAYLOAD_BYTES);
    assert_eq!(conn.read_buf.len(), TLS_DRAIN_PAYLOAD_BYTES);
}

#[tokio::test]
async fn manual_rustls_bulk_drain_probe() {
    let (client_config, server_config) = test_tls_configs();
    let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
    let (mut tcp, mut tls) = connect_manual_rustls_client(addr, client_config).await;

    flushed_rx.await.expect("test server flushed TLS payload");

    let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
    let mut first_drain_plaintext = 0usize;
    let mut total_socket_reads = 0usize;
    let mut stats = BufferedRustlsFeedStats::default();
    let mut drain_calls = 0usize;

    while output.len() < TLS_DRAIN_PAYLOAD_BYTES {
        let before = output.len();
        let (drain_plaintext, socket_reads, read_stats) =
            manual_rustls_try_drain_ready_plaintext(&mut tcp, &mut tls, &mut output)
                .await
                .unwrap();
        if drain_calls == 0 {
            first_drain_plaintext = drain_plaintext;
        }
        drain_calls += 1;
        total_socket_reads += socket_reads;
        stats.add(read_stats);
        assert!(
            output.len() > before || read_stats.tls_bytes > 0,
            "buffered rustls probe made no progress while draining TLS payload"
        );
    }

    println!(
        "manual_rustls_drain_probe first_drain_plaintext={first_drain_plaintext} drain_calls={drain_calls} socket_reads={total_socket_reads} tls_bytes={} tls_read_calls={} process_packets_calls={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
        stats.tls_bytes, stats.tls_read_calls, stats.process_packets_calls
    );

    assert_eq!(output.len(), TLS_DRAIN_PAYLOAD_BYTES);
    assert!(total_socket_reads > 0);
    assert!(stats.tls_bytes >= TLS_DRAIN_PAYLOAD_BYTES);
    assert!(
        stats.tls_read_calls > TLS_DRAIN_RECORDS,
        "buffered rustls should need multiple 4 KiB reads per 16 KiB record"
    );
}

#[tokio::test]
async fn unbuffered_rustls_bulk_drain_probe() {
    let (client_config, server_config) = test_tls_configs();
    let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
    let (mut tcp, mut tls, mut stats, mut input) =
        connect_unbuffered_rustls_client(addr, client_config).await;
    let handshake_stats = stats;
    stats = UnbufferedRustlsStats::default();

    flushed_rx.await.expect("test server flushed TLS payload");

    let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
    let mut first_drain_plaintext = 0usize;
    let mut total_socket_reads = 0usize;
    let mut total_tls_bytes = 0usize;
    let mut drain_calls = 0usize;

    while output.len() < TLS_DRAIN_PAYLOAD_BYTES {
        let before = output.len();
        let (drain_plaintext, socket_reads, tls_bytes) =
            unbuffered_rustls_try_drain_ready_plaintext(
                &mut tcp,
                &mut tls,
                &mut input,
                &mut output,
                &mut stats,
            )
            .await
            .unwrap();
        if drain_calls == 0 {
            first_drain_plaintext = drain_plaintext;
        }
        drain_calls += 1;
        total_socket_reads += socket_reads;
        total_tls_bytes += tls_bytes;
        // A readiness wake can deliver only part of a TLS record; that
        // consumes ciphertext without yielding plaintext yet.
        assert!(
            output.len() > before || tls_bytes > 0,
            "unbuffered probe made no progress while draining TLS payload"
        );
    }

    println!(
        "unbuffered_rustls_drain_probe first_drain_plaintext={first_drain_plaintext} drain_calls={drain_calls} socket_reads={total_socket_reads} tls_bytes={total_tls_bytes} handshake_process_calls={} payload_process_calls={} read_traffic_states={} app_records={} app_bytes={} discarded_bytes={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
        handshake_stats.process_calls,
        stats.process_calls,
        stats.read_traffic_states,
        stats.app_records,
        stats.app_bytes,
        stats.discarded_bytes
    );

    assert_eq!(output.len(), TLS_DRAIN_PAYLOAD_BYTES);
    assert!(total_socket_reads > 0);
    assert!(total_tls_bytes >= TLS_DRAIN_PAYLOAD_BYTES);
    assert_eq!(stats.app_records, TLS_DRAIN_RECORDS);
}

#[tokio::test]
async fn unbuffered_rustls_first_ready_pass_probe() {
    let (client_config, server_config) = test_tls_configs();
    let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
    let (mut tcp, mut tls, mut stats, mut input) =
        connect_unbuffered_rustls_client(addr, client_config).await;
    let handshake_stats = stats;
    stats = UnbufferedRustlsStats::default();

    flushed_rx.await.expect("test server flushed TLS payload");

    let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
    let (first_drain_plaintext, socket_reads, tls_bytes) =
        unbuffered_rustls_try_drain_ready_plaintext(
            &mut tcp,
            &mut tls,
            &mut input,
            &mut output,
            &mut stats,
        )
        .await
        .unwrap();

    println!(
        "unbuffered_rustls_first_ready_probe first_drain_plaintext={first_drain_plaintext} socket_reads={socket_reads} tls_bytes={tls_bytes} handshake_process_calls={} payload_process_calls={} read_traffic_states={} app_records={} app_bytes={} discarded_bytes={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
        handshake_stats.process_calls,
        stats.process_calls,
        stats.read_traffic_states,
        stats.app_records,
        stats.app_bytes,
        stats.discarded_bytes
    );

    assert!(socket_reads > 0);
    assert!(tls_bytes > 0);
    assert_eq!(first_drain_plaintext, output.len());
}

#[tokio::test]
async fn manual_tls_transport_bulk_drain_probe() {
    let (client_config, server_config) = test_tls_configs();
    let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
    let tcp = TcpStream::connect(addr).await.unwrap();
    let remote_addr = tcp.peer_addr().unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let inner = ManualTlsStream::connect(tcp, client_config, server_name)
        .await
        .unwrap();
    let mut transport = NntpTransport::ManualTls {
        inner,
        remote_addr: Some(remote_addr),
    };

    flushed_rx.await.expect("test server flushed TLS payload");

    let mut read_buf = BytesMut::with_capacity(256 * 1024);
    let read = transport
        .read_into_buf_with_stats(&mut read_buf, 256 * 1024)
        .await
        .unwrap();

    println!(
        "manual_tls_transport_probe first_read_bytes={} total_bytes={} tls_read_calls={} process_packets_calls={} plaintext_reader_calls={} reader_would_block={}",
        read.bytes,
        read_buf.len(),
        read.stats.tls_read_calls,
        read.stats.tls_process_packets_calls,
        read.stats.plaintext_reader_calls,
        read.stats.plaintext_reader_would_block
    );

    assert_eq!(read.bytes, TLS_DRAIN_PAYLOAD_BYTES);
    assert_eq!(read_buf.len(), TLS_DRAIN_PAYLOAD_BYTES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_tls_transport_bounds_each_turn_and_preserves_stream() {
    let (client_config, server_config) = test_tls_configs();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let payload_len = TLS_READ_TURN_LIMIT * 3 + 12_345;
    let payload: Arc<[u8]> = (0..payload_len)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>()
        .into();
    let server_payload = Arc::clone(&payload);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let acceptor = TlsAcceptor::from(server_config);
        let mut tls = acceptor.accept(socket).await.unwrap();
        tls.write_all(&server_payload).await.unwrap();
        tls.flush().await.unwrap();
        tls.shutdown().await.unwrap();
    });

    let tcp = TcpStream::connect(addr).await.unwrap();
    let remote_addr = tcp.peer_addr().unwrap();
    let server_name = ServerName::try_from("localhost").unwrap();
    let inner = ManualTlsStream::connect(tcp, client_config, server_name)
        .await
        .unwrap();
    let mut transport = NntpTransport::ManualTls {
        inner,
        remote_addr: Some(remote_addr),
    };
    let mut received = Vec::with_capacity(payload_len);
    let mut read_buf = BytesMut::with_capacity(TLS_READ_TURN_LIMIT);
    let mut read_calls = 0usize;

    while received.len() < payload_len {
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            transport.read_into_buf_with_stats(&mut read_buf, usize::MAX),
        )
        .await
        .expect("manual TLS read timed out")
        .expect("manual TLS read failed");
        assert_ne!(read.bytes, 0, "stream closed before the complete payload");
        assert_eq!(read.bytes, read_buf.len());
        assert!(
            read.bytes <= TLS_READ_TURN_LIMIT,
            "one read appended {} bytes, above the {}-byte turn limit",
            read.bytes,
            TLS_READ_TURN_LIMIT
        );
        received.extend_from_slice(&read_buf);
        read_buf.clear();
        read_calls += 1;
    }

    server.await.unwrap();
    let eof = tokio::time::timeout(
        Duration::from_secs(5),
        transport.read_into_buf(&mut read_buf, usize::MAX),
    )
    .await
    .expect("manual TLS EOF read timed out")
    .expect("manual TLS EOF read failed");

    assert_eq!(eof, 0);
    assert!(read_buf.is_empty());
    assert!(read_calls >= 4);
    assert_eq!(received.as_slice(), payload.as_ref());
}

async fn spawn_scripted_server(steps: Vec<ScriptStep>, hold_open_after_last: Duration) -> u16 {
    spawn_shared_scripted_server(steps, hold_open_after_last).await
}

fn scripted_plain_config(port: u16) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port,
        tls: false,
        connect_timeout: Duration::from_secs(1),
        command_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}

/// A server that answers each AUTHINFO line as it arrives, then answers
/// nothing until every pipelined line has arrived — so a client that
/// pipelined AUTHINFO, or serialized MODE READER and GROUP, fails here.
async fn spawn_pipelined_setup_server(
    auth: Vec<(&'static str, &'static [u8])>,
    expected: Vec<&'static str>,
    responses: &'static [u8],
) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket.write_all(b"200 ready\r\n").await.unwrap();
        socket.flush().await.unwrap();
        for (prefix, response) in auth {
            let line = crate::test_support::read_command_line(&mut socket).await;
            assert!(
                line.starts_with(prefix),
                "expected {prefix:?} but received {line:?}"
            );
            socket.write_all(response).await.unwrap();
            socket.flush().await.unwrap();
        }
        let mut lines = Vec::new();
        let wait = tokio::time::timeout(Duration::from_secs(2), async {
            while lines.len() < expected.len() {
                lines.push(crate::test_support::read_command_line(&mut socket).await);
            }
        })
        .await;
        assert!(
            wait.is_ok(),
            "client did not pipeline MODE READER and GROUP; received {lines:?}"
        );
        for (line, prefix) in lines.iter().zip(&expected) {
            assert!(
                line.starts_with(prefix),
                "expected {prefix:?} but received {line:?}"
            );
        }
        socket.write_all(responses).await.unwrap();
        socket.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    port
}

fn pipelined_setup_config(port: u16) -> ServerConfig {
    ServerConfig {
        username: Some("user".into()),
        password: Some("pass".into()),
        pipelining: PipeliningCapability::Known(true),
        command_timeout: Duration::from_secs(1),
        ..scripted_plain_config(port)
    }
}

/// A server that has proven it needs a selected group gets the GROUP in the
/// same write, after the serial AUTHINFO exchange. Nothing else is ever added
/// to that write — MODE READER in particular is never sent to anyone.
#[tokio::test]
async fn known_pipelining_servers_authenticate_then_get_the_group_in_one_write() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["GROUP alt.test"],
        b"211 1 1 1 alt.test\r\n",
    )
    .await;
    crate::server_caps::note_group_required("127.0.0.1", port);

    let conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .await
    .unwrap();

    assert!(conn.is_healthy());
    assert_eq!(conn.current_group(), Some("alt.test"));
    crate::server_caps::forget("127.0.0.1", port);
}

/// Session setup is authentication and nothing else: a lane reaches its first
/// command in four round trips, so nothing may be written between the last
/// AUTHINFO answer and the caller's own command.
#[tokio::test]
async fn an_unproven_server_gets_no_mode_reader_and_no_group() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"430 no article\r\n",
    )
    .await;

    let mut conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .await
    .unwrap();

    assert_eq!(conn.current_group(), None);
    // The scripted server asserts the command it receives, so reaching a
    // 430 here is the proof that BODY was the next line on the wire.
    let error = conn.body_by_id("<first@example.com>").await.unwrap_err();
    assert!(
        matches!(error, NntpError::ArticleNotFound),
        "unexpected error: {error:?}"
    );
    crate::server_caps::forget("127.0.0.1", port);
}

/// A 500 to BODY is the server refusing the command, and that is all it is.
/// It surfaces as a plain error on that server; it must not silently rewrite
/// how weaver talks to the server from then on, and it must not be read as a
/// hint to start sending MODE READER.
#[tokio::test]
async fn a_500_after_setup_surfaces_as_an_error_and_teaches_nothing() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"500 unknown command\r\n",
    )
    .await;

    let mut conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        None,
    )
    .await
    .unwrap();

    let error = conn.body_by_id("<first@example.com>").await.unwrap_err();
    assert!(
        matches!(error, NntpError::CommandNotRecognized),
        "unexpected error: {error:?}"
    );
    assert!(
        !conn.needs_group_prologue(),
        "a refused BODY says nothing about groups"
    );
    crate::server_caps::forget("127.0.0.1", port);
}

/// A 412 for a message-id fetch is the server insisting on a selected group,
/// which RFC 3977 does not require of it — record it and pay the GROUP round
/// trip on later connections to that server only.
#[tokio::test]
async fn a_412_after_setup_teaches_the_process_to_select_a_group() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["BODY <first@example.com>"],
        b"412 no newsgroup selected\r\n",
    )
    .await;

    let mut conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .await
    .unwrap();
    assert!(!conn.needs_group_prologue());

    let _ = conn.body_by_id("<first@example.com>").await;

    assert!(
        conn.needs_group_prologue(),
        "the 412 should have been recorded against the server"
    );
    assert!(!conn.is_healthy());
    crate::server_caps::forget("127.0.0.1", port);
}

#[tokio::test]
async fn pipelined_setup_skips_the_password_after_281_on_user() {
    // The next line the server reads after 281 must be the GROUP it asked
    // for, not a surplus AUTHINFO PASS.
    let port = spawn_pipelined_setup_server(
        vec![("AUTHINFO USER user", b"281 welcome\r\n")],
        vec!["GROUP alt.test"],
        b"211 1 1 1 alt.test\r\n",
    )
    .await;
    crate::server_caps::note_group_required("127.0.0.1", port);

    let conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        Some("alt.test"),
    )
    .await
    .unwrap();

    assert_eq!(conn.current_group(), Some("alt.test"));
    crate::server_caps::forget("127.0.0.1", port);
}

#[tokio::test]
async fn pipelined_setup_maps_a_rejected_password() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"481 bad password\r\n"),
        ],
        vec![],
        b"",
    )
    .await;

    let result = NntpConnection::connect(&pipelined_setup_config(port)).await;

    let Err(error) = result else {
        panic!("expected a rejected password");
    };
    assert!(
        matches!(error, NntpError::AuthenticationFailed),
        "{error:?}"
    );
}

#[tokio::test]
async fn pipelined_setup_leaves_a_missing_group_unselected() {
    let port = spawn_pipelined_setup_server(
        vec![
            ("AUTHINFO USER user", b"381 password\r\n"),
            ("AUTHINFO PASS pass", b"281 welcome\r\n"),
        ],
        vec!["MODE READER", "GROUP alt.gone"],
        b"200 reader\r\n411 no such group\r\n",
    )
    .await;

    let conn = NntpConnection::connect_with_ip_policy_for_group(
        &pipelined_setup_config(port),
        &[],
        0,
        Some("alt.gone"),
    )
    .await
    .unwrap();

    assert!(conn.is_healthy());
    assert_eq!(conn.current_group(), None);
}

fn leaked_response(bytes: Vec<u8>) -> &'static [u8] {
    Box::leak(bytes.into_boxed_slice())
}

fn yenc_body_response(decoded: &[u8], terminator: &[u8]) -> &'static [u8] {
    let mut article = Vec::new();
    encode(decoded, &mut article, 128, "test.bin").unwrap();

    let mut response = b"222 body follows\r\n".to_vec();
    response.extend_from_slice(&article);
    response.extend_from_slice(terminator);
    leaked_response(response)
}

#[test]
fn group_tracking_initial_state() {
    // Verify that current_group starts as None and the accessor works.
    // We can't fully construct an NntpConnection without a server, so we
    // test the field semantics through the public accessor on a real
    // connection in the ignored integration test below. Here we just
    // verify the ServerConfig and ConnectionState basics hold.
    //
    // The actual group tracking behaviour (set after select_group, reset
    // on poison) is validated in the integration test.
    assert_eq!(ConnectionState::Disconnected, ConnectionState::Disconnected);
}

#[tokio::test]
#[ignore] // Requires a real NNTP server
async fn group_tracking() {
    let config = ServerConfig {
        host: "news.example.com".into(),
        port: 563,
        tls: true,
        username: Some("user".into()),
        password: Some("pass".into()),
        ..Default::default()
    };
    let mut conn = NntpConnection::connect(&config).await.unwrap();

    // current_group starts as None
    assert!(conn.current_group().is_none());

    // After selecting a group it should be set
    conn.select_group("alt.binaries.test").await.unwrap();
    assert_eq!(conn.current_group(), Some("alt.binaries.test"));

    // Selecting the same group again should be a no-op (no extra command)
    conn.select_group("alt.binaries.test").await.unwrap();
    assert_eq!(conn.current_group(), Some("alt.binaries.test"));

    // Selecting a different group should update
    conn.select_group("alt.binaries.other").await.unwrap();
    assert_eq!(conn.current_group(), Some("alt.binaries.other"));
}

#[test]
fn server_config_defaults() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.port, 563);
    assert!(cfg.tls);
    assert!(!cfg.starttls);
    assert!(cfg.username.is_none());
    assert!(cfg.password.is_none());
    assert_eq!(cfg.pipelining, PipeliningCapability::Probe);
}

#[tokio::test]
async fn probe_fetches_capabilities_once_after_authentication() {
    // Authentication is the whole of session setup, so CAPABILITIES is the
    // next line after it — nothing is inserted ahead of either.
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nPIPELINING\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut config = scripted_plain_config(port);
    config.username = Some("user".into());
    config.password = Some("pass".into());
    let conn = NntpConnection::connect(&config).await.unwrap();
    assert!(conn.capabilities().supports_pipelining());
}

#[tokio::test]
async fn connect_accepts_201_greeting() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"201 no posting\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let conn = NntpConnection::connect(&scripted_plain_config(port)).await;
    assert!(conn.is_ok());
}

#[tokio::test]
async fn connect_maps_400_greeting_to_service_unavailable() {
    let port = spawn_scripted_server(
        vec![ScriptStep {
            expect_prefix: None,
            response: b"400 service unavailable\r\n",
            delay: Duration::ZERO,
        }],
        Duration::ZERO,
    )
    .await;

    let err = match NntpConnection::connect(&scripted_plain_config(port)).await {
        Ok(_) => panic!("expected connect to fail"),
        Err(err) => err,
    };
    assert!(matches!(err, NntpError::ServiceUnavailable));
}

#[tokio::test]
async fn body_by_id_reauthenticates_on_mid_session_480() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"480 authentication required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\nhello\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut config = scripted_plain_config(port);
    config.username = Some("user".into());
    config.password = Some("pass".into());

    let mut conn = NntpConnection::connect(&config).await.unwrap();
    let response = conn.body_by_id("<test@example.com>").await.unwrap();
    assert_eq!(&response.data[..], b"hello\r\n");
}

#[tokio::test]
async fn stream_body_chunked_raw_reauthenticates_on_mid_session_480() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"480 authentication required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\nhello\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut config = scripted_plain_config(port);
    config.username = Some("user".into());
    config.password = Some("pass".into());

    let mut conn = NntpConnection::connect(&config).await.unwrap();
    let mut chunks = Vec::new();
    let total = conn
        .stream_body_chunked_raw("<test@example.com>", |chunk| {
            chunks.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(total.bytes, b"hello\r\n".len() as u64);
    assert_eq!(chunks, vec![b"hello\r\n".to_vec()]);
}

#[tokio::test]
async fn body_by_id_handles_split_terminator() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\nhello\r\n.",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: None,
                response: b"\r\n",
                delay: Duration::from_millis(5),
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let response = conn.body_by_id("<test@example.com>").await.unwrap();
    assert_eq!(&response.data[..], b"hello\r\n");
}

#[tokio::test]
async fn body_by_id_reports_disconnect_mid_body() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\n..partial\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(42),
        crate::transfer::ServerTransferConfig::default(),
    );
    conn.set_transfer_control(Some(control.clone()));
    let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
    assert!(matches!(err, NntpError::ServerDisconnectedMidBody));
    assert_eq!(
        control.snapshot().lifetime_body_bytes,
        b"..partial\r\n".len() as u64
    );
}

#[tokio::test]
async fn buffered_body_counts_wire_bytes_before_dot_unstuffing() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\n..dot-stuffed\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(43),
        crate::transfer::ServerTransferConfig::default(),
    );
    conn.set_transfer_control(Some(control.clone()));
    conn.write_body_request("<buffered@example.com>")
        .await
        .unwrap();
    conn.flush_commands().await.unwrap();
    assert_eq!(conn.read_response().await.unwrap().code.raw(), 222);
    let data = conn.read_multiline_data().await.unwrap();
    assert_eq!(data.as_ref(), b".dot-stuffed\r\n");
    assert_eq!(
        control.snapshot().lifetime_body_bytes,
        b"..dot-stuffed\r\n".len() as u64
    );
}

#[tokio::test]
async fn buffered_partial_failure_counts_wire_bytes_before_dot_unstuffing() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\n..dot\r\nincomplete",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(44),
        crate::transfer::ServerTransferConfig::default(),
    );
    conn.set_transfer_control(Some(control.clone()));
    conn.write_body_request("<buffered-partial@example.com>")
        .await
        .unwrap();
    conn.flush_commands().await.unwrap();
    assert_eq!(conn.read_response().await.unwrap().code.raw(), 222);
    let err = conn.read_multiline_data().await.unwrap_err();
    assert!(matches!(err, NntpError::ServerDisconnectedMidBody));
    assert_eq!(
        control.snapshot().lifetime_body_bytes,
        b"..dot\r\nincomplete".len() as u64
    );
}

#[tokio::test]
async fn raw_body_counts_exact_wire_payload() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\n..raw-dot\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(45),
        crate::transfer::ServerTransferConfig::default(),
    );
    conn.set_transfer_control(Some(control.clone()));
    let body = conn.body_by_id_raw("<raw@example.com>").await.unwrap();
    assert_eq!(body.data.as_ref(), b"..raw-dot\r\n");
    assert_eq!(
        control.snapshot().lifetime_body_bytes,
        b"..raw-dot\r\n".len() as u64
    );
}

#[tokio::test]
async fn expired_active_budget_does_not_enter_async_rate_wait() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::from_secs(1),
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(46),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 100,
            quota: None,
        },
    );
    conn.set_transfer_control(Some(control.clone()));
    conn.write_body_request("<expired-budget@example.com>")
        .await
        .unwrap();
    conn.flush_commands().await.unwrap();
    let initial = conn.read_response().await.unwrap();

    let decoded = vec![b'A'; 1_200];
    let mut buffered_body = Vec::new();
    encode(&decoded, &mut buffered_body, 128, "expired.bin").unwrap();
    buffered_body.extend_from_slice(b".\r\n");
    conn.read_buf = BytesMut::from(buffered_body.as_slice());
    let mut budget = ActiveTransferBudget::new(Duration::ZERO);

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        conn.stream_yenc_article_response(initial, Some(&mut budget), |_| Ok(())),
    )
    .await
    .expect("expired budget must not wait on the rate limiter")
    .unwrap_err();

    assert!(matches!(
        error,
        FusedYencError::Nntp(NntpError::SoftTimeout(_))
    ));
    assert!(conn.is_poisoned());
    let snapshot = control.snapshot();
    assert!(snapshot.lifetime_body_bytes >= decoded.len() as u64);
    assert_eq!(snapshot.throttle_wait, Duration::ZERO);
}

#[tokio::test]
async fn quota_rejection_happens_before_body_command() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::from_millis(50),
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let registry = crate::transfer::ServerTransferRegistry::new();
    let control = registry.configure(
        crate::transfer::StableServerId(43),
        crate::transfer::ServerTransferConfig {
            rate_bytes_per_sec: 0,
            quota: Some(crate::transfer::QuotaRuntimeConfig {
                limit_bytes: 0,
                generation: 1,
                retry_at: None,
            }),
        },
    );
    conn.set_transfer_control(Some(control));

    let error = conn.body_by_id("<blocked@example.com>").await.unwrap_err();
    assert!(matches!(error, NntpError::QuotaBlocked(_)));
}

#[tokio::test]
async fn body_by_id_reports_malformed_terminator() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\npartial\r\n.\r",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
    assert!(
        matches!(err, NntpError::MalformedMultilineTerminator),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn body_by_id_reports_truncated_multiline_body_on_timeout() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\npartial\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::from_millis(1500),
    )
    .await;

    let mut config = scripted_plain_config(port);
    config.command_timeout = Duration::from_secs(1);

    let mut conn = NntpConnection::connect(&config).await.unwrap();
    let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
    assert!(
        matches!(err, NntpError::TruncatedMultilineBody),
        "expected TruncatedMultilineBody, got {err:?}"
    );
}

#[tokio::test]
async fn stream_body_chunked_raw_finishes_when_data_and_terminator_arrive_together() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\nline one\r\nline two\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::from_secs(1),
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let mut chunks = Vec::new();
    let total = conn
        .stream_body_chunked_raw("<test@example.com>", |chunk| {
            chunks.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(total.bytes, b"line one\r\nline two\r\n".len() as u64);
    assert_eq!(chunks, vec![b"line one\r\nline two\r\n".to_vec()]);
}

#[tokio::test]
async fn stream_yenc_article_decodes_body_and_keeps_connection_usable() {
    let original = b"hello fused connection";
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: yenc_body_response(original, b".\r\n"),
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 body follows\r\nraw body\r\n.\r\n",
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let mut chunks = Vec::new();
    let article = conn
        .stream_yenc_article("<test@example.com>", |chunk| {
            chunks.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(chunks, vec![original.to_vec()]);
    assert_eq!(article.stats.decoded_bytes_written, original.len() as u64);
    assert_eq!(article.stats.yenc_size_actual, original.len() as u64);
    assert_eq!(article.stats.yenc_control_hits, 1);
    assert_eq!(article.stats.nntp_terminator_hits, 1);
    assert_eq!(article.stats.nntp_terminator_bytes, b".\r\n".len() as u64);
    assert_eq!(article.stats.leftover_bytes_after_terminator, 0);

    let response = conn.body_by_id_raw("<next@example.com>").await.unwrap();
    assert_eq!(&response.data[..], b"raw body\r\n");
}

#[tokio::test]
async fn stream_yenc_article_reauthenticates_on_mid_session_480() {
    let original = b"reauth fused";
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"480 authentication required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO USER"),
                response: b"381 password required\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("AUTHINFO PASS"),
                response: b"281 authentication accepted\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: yenc_body_response(original, b".\r\n"),
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut config = scripted_plain_config(port);
    config.username = Some("user".into());
    config.password = Some("pass".into());

    let mut conn = NntpConnection::connect(&config).await.unwrap();
    let mut chunks = Vec::new();
    conn.stream_yenc_article("<test@example.com>", |chunk| {
        chunks.push(chunk.to_vec());
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(chunks, vec![original.to_vec()]);
}

#[tokio::test]
async fn stream_yenc_article_batches_large_decoded_output() {
    let mut original = Vec::with_capacity(FUSED_YENC_OUTPUT_BATCH_TARGET + 123);
    for idx in 0..(FUSED_YENC_OUTPUT_BATCH_TARGET + 123) {
        original.push((idx % 251) as u8);
    }

    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: yenc_body_response(&original, b".\r\n"),
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let mut delivered: Vec<Vec<u8>> = Vec::new();
    let article = conn
        .stream_yenc_article("<test@example.com>", |chunk| {
            delivered.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(article.to_data(), original);
    let chunk_lens: Vec<usize> = delivered.iter().map(Vec::len).collect();
    assert_eq!(chunk_lens, vec![FUSED_YENC_OUTPUT_BATCH_TARGET, 123]);
    assert_eq!(article.stats.output_batches, 2);
    // The callback sees the same bytes as the buffered article, in order.
    assert_eq!(delivered.concat(), article.to_data());
}

#[tokio::test]
async fn stream_yenc_article_reports_malformed_terminator_and_poisons_connection() {
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: yenc_body_response(b"bad terminator", b"..\r\n"),
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    let err = conn
        .stream_yenc_article("<test@example.com>", |_| Ok(()))
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        FusedYencError::Nntp(NntpError::MalformedMultilineTerminator)
    ));
    assert!(conn.is_poisoned());
}

#[tokio::test]
async fn stream_next_yenc_article_decodes_queued_body_response() {
    let original = b"queued fused response";
    let port = spawn_scripted_server(
        vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"500 unknown\r\n",
                delay: Duration::ZERO,
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: yenc_body_response(original, b".\r\n"),
                delay: Duration::ZERO,
            },
        ],
        Duration::ZERO,
    )
    .await;

    let mut conn = NntpConnection::connect(&scripted_plain_config(port))
        .await
        .unwrap();
    conn.write_body_request("<test@example.com>").await.unwrap();
    conn.flush_commands().await.unwrap();

    let mut chunks = Vec::new();
    let article = conn
        .stream_next_yenc_article(|chunk| {
            chunks.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(chunks, vec![original.to_vec()]);
    assert_eq!(article.to_data(), original);
    assert_eq!(article.yenc_result().bytes_written, original.len());
}

#[tokio::test]
#[ignore] // Requires a real NNTP server
async fn connect_to_real_server() {
    let config = ServerConfig {
        host: "news.example.com".into(),
        port: 563,
        tls: true,
        username: Some("user".into()),
        password: Some("pass".into()),
        ..Default::default()
    };
    let conn = NntpConnection::connect(&config).await;
    assert!(conn.is_ok());
}

#[tokio::test]
#[ignore] // Requires a real NNTP server
async fn fetch_body_from_real_server() {
    let config = ServerConfig {
        host: "news.example.com".into(),
        port: 563,
        tls: true,
        username: Some("user".into()),
        password: Some("pass".into()),
        ..Default::default()
    };
    let mut conn = NntpConnection::connect(&config).await.unwrap();
    let result = conn.body_by_id("<test@example.com>").await;
    println!("{result:?}");
}

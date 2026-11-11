// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A download lane's read buffers stay within the sizes its buffer profile
// gives them, on every transport, while it reads articles far larger than
// either buffer.

use super::idle::spawn_plain_body_server;
use super::*;
use crate::blocking::read_probe;

const ARTICLE_BYTES: usize = 1024 * 1024 + 4321;
const ARTICLES: usize = 4;

fn article(index: usize) -> Vec<u8> {
    (0..ARTICLE_BYTES)
        .map(|i| (i.wrapping_mul(31) + index) as u8)
        .collect()
}

// Reads every article, two requests in flight at a time, and returns what the
// probe recorded on this thread.
fn read_articles(conn: &mut BlockingNntpConnection, ids: &[&str]) -> read_probe::Samples {
    read_probe::take();
    read_probe::ENABLED.store(true, Ordering::Relaxed);
    for (pair, batch) in ids.chunks(2).enumerate() {
        for id in batch {
            conn.write_body_request(id).unwrap();
        }
        conn.flush_commands().unwrap();
        for (offset, _) in batch.iter().enumerate() {
            let data = conn.stream_next_yenc_article().unwrap().into_data();
            assert_eq!(data, article(pair * 2 + offset));
        }
    }
    read_probe::take()
}

fn assert_within(samples: &read_probe::Samples, profile: NntpBufferProfile, tls_buffer: bool) {
    assert!(!samples.added.is_empty(), "the probe saw the reads");
    assert!(
        samples.read_buf_capacity_max <= profile.read_buf_capacity,
        "read buffer reached {} bytes for a profile of {profile:?}",
        samples.read_buf_capacity_max
    );
    if tls_buffer {
        assert!(
            samples.transport_buffer_max <= profile.socket_read_size,
            "TLS read buffer reached {} bytes for a profile of {profile:?}",
            samples.transport_buffer_max
        );
    } else {
        assert_eq!(samples.transport_buffer_max, 0, "no transport buffer");
    }
}

fn tls_lane_stays_within(profile: NntpBufferProfile, backend: NntpTlsBackend) {
    let ids = ["<a@test>", "<b@test>", "<c@test>", "<d@test>"];
    let (mut config, handle, ca_path) = spawn_tls_nntp_server(
        ids.iter()
            .enumerate()
            .map(|(index, id)| (*id, TestArticle::Body(article(index))))
            .collect(),
    );
    config.buffer_profile = profile;
    config.pipelining = crate::connection::PipeliningCapability::Known(true);
    let mut conn = connect_with_backend(&config, backend);
    let samples = read_articles(&mut conn, &ids);
    assert_within(&samples, profile, backend == NntpTlsBackend::ManualRustls);
    conn.quit().unwrap();
    handle.join().unwrap();
    let _ = std::fs::remove_file(ca_path);
}

fn plain_lane_stays_within(profile: NntpBufferProfile) {
    let (mut config, server) = spawn_plain_body_server((0..ARTICLES).map(article).collect());
    config.buffer_profile = profile;
    let mut conn = BlockingNntpConnection::connect(&config).unwrap();
    let samples = read_articles(&mut conn, &["<a@test>", "<b@test>", "<c@test>", "<d@test>"]);
    assert_within(&samples, profile, false);
    conn.quit().unwrap();
    server.join().unwrap();
}

// Bytes queued on a socket and not yet read.
#[cfg(unix)]
fn queued(tcp: &TcpStream) -> usize {
    use std::os::fd::AsRawFd;
    let mut queued: libc::c_int = 0;
    // SAFETY: FIONREAD writes one int through the pointer it is handed.
    let rc = unsafe { libc::ioctl(tcp.as_raw_fd(), libc::FIONREAD, &mut queued) };
    assert_eq!(rc, 0, "FIONREAD");
    queued as usize
}

// A rustls lane whose socket reads are full, and which leaves part of a line
// behind after each read as the decoder does, keeps its read buffer at the
// size it was given. Each read waits until the whole chunk is queued, so
// every read returns exactly one chunk whatever the machine's speed.
#[cfg(unix)]
#[test]
fn full_rustls_reads_never_grow_the_read_buffer() {
    const READS: usize = 12;
    const RESIDUAL: usize = 200;
    let read_size = ordinary().socket_read_size;
    let read_buf_capacity = ordinary().read_buf_capacity;
    // Full reads of slightly different sizes, so that over the run some read
    // fills the buffer to within one drain chunk of its end.
    let chunk_size = move |read: usize| read_size - (read % 5) * 3000;

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let ca = unique_ca_path("full-rustls-reads");
    std::fs::write(&ca, cert.cert.pem()).unwrap();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let mut config = RustlsServerConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], key)
    .unwrap();
    // No tickets: every byte after the handshake is application data.
    config.send_tls13_tickets = 0;
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ack, acks) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let (mut tcp, _) = listener.accept().unwrap();
        let mut tls = tokio_rustls::rustls::ServerConnection::new(Arc::new(config)).unwrap();
        while tls.is_handshaking() {
            tls.complete_io(&mut tcp).unwrap();
        }
        while tls.wants_write() {
            tls.write_tls(&mut tcp).unwrap();
        }
        tls.set_buffer_limit(None);
        let plaintext = vec![b'x'; read_size * (READS + 2)];
        tls.writer().write_all(&plaintext).unwrap();
        let mut ciphertext = Vec::new();
        while tls.wants_write() {
            tls.write_tls(&mut ciphertext).unwrap();
        }
        let mut sent = 0;
        for read in 0..READS {
            let chunk = &ciphertext[sent..sent + chunk_size(read)];
            sent += chunk.len();
            tcp.write_all(chunk).unwrap();
            if acks.recv().is_err() {
                return;
            }
        }
    });

    let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let watch = tcp.try_clone().unwrap();
    let mut stream = BlockingManualTlsStream::connect(
        tcp,
        "localhost",
        Some(&ca),
        None,
        TlsCipherPreference::Auto,
        Duration::from_secs(600),
    )
    .unwrap();
    let mut read_buf = BytesMut::with_capacity(read_buf_capacity);
    for read in 0..READS {
        while queued(&watch) < chunk_size(read) {
            std::thread::yield_now();
        }
        let (n, _) = stream
            .read_into_buf(&mut read_buf, read_size, Duration::from_secs(600))
            .unwrap();
        assert!(n > 0);
        assert!(
            read_buf.capacity() <= read_buf_capacity,
            "read {read} grew the read buffer to {} bytes",
            read_buf.capacity()
        );
        // The decoder takes every whole line and leaves a partial one.
        let keep = RESIDUAL.min(read_buf.len());
        let consumed = read_buf.len() - keep;
        bytes::Buf::advance(&mut read_buf, consumed);
        ack.send(()).unwrap();
    }
    drop(ack);
    server.join().unwrap();
    std::fs::remove_file(ca).unwrap();
}

fn ordinary() -> NntpBufferProfile {
    NntpBufferProfile::adaptive(8 << 30, 20)
}

#[test]
fn a_plain_lane_keeps_its_read_buffer_at_the_ordinary_size() {
    plain_lane_stays_within(ordinary());
}

#[test]
fn a_rustls_lane_keeps_its_read_buffers_at_the_ordinary_size() {
    tls_lane_stays_within(ordinary(), NntpTlsBackend::ManualRustls);
}

#[cfg(not(windows))]
#[test]
fn an_s2n_lane_keeps_its_read_buffer_at_the_ordinary_size() {
    let _guard = s2n_test_guard();
    tls_lane_stays_within(ordinary(), NntpTlsBackend::S2n);
}

// The profile at the most connections any host gives: every buffer at its
// floor.
fn crowded() -> NntpBufferProfile {
    NntpBufferProfile::adaptive(2 << 30, 5000)
}

#[test]
fn a_plain_lane_keeps_its_read_buffer_at_the_floor() {
    plain_lane_stays_within(crowded());
}

#[test]
fn a_rustls_lane_keeps_its_read_buffers_at_the_floor() {
    tls_lane_stays_within(crowded(), NntpTlsBackend::ManualRustls);
}

#[cfg(not(windows))]
#[test]
fn an_s2n_lane_keeps_its_read_buffer_at_the_floor() {
    let _guard = s2n_test_guard();
    tls_lane_stays_within(crowded(), NntpTlsBackend::S2n);
}

const GIB: u64 = 1 << 30;
const KIB: usize = 1024;

fn budget(available: u64) -> usize {
    (available / 32).min(256 << 20) as usize
}

#[test]
fn ordinary_connection_counts_keep_the_full_sizes() {
    for available in [2 * GIB, 8 * GIB, 16 * GIB, 64 * GIB] {
        for connections in 1..=100 {
            assert_eq!(
                NntpBufferProfile::adaptive(available, connections),
                NntpBufferProfile {
                    read_buf_capacity: 256 * KIB,
                    socket_read_size: 128 * KIB,
                },
                "{connections} connections on {} GiB",
                available / GIB
            );
        }
    }
}

#[test]
fn every_connection_count_fits_the_budget_or_sits_at_the_floor() {
    let floor = NntpBufferProfile::default();
    assert_eq!(floor.socket_read_size, 32 * KIB);
    assert_eq!(floor.read_buf_capacity, 64 * KIB);
    for available in [GIB, 2 * GIB, 4 * GIB, 8 * GIB, 64 * GIB, 512 * GIB] {
        let mut last = usize::MAX;
        for connections in (1..=20_000).step_by(7) {
            let profile = NntpBufferProfile::adaptive(available, connections);
            assert_eq!(profile.read_buf_capacity, 2 * profile.socket_read_size);
            assert!(
                profile.per_connection_bytes() <= last,
                "more connections never get more memory each"
            );
            last = profile.per_connection_bytes();
            assert!(
                profile == floor
                    || profile.per_connection_bytes() * connections <= budget(available),
                "{connections} connections on {available} bytes: {profile:?}"
            );
        }
    }
}

#[test]
fn zero_connections_get_the_whole_budget() {
    assert_eq!(
        NntpBufferProfile::adaptive(8 * GIB, 0),
        NntpBufferProfile::adaptive(8 * GIB, 1)
    );
}

// The size table, printed for the record: run with `--no-capture`.
#[test]
fn buffer_size_table() {
    eprintln!(
        "BUFFER_TABLE host connections read_buf socket_read tls_total_mib plain_total_mib budget_mib"
    );
    for available in [2 * GIB, 8 * GIB, 64 * GIB] {
        for connections in [20, 50, 100, 500, 1000, 2000, 5000] {
            let profile = NntpBufferProfile::adaptive(available, connections);
            let mib = |bytes: usize| bytes as f64 / (1 << 20) as f64;
            let tls_total = profile.per_connection_bytes() * connections;
            let plain_total = profile.read_buf_capacity * connections;
            eprintln!(
                "BUFFER_TABLE {}GiB {connections} {}K {}K {:.1} {:.1} {:.0}",
                available / GIB,
                profile.read_buf_capacity / KIB,
                profile.socket_read_size / KIB,
                mib(tls_total),
                mib(plain_total),
                mib(budget(available)),
            );
            assert!(tls_total <= budget(available) || profile == NntpBufferProfile::default());
        }
    }
}

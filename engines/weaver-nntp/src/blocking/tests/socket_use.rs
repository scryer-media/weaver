// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A manual measurement of how a download lane uses its read buffers against a
// real NNTP server, never part of the suite. It records the bytes each read
// returns and the high-water fill of `read_buf` and of the transport's own
// buffer. Elapsed time is printed, never asserted.
//
// Configured through the environment:
// - `WEAVER_SOCKET_USE_ADDR`: host:port of the server.
// - `WEAVER_SOCKET_USE_TLS`: `1` for implicit TLS.
// - `WEAVER_SOCKET_USE_CA`: CA PEM for a TLS server.
// - `WEAVER_SOCKET_USE_IDS`: file of message-ids, one per line.
// - `WEAVER_SOCKET_USE_CONNECTIONS`: lane count.
// - `WEAVER_SOCKET_USE_DEPTH`: BODY requests in flight per lane (default 2).
// - `WEAVER_SOCKET_USE_USER` / `WEAVER_SOCKET_USE_PASS`: credentials.
// - `WEAVER_SOCKET_USE_BACKEND`: `rustls` or `s2n` for a TLS server.

use super::*;
use std::sync::atomic::AtomicUsize;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

// User plus system CPU this process has used, in seconds.
#[cfg(unix)]
fn process_cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `getrusage` fills the struct it is handed.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1e6;
    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> f64 {
    0.0
}

fn percentile(sorted: &[u32], fraction: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn summary(label: &str, values: &mut [u32]) -> String {
    values.sort_unstable();
    let near_full = values.iter().filter(|&&v| v >= 240 * 1024).count();
    format!(
        "{label}: n={} median={} p95={} p99={} max={} reads>=240KiB={}",
        values.len(),
        percentile(values, 0.5),
        percentile(values, 0.95),
        percentile(values, 0.99),
        values.last().copied().unwrap_or(0),
        near_full,
    )
}

#[test]
#[ignore = "manual measurement against an NNTP server"]
fn measure_read_buffer_use() {
    let addr = env("WEAVER_SOCKET_USE_ADDR").expect("WEAVER_SOCKET_USE_ADDR");
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let tls = env("WEAVER_SOCKET_USE_TLS").as_deref() == Some("1");
    let connections: usize = env("WEAVER_SOCKET_USE_CONNECTIONS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(8);
    let depth: usize = env("WEAVER_SOCKET_USE_DEPTH")
        .map(|v| v.parse().unwrap())
        .unwrap_or(2);
    let backend = match env("WEAVER_SOCKET_USE_BACKEND").as_deref() {
        Some("s2n") => {
            #[cfg(not(windows))]
            {
                Some(NntpTlsBackend::S2n)
            }
            #[cfg(windows)]
            {
                panic!("s2n is not built on Windows")
            }
        }
        Some("rustls") => Some(NntpTlsBackend::ManualRustls),
        _ => None,
    };
    let ids: Vec<String> = std::fs::read_to_string(env("WEAVER_SOCKET_USE_IDS").unwrap())
        .unwrap()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.starts_with('<') {
                line.to_string()
            } else {
                format!("<{line}>")
            }
        })
        .collect();
    // The ordinary case: a host with plenty of memory.
    let profile = NntpBufferProfile::adaptive(8 << 30, connections);
    let config = ServerConfig {
        dialer: crate::route_dialer::RouteDialer::direct_for_tests(),
        host: host.to_string(),
        port: port.parse().unwrap(),
        tls,
        starttls: false,
        username: env("WEAVER_SOCKET_USE_USER"),
        password: env("WEAVER_SOCKET_USE_PASS"),
        connect_timeout: Duration::from_secs(30),
        command_timeout: Duration::from_secs(60),
        buffer_profile: profile,
        tls_ca_cert: env("WEAVER_SOCKET_USE_CA").map(std::path::PathBuf::from),
        tls_name_mismatch_certificate_der: None,
        pipelining: crate::connection::PipeliningCapability::Known(depth > 1),
        pipelining_depth: None,
        tls_cipher_preference: TlsCipherPreference::Auto,
    };

    crate::blocking::read_probe::ENABLED.store(true, Ordering::Relaxed);
    let ids = Arc::new(ids);
    let next = Arc::new(AtomicUsize::new(0));
    let missing = Arc::new(AtomicUsize::new(0));
    let cpu_started = process_cpu_seconds();
    let started = Instant::now();
    let workers: Vec<_> = (0..connections)
        .map(|_| {
            let ids = Arc::clone(&ids);
            let next = Arc::clone(&next);
            let missing = Arc::clone(&missing);
            let config = config.clone();
            std::thread::spawn(move || {
                let mut conn =
                    BlockingNntpConnection::connect_with_backend(&config, backend, None).unwrap();
                let mut decoded = 0u64;
                loop {
                    let first = next.fetch_add(depth, Ordering::Relaxed);
                    if first >= ids.len() {
                        break;
                    }
                    let batch = &ids[first..(first + depth).min(ids.len())];
                    for id in batch {
                        conn.write_body_request(id).unwrap();
                    }
                    conn.flush_commands().unwrap();
                    for id in batch {
                        let article = match conn.stream_next_yenc_article() {
                            Ok(article) => article,
                            // A missing article is counted, not fatal: the
                            // connection stays framed and carries on.
                            Err(FusedYencError::Nntp(
                                NntpError::ArticleNotFound | NntpError::NoSuchArticle { .. },
                            )) => {
                                missing.fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            Err(error) => panic!("{id}: {error:?}"),
                        };
                        let result = article.body.yenc().expect("yEnc");
                        assert_ne!(
                            result.crc_status,
                            weaver_yenc::CrcVerification::Mismatch,
                            "{id}: CRC mismatch"
                        );
                        decoded += article.stats.decoded_bytes_written;
                    }
                }
                let _ = conn.quit();
                (decoded, crate::blocking::read_probe::take())
            })
        })
        .collect();
    let mut decoded = 0u64;
    let mut added = Vec::new();
    let mut socket = Vec::new();
    let mut read_buf_len_max = 0;
    let mut read_buf_capacity_max = 0;
    let mut transport_buffer_max = 0;
    for worker in workers {
        let (bytes, samples) = worker.join().unwrap();
        decoded += bytes;
        added.extend(samples.added);
        socket.extend(samples.socket);
        read_buf_len_max = read_buf_len_max.max(samples.read_buf_len_max);
        read_buf_capacity_max = read_buf_capacity_max.max(samples.read_buf_capacity_max);
        transport_buffer_max = transport_buffer_max.max(samples.transport_buffer_max);
    }
    crate::blocking::read_probe::ENABLED.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    let cpu = process_cpu_seconds() - cpu_started;
    eprintln!(
        "SOCKET_USE tls={tls} backend={backend:?} connections={connections} depth={depth} articles={} missing={} decoded_mib={:.1} elapsed_s={:.2} mib_s={:.1} cpu_s={cpu:.2} cpu_ms_per_gib={:.1} profile={profile:?}",
        ids.len(),
        missing.load(Ordering::Relaxed),
        decoded as f64 / (1 << 20) as f64,
        elapsed.as_secs_f64(),
        decoded as f64 / (1 << 20) as f64 / elapsed.as_secs_f64(),
        cpu * 1000.0 / (decoded as f64 / (1u64 << 30) as f64),
    );
    eprintln!("SOCKET_USE {}", summary("added_per_read", &mut added));
    eprintln!("SOCKET_USE {}", summary("socket_or_recv_per_call", &mut socket));
    eprintln!(
        "SOCKET_USE read_buf_len_max={read_buf_len_max} read_buf_capacity_max={read_buf_capacity_max} transport_buffer_max={transport_buffer_max}"
    );
}

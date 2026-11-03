// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrent engine's throughput on loopback, with no swarm: a session
// fetches a large hybrid torrent from scripted seeders, through the piece
// storage into its own files, then serves every block of it to scripted
// leechers. The scripted peers run in a child process, so this process's CPU,
// system calls and context switches are the engine's alone.
//
// Ignored: it moves gigabytes and is run on request, in an optimised build:
//
//   cargo nextest run --locked --cargo-profile matrix -p weaver-server-core \
//     --run-ignored only --no-capture -E 'test(=torrents::storage::tests::bench::torrent_loopback_throughput)'
//
// Sizes come from the environment: WEAVER_BENCH_FILE_MIB (each file, default
// 4096), WEAVER_BENCH_FILES (default 2), WEAVER_BENCH_PIECE_KIB (default
// 4096), WEAVER_BENCH_PEERS (default 8), WEAVER_BENCH_DEPTH (requests a
// scripted leecher keeps out, default 250), WEAVER_BENCH_DIR (where the torrent
// writes, default the system temporary directory).

use std::collections::{HashSet, VecDeque};
use std::process::Stdio;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use weaver_torrent::core::merkle;
use weaver_torrent::sha1w::{ISha1, Sha1};

use super::*;

const COUNTERPART: &str = "torrents::storage::tests::bench::torrent_loopback_counterpart";
const BLOCK: u32 = 16 * 1024;
const SEED: u64 = 0x5745_4156_4552_0001;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[derive(Clone, Copy)]
struct Shape {
    files: u64,
    file_length: u64,
    piece_length: u64,
}

impl Shape {
    fn from_env() -> Self {
        Self {
            files: env_u64("WEAVER_BENCH_FILES", 2),
            file_length: env_u64("WEAVER_BENCH_FILE_MIB", 4096) << 20,
            piece_length: env_u64("WEAVER_BENCH_PIECE_KIB", 4096) << 10,
        }
    }

    fn total(&self) -> u64 {
        self.files * self.file_length
    }

    fn pieces(&self) -> u32 {
        (self.total() / self.piece_length) as u32
    }
}

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// The payload at `offset` in the torrent: every 8 bytes a function of where
// they sit, so either side makes any block without a file behind it.
fn fill(offset: u64, into: &mut [u8]) {
    debug_assert!(offset.is_multiple_of(8) && into.len().is_multiple_of(8));
    for (at, word) in into.chunks_exact_mut(8).enumerate() {
        word.copy_from_slice(&splitmix((offset / 8 + at as u64) ^ SEED).to_le_bytes());
    }
}

// Files whose lengths are whole pieces, so the hybrid's v1 listing needs no
// padding and a v1 offset is the offset in the files laid end to end.
fn build(shape: Shape) -> (bytes::Bytes, [u8; 20]) {
    assert!(shape.file_length.is_multiple_of(shape.piece_length));
    assert!(shape.file_length >= 2 * shape.piece_length);
    let pieces = shape.pieces() as usize;
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let mut hashes = vec![([0u8; 20], weaver_torrent::core::Id32::default()); pieces];
    let slots: Vec<std::sync::Mutex<&mut _>> =
        hashes.iter_mut().map(std::sync::Mutex::new).collect();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let mut buf = vec![0u8; shape.piece_length as usize];
                loop {
                    let piece = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if piece >= pieces {
                        break;
                    }
                    fill(piece as u64 * shape.piece_length, &mut buf);
                    let mut sha1 = Sha1::new();
                    sha1.update(&buf);
                    let blocks: Vec<_> = buf
                        .chunks(merkle::BLOCK_SIZE as usize)
                        .map(merkle::hash_block)
                        .collect();
                    let root = merkle::piece_root(&blocks, shape.file_length, shape.piece_length);
                    **slots[piece].lock().unwrap() = (sha1.finish(), root);
                }
            });
        }
    });
    drop(slots);
    let per_file = (shape.file_length / shape.piece_length) as usize;
    let bstr = |bytes: &[u8]| {
        let mut out = format!("{}:", bytes.len()).into_bytes();
        out.extend_from_slice(bytes);
        out
    };
    let mut layers = Vec::new();
    let mut info = b"d9:file treed".to_vec();
    for file in 0..shape.files as usize {
        let layer: Vec<_> = hashes[file * per_file..(file + 1) * per_file]
            .iter()
            .map(|(_, root)| *root)
            .collect();
        let root = merkle::layer_root(&layer, shape.piece_length / merkle::BLOCK_SIZE);
        info.extend(bstr(format!("f{file}.bin").as_bytes()));
        info.extend_from_slice(format!("d0:d6:lengthi{}e", shape.file_length).as_bytes());
        info.extend(bstr(b"pieces root"));
        info.extend(bstr(&root.0));
        info.extend_from_slice(b"ee");
        layers.push((root, layer));
    }
    info.extend_from_slice(b"e5:filesl");
    for file in 0..shape.files {
        info.extend_from_slice(format!("d6:lengthi{}e4:pathl", shape.file_length).as_bytes());
        info.extend(bstr(format!("f{file}.bin").as_bytes()));
        info.extend_from_slice(b"ee");
    }
    info.extend_from_slice(b"e12:meta versioni2e4:name5:bench12:piece length");
    info.extend_from_slice(format!("i{}e6:pieces", shape.piece_length).as_bytes());
    info.extend(bstr(
        &hashes
            .iter()
            .flat_map(|(sha1, _)| *sha1)
            .collect::<Vec<_>>(),
    ));
    info.extend_from_slice(b"e");
    let mut sha1 = Sha1::new();
    sha1.update(&info);
    let info_hash = sha1.finish();

    let mut torrent = b"d4:info".to_vec();
    torrent.extend_from_slice(&info);
    torrent.extend_from_slice(b"12:piece layersd");
    layers.sort_by_key(|a| a.0.0);
    for (root, layer) in layers {
        torrent.extend(bstr(&root.0));
        torrent.extend(bstr(&layer.iter().flat_map(|h| h.0).collect::<Vec<u8>>()));
    }
    torrent.extend_from_slice(b"ee");
    (torrent.into(), info_hash)
}

// What this process has spent so far.
#[derive(Clone, Copy)]
struct Usage {
    at: Instant,
    user_us: u64,
    system_us: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
    minor_faults: u64,
    syscalls: Option<u64>,
}

fn usage() -> Usage {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    // SAFETY: `ru` is a valid, writable rusage.
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let micros = |tv: libc::timeval| tv.tv_sec as u64 * 1_000_000 + tv.tv_usec as u64;
    Usage {
        at: Instant::now(),
        user_us: micros(ru.ru_utime),
        system_us: micros(ru.ru_stime),
        voluntary_switches: ru.ru_nvcsw as u64,
        involuntary_switches: ru.ru_nivcsw as u64,
        minor_faults: ru.ru_minflt as u64,
        syscalls: syscalls(),
    }
}

// Unix and Mach system calls this process has made, from the task's own
// counters: no tracer, no root.
#[cfg(target_os = "macos")]
fn syscalls() -> Option<u64> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    // SAFETY: `info` is a valid, writable proc_taskinfo of `size` bytes.
    let got = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    (got == size).then(|| {
        u64::from(info.pti_syscalls_unix as u32) + u64::from(info.pti_syscalls_mach as u32)
    })
}

#[cfg(not(target_os = "macos"))]
fn syscalls() -> Option<u64> {
    None
}

fn report(phase: &str, bytes: u64, from: Usage, to: Usage) -> String {
    let mib = bytes as f64 / f64::from(1 << 20);
    let wall = to.at.duration_since(from.at).as_secs_f64();
    let user = (to.user_us - from.user_us) as f64 / 1000.0;
    let system = (to.system_us - from.system_us) as f64 / 1000.0;
    let syscalls = match (from.syscalls, to.syscalls) {
        (Some(from), Some(to)) => format!("{:.1}", to.wrapping_sub(from) as f64 / mib),
        _ => "n/a".to_owned(),
    };
    format!(
        "BENCH {phase}: {mib:.0} MiB in {wall:.2} s = {:.0} MiB/s; cpu {:.3} ms/MiB \
         (user {:.3}, sys {:.3}); syscalls {syscalls}/MiB; csw vol {:.2}/MiB invol {:.2}/MiB; \
         minor faults {:.2}/MiB",
        mib / wall,
        (user + system) / mib,
        user / mib,
        system / mib,
        (to.voluntary_switches - from.voluntary_switches) as f64 / mib,
        (to.involuntary_switches - from.involuntary_switches) as f64 / mib,
        (to.minor_faults - from.minor_faults) as f64 / mib,
    )
}

fn counterpart(role: &str, info_hash: &[u8; 20], shape: Shape) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args([
            COUNTERPART,
            "--exact",
            "--ignored",
            "--nocapture",
            "--test-threads",
            "1",
        ])
        .env("WEAVER_BENCH_ROLE", role)
        .env("WEAVER_BENCH_INFO_HASH", hex(info_hash))
        .env("WEAVER_BENCH_FILES", shape.files.to_string())
        .env(
            "WEAVER_BENCH_FILE_MIB",
            (shape.file_length >> 20).to_string(),
        )
        .env(
            "WEAVER_BENCH_PIECE_KIB",
            (shape.piece_length >> 10).to_string(),
        )
        .stdout(Stdio::piped())
        .kill_on_drop(true);
    command
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> [u8; 20] {
    let mut out = [0u8; 20];
    for (at, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[at * 2..at * 2 + 2], 16).unwrap();
    }
    out
}

// The next `BENCH <word>` line the child prints, its value.
async fn line<R: tokio::io::AsyncBufRead + Unpin>(
    lines: &mut tokio::io::Lines<R>,
    word: &str,
) -> String {
    let prefix = format!("BENCH {word} ");
    loop {
        let line = lines
            .next_line()
            .await
            .unwrap()
            .expect("counterpart exited");
        // The harness prints the test's name ahead of its first line.
        if let Some(at) = line.find(&prefix) {
            return line[at + prefix.len()..].to_owned();
        }
    }
}

// A session on loopback alone, as the tests' own, that takes every scripted
// peer: they all come from one address (libtorrent
// `allow_multiple_connections_per_ip`). Its peers time out as production's
// do: a scripted leecher we choke says nothing until we unchoke it.
async fn bench_session(dir: &Path) -> Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            connect: Some(weaver_torrent::ConnectionOptions {
                peer_opts: Some(weaver_torrent::PeerConnectionOptions {
                    read_write_timeout: Some(crate::torrents::sessions::PEER_TIMEOUT),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            persistence: None,
            allow_multiple_connections_per_ip: true,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..weaver_torrent::SessionOptions::new(std::sync::Arc::new(
                weaver_torrent::network::KernelSockets::new(None).expect("the kernel's sockets"),
            ))
        },
    )
    .await
    .unwrap()
}

// With WEAVER_BENCH_TRACE set, the engine's debug events whose text holds
// it go to stderr: why a connection ended, say.
struct Trace(String);

struct Text(String);

impl tracing::field::Visit for Text {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        let _ = write!(self.0, " {}={value:?}", field.name());
    }
}

impl tracing::Subscriber for Trace {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= tracing::Level::DEBUG
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut text = Text(String::new());
        event.record(&mut text);
        if text.0.contains(&self.0) {
            eprintln!("bench trace {}:{}", event.metadata().target(), text.0);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a throughput bench: run on request, optimised"]
async fn torrent_loopback_throughput() {
    if let Ok(needle) = std::env::var("WEAVER_BENCH_TRACE") {
        tracing::subscriber::set_global_default(Trace(needle)).unwrap();
    }
    let shape = Shape::from_env();
    let peers = env_u64("WEAVER_BENCH_PEERS", 8);
    let depth = env_u64("WEAVER_BENCH_DEPTH", 250);
    let built = Instant::now();
    let (torrent, info_hash) = build(shape);
    println!(
        "BENCH torrent: {} files of {} MiB, {} KiB pieces, hybrid, built in {:.1} s",
        shape.files,
        shape.file_length >> 20,
        shape.piece_length >> 10,
        built.elapsed().as_secs_f64()
    );
    let metainfo = torrent_from_bytes(&torrent).unwrap();
    let info = metainfo.info.data.validate().unwrap();
    let layout = Arc::new(layout_of(&info).unwrap());
    let dir = match std::env::var_os("WEAVER_BENCH_DIR") {
        Some(base) => tempfile::tempdir_in(base).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };

    // Download: the session fetches every piece from the scripted seeders.
    let mut seeders = counterpart("seed", &info_hash, shape)
        .env("WEAVER_BENCH_PEERS", peers.to_string())
        .spawn()
        .unwrap();
    let mut seeder_lines = BufReader::new(seeders.stdout.take().unwrap()).lines();
    let ports: Vec<SocketAddr> = line(&mut seeder_lines, "PORTS")
        .await
        .split(',')
        .map(|port| SocketAddr::from((Ipv4Addr::LOCALHOST, port.parse::<u16>().unwrap())))
        .collect();
    let (watched, mut in_hand) = watched_store(&layout, &dir.path().join("job"));
    let factory = PieceStorageFactory::new(
        Arc::clone(&layout),
        Arc::clone(&watched) as Arc<dyn Pieces>,
        unbounded(),
    );
    let leecher = bench_session(&dir.path().join("session")).await;
    let started = usage();
    let _handle = leecher
        .add_torrent(
            AddTorrent::from_bytes(torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().join("unused").to_string_lossy().into_owned()),
                overwrite: true,
                initial_peers: Some(ports),
                storage_factory: Some(factory.boxed_factory()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    in_hand.wait_for(|whole| *whole).await.unwrap();
    let fetched = usage();
    println!("{}", report("download", shape.total(), started, fetched));
    drop(seeder_lines);
    seeders.kill().await.unwrap();

    // Upload: every scripted leecher asks for every block, at most
    // WEAVER_BENCH_UPLOAD_MIB_S a second when set, so two builds can be held
    // to one rate.
    if let Ok(rate) = std::env::var("WEAVER_BENCH_UPLOAD_MIB_S") {
        let bps = rate.parse::<u32>().unwrap() << 20;
        _handle.set_ratelimits(weaver_torrent::limits::LimitsConfig {
            upload_bps: weaver_torrent::limits::LimitKind::Bps(
                std::num::NonZeroU32::new(bps).unwrap(),
            ),
            download_bps: Default::default(),
        });
    }
    let listen = leecher.listen_addr().unwrap();
    let mut leechers = counterpart("leech", &info_hash, shape)
        .env("WEAVER_BENCH_PEERS", peers.to_string())
        .env("WEAVER_BENCH_DEPTH", depth.to_string())
        .env("WEAVER_BENCH_PORT", listen.port().to_string())
        .spawn()
        .unwrap();
    let mut leecher_lines = BufReader::new(leechers.stdout.take().unwrap()).lines();
    let serving = usage();
    let served: u64 = line(&mut leecher_lines, "SERVED").await.parse().unwrap();
    let done = usage();
    println!("{}", report("upload", served, serving, done));
    while leecher_lines.next_line().await.unwrap().is_some() {}
    assert!(leechers.wait().await.unwrap().success());
    leecher.stop().await;
}

// The scripted peers, run by the bench in a child process. Without the
// bench's environment it does nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "the bench's scripted peers"]
async fn torrent_loopback_counterpart() {
    let Ok(role) = std::env::var("WEAVER_BENCH_ROLE") else {
        return;
    };
    let shape = Shape::from_env();
    let info_hash = unhex(&std::env::var("WEAVER_BENCH_INFO_HASH").unwrap());
    let peers = env_u64("WEAVER_BENCH_PEERS", 8);
    match role.as_str() {
        "seed" => {
            let mut ports = Vec::new();
            let mut listeners = Vec::new();
            for _ in 0..peers {
                let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
                ports.push(listener.local_addr().unwrap().port().to_string());
                listeners.push(listener);
            }
            println!("BENCH PORTS {}", ports.join(","));
            for listener in listeners {
                tokio::spawn(async move {
                    loop {
                        let (stream, _) = listener.accept().await.unwrap();
                        tokio::spawn(seed(stream, info_hash, shape));
                    }
                });
            }
            // The seeders serve until the bench kills this process.
            std::future::pending::<()>().await;
        }
        "leech" => {
            let depth = env_u64("WEAVER_BENCH_DEPTH", 250) as usize;
            let port: u16 = std::env::var("WEAVER_BENCH_PORT").unwrap().parse().unwrap();
            let started = Instant::now();
            let tasks: Vec<_> = (0..peers)
                .map(|at| {
                    let start = (u64::from(shape.pieces()) * at / peers) as u32;
                    tokio::spawn(leech(port, info_hash, shape, start, depth, at as u8))
                })
                .collect();
            let (mut served, mut lost) = (0, 0);
            for task in tasks {
                let (taken, dropped) = task.await.unwrap();
                served += taken;
                lost += dropped;
            }
            eprintln!("leechers lost {lost} connections");
            eprintln!("leechers took {:.2} s", started.elapsed().as_secs_f64());
            println!("BENCH SERVED {served}");
        }
        other => panic!("no bench role {other}"),
    }
}

fn handshake(info_hash: &[u8; 20], tag: u8) -> Vec<u8> {
    let mut out = vec![19];
    out.extend_from_slice(b"BitTorrent protocol");
    out.extend_from_slice(&[0; 8]);
    out.extend_from_slice(info_hash);
    out.extend_from_slice(b"-WB0001-bench-peer");
    out.push(b'0' + tag / 128);
    out.push(tag);
    out
}

async fn read_handshake(
    stream: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    info_hash: &[u8; 20],
) {
    let mut theirs = [0u8; 68];
    stream.read_exact(&mut theirs).await.unwrap();
    assert_eq!(&theirs[28..48], info_hash);
}

// One message: its id and payload, or None for a keep-alive.
async fn message(
    stream: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    payload: &mut Vec<u8>,
) -> std::io::Result<Option<u8>> {
    let length = stream.read_u32().await? as usize;
    if length == 0 {
        return Ok(None);
    }
    let id = stream.read_u8().await?;
    payload.resize(length - 1, 0);
    stream.read_exact(payload).await?;
    Ok(Some(id))
}

fn frame(out: &mut Vec<u8>, id: u8, payload: &[&[u8]]) {
    let length = 1 + payload.iter().map(|part| part.len()).sum::<usize>();
    out.extend_from_slice(&(length as u32).to_be_bytes());
    out.push(id);
    for part in payload {
        out.extend_from_slice(part);
    }
}

fn bitfield(pieces: u32) -> Vec<u8> {
    let mut bits = vec![0xff; pieces.div_ceil(8) as usize];
    let spare = bits.len() as u32 * 8 - pieces;
    if spare > 0 {
        *bits.last_mut().unwrap() = 0xff << spare;
    }
    bits
}

// A seeder that has every piece, unchokes at once and answers every request.
async fn seed(stream: TcpStream, info_hash: [u8; 20], shape: Shape) {
    stream.set_nodelay(true).unwrap();
    let (read, mut write) = stream.into_split();
    let mut read = BufReader::with_capacity(64 * 1024, read);
    read_handshake(&mut read, &info_hash).await;
    let mut out = handshake(&info_hash, 255);
    frame(&mut out, 5, &[&bitfield(shape.pieces())]);
    frame(&mut out, 1, &[]);
    write.write_all(&out).await.unwrap();
    out.clear();
    let mut payload = Vec::new();
    while let Ok(id) = message(&mut read, &mut payload).await {
        if id == Some(6) {
            let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
            let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
            let length = u32::from_be_bytes(payload[8..12].try_into().unwrap());
            let at = out.len();
            out.extend_from_slice(&(9 + length).to_be_bytes());
            out.push(7);
            out.extend_from_slice(&payload[0..8]);
            out.resize(at + 13 + length as usize, 0);
            fill(
                u64::from(piece) * shape.piece_length + u64::from(begin),
                &mut out[at + 13..],
            );
        }
        if read.buffer().is_empty() || out.len() >= 1 << 20 {
            if write.write_all(&out).await.is_err() {
                return;
            }
            out.clear();
        }
    }
}

// A leecher that wants every block from `start` on, around to `start` again,
// keeping `depth` requests out while unchoked. A connection the session
// ends is made again, and what it had out is asked for again. Returns the
// payload it took and the connections it lost.
async fn leech(
    port: u16,
    info_hash: [u8; 20],
    shape: Shape,
    start: u32,
    depth: usize,
    tag: u8,
) -> (u64, u64) {
    let pieces = shape.pieces();
    let blocks_per_piece = (shape.piece_length / u64::from(BLOCK)) as u32;
    let total = u64::from(pieces) * u64::from(blocks_per_piece);
    let mut next = 0u64;
    let mut again: VecDeque<(u32, u32)> = VecDeque::new();
    let mut out_now: HashSet<(u32, u32)> = HashSet::new();
    let mut taken = 0u64;
    let mut lost = 0u64;
    let mut duplicates = 0u64;
    let mut payload = Vec::new();
    let mut expect = [0u8; 8];
    loop {
        let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        stream.set_nodelay(true).unwrap();
        let (read, mut write) = stream.into_split();
        let mut read = BufReader::with_capacity(256 * 1024, read);
        let mut out = handshake(&info_hash, tag);
        frame(&mut out, 2, &[]);
        write.write_all(&out).await.unwrap();
        out.clear();
        read_handshake(&mut read, &info_hash).await;
        let mut unchoked = false;
        let ended = loop {
            let id = match message(&mut read, &mut payload).await {
                Ok(id) => id,
                Err(error) => break error,
            };
            match id {
                Some(0) => {
                    unchoked = false;
                    again.extend(out_now.drain());
                }
                Some(1) => unchoked = true,
                Some(7) => {
                    let piece = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                    let begin = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                    // A block the session sent as it choked us was given up
                    // for lost: it is taken all the same, and when it was
                    // asked for again meanwhile, the second copy is a
                    // duplicate. Either way the session sent it.
                    let asked = out_now.remove(&(piece, begin))
                        || again
                            .iter()
                            .position(|block| *block == (piece, begin))
                            .and_then(|at| again.remove(at))
                            .is_some();
                    duplicates += u64::from(!asked);
                    fill(
                        u64::from(piece) * shape.piece_length + u64::from(begin),
                        &mut expect,
                    );
                    assert_eq!(payload[8..16], expect, "block {piece}/{begin}");
                    taken += (payload.len() - 8) as u64;
                }
                _ => {}
            }
            if unchoked {
                while out_now.len() < depth {
                    let block = match again.pop_front() {
                        Some(block) => block,
                        None if next < total => {
                            let index =
                                (next + u64::from(start) * u64::from(blocks_per_piece)) % total;
                            next += 1;
                            (
                                (index / u64::from(blocks_per_piece)) as u32,
                                (index % u64::from(blocks_per_piece)) as u32 * BLOCK,
                            )
                        }
                        None => break,
                    };
                    out_now.insert(block);
                    frame(
                        &mut out,
                        6,
                        &[
                            &block.0.to_be_bytes(),
                            &block.1.to_be_bytes(),
                            &BLOCK.to_be_bytes(),
                        ],
                    );
                }
            }
            if next == total && again.is_empty() && out_now.is_empty() {
                if duplicates > 0 {
                    eprintln!("bench leecher {tag}: {duplicates} blocks came twice");
                }
                return (taken, lost);
            }
            if !out.is_empty() && read.buffer().is_empty() {
                if let Err(error) = write.write_all(&out).await {
                    break error;
                }
                out.clear();
            }
        };
        lost += 1;
        eprintln!(
            "bench leecher {tag}: connection lost ({ended}) with {} MiB taken, {} out",
            taken >> 20,
            out_now.len()
        );
        again.extend(out_now.drain());
    }
}

// Idle torrents at scale: one session seeds WEAVER_BENCH_TORRENTS small
// torrents (counts separated by commas, default "1000,2000,4000"), with no
// peer, tracker or DHT, and at each count reports the process's resident
// memory, the runtime's live tasks and open descriptors, and what the idle
// session costs over WEAVER_BENCH_IDLE_SECS (default 5): CPU, system calls
// and context switches. The slope between counts is the per-torrent cost.
//
//   cargo nextest run --locked --cargo-profile matrix -p weaver-server-core \
//     --run-ignored only --no-capture -E 'test(=torrents::storage::tests::bench::torrent_idle_scale)'
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "a scale bench: run on request, optimised"]
async fn torrent_idle_scale() {
    let counts: Vec<usize> = std::env::var("WEAVER_BENCH_TORRENTS")
        .unwrap_or_else(|_| "1000,2000,4000".into())
        .split(',')
        .map(|count| count.trim().parse().unwrap())
        .collect();
    let idle = std::time::Duration::from_secs(env_u64("WEAVER_BENCH_IDLE_SECS", 5));
    let dir = match std::env::var_os("WEAVER_BENCH_DIR") {
        Some(base) => tempfile::tempdir_in(base).unwrap(),
        None => tempfile::tempdir().unwrap(),
    };
    let spawner = BlockingSpawner::new(4);
    let session = bench_session(&dir.path().join("session")).await;
    let runtime = tokio::runtime::Handle::current().metrics();
    let mut previous: Option<(usize, u64, usize)> = None;
    let mut added = 0;
    let mut handles = Vec::new();
    for count in counts {
        let adding = Instant::now();
        while added < count {
            let set = dir.path().join(format!("set-{added}"));
            std::fs::create_dir(&set).unwrap();
            let mut bytes = vec![0u8; 2 * BLOCK as usize];
            fill(splitmix(SEED ^ added as u64), &mut bytes);
            std::fs::write(set.join(format!("{added}.bin")), &bytes).unwrap();
            let created = create_torrent(
                &set,
                CreateTorrentOptions {
                    name: None,
                    trackers: Vec::new(),
                    piece_length: Some(BLOCK),
                    ssl_cert: None,
                },
                &spawner,
            )
            .await
            .unwrap();
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(created.as_bytes().unwrap()),
                    Some(AddTorrentOptions {
                        output_folder: Some(set.to_string_lossy().into_owned()),
                        overwrite: true,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap();
            handle.wait_until_completed().await.unwrap();
            handles.push(handle);
            added += 1;
        }
        let added_in = adding.elapsed();
        let resident = resident_bytes().unwrap_or(0);
        let tasks = runtime.num_alive_tasks();
        let fds = std::fs::read_dir("/dev/fd").map_or(0, |dir| dir.count());
        let from = usage();
        tokio::time::sleep(idle).await;
        let to = usage();
        let secs = to.at.duration_since(from.at).as_secs_f64();
        let per_sec = |a: u64, b: u64| (b - a) as f64 / secs;
        let slope = previous.map_or(String::new(), |(n, rss, t)| {
            let more = (count - n) as f64;
            format!(
                "; per added torrent {:.1} KiB, {:.2} tasks",
                (resident as f64 - rss as f64) / more / 1024.0,
                (tasks as f64 - t as f64) / more
            )
        });
        println!(
            "BENCH idle {count} torrents: added in {:.1} s; resident {} MiB; tasks {tasks}; fds {fds}; \
             idle cpu {:.1} ms/s, syscalls {}/s, csw vol {:.0}/s invol {:.0}/s{slope}",
            added_in.as_secs_f64(),
            resident >> 20,
            (per_sec(from.user_us, to.user_us) + per_sec(from.system_us, to.system_us)) / 1000.0,
            from.syscalls
                .zip(to.syscalls)
                .map_or("?".into(), |(a, b)| format!("{:.0}", per_sec(a, b))),
            per_sec(from.voluntary_switches, to.voluntary_switches),
            per_sec(from.involuntary_switches, to.involuntary_switches),
        );
        previous = Some((count, resident, tasks));
    }
    drop(handles);
    session.stop().await;
}

// This process's resident memory, from the task's own counters.
#[cfg(target_os = "macos")]
fn resident_bytes() -> Option<u64> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
    // SAFETY: `info` is a valid, writable proc_taskinfo of `size` bytes.
    let got = unsafe {
        libc::proc_pidinfo(
            std::process::id() as libc::c_int,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    (got == size).then_some(info.pti_resident_size)
}

#[cfg(not(target_os = "macos"))]
fn resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages * 4096)
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bytes::Bytes;
use tracing::info;

use crate::{
    AddTorrentOptions, ConnectionOptions, Session, SessionOptions, listen::ListenerOptions,
    tests::test_util::setup_test_logging,
};

// A torrent of a canary file `create_canary` writes, served by the other
// client.
const TORRENT_FILENAME: &str = "/tmp/canary.torrent";

// Where to download
const OUTPUT_FOLDER: &str = "/tmp/utptest";

// It's hard to find the binary in target/.../deps/librqbit*, so symlink itself
// here for easy profiling.
#[cfg(unix)]
const BINARY_SYMLINK: &str = "/tmp/rtest";

const DEFAULT_LISTEN_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 57318);

// Serve the above file from the other client.
const DEFAULT_OTHER_CLIENT_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 27312);

fn parse_listen_and_client_addr() -> (SocketAddr, SocketAddr) {
    let listen =
        std::env::var("E2E_LISTEN_ADDR").map_or(DEFAULT_LISTEN_ADDR, |v| v.parse().unwrap());
    let client =
        std::env::var("E2E_CLIENT_ADDR").map_or(DEFAULT_OTHER_CLIENT_ADDR, |v| v.parse().unwrap());
    (listen, client)
}

// A file of `mib` MiB whose bytes count 0 to 255 over and over, so a
// misplaced byte shows where it landed.
fn write_canary(path: &std::path::Path, mib: u64) -> std::io::Result<()> {
    use std::io::Write;
    let pattern: Vec<u8> = (0..=u8::MAX).collect();
    let mut file = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?,
    );
    for _ in 0..mib * 1024 * 1024 / pattern.len() as u64 {
        file.write_all(&pattern)?;
    }
    file.flush()
}

// Writes the canary: E2E_CANARY_PATH (default /tmp/canary_4096m) of
// E2E_CANARY_MIB MiB (default 4096).
#[test]
#[ignore]
fn create_canary() {
    let path = std::env::var("E2E_CANARY_PATH").unwrap_or_else(|_| "/tmp/canary_4096m".into());
    let mib = std::env::var("E2E_CANARY_MIB").map_or(4096, |v| v.parse().unwrap());
    write_canary(std::path::Path::new(&path), mib).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_utp_with_another_client() {
    let (listen_addr, client_addr) = parse_listen_and_client_addr();
    test_with_another_client(
        crate::SessionOptions {
            dht: None,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: crate::listen::ListenerMode::UtpOnly,
                listen_addr,
                ..Default::default()
            }),
            connect: Some(ConnectionOptions {
                enable_tcp: false,
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
        client_addr,
    )
    .await
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_tcp_with_another_client() {
    let (listen_addr, client_addr) = parse_listen_and_client_addr();
    test_with_another_client(
        crate::SessionOptions {
            dht: None,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: crate::listen::ListenerMode::TcpOnly,
                listen_addr,
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
        client_addr,
    )
    .await
}

// A test to download a canary file from another torrent client on 127.0.0.1:27312.
// Disabled, uncomment if developing / testing / benchmarking.
//
// The canary file is written by `create_canary`, then served from the other
// torrent client.
async fn test_with_another_client(sopts: SessionOptions, addr: SocketAddr) {
    #[cfg(unix)]
    {
        let test_filename = {
            let f = std::path::PathBuf::from(std::env::args().next().unwrap());
            f.read_link().unwrap_or(f)
        };
        if std::fs::exists(BINARY_SYMLINK).unwrap() {
            std::fs::remove_file(BINARY_SYMLINK).unwrap();
        }
        std::os::unix::fs::symlink(test_filename, BINARY_SYMLINK).unwrap();
    }

    setup_test_logging();

    let t = std::fs::read(TORRENT_FILENAME).unwrap();

    let session = Session::new_with_opts(OUTPUT_FOLDER.into(), sopts)
        .await
        .unwrap();

    let handle = session
        .add_torrent(
            crate::AddTorrent::TorrentFileBytes(Bytes::from(t)),
            Some(AddTorrentOptions {
                overwrite: true,
                initial_peers: Some(vec![addr]),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();

    // Print stats periodically.
    tokio::spawn({
        let handle = handle.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let stats = handle.stats();
                info!("{stats:}");
            }
        }
    });

    handle.wait_until_completed().await.unwrap();

    // wait for all final FIN-ACKs to be sent
    tokio::time::sleep(Duration::from_millis(100)).await;
}

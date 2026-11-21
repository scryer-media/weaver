// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

// TODO: this now stores only the routing table, but we also need AT LEAST the same socket address...

use crate::core::net::{DatagramSocket, Resolver};
use crate::core::spawn_utils::spawn;
use crate::network::SocketFactory;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use tracing::{debug_span, error, info, trace, warn};

use crate::dht::routing_table::RoutingTable;
use crate::dht::{Dht, DhtConfig, DhtState};

/// Configuration for DHT persistence (periodic dump of the routing tables). The
/// peers announced to us are not kept: each lives 45 minutes at most.
/// When provided, the DHT state will be serialized to disk periodically.
pub struct DhtPersistenceConfig {
    /// How often to dump state. Defaults to 60s.
    pub dump_interval: Option<Duration>,
    /// Path to the JSON file.
    pub config_filename: PathBuf,
}

/// Compute the DHT listen address from the explicit/stored port preferences and
/// the session-level `ipv4_only` flag.
///
/// The bind IP is derived solely from `ipv4_only` (`0.0.0.0` if true, `[::]`
/// otherwise); any persisted IP is intentionally ignored. The port is chosen
/// in priority order: explicit -> stored -> 0 (random).
pub fn dht_listen_addr(port: Option<u16>, stored_port: Option<u16>, ipv4_only: bool) -> SocketAddr {
    let ip: IpAddr = if ipv4_only {
        Ipv4Addr::UNSPECIFIED.into()
    } else {
        Ipv6Addr::UNSPECIFIED.into()
    };
    SocketAddr::new(ip, port.or(stored_port).unwrap_or(0))
}

#[derive(Serialize, Deserialize)]
struct DhtSerialize<Table> {
    addr: SocketAddr,
    table: Table,
    // option for backwards compat
    table_v6: Option<Table>,
}

pub struct PersistentDht {
    // config_filename: PathBuf,
}

// What has entered each table so far: a dump is due only once it changed.
// A node enters, or takes a bad one's place, and nothing else changes what
// is saved of a table.
fn entered(dht: &Dht) -> (u64, u64) {
    dht.with_routing_tables(|v4, v6| (v4.entries(), v6.entries()))
}

// Copies the tables on the runtime, a few hundred nodes at most, and
// serializes and writes them off it.
async fn dump_dht(dht: &Dht, filename: &Path, tempfile_name: &Path) -> anyhow::Result<()> {
    let addr = dht.listen_addr();
    let dump = dht.with_routing_tables(|v4, v6| DhtSerialize {
        addr,
        table: v4.clone(),
        table_v6: Some(v6.clone()),
    });
    let (filename, tempfile_name) = (filename.to_owned(), tempfile_name.to_owned());
    tokio::task::spawn_blocking(move || write_dump(&dump, &filename, &tempfile_name))
        .await
        .context("the DHT dump did not finish")?
}

fn write_dump(
    dump: &DhtSerialize<RoutingTable>,
    filename: &Path,
    tempfile_name: &Path,
) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(tempfile_name)
        .with_context(|| format!("error opening {tempfile_name:?}"))?;
    let mut file = BufWriter::new(file);
    serde_json::to_writer(&mut file, dump)
        .with_context(|| format!("error serializing DHT routing table to {tempfile_name:?}"))?;
    file.into_inner()
        .map_err(|e| e.into_error())
        .with_context(|| format!("error writing {tempfile_name:?}"))?;
    trace!("dumped DHT to {:?}", &tempfile_name);

    std::fs::rename(tempfile_name, filename)
        .with_context(|| format!("error renaming {tempfile_name:?} to {filename:?}"))
}

// The saved tables, if any: none when there is no file or it cannot be read
// as one.
fn read_dump(config_filename: &Path) -> anyhow::Result<Option<DhtSerialize<RoutingTable>>> {
    if let Some(parent) = config_filename.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("error creating dir {:?}", parent))?;
    }
    match OpenOptions::new().read(true).open(config_filename) {
        Ok(dht_json) => {
            let reader = BufReader::new(dht_json);
            match serde_json::from_reader::<_, DhtSerialize<RoutingTable>>(reader) {
                Ok(r) => {
                    info!(filename=?config_filename, "loaded DHT routing table from");
                    Ok(Some(r))
                }
                Err(e) => {
                    warn!(
                        filename=?config_filename,
                        "DHT: cannot deserialize routing table: {:#}",
                        e
                    );
                    Ok(None)
                }
            }
        }
        Err(e) => match e.kind() {
            std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(e).with_context(|| format!("error reading {config_filename:?}")),
        },
    }
}

impl PersistentDht {
    #[inline(never)]
    pub fn create<'a>(
        persistence_config: DhtPersistenceConfig,
        port: Option<u16>,
        ipv4_only: bool,
        bootstrap_addrs: Option<Vec<String>>,
        cancellation_token: Option<CancellationToken>,
        sockets: Arc<dyn SocketFactory>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        Self::create_from_config(
            persistence_config,
            port,
            ipv4_only,
            DhtConfig {
                bootstrap_addrs,
                cancellation_token,
                sockets: Some(sockets),
                ..Default::default()
            },
        )
    }

    /// Like [`PersistentDht::create`], but the DHT sends and receives
    /// through `socket`, so `port`, `ipv4_only` and any persisted port are
    /// ignored.
    ///
    /// Bootstrap hostnames are looked up through `resolver`; with none, only
    /// bootstrap addresses are asked.
    #[inline(never)]
    #[allow(clippy::too_many_arguments)]
    pub fn create_with_socket<'a>(
        persistence_config: DhtPersistenceConfig,
        port: Option<u16>,
        ipv4_only: bool,
        bootstrap_addrs: Option<Vec<String>>,
        cancellation_token: Option<CancellationToken>,
        socket: Arc<dyn DatagramSocket>,
        resolver: Option<Arc<dyn Resolver>>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        Self::create_from_config(
            persistence_config,
            port,
            ipv4_only,
            DhtConfig {
                bootstrap_addrs,
                cancellation_token,
                socket: Some(socket),
                resolver,
                ..Default::default()
            },
        )
    }

    /// Like [`PersistentDht::create_with_socket`], with the rest of the DHT's
    /// settings taken from `config`. The persisted state supplies `peer_id`, the
    /// routing tables and the listen address, so those fields of
    /// `config` are ignored; `port` and `ipv4_only` choose the listen address as
    /// in [`PersistentDht::create`].
    #[inline(never)]
    pub fn create_from_config<'a>(
        persistence_config: DhtPersistenceConfig,
        port: Option<u16>,
        ipv4_only: bool,
        config: DhtConfig,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        async move {
            let config_filename = persistence_config.config_filename;

            info!(
                filename=?config_filename,
                "will store DHT routing table periodically",
            );

            let de = {
                let config_filename = config_filename.clone();
                tokio::task::spawn_blocking(move || read_dump(&config_filename))
                    .await
                    .context("reading the saved DHT did not finish")??
            };
            let (stored_port, routing_table, routing_table_v6) = de
                .map(|de| (Some(de.addr.port()), Some(de.table), de.table_v6))
                .unwrap_or((None, None, None));

            let listen_addr = dht_listen_addr(port, stored_port, ipv4_only);
            let peer_id = routing_table.as_ref().map(|r| r.id());

            // The dump task is tracked with the DHT's worker, so whoever
            // waits for the DHT to stop also waits for its last dump.
            let tasks = config.tasks.clone();
            let dht_config = DhtConfig {
                peer_id,
                routing_table,
                routing_table_v6,
                listen_addr: Some(listen_addr),
                ..config
            };
            let dht = DhtState::with_config(dht_config).await?;
            // What was restored, or an empty table, is not written again.
            let mut dumped = entered(&dht);
            let dumper = {
                let dht = dht.clone();
                let cancelled = dht.cancellation_token().clone();
                let dump_interval = persistence_config
                    .dump_interval
                    .unwrap_or_else(|| Duration::from_secs(60));
                async move {
                    // Each DHT dumps through a temp file of its own, so a
                    // rename always moves in one whole dump, even while
                    // two DHTs share the directory.
                    static INSTANCES: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(0);
                    let instance = INSTANCES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let tempfile_name = {
                        let file_name = format!("dht.json.tmp.{}.{instance}", std::process::id());
                        let mut tmp = config_filename.clone();
                        tmp.set_file_name(file_name);
                        tmp
                    };

                    // At most one dump every `dump_interval`, only when a
                    // node entered since the last, and one more as the DHT
                    // stops, so a DHT stopped before its first interval
                    // still leaves its table behind.
                    loop {
                        let stopping = tokio::select! {
                            () = tokio::time::sleep(dump_interval) => false,
                            () = cancelled.cancelled() => true,
                        };
                        let now = entered(&dht);
                        if now != dumped {
                            match dump_dht(&dht, &config_filename, &tempfile_name).await {
                                Ok(()) => {
                                    trace!(filename=?config_filename, "dumped DHT");
                                    dumped = now;
                                }
                                Err(e) => {
                                    error!(filename=?config_filename, "error dumping DHT: {:#}", e)
                                }
                            }
                        }
                        if stopping {
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                }
            };
            spawn(
                debug_span!("dht_persistence"),
                "dht_persistence",
                tasks.unwrap_or_default().track_future(dumper),
            );

            Ok(dht)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_port_no_stored_v6() {
        let addr = dht_listen_addr(None, None, false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn no_port_no_stored_v4() {
        let addr = dht_listen_addr(None, None, true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn explicit_port_v6() {
        let addr = dht_listen_addr(Some(6881), None, false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn explicit_port_v4() {
        let addr = dht_listen_addr(Some(6881), None, true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn stored_port_only_v6() {
        let addr = dht_listen_addr(None, Some(12345), false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 12345);
    }

    #[test]
    fn stored_port_only_v4() {
        let addr = dht_listen_addr(None, Some(12345), true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 12345);
    }

    #[test]
    fn explicit_overrides_stored_v6() {
        let addr = dht_listen_addr(Some(6881), Some(12345), false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn explicit_overrides_stored_v4() {
        let addr = dht_listen_addr(Some(6881), Some(12345), true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    // A socket that drops what is sent and never receives.
    struct Sink;

    impl DatagramSocket for Sink {
        fn bind_addr(&self) -> SocketAddr {
            (Ipv4Addr::LOCALHOST, 6881).into()
        }
        fn recv_from<'a>(
            &'a self,
            _: &'a mut [u8],
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
            Box::pin(std::future::pending())
        }
        fn send_to<'a>(
            &'a self,
            buf: &'a [u8],
            _: SocketAddr,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<usize>> {
            Box::pin(std::future::ready(Ok(buf.len())))
        }
        fn poll_send_to(
            &self,
            _: &mut std::task::Context<'_>,
            buf: &[u8],
            _: SocketAddr,
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Ok(buf.len()))
        }
    }

    // Runs a persistent DHT on `file`, with no routers and an interval no
    // test reaches, through `run`, then stops it and waits until it has.
    async fn run_dht(file: &Path, run: impl FnOnce(&Dht)) {
        let token = CancellationToken::new();
        let tasks = tokio_util::task::TaskTracker::new();
        let dht = PersistentDht::create_from_config(
            DhtPersistenceConfig {
                dump_interval: Some(Duration::from_secs(24 * 3600)),
                config_filename: file.to_owned(),
            },
            None,
            false,
            DhtConfig {
                bootstrap_addrs: Some(Vec::new()),
                cancellation_token: Some(token.clone()),
                socket: Some(Arc::new(Sink)),
                tasks: Some(tasks.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        run(&dht);
        token.cancel();
        tasks.close();
        tasks.wait().await;
    }

    // A DHT that stops saves its tables as it stops, its first interval
    // not yet over, both families; the next run on the file starts from
    // them. One that stops with nothing new writes nothing.
    #[tokio::test]
    async fn test_the_tables_are_saved_as_the_dht_stops() {
        use crate::core::hash_id::Id20;
        use std::time::Instant;

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("dht.json");

        // Nothing entered: nothing written.
        run_dht(&file, |_| {}).await;
        assert!(!file.exists());

        let v4 = (
            Id20::new([0x81; 20]),
            SocketAddr::from(([5, 6, 7, 8], 6881)),
        );
        let v6 = (
            Id20::new([0x82; 20]),
            "[2001:db8::1]:6881".parse::<SocketAddr>().unwrap(),
        );
        let mut id = None;
        run_dht(&file, |dht| {
            id = Some(dht.stats().id);
            dht.with_routing_tables_mut(|t4, t6| {
                t4.add_node(v4.0, v4.1);
                t4.mark_response(&v4.0, Instant::now());
                t6.add_node(v6.0, v6.1);
                t6.mark_response(&v6.0, Instant::now());
            });
        })
        .await;
        assert!(file.exists());

        // Restored, both families; stopped unchanged, it writes nothing.
        run_dht(&file, |dht| {
            assert_eq!(Some(dht.stats().id), id);
            dht.with_routing_tables(|t4, t6| {
                assert_eq!(
                    t4.iter().map(|n| (n.id(), n.addr())).collect::<Vec<_>>(),
                    [v4]
                );
                assert_eq!(
                    t6.iter().map(|n| (n.id(), n.addr())).collect::<Vec<_>>(),
                    [v6]
                );
            });
            std::fs::remove_file(&file).unwrap();
        })
        .await;
        assert!(!file.exists());
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

// TODO: this now stores only the routing table, but we also need AT LEAST the same socket address...

use crate::core::net::{DatagramSocket, Resolver};
use crate::core::spawn_utils::spawn_with_cancel;
use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use librqbit_dualstack_sockets::BindDevice;
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

fn dump_dht(dht: &Dht, filename: &Path, tempfile_name: &Path) -> anyhow::Result<()> {
    let file = OpenOptions::new()
        .truncate(true)
        .create(true)
        .write(true)
        .open(tempfile_name)
        .with_context(|| format!("error opening {tempfile_name:?}"))?;
    let mut file = BufWriter::new(file);

    let addr = dht.listen_addr();
    match dht.with_routing_tables(|v4, v6| {
        serde_json::to_writer(
            &mut file,
            &DhtSerialize {
                addr,
                table: v4,
                table_v6: Some(v6),
            },
        )
    }) {
        Ok(_) => {
            trace!("dumped DHT to {:?}", &tempfile_name);
        }
        Err(e) => {
            return Err(e).with_context(|| {
                format!("error serializing DHT routing table to {tempfile_name:?}")
            });
        }
    }

    std::fs::rename(tempfile_name, filename)
        .with_context(|| format!("error renaming {tempfile_name:?} to {filename:?}"))
}

impl PersistentDht {
    #[inline(never)]
    pub fn create<'a>(
        persistence_config: DhtPersistenceConfig,
        port: Option<u16>,
        ipv4_only: bool,
        bootstrap_addrs: Option<Vec<String>>,
        cancellation_token: Option<CancellationToken>,
        bind_device: Option<&'a BindDevice>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        Self::create_with_socket(
            persistence_config,
            port,
            ipv4_only,
            bootstrap_addrs,
            cancellation_token,
            bind_device,
            None,
            None,
        )
    }

    /// Like [`PersistentDht::create`], but when `socket` is set the DHT sends
    /// and receives through it instead of binding a kernel UDP socket. In that
    /// case `port`, `ipv4_only`, `bind_device` and any persisted port are
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
        bind_device: Option<&'a BindDevice>,
        socket: Option<Arc<dyn DatagramSocket>>,
        resolver: Option<Arc<dyn Resolver>>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        Self::create_from_config(
            persistence_config,
            port,
            ipv4_only,
            DhtConfig {
                bootstrap_addrs,
                cancellation_token,
                bind_device,
                socket,
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
        config: DhtConfig<'a>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        async move {
            let config_filename = persistence_config.config_filename;

            info!(
                filename=?config_filename,
                "will store DHT routing table periodically",
            );

            if let Some(parent) = config_filename.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("error creating dir {:?}", parent))?;
            }

            let de = match OpenOptions::new().read(true).open(&config_filename) {
                Ok(dht_json) => {
                    let reader = BufReader::new(dht_json);
                    match serde_json::from_reader::<_, DhtSerialize<RoutingTable>>(reader) {
                        Ok(r) => {
                            info!(filename=?config_filename, "loaded DHT routing table from");
                            Some(r)
                        }
                        Err(e) => {
                            warn!(
                                filename=?config_filename,
                                "DHT: cannot deserialize routing table: {:#}",
                                e
                            );
                            None
                        }
                    }
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => None,
                    _ => {
                        return Err(e)
                            .with_context(|| format!("error reading {config_filename:?}"));
                    }
                },
            };
            let (stored_port, routing_table) = de
                .map(|de| (Some(de.addr.port()), Some(de.table)))
                .unwrap_or((None, None));

            let listen_addr = dht_listen_addr(port, stored_port, ipv4_only);
            let peer_id = routing_table.as_ref().map(|r| r.id());

            let dht_config = DhtConfig {
                peer_id,
                routing_table,
                routing_table_v6: None,
                listen_addr: Some(listen_addr),
                ..config
            };
            let dht = DhtState::with_config(dht_config).await?;
            spawn_with_cancel::<anyhow::Error>(
                debug_span!("dht_persistence"),
                "dht_persistence",
                dht.cancellation_token().clone(),
                {
                    let dht = dht.clone();
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
                            let file_name =
                                format!("dht.json.tmp.{}.{instance}", std::process::id());
                            let mut tmp = config_filename.clone();
                            tmp.set_file_name(file_name);
                            tmp
                        };

                        loop {
                            trace!("sleeping for {:?}", &dump_interval);
                            tokio::time::sleep(dump_interval).await;

                            match dump_dht(&dht, &config_filename, &tempfile_name) {
                                Ok(_) => trace!(filename=?config_filename, "dumped DHT"),
                                Err(e) => {
                                    error!(filename=?config_filename, "error dumping DHT: {:#}", e)
                                }
                            }
                        }
                    }
                },
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
}

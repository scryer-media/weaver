// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::options::SessionOptions;
use super::options::SessionPersistenceConfig;
use super::ports::AnnouncePort;
use super::ports::port_map_requests;
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use crate::core::net::{HttpClients, NoClients, NoNames, Resolver};
use crate::dht::{DhtBuilder, DhtConfig, Id20, PersistentDht, dht_listen_addr};
use crate::lsd::{LocalServiceDiscovery, LocalServiceDiscoveryOptions};
use crate::torrent_state::live::{RateTick, choker::Choker};
use crate::tracker::{HttpAnnounces, StoppedAnnounces, UdpTrackerClient};
use crate::{
    bitv_factory::{BitVFactory, NonPersistentBitVFactory},
    connection_slots::ConnectionSlots,
    ip_ranges::IpRanges,
    limits::Limits,
    session_persistence::{SessionPersistenceStore, json::JsonSessionPersistenceStore},
    session_stats::SessionStats,
    spawn_utils::BlockingSpawner,
    stream_connect::{StreamConnector, StreamConnectorArgs},
};
use anyhow::Context;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use parking_lot::RwLock;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, debug_span, error, info, warn};
use weaver_tunnel::egress::SocketEgress;

use super::external_ip::ExternalIps;

use super::{Session, dht_client_version, session_utp_opts, utp_shards};

impl Session {
    /// Create a new session with options.
    #[inline(never)]
    pub fn new_with_opts(
        default_output_folder: PathBuf,
        mut opts: SessionOptions,
    ) -> BoxFuture<'static, anyhow::Result<Arc<Self>>> {
        async move {
            let identity = opts.identity.take().unwrap_or_default();
            let token = opts.cancellation_token.take().unwrap_or_default();
            let tasks = TaskTracker::new();
            let swarm = token.child_token();
            let swarm_tasks = TaskTracker::new();
            // Every name the session looks up, on whichever socket.
            let resolver: Arc<dyn Resolver> =
                opts.resolver.take().unwrap_or_else(|| Arc::new(NoNames));

            let lsd_egress = match opts.bind_device_name.as_ref() {
                Some(name) => SocketEgress::existing_interface(name)
                    .with_context(|| format!("error creating bind device {name}"))?,
                None => SocketEgress::System,
            };

            // The sockets the DHT and the UDP trackers bind.
            let datagram_sockets = Arc::clone(&opts.socket_factory);

            let connection_slots = match &opts.shared_limits {
                Some(shared) => Arc::clone(&shared.slots),
                None => Arc::new(ConnectionSlots::new(
                    opts.connections_limit,
                    opts.connections_slack,
                    opts.connection_speed,
                )),
            };
            // One uTP thread for the session's uTP sockets, started with the
            // first: each socket's I/O and its streams run there, as
            // libtorrent runs uTP on its one network thread. A
            // single-threaded runtime keeps them on itself.
            let utp_shards = utp_shards();
            // The port the peer listener was asked for: 0 when the factory
            // chooses it.
            let asked_listen_port = opts.listen.as_ref().map(|l| l.listen_addr.port());
            let mut listen_result = if let Some(mut listen_opts) = opts.listen.take() {
                // uTP drops a SYN once its streams pass twice the limit.
                let connections_limit = connection_slots.limit();
                session_utp_opts(
                    listen_opts.utp_opts.get_or_insert_with(Default::default),
                    connections_limit,
                    utp_shards.as_ref(),
                );
                Some(
                    listen_opts
                        .start(
                            opts.root_span.as_ref().and_then(|s| s.id()),
                            swarm.child_token(),
                            &opts.socket_factory,
                            &connection_slots,
                        )
                        .await
                        .context(crate::listen::ListenFailed)?,
                )
            } else {
                None
            };

            let enable_utp = opts.connect.as_ref().and_then(|connect| connect.enable_utp);
            let utp_on_demand = opts
                .connect
                .as_ref()
                .is_some_and(|connect| connect.utp_on_demand);
            let enable_tcp = opts
                .connect
                .as_ref()
                .is_none_or(|connect| connect.enable_tcp);
            let listening_utp = listen_result
                .as_ref()
                .and_then(|listen| listen.utp_socket.clone());
            let mut utp_rebind = None;
            let new_outgoing_utp = if enable_utp == Some(true) && listening_utp.is_none() {
                let mut utp_opts = weaver_utp::SocketOpts {
                    cancellation_token: swarm.child_token(),
                    parent_span: opts.root_span.as_ref().and_then(|span| span.id()),
                    dont_wait_for_lastack: true,
                    ..Default::default()
                };
                session_utp_opts(&mut utp_opts, connection_slots.limit(), utp_shards.as_ref());
                let address = if opts.ipv4_only {
                    SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
                } else {
                    SocketAddr::from((std::net::Ipv6Addr::UNSPECIFIED, 0))
                };
                let receive_buffer =
                    crate::udp_options::receive_buffer_for(connection_slots.limit());
                let rebind = crate::stream_connect::UtpRebind {
                    address,
                    opts: utp_opts.clone(),
                    receive_buffer,
                    connection_slots: Arc::clone(&connection_slots),
                };
                if utp_on_demand {
                    // Bound by the first dial whose torrent dials over uTP,
                    // and never when none does.
                    utp_rebind = Some(rebind);
                    None
                } else {
                    match crate::listen::bind_utp(
                        opts.socket_factory.as_ref(),
                        address,
                        utp_opts.clone(),
                        receive_buffer,
                        Some(&connection_slots),
                    )
                    .await
                    .context("error binding outgoing uTP socket through socket factory")
                    {
                        Ok(socket) => Some(socket),
                        // As with the listener, TCP still connects without uTP,
                        // and the next dial that wants uTP binds it again.
                        Err(e) if enable_tcp => {
                            error!("{e:#}");
                            utp_rebind = Some(rebind);
                            None
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                None
            };
            let outgoing_utp = if enable_utp == Some(false) {
                None
            } else {
                listening_utp.or_else(|| new_outgoing_utp.clone())
            };
            let utp_sockets = listen_result
                .iter()
                .flat_map(|listen| {
                    listen
                        .utp_socket
                        .iter()
                        .chain(listen.ssl.iter().filter_map(|ssl| ssl.utp_socket.as_ref()))
                })
                .chain(new_outgoing_utp.iter())
                .cloned()
                .collect();

            // The peer listener's uTP socket, which the DHT shares when it
            // runs on the same port.
            let dht_share = listen_result
                .as_mut()
                .and_then(|listen| listen.dht_share.take());
            let http_clients: Arc<dyn HttpClients> = opts
                .http_client
                .take()
                .unwrap_or_else(|| Arc::new(NoClients::over(Arc::clone(&opts.socket_factory))));

            let mut dht_on_listener = false;
            let dht = if let Some(dht_config) = opts.dht.take() {
                // The IPv6 node waits for an IPv6 address of the route that
                // gives a node (see `Session::follow_ipv6_route`); on a
                // proxied route it runs while the proxy carries IPv6.
                let (ipv6_waits, ipv6_proving) = if opts.ipv4_only {
                    (true, false)
                } else {
                    match crate::route_interface::ipv6_dht(http_clients.as_ref()) {
                        crate::route_interface::Ipv6Dht::Runs => (false, false),
                        crate::route_interface::Ipv6Dht::Proxied => (false, true),
                        crate::route_interface::Ipv6Dht::None => (true, false),
                    }
                };
                // The DHT on the port uTP listens on, or asked for the port
                // the listener was asked for, shares uTP's socket, as
                // libtorrent runs uTP and its DHT on one socket per listen
                // port (session_impl.cpp:2648-2663): a second socket on the
                // port fails to bind, or takes uTP's datagrams. Elsewhere it
                // runs over a datagram socket the factory binds; the factory
                // decides the real address.
                let dht_share = dht_share.filter(|share| {
                    dht_config
                        .port
                        .is_some_and(|port| port == share.port() || Some(port) == asked_listen_port)
                });
                dht_on_listener = dht_share.is_some();
                let dht_socket = Some(match dht_share {
                    Some(share) => share.dht_socket(),
                    None => datagram_sockets
                        .bind_udp(dht_listen_addr(dht_config.port, None, opts.ipv4_only))
                        .await
                        .context("error binding DHT socket through socket factory")?,
                });
                let dht = if let Some(persistence_config) = dht_config.persistence {
                    PersistentDht::create_from_config(
                        persistence_config,
                        dht_config.port,
                        opts.ipv4_only,
                        DhtConfig {
                            bootstrap_addrs: dht_config.bootstrap_addrs,
                            cancellation_token: Some(swarm.clone()),
                            socket: dht_socket,
                            sockets: Some(datagram_sockets.clone()),
                            resolver: Some(resolver.clone()),
                            client_version: Some(dht_client_version()),
                            tasks: Some(swarm_tasks.clone()),
                            ipv6_waits,
                            ipv6_proving,
                            ..Default::default()
                        },
                    )
                    .await
                    .context("error initializing persistent DHT")?
                } else {
                    let listen_addr = dht_listen_addr(dht_config.port, None, opts.ipv4_only);
                    DhtBuilder::with_config(DhtConfig {
                        bootstrap_addrs: dht_config.bootstrap_addrs,
                        cancellation_token: Some(swarm.child_token()),
                        listen_addr: Some(listen_addr),
                        socket: dht_socket,
                        sockets: Some(datagram_sockets.clone()),
                        resolver: Some(resolver.clone()),
                        client_version: Some(dht_client_version()),
                        tasks: Some(swarm_tasks.clone()),
                        ipv6_waits,
                        ipv6_proving,
                        ..Default::default()
                    })
                    .await
                    .context("error initializing DHT")?
                };

                Some(dht)
            } else {
                None
            };
            let peer_opts = opts
                .connect
                .as_ref()
                .and_then(|p| p.peer_opts)
                .unwrap_or_default();

            async fn persistence_factory(
                opts: &SessionOptions,
                spawner: BlockingSpawner,
            ) -> anyhow::Result<(
                Option<Arc<dyn SessionPersistenceStore>>,
                Arc<dyn BitVFactory>,
            )> {
                macro_rules! make_result {
                    ($store:expr) => {
                        if opts.fastresume {
                            Ok((Some($store.clone()), $store))
                        } else {
                            Ok((Some($store), Arc::new(NonPersistentBitVFactory {})))
                        }
                    };
                }

                match &opts.persistence {
                    Some(SessionPersistenceConfig::Json { folder }) => {
                        let s = Arc::new(
                            JsonSessionPersistenceStore::new(folder.clone(), spawner)
                                .await
                                .context("error initializing JsonSessionPersistenceStore")?,
                        );

                        make_result!(s)
                    }
                    None => Ok((None, Arc::new(NonPersistentBitVFactory {}))),
                }
            }

            const DEFAULT_BLOCKING_THREADS_IF_NOT_SET: usize = 8;
            let spawner = BlockingSpawner::new(
                opts.runtime_worker_threads
                    .unwrap_or(DEFAULT_BLOCKING_THREADS_IF_NOT_SET),
            );

            let (persistence, bitv_factory) = persistence_factory(&opts, spawner.clone())
                .await
                .context("error initializing session persistence store")?;

            let stream_connector = Arc::new(
                StreamConnector::new(StreamConnectorArgs {
                    enable_tcp,
                    utp_socket: outgoing_utp,
                    utp_rebind,
                    ipv4_only: opts.ipv4_only,
                    socket_factory: opts.socket_factory.clone(),
                    encryption: opts.encryption,
                })
                .await
                .context("error creating stream connector")?,
            );
            stream_connector.set_dial_orders(opts.dial_orders);

            let allowlist = if let Some(allowlist_url) = opts.allowlist_url {
                info!(url = allowlist_url, "loading p2p allowlist");
                let al = IpRanges::load_from_url(&*http_clients, &allowlist_url)
                    .await
                    .with_context(|| format!("error reading allowlist from {allowlist_url}"))?;
                info!(len = al.len(), "loaded allowlist");
                Some(al)
            } else {
                None
            };

            let udp_tracker_client = {
                let addr: SocketAddr = if opts.ipv4_only {
                    (Ipv4Addr::UNSPECIFIED, 0).into()
                } else {
                    (Ipv6Addr::UNSPECIFIED, 0).into()
                };
                let socket = datagram_sockets
                    .bind_udp(addr)
                    .await
                    .context("error binding the UDP tracker socket")?;
                UdpTrackerClient::new_with_socket(token.clone(), socket, Some(resolver.clone()))
            };

            // Local discovery runs unless disabled, from the start, whether
            // or not its groups can be joined yet: one that cannot is
            // joined by the first rejoin that can, and its torrents are
            // announced from then on.
            let lsd = (!opts.disable_local_service_discovery).then(|| {
                let lsd = LocalServiceDiscovery::new(LocalServiceDiscoveryOptions {
                    cancel_token: swarm.clone(),
                    egress: lsd_egress,
                    tasks: Some(swarm_tasks.clone()),
                    #[cfg(test)]
                    join: opts.lsd_join.take().or_else(|| {
                        opts.lsd_socket.take().map(|socket| {
                            crate::lsd::scripted_join(Arc::new(std::sync::Mutex::new(
                                [Ok(socket)].into(),
                            )))
                        })
                    }),
                    ..Default::default()
                });
                if let Err(e) = lsd.join() {
                    warn!("error starting local service discovery: {e:#}");
                }
                lsd
            });

            let started = tokio::time::Instant::now();
            let external_ips = Arc::new(ExternalIps::new(started));
            let session = Arc::new(Self {
                persistence,
                bitv_factory,
                identity,
                dht,
                peer_opts,
                spawner: spawner.clone(),
                output_folder: default_output_folder,
                next_id: AtomicUsize::new(0),
                db: RwLock::new(Default::default()),
                _cancellation_token_drop_guard: token.clone().drop_guard(),
                cancellation_token: token,
                tasks,
                swarm,
                swarm_tasks,
                announce_port: Arc::new(AnnouncePort::new(
                    listen_result
                        .as_ref()
                        .and_then(|l| l.announce_port.and(Some(l.addr.port()))),
                    listen_result
                        .as_ref()
                        .and_then(|l| l.announce_port.filter(|_| l.announce_port_set)),
                )),
                listen_addr: listen_result.as_ref().map(|l| l.addr),
                dht_on_listener,
                ssl_port: Arc::new(AnnouncePort::new(
                    listen_result
                        .as_ref()
                        .and_then(|l| l.ssl.as_ref().zip(l.announce_port))
                        .map(|(ssl, _)| ssl.addr.port()),
                    None,
                )),
                ssl_listen_addr: listen_result
                    .as_ref()
                    .and_then(|l| l.ssl.as_ref())
                    .map(|ssl| ssl.addr),
                external_ips,
                started,
                utp_sockets,
                utp: listen_result
                    .as_ref()
                    .is_some_and(|l| l.utp_socket.is_some()),
                #[cfg(test)]
                utp_threads: utp_shards.as_ref().map(|shards| shards.name().to_owned()),
                default_storage_factory: opts.default_storage_factory,
                http_clients,
                resolver,
                connector: stream_connector,
                root_span: opts.root_span,
                stats: Arc::new(SessionStats::new()),
                concurrent_initialize_semaphore: match &opts.shared_limits {
                    Some(shared) => Arc::clone(&shared.checks),
                    None => Arc::new(tokio::sync::Semaphore::new(
                        opts.concurrent_init_limit.unwrap_or(1).max(1),
                    )),
                },
                udp_tracker_client,
                connection_slots,
                ratelimits: Limits::new(opts.ratelimits),
                ipv4_only: opts.ipv4_only,
                trackers: opts.trackers,
                disable_trackers: opts.disable_trackers,
                trackers_changed: opts.trackers_changed.clone(),
                http_announces: match &opts.shared_limits {
                    Some(shared) => shared.http_announces.clone(),
                    None => HttpAnnounces::default(),
                },
                stopped_announces: StoppedAnnounces::new(
                    opts.tracker_stopped_timeout
                        .unwrap_or(StoppedAnnounces::DEFAULT_TIMEOUT),
                ),
                peer_limit: opts.peer_limit,
                choker: Arc::new(Choker::new(opts.unchoke_slots_limit)),
                rate_tick: Arc::new(RateTick::default()),
                disable_pex: opts.disable_pex,
                allow_multiple_connections_per_ip: Arc::new(AtomicBool::new(
                    opts.allow_multiple_connections_per_ip,
                )),
                allow_idna: opts.allow_idna,
                bad_data_bans: Default::default(),
                ip_filter: RwLock::new(opts.ip_filter),
                allowlist,
                lsd,
                #[cfg(test)]
                handed_out: Default::default(),
            });

            if let Some(mut listen) = listen_result {
                let requests = (listen.announce_port.is_some()
                    && listen.enable_upnp_port_forwarding)
                    .then(|| port_map_requests(&listen));
                let max = listen.max_pending_incoming_handshake_checks;
                let forwarded = listen
                    .tcp_socket
                    .as_ref()
                    .and_then(|listener| listener.port_mappings());
                let plain = crate::listen::Listening {
                    tcp_socket: listen.tcp_socket.take(),
                    utp_socket: listen.utp_socket.take(),
                    addr: listen.addr,
                };
                session.spawn_listeners(plain, false, max);
                if let Some(ssl) = listen.ssl.take() {
                    session.spawn_listeners(ssl, true, max);
                }
                let port_map_events = opts.port_map_events.take();
                if let Some(requests) = requests {
                    session.start_port_mapping(
                        requests,
                        opts.bind_device_name.as_deref(),
                        port_map_events.clone(),
                    );
                }
                // A forwarded port reaches the plain listener only: the SSL
                // one gets no mapping through it.
                if let Some(forwarded) = forwarded {
                    session.follow_port_mappings(
                        forwarded,
                        super::ports::Reports::Announce { ssl: None },
                        port_map_events,
                    );
                }
            }

            if let Some(persistence) = session.persistence.as_ref() {
                info!("will use {persistence:?} for session persistence");

                let mut ps = persistence.stream_all().await?;
                let mut added_all = false;
                let mut futs = FuturesUnordered::new();

                while !added_all || !futs.is_empty() {
                    // NOTE: this closure exists purely to workaround rustfmt screwing up when inlining it.
                    let add_torrent_span = |info_hash: &Id20| -> tracing::Span {
                        debug_span!(parent: session.rs(), "add_torrent", info_hash=?info_hash)
                    };
                    tokio::select! {
                        Some(res) = futs.next(), if !futs.is_empty() => {
                            if let Err(e) = res {
                                error!("error adding torrent to session: {e:#}");
                            }
                        }
                        st = ps.next(), if !added_all => {
                            match st {
                                Some(st) => {
                                    let (id, st) = st?;
                                    let span = add_torrent_span(st.info_hash());
                                    let (add_torrent, mut opts) = st.into_add_torrent()?;
                                    opts.preferred_id = Some(id);
                                    let fut = session.add_torrent(add_torrent, Some(opts));
                                    let fut = fut.instrument(span);
                                    futs.push(fut);
                                },
                                None => added_all = true
                            };
                        }
                    };
                }
            }

            session.start_speed_estimator_updater();
            session.start_peer_turnover();
            session.start_choker();
            session.spawn(
                debug_span!(parent: session.rs(), "rate_tick"),
                "rate_tick",
                {
                    let tick = session.rate_tick.clone();
                    async move {
                        tick.run().await;
                        Ok(())
                    }
                },
            );

            Ok(session)
        }
        .boxed()
    }
}

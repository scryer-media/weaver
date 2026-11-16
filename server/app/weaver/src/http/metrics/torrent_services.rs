// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What each open torrent session asked of the services it finds peers
// through, what became of the connections peers opened to it, and what
// held it back.
//
// Every figure is a count the session keeps and the torrent service read
// when it last published: rendering reads the published copy, and does the
// same work however many torrents and trackers a session has. No label
// names a tracker, a peer or a torrent.

use weaver_server_core::torrents::metrics::dht::{DhtIgnored, DhtQuery, DhtQueryEnd};
use weaver_server_core::torrents::metrics::incoming::IncomingEnd;
use weaver_server_core::torrents::metrics::network::{HolepunchEvent, PortMapResult, PortMapper};
use weaver_server_core::torrents::metrics::swarm::Transport;
use weaver_server_core::torrents::metrics::tracker::{AnnounceEvent, TrackerKind, TrackerResult};
use weaver_server_core::torrents::metrics::web_seed::WebSeedResult;
use weaver_server_core::torrents::metrics::{CheckSeries, SessionSeries};

use super::catalog as f;
use super::encode::Encoder;

fn seconds(nanos: u64) -> f64 {
    nanos as f64 / 1e9
}

pub(super) fn render(out: &mut Encoder, routes: &[SessionSeries]) {
    let open = || routes.iter().filter(|session| session.open);
    for session in open() {
        for kind in TrackerKind::ALL {
            for event in AnnounceEvent::ALL {
                for result in TrackerResult::ALL {
                    out.sample(
                        &f::TORRENT_TRACKER_ANNOUNCES,
                        &[
                            ("route", session.route.as_str()),
                            ("kind", kind.as_str()),
                            ("event", event.as_str()),
                            ("result", result.as_str()),
                        ],
                        session.trackers.announces[*kind as usize][*event as usize]
                            [*result as usize],
                    );
                }
            }
        }
    }
    for session in open() {
        for kind in TrackerKind::ALL {
            out.sample_f64(
                &f::TORRENT_TRACKER_ANNOUNCE_SECONDS,
                &[("route", session.route.as_str()), ("kind", kind.as_str())],
                seconds(session.trackers.announce_nanos[*kind as usize]),
            );
        }
    }
    for session in open() {
        for kind in TrackerKind::ALL {
            out.sample(
                &f::TORRENT_TRACKER_PEERS,
                &[("route", session.route.as_str()), ("kind", kind.as_str())],
                session.trackers.peers[*kind as usize],
            );
        }
    }
    dht(out, routes);
    incoming(out, routes);
    web_seeds(out, routes);
    network(out, routes);
}

/// The piece checks of the whole process, once and under no route.
pub(super) fn render_checks(out: &mut Encoder, check: &CheckSeries) {
    out.sample(&f::TORRENT_CHECK_READ_CALLS, &[], check.read_calls);
    out.sample(&f::TORRENT_CHECK_READ_BYTES, &[], check.read_bytes);
    out.sample_f64(
        &f::TORRENT_CHECK_READ_SECONDS,
        &[],
        seconds(check.read_nanos),
    );
    out.sample(&f::TORRENT_CHECK_READ_SEEKS, &[], check.read_seeks);
    out.sample(&f::TORRENT_CHECK_PIECES_HASHED, &[], check.pieces_hashed);
    out.sample_f64(
        &f::TORRENT_CHECK_HASH_SECONDS,
        &[],
        seconds(check.hash_nanos),
    );
    out.sample(
        &f::TORRENT_SEED_CHECKS_STARTED,
        &[],
        check.seed_checks_started,
    );
    out.sample(
        &f::TORRENT_SEED_CHECKS_JOINED,
        &[],
        check.seed_checks_joined,
    );
}

fn network(out: &mut Encoder, routes: &[SessionSeries]) {
    let open = || routes.iter().filter(|session| session.open);
    for session in open() {
        for mapper in PortMapper::ALL {
            for result in PortMapResult::ALL {
                out.sample(
                    &f::TORRENT_PORT_MAPPING_REPORTS,
                    &[
                        ("route", session.route.as_str()),
                        ("mapper", mapper.as_str()),
                        ("result", result.as_str()),
                    ],
                    session.network.port_maps[*mapper as usize][*result as usize],
                );
            }
        }
    }
    for session in open() {
        for event in HolepunchEvent::ALL {
            out.sample(
                &f::TORRENT_HOLEPUNCH_EVENTS,
                &[("route", session.route.as_str()), ("event", event.as_str())],
                session.network.holepunch[*event as usize],
            );
        }
    }
    let lsd = || open().filter_map(|session| Some((session.route.as_str(), session.lsd?)));
    for (route, lsd) in lsd() {
        out.sample(&f::TORRENT_LSD_SENT, &[("route", route)], lsd.sent);
    }
    for (route, lsd) in lsd() {
        out.sample(&f::TORRENT_LSD_RECEIVED, &[("route", route)], lsd.received);
    }
    for (route, lsd) in lsd() {
        out.sample(
            &f::TORRENT_LSD_PEERS_FOUND,
            &[("route", route)],
            lsd.peers_found,
        );
    }
    const DIRECTIONS: [&str; 2] = ["down", "up"];
    for session in open() {
        for (direction, waits) in DIRECTIONS.into_iter().zip(session.limiter.waits) {
            out.sample(
                &f::TORRENT_RATE_LIMIT_WAITS,
                &[("route", session.route.as_str()), ("direction", direction)],
                waits,
            );
        }
    }
    for session in open() {
        for (direction, nanos) in DIRECTIONS.into_iter().zip(session.limiter.wait_nanos) {
            out.sample_f64(
                &f::TORRENT_RATE_LIMIT_WAIT_SECONDS,
                &[("route", session.route.as_str()), ("direction", direction)],
                seconds(nanos),
            );
        }
    }
}

fn web_seeds(out: &mut Encoder, routes: &[SessionSeries]) {
    let open = || routes.iter().filter(|session| session.open);
    for session in open() {
        for result in WebSeedResult::ALL {
            out.sample(
                &f::TORRENT_WEB_SEED_REQUESTS,
                &[
                    ("route", session.route.as_str()),
                    ("result", result.as_str()),
                ],
                session.web_seeds.requests[*result as usize],
            );
        }
    }
    let per_route = |out: &mut Encoder, family, value: fn(&SessionSeries) -> u64| {
        for session in open() {
            out.sample(family, &[("route", session.route.as_str())], value(session));
        }
    };
    per_route(out, &f::TORRENT_WEB_SEED_BYTES, |s| s.web_seeds.bytes);
    per_route(out, &f::TORRENT_WEB_SEED_WAITS, |s| s.web_seeds.waits);
    per_route(out, &f::TORRENT_WEB_SEED_FAILED, |s| s.web_seeds.failed);
}

fn incoming(out: &mut Encoder, routes: &[SessionSeries]) {
    let open = || routes.iter().filter(|session| session.open);
    for session in open() {
        for transport in Transport::ALL {
            for end in IncomingEnd::ALL {
                out.sample(
                    &f::TORRENT_INCOMING_CONNECTIONS,
                    &[
                        ("route", session.route.as_str()),
                        ("transport", transport.as_str()),
                        ("result", end.as_str()),
                    ],
                    session.incoming.ends[*transport as usize][*end as usize],
                );
            }
        }
    }
    for session in open() {
        out.sample(
            &f::TORRENT_UNCHOKE_SLOTS_HELD,
            &[("route", session.route.as_str())],
            session.unchoke_slots_held,
        );
    }
}

fn dht(out: &mut Encoder, routes: &[SessionSeries]) {
    let running = || {
        routes
            .iter()
            .filter(|session| session.open && session.dht.is_some())
    };
    for session in running() {
        for query in DhtQuery::ALL {
            out.sample(
                &f::TORRENT_DHT_QUERIES_SENT,
                &[("route", session.route.as_str()), ("type", query.as_str())],
                session.dht_counts.sent[*query as usize],
            );
        }
    }
    for session in running() {
        for query in DhtQuery::ALL {
            for end in DhtQueryEnd::ALL {
                out.sample(
                    &f::TORRENT_DHT_QUERY_RESULTS,
                    &[
                        ("route", session.route.as_str()),
                        ("type", query.as_str()),
                        ("result", end.as_str()),
                    ],
                    session.dht_counts.ended[*query as usize][*end as usize],
                );
            }
        }
    }
    for session in running() {
        for query in DhtQuery::ALL {
            out.sample(
                &f::TORRENT_DHT_QUERIES_RECEIVED,
                &[("route", session.route.as_str()), ("type", query.as_str())],
                session.dht_counts.received[*query as usize],
            );
        }
    }
    for session in running() {
        for why in DhtIgnored::ALL {
            out.sample(
                &f::TORRENT_DHT_IGNORED,
                &[("route", session.route.as_str()), ("reason", why.as_str())],
                session.dht_counts.ignored[*why as usize],
            );
        }
    }
    for session in running() {
        out.sample(
            &f::TORRENT_DHT_PEERS_FOUND,
            &[("route", session.route.as_str())],
            session.dht_counts.peers_found,
        );
    }
}

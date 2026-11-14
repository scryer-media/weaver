// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What each open torrent session asked of the services it finds peers
// through, and what became of the connections peers opened to it.
//
// Every figure is a count the session keeps and the torrent service read
// when it last published: rendering reads the published copy, and does the
// same work however many torrents and trackers a session has. No label
// names a tracker, a peer or a torrent.

use weaver_server_core::torrents::metrics::SessionSeries;
use weaver_server_core::torrents::metrics::dht::{DhtIgnored, DhtQuery, DhtQueryEnd};
use weaver_server_core::torrents::metrics::incoming::IncomingEnd;
use weaver_server_core::torrents::metrics::swarm::Transport;
use weaver_server_core::torrents::metrics::tracker::{AnnounceEvent, TrackerKind, TrackerResult};

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
    for session in open() {
        for kind in TrackerKind::ALL {
            for result in TrackerResult::ALL {
                out.sample(
                    &f::TORRENT_TRACKER_SCRAPES,
                    &[
                        ("route", session.route.as_str()),
                        ("kind", kind.as_str()),
                        ("result", result.as_str()),
                    ],
                    session.trackers.scrapes[*kind as usize][*result as usize],
                );
            }
        }
    }
    for session in open() {
        for kind in TrackerKind::ALL {
            out.sample_f64(
                &f::TORRENT_TRACKER_SCRAPE_SECONDS,
                &[("route", session.route.as_str()), ("kind", kind.as_str())],
                seconds(session.trackers.scrape_nanos[*kind as usize]),
            );
        }
    }
    dht(out, routes);
    incoming(out, routes);
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

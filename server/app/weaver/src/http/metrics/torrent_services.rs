// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What each open torrent session asked of the services it finds peers
// through.
//
// Every figure is a count the session keeps and the torrent service read
// when it last published: rendering reads the published copy, and does the
// same work however many torrents and trackers a session has. No label
// names a tracker, a peer or a torrent.

use weaver_server_core::torrents::metrics::SessionSeries;
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
}

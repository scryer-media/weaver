// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What the peer connections of each open torrent session did.
//
// Every figure is a count the session keeps and the torrent service read
// when it last published: rendering reads the published copy, and does the
// same work however many peers and torrents a session has. Every label
// value is one of a fixed few, taken from the engine's own enums, so a kind
// added there is rendered here.

use weaver_server_core::torrents::metrics::SessionSeries;
use weaver_server_core::torrents::metrics::swarm::{
    BlockArrival, ByteKind, ClosePhase, CloseReason, Direction, Encryption, PeerSource, PieceCheck,
    PiecePick, RequestEvent, Transport, Waste,
};

use super::catalog as f;
use super::encode::{Encoder, MetricFamily};

pub(super) fn render(out: &mut Encoder, routes: &[SessionSeries]) {
    let open = || routes.iter().filter(|session| session.open);
    for session in open() {
        for (direction, kind, count) in session.swarm.messages() {
            out.sample(
                &f::TORRENT_PEER_MESSAGES,
                &[
                    ("route", session.route.as_str()),
                    ("direction", direction.as_str()),
                    ("type", kind.as_str()),
                ],
                count,
            );
        }
    }
    for session in open() {
        for transport in Transport::ALL {
            for direction in Direction::ALL {
                for encryption in Encryption::ALL {
                    out.sample(
                        &f::TORRENT_PEER_HANDSHAKES,
                        &[
                            ("route", session.route.as_str()),
                            ("transport", transport.as_str()),
                            ("direction", direction.as_str()),
                            ("encryption", encryption.as_str()),
                        ],
                        session.swarm.handshakes[*transport as usize][*direction as usize]
                            [*encryption as usize],
                    );
                }
            }
        }
    }
    for session in open() {
        for phase in ClosePhase::ALL {
            for reason in CloseReason::ALL {
                out.sample(
                    &f::TORRENT_PEER_DISCONNECTS,
                    &[
                        ("route", session.route.as_str()),
                        ("phase", phase.as_str()),
                        ("reason", reason.as_str()),
                    ],
                    session.swarm.closes[*phase as usize][*reason as usize],
                );
            }
        }
    }
    for session in open() {
        for transport in Transport::ALL {
            for direction in Direction::ALL {
                for kind in ByteKind::ALL {
                    out.sample(
                        &f::TORRENT_PEER_BYTES,
                        &[
                            ("route", session.route.as_str()),
                            ("transport", transport.as_str()),
                            ("direction", direction.as_str()),
                            ("kind", kind.as_str()),
                        ],
                        session.swarm.bytes[*transport as usize][*direction as usize]
                            [*kind as usize],
                    );
                }
            }
        }
    }
    let mut by_kind =
        |family: &'static MetricFamily,
         label: &'static str,
         counts: &dyn Fn(&SessionSeries) -> Vec<(&'static str, u64)>| {
            for session in open() {
                for (value, count) in counts(session) {
                    out.sample(
                        family,
                        &[("route", session.route.as_str()), (label, value)],
                        count,
                    );
                }
            }
        };
    by_kind(&f::TORRENT_PEER_REQUEST_EVENTS, "event", &|session| {
        let counts = &session.swarm.request_events;
        RequestEvent::ALL
            .iter()
            .map(|event| (event.as_str(), counts[*event as usize]))
            .collect()
    });
    by_kind(&f::TORRENT_PEER_BLOCKS_RECEIVED, "request", &|session| {
        let counts = &session.swarm.blocks;
        BlockArrival::ALL
            .iter()
            .map(|arrival| (arrival.as_str(), counts[*arrival as usize]))
            .collect()
    });
    by_kind(&f::TORRENT_PIECE_CHECKS, "result", &|session| {
        let counts = &session.swarm.piece_checks;
        PieceCheck::ALL
            .iter()
            .map(|result| (result.as_str(), counts[*result as usize]))
            .collect()
    });
    by_kind(&f::TORRENT_PIECE_PICKS, "result", &|session| {
        let counts = &session.swarm.piece_picks;
        PiecePick::ALL
            .iter()
            .map(|result| (result.as_str(), counts[*result as usize]))
            .collect()
    });
    by_kind(&f::TORRENT_SESSION_WASTED, "reason", &|session| {
        let counts = &session.swarm.wasted_bytes;
        Waste::ALL
            .iter()
            .map(|why| (why.as_str(), counts[*why as usize]))
            .collect()
    });
    by_kind(&f::TORRENT_SESSION_PEERS_NAMED, "source", &|session| {
        let counts = &session.swarm.peers_named;
        PeerSource::ALL
            .iter()
            .map(|source| (source.as_str(), counts[*source as usize]))
            .collect()
    });
    by_kind(
        &f::TORRENT_SESSION_CONNECTED_PEERS,
        "transport",
        &|session| {
            vec![
                ("tcp", session.connected.tcp.into()),
                ("utp", session.connected.utp.into()),
            ]
        },
    );
    by_kind(
        &f::TORRENT_SESSION_OUTGOING_ENCRYPTION,
        "outcome",
        &|session| {
            ["plaintext", "rc4", "header_only", "failed", "cancelled"]
                .into_iter()
                .zip(session.outgoing_encryption)
                .collect()
        },
    );
    for session in open() {
        out.sample(
            &f::TORRENT_SESSION_CONNECTED_SEEDERS,
            &[("route", session.route.as_str())],
            session.connected.seeders,
        );
    }
    for session in open() {
        out.sample(
            &f::TORRENT_SESSION_PIECE_STEALS,
            &[("route", session.route.as_str())],
            session.piece_steals,
        );
    }
    for session in open() {
        out.sample(
            &f::TORRENT_SESSION_PEERS_DROPPED_UNTRACKED,
            &[("route", session.route.as_str())],
            session.untracked_dropped,
        );
    }
    for session in open() {
        out.sample(
            &f::TORRENT_UTP_SYN_FLOOD_DROPPED,
            &[("route", session.route.as_str())],
            session.utp.syn_flood_dropped,
        );
    }
    for session in open() {
        out.sample(
            &f::TORRENT_UTP_SYN_RESETS,
            &[("route", session.route.as_str())],
            session.utp.syn_rst,
        );
    }
}

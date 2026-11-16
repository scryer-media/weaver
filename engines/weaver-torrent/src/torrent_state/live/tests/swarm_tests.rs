// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a connection does is counted in its session, under what it was.

use super::*;
use crate::session_stats::swarm::{
    BlockArrival, ByteKind, ClosePhase, CloseReason, Direction, Encryption, PeerSource, PieceCheck,
    PiecePick, RequestEvent, Transport,
};

// One connection, from its handshake to its end: each thing it does adds
// one to the count named for it, and to no other.
#[tokio::test]
async fn what_a_connection_does_is_counted_under_what_it_was() {
    let h = Harness::new("swarm_counts").await;
    let counted = || h.session_stats.swarm.snapshot();
    assert_eq!(counted(), Default::default());

    let mut expected = crate::session_stats::swarm::SwarmSnapshot::default();

    let (peer, _rx) = h.peer("127.0.0.1:6881");
    let handler = &peer;

    // Bytes before the handshake has said what carries the connection are
    // under no transport.
    handler.on_protocol_bytes(Channel::Upload, 68);
    assert_eq!(counted(), expected);

    handler
        .on_handshake(
            Handshake::new(Id20::new([7; 20]), client_id(peer.addr.addr().unwrap())),
            ConnectionKind::Utp,
            false,
        )
        .unwrap();
    handler.on_handshake_encryption(crate::mse::Encryption::Rc4);
    let (utp, inward, outward) = (
        Transport::Utp as usize,
        Direction::In as usize,
        Direction::Out as usize,
    );
    expected.handshakes[utp][inward][Encryption::Rc4 as usize] = 1;
    assert_eq!(counted(), expected);

    handler.on_protocol_bytes(Channel::Upload, 13);
    handler.on_protocol_bytes(Channel::Download, 17);
    handler.on_uploaded_bytes(100);
    expected.bytes[utp][outward][ByteKind::Protocol as usize] = 13;
    expected.bytes[utp][inward][ByteKind::Protocol as usize] = 17;
    expected.bytes[utp][outward][ByteKind::Payload as usize] = 100;
    assert_eq!(counted(), expected);

    // It takes a piece, is asked for one block of it and sends two.
    peer.on_i_am_unchoked();
    let piece = peer.acquire_next_piece().unwrap().piece().unwrap().get();
    expected.piece_picks[PiecePick::Reserved as usize] = 1;
    assert_eq!(counted(), expected);
    h.peers
        .with_live_mut(peer.addr, "test", |live| {
            live.add_inflight_request(h.chunk(piece, 0))
        })
        .unwrap();
    h.deliver(&peer, (piece, 0)).await;
    h.deliver(&peer, (piece, CHUNK_SIZE)).await;
    expected.blocks[BlockArrival::Requested as usize] = 1;
    expected.blocks[BlockArrival::Unrequested as usize] = 1;
    expected.bytes[utp][inward][ByteKind::Payload as usize] = u64::from(CHUNK_SIZE) * 2;
    assert_eq!(counted(), expected);

    // The rest of the piece arrives, and the piece passes.
    h.deliver(&peer, (piece, CHUNK_SIZE * 2)).await;
    h.deliver(&peer, (piece, CHUNK_SIZE * 3)).await;
    expected.blocks[BlockArrival::Unrequested as usize] = 3;
    expected.bytes[utp][inward][ByteKind::Payload as usize] = u64::from(PIECE_LENGTH);
    expected.piece_checks[PieceCheck::Passed as usize] = 1;
    assert_eq!(counted(), expected);

    peer.on_requests_timed_out().unwrap();
    expected.request_events[RequestEvent::TimedOut as usize] = 1;
    assert_eq!(counted(), expected);

    // It asks for a block of the piece we do not have.
    let _ = peer.on_download_request(Request::new(1 - piece, 0, CHUNK_SIZE));
    expected.request_events[RequestEvent::Invalid as usize] = 1;
    assert_eq!(counted(), expected);

    peer.on_peer_died(Some(Error::PeerDisconnected)).unwrap();
    expected.closes[ClosePhase::Live as usize][CloseReason::PeerClosed as usize] = 1;
    assert_eq!(counted(), expected);

    // Two sources name a peer, one of them twice. Only the namings are
    // read from here on: a peer that is named may be dialled.
    for source in [
        PeerSourceFlag::Pex,
        PeerSourceFlag::Pex,
        PeerSourceFlag::Dht,
    ] {
        h.add_peer("127.0.0.9:6881".parse().unwrap(), source);
    }
    let mut named = [0; PeerSource::COUNT];
    named[PeerSource::Pex as usize] = 2;
    named[PeerSource::Dht as usize] = 1;
    assert_eq!(counted().peers_named, named);
}

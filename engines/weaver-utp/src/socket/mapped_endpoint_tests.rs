// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV6};

use super::{ControlRequest, RequestWithSpan, UtpSocket};
use crate::{
    message::UtpMessage,
    raw::{Type, UtpHeader},
    test_util::{ADDR_1, env::MockUtpEnvironment, transport::RememberingTransport},
};

#[tokio::test(start_paused = true)]
async fn a_mapped_source_routes_to_the_existing_ipv4_stream() {
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        RememberingTransport::new(ADDR_1),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let peer = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 6881));
    let mapped = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(), 6881));
    let (sender, mut received) = crate::ingress::channel(socket.opts().vsock_rx_bufsize);
    dispatcher.streams.insert((peer, 7), sender);
    dispatcher
        .on_recv(
            mapped,
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::Data,
                    connection_id: 7.into(),
                    ..Default::default()
                },
                b"mapped payload",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        received.try_recv().unwrap().message.payload(),
        b"mapped payload"
    );
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

async fn complete_handshake(peer: SocketAddr, source: SocketAddr) {
    let transport = RememberingTransport::new(ADDR_1);
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        transport.clone(),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let (sent, mut connected) = tokio::sync::oneshot::channel();
    dispatcher
        .on_control(ControlRequest::ConnectRequest(
            peer,
            0,
            RequestWithSpan::new(sent),
        ))
        .await;
    let syn = transport.take_sent_utpmessages().pop().unwrap().header;
    dispatcher
        .on_recv(
            source,
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::State,
                    connection_id: syn.connection_id,
                    seq_nr: 1.into(),
                    ack_nr: syn.seq_nr,
                    wnd_size: 1024,
                    ..Default::default()
                },
                b"",
            ),
        )
        .await
        .unwrap();
    let stream = connected.try_recv().unwrap().unwrap();
    assert_eq!(stream.remote_addr(), peer);
    assert!(
        dispatcher
            .streams
            .contains_key(&(peer, syn.connection_id.0))
    );
    drop(stream);
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_mapped_handshake_source_matches_the_ipv4_connection_request() {
    complete_handshake(
        (Ipv4Addr::new(192, 0, 2, 1), 6881).into(),
        (Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(), 6881).into(),
    )
    .await;
}

#[tokio::test(start_paused = true)]
async fn a_true_ipv6_handshake_preserves_scope_and_flow_information() {
    let peer = SocketAddr::V6(SocketAddrV6::new(
        "fe80::1234".parse().unwrap(),
        6881,
        17,
        9,
    ));
    complete_handshake(peer, peer).await;
}

#[tokio::test(start_paused = true)]
async fn an_ipv4_syn_and_its_mapped_retransmission_share_one_pending_accept() {
    let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
        RememberingTransport::new(ADDR_1),
        MockUtpEnvironment::new(),
        Default::default(),
    )
    .unwrap();
    let peer = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 6881));
    let mapped = SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(), 6881));
    for source in [peer, mapped] {
        dispatcher
            .on_recv(
                source,
                UtpMessage::new_test(
                    UtpHeader {
                        htype: Type::Syn,
                        connection_id: 7.into(),
                        seq_nr: 11.into(),
                        ..Default::default()
                    },
                    b"",
                ),
            )
            .await
            .unwrap();
    }
    assert_eq!(dispatcher.accept_queue.syns.len(), 1);
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

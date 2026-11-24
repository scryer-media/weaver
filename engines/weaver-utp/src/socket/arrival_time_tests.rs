// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use super::{ControlRequest, RequestWithSpan, UtpSocket};
use crate::{
    message::UtpMessage,
    raw::{Type, UtpHeader},
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
    traits::UtpEnvironment,
};

#[tokio::test(start_paused = true)]
async fn an_outgoing_handshake_reports_the_gap_at_receipt_instead_of_dispatch() {
    let transport = RememberingTransport::new(ADDR_1);
    let env = MockUtpEnvironment::new();
    let (socket, mut dispatcher) =
        UtpSocket::new_with_opts_and_dispatcher(transport.clone(), env.clone(), Default::default())
            .unwrap();
    let (request, mut connected) = tokio::sync::oneshot::channel();
    dispatcher
        .on_control(ControlRequest::ConnectRequest(
            ADDR_2,
            0,
            RequestWithSpan::new(request),
        ))
        .await;
    let syn = transport.take_sent_utpmessages().pop().unwrap().header;
    env.increment_now(Duration::from_millis(10));
    let mut response = UtpMessage::new_test(
        UtpHeader {
            htype: Type::State,
            connection_id: syn.connection_id,
            seq_nr: 1.into(),
            ack_nr: syn.seq_nr,
            timestamp_microseconds: 1_000,
            wnd_size: 1024,
            ..Default::default()
        },
        b"",
    );
    response.received = env.now();
    env.increment_now(Duration::from_millis(50));
    dispatcher.on_recv(ADDR_2, response).await.unwrap();
    let mut stream = connected.try_recv().unwrap().unwrap();
    stream.write_all(b"x").await.unwrap();
    transport
        .wait_for_sent(|header| header.htype == Type::Data && header.seq_nr == syn.seq_nr + 1)
        .await;
    let sent = transport.take_sent_utpmessages();
    let data = sent
        .iter()
        .find(|message| message.header.htype == Type::Data)
        .unwrap();
    assert_eq!(data.header.timestamp_difference_microseconds, 9_000);
    drop(stream);
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_cached_syn_keeps_its_arrival_gap_until_the_stream_starts() {
    let transport = RememberingTransport::new(ADDR_1);
    let env = MockUtpEnvironment::new();
    let (socket, mut dispatcher) =
        UtpSocket::new_with_opts_and_dispatcher(transport.clone(), env.clone(), Default::default())
            .unwrap();
    env.increment_now(Duration::from_millis(10));
    let mut syn = UtpMessage::new_test(
        UtpHeader {
            htype: Type::Syn,
            connection_id: 7.into(),
            seq_nr: 1.into(),
            timestamp_microseconds: 1_000,
            ..Default::default()
        },
        b"",
    );
    syn.received = env.now();
    env.increment_now(Duration::from_millis(40));
    dispatcher.on_recv(ADDR_2, syn).await.unwrap();
    assert_eq!(dispatcher.accept_queue.syns.len(), 1);
    env.increment_now(Duration::from_millis(50));
    let (accept, mut accepted) = tokio::sync::oneshot::channel();
    dispatcher.accept_queue.next_available_acceptor = Some(RequestWithSpan::new(accept));
    dispatcher.cleanup_accept_queue().unwrap();
    let starter = accepted.try_recv().unwrap();
    env.increment_now(Duration::from_millis(50));
    let stream = starter.start().unwrap();
    transport
        .wait_for_sent(|header| header.htype == Type::State && header.connection_id.0 == 7)
        .await;
    let sent = transport.take_sent_utpmessages();
    let response = sent
        .iter()
        .find(|message| message.header.htype == Type::State)
        .unwrap();
    assert_eq!(response.header.timestamp_difference_microseconds, 9_000);
    drop(stream);
    drop(dispatcher);
    socket.shutdown().await.unwrap();
}

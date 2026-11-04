// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

mod basics;
mod congestion;
mod delay;
mod delayed_ack;
mod fast_retransmit;
mod flow_control;
mod guards;
mod inactivity_timeout;
mod mtu_ceiling;
mod mtu_probing;
mod nagle;
mod probe_payload;
mod retransmit_timer;
mod rtte;
mod shutdown;
mod stream_api;
mod timeouts;

use std::{
    num::{NonZero, NonZeroUsize},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use futures::FutureExt;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

use crate::{
    SocketOpts, UtpSocket, UtpStream,
    constants::{UDP_HEADER, UTP_HEADER},
    message::UtpMessage,
    raw::{Type::*, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter, VirtualSocket},
    test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
    traits::UtpEnvironment,
};

fn make_msg(header: UtpHeader, payload: &str) -> UtpMessage {
    UtpMessage::new_test(header, payload.as_bytes())
}

const fn calc_mtu_for_mss(mss: usize) -> NonZeroUsize {
    const IP_HEADER: usize = 20;
    NonZero::new(mss + UTP_HEADER as usize + UDP_HEADER as usize + IP_HEADER).unwrap()
}

struct TestVsock {
    transport: RememberingTransport,
    env: MockUtpEnvironment,
    _socket: Arc<UtpSocket<RememberingTransport, MockUtpEnvironment>>,
    vsock: VirtualSocket<RememberingTransport, MockUtpEnvironment>,
    stream: Option<UtpStream>,
    tx: UnboundedSender<UtpMessage>,
}

impl TestVsock {
    fn send_msg(&mut self, header: UtpHeader, payload: &str) {
        let mut message = make_msg(header, payload);
        message.received = self.env.now();
        self.tx.send(message).unwrap()
    }

    fn send_data(&mut self, seq_nr: impl Into<SeqNr>, ack_nr: impl Into<SeqNr>, payload: &str) {
        let header = UtpHeader {
            htype: Data,
            connection_id: self.vsock.conn_id_send + 1,
            timestamp_microseconds: self.vsock.timestamp_microseconds(),
            timestamp_difference_microseconds: 0,
            wnd_size: 1024 * 1024,
            seq_nr: seq_nr.into(),
            ack_nr: ack_nr.into(),
            extensions: Default::default(),
        };
        self.send_msg(header, payload);
    }

    fn take_sent(&self) -> Vec<UtpMessage> {
        self.transport.take_sent_utpmessages()
    }

    #[track_caller]
    fn assert_sent_empty(&self) {
        assert_eq!(self.take_sent(), Vec::<UtpMessage>::new())
    }

    #[track_caller]
    fn assert_sent_empty_msg(&self, msg: &'static str) {
        assert_eq!(self.take_sent(), Vec::<UtpMessage>::new(), "{}", msg)
    }

    async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
        self.stream.as_mut().unwrap().read_all_available().await
    }

    async fn process_all_available_incoming(&mut self) {
        std::future::poll_fn(|cx| {
            if self.vsock.rx.is_empty() {
                return Poll::Ready(());
            }
            self.vsock.poll_unpin(cx).map(|r| r.unwrap())
        })
        .await
    }

    async fn poll_once(&mut self) -> Poll<crate::Result<()>> {
        std::future::poll_fn(|cx| {
            let res = self.vsock.poll_unpin(cx);
            Poll::Ready(res)
        })
        .await
    }

    async fn poll_once_assert_pending(&mut self) {
        let res = self.poll_once().await;
        match res {
            Poll::Pending => {}
            Poll::Ready(res) => {
                res.unwrap();
                panic!("poll returned Ready, but expected Pending");
            }
        };
    }

    async fn poll_once_assert_ready(&mut self) -> crate::Result<()> {
        let res = self.poll_once().await;
        match res {
            Poll::Pending => {
                panic!("expected poll to return Ready, but got Pending")
            }
            Poll::Ready(res) => res,
        }
    }
}

fn make_test_vsock_args(opts: SocketOpts, args: StreamArgs, env: MockUtpEnvironment) -> TestVsock {
    make_test_vsock_on(RememberingTransport::new(ADDR_1), opts, args, env)
}

fn make_test_vsock_on(
    transport: RememberingTransport,
    opts: SocketOpts,
    args: StreamArgs,
    env: MockUtpEnvironment,
) -> TestVsock {
    let socket = UtpSocket::new_with_opts(transport.clone(), env.clone(), opts).unwrap();
    let (tx, rx) = unbounded_channel();

    let UtpStreamStarter { stream, vsock, .. } = UtpStreamStarter::new(&socket, ADDR_2, rx, args);

    TestVsock {
        transport,
        env,
        _socket: socket,
        vsock,
        stream: Some(stream),
        tx,
    }
}

// Remote's seq_nr == 1
// Our seq_nr == 101
fn make_test_vsock(opts: SocketOpts, is_incoming: bool) -> TestVsock {
    let env = MockUtpEnvironment::new();
    let args = test_stream_args(&env, is_incoming);
    make_test_vsock_args(opts, args, env)
}

fn test_stream_args(env: &MockUtpEnvironment, is_incoming: bool) -> StreamArgs {
    if is_incoming {
        let remote_syn = UtpHeader {
            htype: Syn,
            ..Default::default()
        };
        StreamArgs::new_incoming(101.into(), &remote_syn)
    } else {
        let remote_ack = UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        };
        let now = env.now();
        env.increment_now(Duration::from_secs(1));
        StreamArgs::new_outgoing(&remote_ack, now, env.now())
    }
}

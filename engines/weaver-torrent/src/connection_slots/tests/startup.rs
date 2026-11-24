// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    io,
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    ListenerMode, ListenerOptions,
    connection_slots::ConnectionSlots,
    network::{
        BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket, KernelSockets,
        SocketFactory, StreamListener,
    },
};

struct GatedSslSockets {
    kernel: KernelSockets,
    binds: AtomicUsize,
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl SocketFactory for GatedSslSockets {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        self.kernel.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.kernel.listen_tcp(addr)
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.kernel.bind_udp(addr)
    }

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            if self.binds.fetch_add(1, Ordering::Relaxed) == 1 {
                self.entered
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.unwrap();
            }
            self.kernel.bind_utp(addr, receive_buffer).await
        })
    }
}

#[tokio::test]
async fn plain_and_ssl_sockets_register_before_startup_can_receive() {
    let slots = Arc::new(ConnectionSlots::new(3, 0, NonZeroU32::new(30).unwrap()));
    let (entered, ssl_binding) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let factory: Arc<dyn SocketFactory> = Arc::new(GatedSslSockets {
        kernel: KernelSockets::new(None).unwrap(),
        binds: AtomicUsize::new(0),
        entered: Mutex::new(Some(entered)),
        release: Mutex::new(Some(released)),
    });
    let token = CancellationToken::new();
    let opening = tokio::spawn({
        let slots = slots.clone();
        let token = token.clone();
        async move {
            ListenerOptions {
                mode: ListenerMode::UtpOnly,
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                ssl_listen_port: Some(0),
                utp_opts: Some(weaver_utp::SocketOpts {
                    connections_limit: std::num::NonZeroUsize::new(99),
                    ..Default::default()
                }),
                ..Default::default()
            }
            .start(None, token, &factory, &slots)
            .await
            .unwrap()
        }
    });
    ssl_binding.await.unwrap();

    let plain = {
        let cap = slots.cap.lock();
        assert_eq!(cap.utp_sockets.len(), 1);
        cap.utp_sockets[0].upgrade().unwrap()
    };
    assert_eq!(plain.connections_limit(), Some(3));
    slots.set_limit(1);
    assert_eq!(plain.connections_limit(), Some(1));

    release.send(()).unwrap();
    let listeners = opening.await.unwrap();
    assert_eq!(listeners.utp_socket.unwrap().0.connections_limit(), Some(1));
    assert_eq!(
        listeners
            .ssl
            .unwrap()
            .utp_socket
            .unwrap()
            .0
            .connections_limit(),
        Some(1)
    );
    assert_eq!(slots.cap.lock().utp_sockets.len(), 2);
    token.cancel();
}

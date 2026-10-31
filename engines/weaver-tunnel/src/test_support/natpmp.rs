// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A NAT-PMP gateway on loopback, for a test peer to relay a tunnel's
// requests to.
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// The public address the gateway names.
pub const PUBLIC_ADDRESS: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);

/// A gateway that forwards one port for two hours to whoever asks, and
/// counts the requests it heard. It stops when dropped.
pub struct NatPmpGateway {
    address: SocketAddr,
    heard: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for NatPmpGateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl NatPmpGateway {
    /// A gateway that forwards `port`.
    pub async fn start(port: u16) -> Self {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind the gateway");
        let address = socket.local_addr().expect("gateway address");
        let heard = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn({
            let heard = heard.clone();
            async move {
                let mut request = [0u8; 64];
                while let Ok((len, from)) = socket.recv_from(&mut request).await {
                    if len < 2 || (request[1] != 0 && len < 12) {
                        continue;
                    }
                    heard.fetch_add(1, Ordering::SeqCst);
                    let mut answer = vec![0, request[1] | 0x80, 0, 0];
                    answer.extend_from_slice(&1u32.to_be_bytes());
                    if request[1] == 0 {
                        answer.extend_from_slice(&PUBLIC_ADDRESS.octets());
                    } else {
                        answer.extend_from_slice(&request[4..6]);
                        answer.extend_from_slice(&port.to_be_bytes());
                        answer.extend_from_slice(&7200u32.to_be_bytes());
                    }
                    if socket.send_to(&answer, from).await.is_err() {
                        return;
                    }
                }
            }
        });
        Self {
            address,
            heard,
            task,
        }
    }

    /// Where the gateway listens.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// How many requests it has answered.
    pub fn heard(&self) -> usize {
        self.heard.load(Ordering::SeqCst)
    }
}

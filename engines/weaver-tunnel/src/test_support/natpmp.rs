// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A NAT-PMP gateway on loopback, for a test peer to relay a tunnel's
// requests to.
use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

/// The public address the gateway names.
pub const PUBLIC_ADDRESS: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);

/// A gateway that forwards one port for two hours to whoever asks, and
/// counts the requests it heard. It speaks NAT-PMP only: it answers any
/// other version, PCP's included, with result 1, unsupported version. It
/// stops when dropped.
pub struct NatPmpGateway {
    address: SocketAddr,
    heard: Arc<AtomicUsize>,
    mapped: Arc<Mapped>,
    task: tokio::task::JoinHandle<()>,
}

/// The mappings the gateway granted: who asked, for which internal port,
/// and the port it gave. A waiter wakes on each.
#[derive(Default)]
struct Mapped {
    granted: Mutex<Vec<Granted>>,
    notify: tokio::sync::Notify,
}

/// A mapping the gateway granted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Granted {
    /// Where the request came from.
    pub from: SocketAddr,
    /// 1 for UDP, 2 for TCP.
    pub opcode: u8,
    pub internal: u16,
    pub external: u16,
}

impl Drop for NatPmpGateway {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl NatPmpGateway {
    /// A gateway that forwards `port`.
    pub async fn start(port: u16) -> Self {
        Self::granting(move |_| port).await
    }

    /// A gateway that forwards, for each internal port asked, the port
    /// `grant` gives for it.
    pub async fn granting(grant: impl Fn(u16) -> u16 + Send + 'static) -> Self {
        let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("bind the gateway");
        let address = socket.local_addr().expect("gateway address");
        let heard = Arc::new(AtomicUsize::new(0));
        let mapped = Arc::new(Mapped::default());
        let task = tokio::spawn({
            let heard = heard.clone();
            let mapped = mapped.clone();
            async move {
                let mut request = [0u8; 64];
                while let Ok((len, from)) = socket.recv_from(&mut request).await {
                    if len < 2 {
                        continue;
                    }
                    let answer = if request[0] != 0 {
                        // NAT-PMP's answer to a version it does not speak.
                        let mut answer = vec![0, request[1] | 0x80, 0, 1];
                        answer.extend_from_slice(&1u32.to_be_bytes());
                        answer
                    } else if request[1] != 0 && len < 12 {
                        continue;
                    } else {
                        let mut answer = vec![0, request[1] | 0x80, 0, 0];
                        answer.extend_from_slice(&1u32.to_be_bytes());
                        if request[1] == 0 {
                            answer.extend_from_slice(&PUBLIC_ADDRESS.octets());
                        } else {
                            let internal = u16::from_be_bytes([request[4], request[5]]);
                            let external = grant(internal);
                            answer.extend_from_slice(&request[4..6]);
                            answer.extend_from_slice(&external.to_be_bytes());
                            answer.extend_from_slice(&7200u32.to_be_bytes());
                            mapped.granted.lock().unwrap().push(Granted {
                                from,
                                opcode: request[1],
                                internal,
                                external,
                            });
                            mapped.notify.notify_waiters();
                        }
                        answer
                    };
                    heard.fetch_add(1, Ordering::SeqCst);
                    if socket.send_to(&answer, from).await.is_err() {
                        return;
                    }
                }
            }
        });
        Self {
            address,
            heard,
            mapped,
            task,
        }
    }

    /// The first mapping it granted for `internal` port over `opcode` (1
    /// for UDP, 2 for TCP), once it has.
    pub async fn granted(&self, opcode: u8, internal: u16) -> Granted {
        loop {
            let notified = self.mapped.notify.notified();
            if let Some(granted) =
                self.mapped.granted.lock().unwrap().iter().find(|granted| {
                    granted.opcode == opcode && granted.internal == internal
                })
            {
                return *granted;
            }
            notified.await;
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

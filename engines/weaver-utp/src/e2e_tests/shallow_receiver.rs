// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A receiver with a shallow datagram queue that its owner reads only once
// per drain interval, run on tokio's paused clock: what a peer whose UDP
// socket keeps the host's default receive buffer looks like to a sender.
// Every datagram that arrives while the queue already holds its bound is
// lost before the receiver sees it.

use std::{
    collections::{BTreeMap, VecDeque},
    future::poll_fn,
    io::{self, IoSlice},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{PollSendToVectored, SocketOpts, Transport, UtpSocket, traits::DefaultUtpEnvironment};

#[derive(Clone, Copy)]
struct Shallow {
    // Datagrams the queue holds between two drains.
    queue_packets: usize,
    drain_every: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
struct Counters {
    // Datagrams that found the shallow queue full.
    dropped: u64,
    // The most datagrams that reached the shallow queue in one interval,
    // dropped ones included.
    peak_arrivals: usize,
    data_sent: u64,
    // The most datagrams on the wire at once.
    peak_in_flight: usize,
}

struct Endpoint {
    shallow: Option<Shallow>,
    // Arrived, waiting for the next drain.
    queue: VecDeque<(SocketAddr, Vec<u8>)>,
    // Drained, for the socket to read.
    readable: VecDeque<(SocketAddr, Vec<u8>)>,
    arrivals: usize,
    waker: Option<Waker>,
}

struct Model {
    one_way: Duration,
    in_flight: BTreeMap<(Instant, u64), (SocketAddr, SocketAddr, Vec<u8>)>,
    ordinal: u64,
    endpoints: BTreeMap<SocketAddr, Endpoint>,
    next_drain: Option<Instant>,
    counters: Counters,
}

impl Model {
    fn next_event(&self) -> Option<Instant> {
        let delivery = self.in_flight.first_key_value().map(|((at, _), _)| *at);
        delivery.into_iter().chain(self.next_drain).min()
    }

    // A drain at an instant comes before deliveries at that instant: they
    // count toward the next interval.
    fn advance(&mut self, now: Instant) -> Vec<Waker> {
        let mut wakes = Vec::new();
        loop {
            let delivery = self.in_flight.first_key_value().map(|((at, _), _)| *at);
            match (self.next_drain, delivery) {
                (Some(drain), _) if drain <= now && delivery.is_none_or(|at| drain <= at) => {
                    let mut every = None;
                    for endpoint in self.endpoints.values_mut() {
                        let Some(shallow) = endpoint.shallow else {
                            continue;
                        };
                        every = Some(shallow.drain_every);
                        endpoint.arrivals = 0;
                        if !endpoint.queue.is_empty() {
                            let queued = std::mem::take(&mut endpoint.queue);
                            endpoint.readable.extend(queued);
                            wakes.extend(endpoint.waker.take());
                        }
                    }
                    self.next_drain = every.map(|every| drain + every);
                }
                (_, Some(at)) if at <= now => {
                    let (_, (source, target, data)) = self.in_flight.pop_first().unwrap();
                    let endpoint = self.endpoints.get_mut(&target).unwrap();
                    match endpoint.shallow {
                        Some(shallow) => {
                            endpoint.arrivals += 1;
                            self.counters.peak_arrivals =
                                self.counters.peak_arrivals.max(endpoint.arrivals);
                            if endpoint.queue.len() >= shallow.queue_packets {
                                self.counters.dropped += 1;
                            } else {
                                endpoint.queue.push_back((source, data));
                            }
                        }
                        None => {
                            endpoint.readable.push_back((source, data));
                            wakes.extend(endpoint.waker.take());
                        }
                    }
                }
                _ => return wakes,
            }
        }
    }
}

struct Network {
    model: Mutex<Model>,
    changed: Notify,
}

impl Network {
    fn new(one_way: Duration) -> Arc<Self> {
        Arc::new(Self {
            model: Mutex::new(Model {
                one_way,
                in_flight: BTreeMap::new(),
                ordinal: 0,
                endpoints: BTreeMap::new(),
                next_drain: None,
                counters: Counters::default(),
            }),
            changed: Notify::new(),
        })
    }

    fn transport(self: &Arc<Self>, address: SocketAddr, shallow: Option<Shallow>) -> Link {
        let mut model = self.model.lock();
        if let Some(shallow) = shallow {
            model.next_drain = Some(Instant::now() + shallow.drain_every);
        }
        model.endpoints.insert(
            address,
            Endpoint {
                shallow,
                queue: VecDeque::new(),
                readable: VecDeque::new(),
                arrivals: 0,
                waker: None,
            },
        );
        Link {
            address,
            network: Arc::clone(self),
        }
    }

    fn send(&self, source: SocketAddr, target: SocketAddr, data: Vec<u8>) -> io::Result<usize> {
        let len = data.len();
        let wakes = {
            let mut model = self.model.lock();
            let now = Instant::now();
            let wakes = model.advance(now);
            if data.first().map(|byte| byte >> 4) == Some(crate::raw::Type::Data as u8) {
                model.counters.data_sent += 1;
            }
            model.ordinal += 1;
            let key = (now + model.one_way, model.ordinal);
            model.in_flight.insert(key, (source, target, data));
            model.counters.peak_in_flight =
                model.counters.peak_in_flight.max(model.in_flight.len());
            wakes
        };
        for waker in wakes {
            waker.wake();
        }
        self.changed.notify_one();
        Ok(len)
    }

    fn counters(&self) -> Counters {
        self.model.lock().counters
    }

    async fn run(self: Arc<Self>, stop: CancellationToken) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (next, wakes) = {
                let mut model = self.model.lock();
                let wakes = model.advance(Instant::now());
                (model.next_event(), wakes)
            };
            for waker in wakes {
                waker.wake();
            }
            match next {
                Some(at) => tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = &mut changed => {},
                    _ = tokio::time::sleep_until(at) => {},
                },
                None => tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = &mut changed => {},
                },
            }
        }
    }
}

#[derive(Clone)]
struct Link {
    address: SocketAddr,
    network: Arc<Network>,
}

impl Transport for Link {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (source, data) = poll_fn(|cx| {
            let mut model = self.network.model.lock();
            let endpoint = model.endpoints.get_mut(&self.address).unwrap();
            match endpoint.readable.pop_front() {
                Some(datagram) => Poll::Ready(datagram),
                None => {
                    endpoint.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await;
        buf[..data.len()].copy_from_slice(&data);
        Ok((data.len(), source))
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> io::Result<usize> {
        self.network.send(self.address, target, buf.to_vec())
    }

    fn poll_send_to(
        &self,
        _cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.network.send(self.address, target, buf.to_vec()))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.address
    }

    fn link_mtu(&self) -> Option<u16> {
        Some(1500)
    }

    fn dont_fragment(&self) -> bool {
        true
    }
}

impl PollSendToVectored for Link {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let data: Vec<u8> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
        Poll::Ready(self.network.send(self.address, target, data))
    }
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn payload(bytes: usize) -> Vec<u8> {
    (0..bytes).map(|index| (index % 251) as u8).collect()
}

// Sends `bytes` from a sender with an ample receive queue to a receiver
// behind `shallow`, and returns what the network counted. The send buffer
// starts at a megabyte, so the window, not the buffer, bounds the flight.
async fn transfer(bytes: usize, one_way: Duration, shallow: Option<Shallow>) -> Counters {
    let network = Network::new(one_way);
    let stop = CancellationToken::new();
    let socket = |transport| {
        UtpSocket::new_with_opts(
            transport,
            DefaultUtpEnvironment {},
            SocketOpts {
                cancellation_token: stop.clone(),
                vsock_tx_bufsize_bytes_initial: NonZeroUsize::new(1024 * 1024),
                ..Default::default()
            },
        )
        .unwrap()
    };
    let sender = socket(network.transport(address(1), None));
    let receiver = socket(network.transport(address(2), shallow));
    let driver = tokio::spawn(Arc::clone(&network).run(stop.clone()));

    let send = async {
        let mut stream = sender.connect(address(2)).await.unwrap();
        stream.write_all(&payload(bytes)).await.unwrap();
        stream.flush().await.unwrap();
        stream
    };
    let receive = async {
        let mut stream = receiver.accept().await.unwrap();
        let mut received = vec![0; bytes];
        stream.read_exact(&mut received).await.unwrap();
        assert!(received == payload(bytes), "the payload arrived intact");
        stream
    };
    let _streams = tokio::join!(send, receive);
    let counters = network.counters();
    sender.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
    stop.cancel();
    driver.await.unwrap();
    counters
}

// A receiver that holds 48 datagrams and reads them once a millisecond, a
// default 212992-byte Linux receive buffer at about 4352 bytes per queued
// datagram, 50 ms away: a multi-megabyte upload reaches it without one lost
// datagram.
#[tokio::test(start_paused = true)]
async fn a_receiver_holding_48_datagrams_per_drain_loses_none() {
    let shallow = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(1),
    };
    let counters = transfer(4 * 1024 * 1024, Duration::from_millis(50), Some(shallow)).await;
    assert!(counters.data_sent > 2800, "{counters:?}");
    // The window opened past what the queue holds.
    assert!(counters.peak_in_flight > 48, "{counters:?}");
    assert!(counters.peak_arrivals <= 48, "{counters:?}");
    assert_eq!(counters.dropped, 0, "{counters:?}");
}

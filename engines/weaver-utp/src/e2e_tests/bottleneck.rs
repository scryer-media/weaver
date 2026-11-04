// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::{BTreeMap, VecDeque},
    future::poll_fn,
    io::{self, IoSlice},
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU16, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use parking_lot::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{Notify, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{PollSendToVectored, SocketOpts, Transport, UtpEnvironment, UtpSocket};

const NETWORK_HEADERS: usize = 28;
const PATH_MTU: usize = 1500;

#[derive(Clone, Copy, Debug)]
struct Bounds {
    bytes: usize,
    packets: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct Usage {
    bytes: usize,
    packets: usize,
}

impl Usage {
    fn fits(self, bytes: usize, bounds: Bounds) -> bool {
        self.packets < bounds.packets && bytes <= bounds.bytes.saturating_sub(self.bytes)
    }

    fn add(&mut self, bytes: usize) {
        self.bytes += bytes;
        self.packets += 1;
    }

    fn remove(&mut self, bytes: usize) {
        self.bytes -= bytes;
        self.packets -= 1;
    }

    fn observe(&mut self, current: Self) {
        self.bytes = self.bytes.max(current.bytes);
        self.packets = self.packets.max(current.packets);
    }
}

#[derive(Clone)]
struct Config {
    bytes_per_second: [u64; 2],
    service: [Bounds; 2],
    propagation: Bounds,
    delivery: Bounds,
    one_way: Duration,
    jitter: [Duration; 4],
    lose_every_data: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bytes_per_second: [128 * 1024, 1024 * 1024],
            service: [Bounds {
                bytes: 16 * 1024,
                packets: 32,
            }; 2],
            propagation: Bounds {
                bytes: 128 * 1024,
                packets: 512,
            },
            delivery: Bounds {
                bytes: 64 * 1024,
                packets: 128,
            },
            one_way: Duration::from_millis(10),
            jitter: [
                Duration::from_millis(32),
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
            ],
            lose_every_data: Some(23),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Counters {
    service_byte_drops: u64,
    service_packet_drops: u64,
    propagation_drops: u64,
    delivery_drops: u64,
    scripted_loss: u64,
    reordered: u64,
    served_wire_bytes: [u64; 2],
    data_delivered: [u64; 2],
    data_while_synthetic_arrivals_active: [u64; 2],
    synthetic_arrived: u64,
    synthetic_served: u64,
    synthetic_delivered: u64,
    service_peak: [Usage; 2],
    propagation_peak: Usage,
    delivery_peak: Usage,
    max_queue_delay: Duration,
}

#[derive(Clone, Copy)]
enum Kind {
    Control,
    Data(usize),
    Synthetic,
}

struct Packet {
    lane: usize,
    source: SocketAddr,
    target: Option<SocketAddr>,
    data: Vec<u8>,
    kind: Kind,
    ordinal: u64,
    jitter: Duration,
    scripted_loss: bool,
}

impl Packet {
    fn wire_bytes(&self) -> usize {
        self.data.len() + NETWORK_HEADERS
    }
}

struct Service {
    finishes: Instant,
    packet: Packet,
}

#[derive(Default)]
struct Lane {
    queue: VecDeque<Service>,
    usage: Usage,
}

struct Endpoint {
    lane: usize,
    flow: Option<usize>,
    queue: VecDeque<Packet>,
    usage: Usage,
    waker: Option<Waker>,
}

struct Synthetic {
    next: Instant,
    interval: Duration,
    remaining: usize,
    payload_bytes: usize,
    application_progress: Arc<[AtomicUsize; 2]>,
    complete: oneshot::Sender<[usize; 2]>,
}

struct Model {
    config: Config,
    lanes: [Lane; 2],
    in_flight: BTreeMap<(Instant, u64), Packet>,
    propagation_usage: Usage,
    endpoints: BTreeMap<SocketAddr, Endpoint>,
    ordinal: u64,
    data_attempts: [u64; 2],
    last_delivered_ordinal: [u64; 2],
    synthetic: Option<Synthetic>,
    counters: Counters,
}

impl Model {
    fn new(config: Config) -> Self {
        assert!(config.bytes_per_second.iter().all(|rate| *rate > 0));
        assert!(config.lose_every_data.is_none_or(|every| every > 0));
        Self {
            config,
            lanes: Default::default(),
            in_flight: BTreeMap::new(),
            propagation_usage: Usage::default(),
            endpoints: BTreeMap::new(),
            ordinal: 0,
            data_attempts: [0; 2],
            last_delivered_ordinal: [0; 2],
            synthetic: None,
            counters: Counters::default(),
        }
    }

    fn endpoint(&mut self, address: SocketAddr, lane: usize, flow: Option<usize>) {
        assert!(
            self.endpoints
                .insert(
                    address,
                    Endpoint {
                        lane,
                        flow,
                        queue: VecDeque::new(),
                        usage: Usage::default(),
                        waker: None,
                    }
                )
                .is_none()
        );
    }

    fn enqueue(
        &mut self,
        lane: usize,
        at: Instant,
        source: SocketAddr,
        target: Option<SocketAddr>,
        data: Vec<u8>,
        kind: Kind,
    ) {
        self.ordinal += 1;
        let (jitter, scripted_loss) = if let Kind::Data(flow) = kind {
            self.data_attempts[flow] += 1;
            let attempt = self.data_attempts[flow];
            (
                self.config.jitter[((attempt - 1) % 4) as usize],
                self.config
                    .lose_every_data
                    .is_some_and(|every| attempt.is_multiple_of(every)),
            )
        } else {
            (Duration::ZERO, false)
        };
        let packet = Packet {
            lane,
            source,
            target,
            data,
            kind,
            ordinal: self.ordinal,
            jitter,
            scripted_loss,
        };
        let bytes = packet.wire_bytes();
        let queue = &mut self.lanes[lane];
        let bounds = self.config.service[lane];
        if bytes > bounds.bytes.saturating_sub(queue.usage.bytes) {
            self.counters.service_byte_drops += 1;
            return;
        }
        if queue.usage.packets >= bounds.packets {
            self.counters.service_packet_drops += 1;
            return;
        }
        let starts = queue.queue.back().map_or(at, |last| last.finishes.max(at));
        self.counters.max_queue_delay = self.counters.max_queue_delay.max(starts - at);
        let nanos = (bytes as u128 * 1_000_000_000)
            .div_ceil(u128::from(self.config.bytes_per_second[lane]));
        let finishes = starts + Duration::from_nanos(u64::try_from(nanos).unwrap());
        queue.usage.add(bytes);
        self.counters.service_peak[lane].observe(queue.usage);
        queue.queue.push_back(Service { finishes, packet });
    }

    fn next_event(&self) -> Option<(Instant, usize)> {
        let service = self
            .lanes
            .iter()
            .enumerate()
            .filter_map(|(lane, queue)| queue.queue.front().map(|next| (next.finishes, lane)));
        let propagation = self
            .in_flight
            .first_key_value()
            .map(|((at, _), _)| (*at, 2));
        let synthetic = self.synthetic.as_ref().map(|load| (load.next, 3));
        service.chain(propagation).chain(synthetic).min()
    }

    // Service, propagation and synthetic arrivals are ordered logical events.
    // In particular, service frees queue capacity before delivery to a socket.
    fn advance(&mut self, now: Instant) -> Vec<Waker> {
        let mut wakes = Vec::new();
        while let Some((at, event)) = self.next_event().filter(|(at, _)| *at <= now) {
            match event {
                lane @ 0..=1 => {
                    let Service { packet, .. } = self.lanes[lane].queue.pop_front().unwrap();
                    let bytes = packet.wire_bytes();
                    self.lanes[lane].usage.remove(bytes);
                    self.counters.served_wire_bytes[lane] += bytes as u64;
                    if matches!(packet.kind, Kind::Synthetic) {
                        self.counters.synthetic_served += 1;
                    }
                    if packet.scripted_loss {
                        self.counters.scripted_loss += 1;
                    } else if !self.propagation_usage.fits(bytes, self.config.propagation) {
                        self.counters.propagation_drops += 1;
                    } else {
                        self.propagation_usage.add(bytes);
                        self.counters
                            .propagation_peak
                            .observe(self.propagation_usage);
                        let delivery = at + self.config.one_way + packet.jitter;
                        assert!(
                            self.in_flight
                                .insert((delivery, packet.ordinal), packet)
                                .is_none()
                        );
                    }
                }
                2 => {
                    let (_, packet) = self.in_flight.pop_first().unwrap();
                    let bytes = packet.wire_bytes();
                    self.propagation_usage.remove(bytes);
                    if packet.ordinal < self.last_delivered_ordinal[packet.lane] {
                        self.counters.reordered += 1;
                    }
                    self.last_delivered_ordinal[packet.lane] =
                        self.last_delivered_ordinal[packet.lane].max(packet.ordinal);
                    let kind = packet.kind;
                    if let Some(target) = packet.target {
                        let endpoint = self.endpoints.get_mut(&target).unwrap();
                        if !endpoint.usage.fits(bytes, self.config.delivery) {
                            self.counters.delivery_drops += 1;
                            continue;
                        }
                        endpoint.usage.add(bytes);
                        self.counters.delivery_peak.observe(endpoint.usage);
                        endpoint.queue.push_back(packet);
                        if let Some(waker) = endpoint.waker.take() {
                            wakes.push(waker);
                        }
                    }
                    match kind {
                        Kind::Data(flow) => {
                            self.counters.data_delivered[flow] += 1;
                            if self.synthetic.is_some() {
                                self.counters.data_while_synthetic_arrivals_active[flow] += 1;
                            }
                        }
                        Kind::Synthetic => self.counters.synthetic_delivered += 1,
                        Kind::Control => {}
                    }
                }
                3 => {
                    let load = self.synthetic.as_mut().unwrap();
                    let bytes = load.payload_bytes;
                    load.remaining -= 1;
                    load.next += load.interval;
                    if load.remaining == 0 {
                        let finished = self.synthetic.take().unwrap();
                        let progress = std::array::from_fn(|flow| {
                            finished.application_progress[flow].load(Ordering::Relaxed)
                        });
                        finished.complete.send(progress).unwrap();
                    }
                    self.counters.synthetic_arrived += 1;
                    self.enqueue(0, at, address(9000), None, vec![0; bytes], Kind::Synthetic);
                }
                _ => unreachable!(),
            }
        }
        wakes
    }

    fn receive(&mut self, address: SocketAddr) -> Option<Packet> {
        let endpoint = self.endpoints.get_mut(&address).unwrap();
        let packet = endpoint.queue.pop_front()?;
        endpoint.usage.remove(packet.wire_bytes());
        Some(packet)
    }
}

struct Network {
    model: Mutex<Model>,
    changed: Notify,
}

impl Network {
    fn new(config: Config) -> Arc<Self> {
        Arc::new(Self {
            model: Mutex::new(Model::new(config)),
            changed: Notify::new(),
        })
    }

    fn transport(
        self: &Arc<Self>,
        address: SocketAddr,
        lane: usize,
        flow: Option<usize>,
    ) -> ModelTransport {
        self.model.lock().endpoint(address, lane, flow);
        ModelTransport {
            address,
            network: Arc::clone(self),
        }
    }

    fn send(&self, source: SocketAddr, target: SocketAddr, data: &[u8]) -> io::Result<usize> {
        if data.len() + NETWORK_HEADERS > PATH_MTU {
            return Err(io::Error::from_raw_os_error(libc::EMSGSIZE));
        }
        let wakes = {
            let mut model = self.model.lock();
            let now = Instant::now();
            let wakes = model.advance(now);
            let endpoint = model.endpoints.get(&source).unwrap();
            let lane = endpoint.lane;
            let kind = match (data.first().map(|byte| byte >> 4), endpoint.flow) {
                (Some(kind), Some(flow)) if kind == crate::raw::Type::Data as u8 => {
                    Kind::Data(flow)
                }
                _ => Kind::Control,
            };
            assert!(model.endpoints.contains_key(&target));
            model.enqueue(lane, now, source, Some(target), data.to_vec(), kind);
            wakes
        };
        for waker in wakes {
            waker.wake();
        }
        self.changed.notify_one();
        Ok(data.len())
    }

    fn start_synthetic(
        &self,
        packets: usize,
        payload_bytes: usize,
        interval: Duration,
        application_progress: Arc<[AtomicUsize; 2]>,
    ) -> oneshot::Receiver<[usize; 2]> {
        assert!(packets > 0 && !interval.is_zero());
        assert!(payload_bytes + NETWORK_HEADERS <= PATH_MTU);
        let mut model = self.model.lock();
        assert!(model.synthetic.is_none());
        let (complete, observed) = oneshot::channel();
        model.synthetic = Some(Synthetic {
            next: Instant::now(),
            interval,
            remaining: packets,
            payload_bytes,
            application_progress,
            complete,
        });
        drop(model);
        self.changed.notify_one();
        observed
    }

    fn counters(&self) -> Counters {
        self.model.lock().counters.clone()
    }

    async fn run(self: Arc<Self>, stop: CancellationToken) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let (next, wakes) = {
                let mut model = self.model.lock();
                let wakes = model.advance(Instant::now());
                (model.next_event().map(|(at, _)| at), wakes)
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
struct ModelTransport {
    address: SocketAddr,
    network: Arc<Network>,
}

impl Transport for ModelTransport {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let packet = poll_fn(|cx| {
            let (result, wakes) = {
                let mut model = self.network.model.lock();
                let wakes = model.advance(Instant::now());
                let result = match model.receive(self.address) {
                    Some(packet) => Poll::Ready(packet),
                    None => {
                        model.endpoints.get_mut(&self.address).unwrap().waker =
                            Some(cx.waker().clone());
                        Poll::Pending
                    }
                };
                (result, wakes)
            };
            for waker in wakes {
                waker.wake();
            }
            result
        })
        .await;
        assert!(packet.data.len() <= buf.len());
        buf[..packet.data.len()].copy_from_slice(&packet.data);
        Ok((packet.data.len(), packet.source))
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> io::Result<usize> {
        self.network.send(self.address, target, buf)
    }

    fn poll_send_to(
        &self,
        _cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.network.send(self.address, target, buf))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.address
    }
    fn link_mtu(&self) -> Option<u16> {
        Some(PATH_MTU as u16)
    }
    fn dont_fragment(&self) -> bool {
        true
    }
}

impl PollSendToVectored for ModelTransport {
    fn poll_send_to_vectored(
        &self,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let data: Vec<u8> = bufs.iter().flat_map(|buf| buf.iter().copied()).collect();
        Poll::Ready(self.network.send(self.address, target, &data))
    }
}

#[derive(Clone)]
struct ModelEnvironment(Arc<AtomicU16>);

impl UtpEnvironment for ModelEnvironment {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn copy(&self) -> Self {
        self.clone()
    }
    fn random_u16(&self) -> u16 {
        self.0.fetch_add(101, Ordering::Relaxed)
    }
}

fn address(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

fn plain_config() -> Config {
    Config {
        bytes_per_second: [1000; 2],
        service: [Bounds {
            bytes: 1000,
            packets: 16,
        }; 2],
        propagation: Bounds {
            bytes: 1000,
            packets: 16,
        },
        delivery: Bounds {
            bytes: 1000,
            packets: 16,
        },
        one_way: Duration::from_millis(50),
        jitter: [Duration::ZERO; 4],
        lose_every_data: None,
    }
}

fn control(model: &mut Model, at: Instant, byte: u8) {
    model.enqueue(
        0,
        at,
        address(1),
        Some(address(2)),
        vec![byte; 100 - NETWORK_HEADERS],
        Kind::Control,
    );
}

#[tokio::test(start_paused = true)]
async fn byte_and_packet_service_limits_are_independent() {
    let now = Instant::now();
    let mut bytes = Model::new(Config {
        service: [Bounds {
            bytes: 150,
            packets: 8,
        }; 2],
        ..plain_config()
    });
    control(&mut bytes, now, 1);
    control(&mut bytes, now, 2);
    assert_eq!(bytes.lanes[0].usage.bytes, 100);
    assert_eq!(bytes.counters.service_byte_drops, 1);
    assert_eq!(bytes.counters.service_packet_drops, 0);

    let mut packets = Model::new(Config {
        service: [Bounds {
            bytes: 1000,
            packets: 1,
        }; 2],
        ..plain_config()
    });
    control(&mut packets, now, 1);
    control(&mut packets, now, 2);
    assert_eq!(packets.lanes[0].usage.packets, 1);
    assert_eq!(packets.counters.service_packet_drops, 1);
    assert_eq!(packets.counters.service_byte_drops, 0);
}

#[tokio::test(start_paused = true)]
async fn two_flows_share_serial_service_before_propagation_and_delivery() {
    let now = Instant::now();
    let mut model = Model::new(Config {
        service: [Bounds {
            bytes: 200,
            packets: 2,
        }; 2],
        ..plain_config()
    });
    model.endpoint(address(2), 1, None);
    model.enqueue(
        0,
        now,
        address(1),
        Some(address(2)),
        vec![1; 100 - NETWORK_HEADERS],
        Kind::Data(0),
    );
    model.enqueue(
        0,
        now,
        address(3),
        Some(address(2)),
        vec![2; 100 - NETWORK_HEADERS],
        Kind::Data(1),
    );
    assert_eq!(
        model.lanes[0].queue[0].finishes,
        now + Duration::from_millis(100)
    );
    assert_eq!(
        model.lanes[0].queue[1].finishes,
        now + Duration::from_millis(200)
    );
    drop(model.advance(now + Duration::from_millis(100)));
    assert_eq!(model.lanes[0].usage.bytes, 100);
    assert_eq!(model.propagation_usage.bytes, 100);
    assert!(model.endpoints[&address(2)].queue.is_empty());
    control(&mut model, now + Duration::from_millis(100), 3);
    assert_eq!(model.lanes[0].usage.bytes, 200);
    drop(model.advance(now + Duration::from_millis(150)));
    assert_eq!(model.receive(address(2)).unwrap().data[0], 1);
    assert_eq!(model.propagation_usage.bytes, 0);
    drop(model.advance(now + Duration::from_millis(350)));
    assert_eq!(model.receive(address(2)).unwrap().data[0], 2);
    assert_eq!(model.receive(address(2)).unwrap().data[0], 3);
    assert_eq!(model.counters.served_wire_bytes[0], 300);
    assert_eq!(model.counters.service_byte_drops, 0);
    assert_eq!(model.counters.max_queue_delay, Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn propagation_and_endpoint_buffers_have_separate_finite_bounds() {
    let now = Instant::now();
    for bounds in [
        Bounds {
            bytes: 100,
            packets: 8,
        },
        Bounds {
            bytes: 1000,
            packets: 1,
        },
    ] {
        let mut model = Model::new(Config {
            propagation: bounds,
            delivery: bounds,
            one_way: Duration::from_secs(1),
            ..plain_config()
        });
        model.endpoint(address(2), 1, None);
        for byte in 1..=3 {
            control(&mut model, now, byte);
        }
        drop(model.advance(now + Duration::from_millis(500)));
        assert_eq!(model.propagation_usage.bytes, 100);
        assert_eq!(model.counters.propagation_drops, 2);
        drop(model.advance(now + Duration::from_millis(1100)));
        assert_eq!(model.endpoints[&address(2)].usage.bytes, 100);
        control(&mut model, now + Duration::from_millis(1100), 4);
        drop(model.advance(now + Duration::from_millis(2200)));
        assert_eq!(model.counters.delivery_drops, 1);
        assert_eq!(model.receive(address(2)).unwrap().data[0], 1);
        assert_eq!(model.endpoints[&address(2)].usage.bytes, 0);
        assert_eq!(model.counters.propagation_peak.packets, 1);
        assert_eq!(model.counters.delivery_peak.packets, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn scripted_loss_and_jitter_produce_exact_delivery_order() {
    let now = Instant::now();
    let mut model = Model::new(Config {
        bytes_per_second: [10_000; 2],
        one_way: Duration::from_millis(10),
        jitter: [
            Duration::from_millis(30),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ],
        lose_every_data: Some(3),
        ..plain_config()
    });
    model.endpoint(address(2), 1, None);
    for byte in 1..=3 {
        model.enqueue(
            0,
            now,
            address(1),
            Some(address(2)),
            vec![byte; 100 - NETWORK_HEADERS],
            Kind::Data(0),
        );
    }
    drop(model.advance(now + Duration::from_millis(100)));
    assert_eq!(model.receive(address(2)).unwrap().data[0], 2);
    assert_eq!(model.receive(address(2)).unwrap().data[0], 1);
    assert!(model.receive(address(2)).is_none());
    assert_eq!(model.counters.scripted_loss, 1);
    assert_eq!(model.counters.reordered, 1);
}

fn payload(flow: usize, bytes: usize) -> Vec<u8> {
    (0..bytes)
        .map(|index| ((index + flow * 73) % 251) as u8)
        .collect()
}

#[tokio::test(start_paused = true)]
async fn two_utp_flows_complete_over_one_queue_with_paced_synthetic_bulk_load() {
    let config = Config::default();
    let network = Network::new(config.clone());
    let transports = [
        network.transport(address(1001), 0, Some(0)),
        network.transport(address(1002), 0, Some(1)),
        network.transport(address(2001), 1, None),
        network.transport(address(2002), 1, None),
    ];
    let stop = CancellationToken::new();
    let sockets: Vec<_> = transports
        .iter()
        .enumerate()
        .map(|(index, transport)| {
            UtpSocket::new_with_opts(
                transport.clone(),
                ModelEnvironment(Arc::new(AtomicU16::new(100 + index as u16 * 1000))),
                SocketOpts {
                    cancellation_token: stop.clone(),
                    ..Default::default()
                },
            )
            .unwrap()
        })
        .collect();
    let driver = tokio::spawn(Arc::clone(&network).run(stop.clone()));
    let (ready0_tx, ready0_rx) = oneshot::channel();
    let (ready1_tx, ready1_rx) = oneshot::channel();
    let (start0_tx, start0_rx) = oneshot::channel();
    let (start1_tx, start1_rx) = oneshot::channel();
    let application_progress = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
    let send0 = async {
        let mut stream = sockets[0].connect(address(2001)).await.unwrap();
        ready0_tx.send(()).unwrap();
        start0_rx.await.unwrap();
        stream.write_all(&payload(0, 128 * 1024)).await.unwrap();
        stream.flush().await.unwrap();
        stream
    };
    let send1 = async {
        let mut stream = sockets[1].connect(address(2002)).await.unwrap();
        ready1_tx.send(()).unwrap();
        start1_rx.await.unwrap();
        stream.write_all(&payload(1, 128 * 1024)).await.unwrap();
        stream.flush().await.unwrap();
        stream
    };
    let receive0 = async {
        let mut stream = sockets[2].accept().await.unwrap();
        let mut bytes = vec![0; 128 * 1024];
        let mut read = 0;
        while read < bytes.len() {
            let count = stream.read(&mut bytes[read..]).await.unwrap();
            assert!(count > 0);
            read += count;
            application_progress[0].store(read, Ordering::Relaxed);
        }
        assert_eq!(bytes, payload(0, bytes.len()));
        stream
    };
    let receive1 = async {
        let mut stream = sockets[3].accept().await.unwrap();
        let mut bytes = vec![0; 128 * 1024];
        let mut read = 0;
        while read < bytes.len() {
            let count = stream.read(&mut bytes[read..]).await.unwrap();
            assert!(count > 0);
            read += count;
            application_progress[1].store(read, Ordering::Relaxed);
        }
        assert_eq!(bytes, payload(1, bytes.len()));
        stream
    };
    let start = async {
        ready0_rx.await.unwrap();
        ready1_rx.await.unwrap();
        let completed = network.start_synthetic(
            256,
            1200,
            Duration::from_millis(12),
            Arc::clone(&application_progress),
        );
        start0_tx.send(()).unwrap();
        start1_tx.send(()).unwrap();
        let progress_at_last_arrival = completed.await.unwrap();
        assert!(
            progress_at_last_arrival.into_iter().all(|bytes| bytes > 0),
            "both applications read data while paced synthetic arrivals were active"
        );
    };
    let _streams = tokio::join!(send0, send1, receive0, receive1, start);
    let counters = network.counters();
    assert_eq!(counters.synthetic_arrived, 256);
    assert!(counters.scripted_loss > 0);
    assert!(counters.reordered > 0);
    assert!(counters.synthetic_served > 0);
    assert!(counters.synthetic_delivered > 0);
    assert!(
        counters
            .data_while_synthetic_arrivals_active
            .iter()
            .all(|packets| *packets > 0)
    );
    assert_eq!(counters.propagation_drops, 0);
    assert_eq!(counters.delivery_drops, 0);
    for lane in 0..2 {
        assert!(counters.service_peak[lane].bytes <= config.service[lane].bytes);
        assert!(counters.service_peak[lane].packets <= config.service[lane].packets);
    }
    assert!(counters.propagation_peak.bytes <= config.propagation.bytes);
    assert!(counters.propagation_peak.packets <= config.propagation.packets);
    assert!(counters.delivery_peak.bytes <= config.delivery.bytes);
    assert!(counters.delivery_peak.packets <= config.delivery.packets);
    println!("BOUNDED MODEL synthetic bulk competition: {counters:?}");
    for socket in &sockets {
        socket.shutdown().await.unwrap();
    }
    stop.cancel();
    driver.await.unwrap();
}

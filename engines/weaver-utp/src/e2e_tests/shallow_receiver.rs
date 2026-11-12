// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A receiver with a shallow datagram queue that its owner reads only once
// per drain interval, run on tokio's paused clock: what a peer whose UDP
// socket keeps the host's default receive buffer looks like to a sender.
// Every datagram that arrives while the queue already holds its bound is
// lost before the receiver sees it. A sender's socket may hold a bound of
// its own, put on the wire once per interval: a send it has no room for
// waits, as a full kernel send buffer makes it.

use std::{
    collections::{BTreeMap, VecDeque},
    future::poll_fn,
    io::{self, IoSlice},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::{Arc, atomic::Ordering},
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

use crate::{
    PollSendToVectored, SocketOpts, Transport, UtpSocket, stream_dispatch::ExpiryCause,
    traits::DefaultUtpEnvironment,
};

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
    // Data datagrams that carried less than a full packet, and the payload
    // bytes of every data datagram.
    short_data: u64,
    data_payload: u64,
    // The most datagrams on the wire at once.
    peak_in_flight: usize,
    // The most data datagrams one endpoint sent at one instant: on the
    // paused clock, every pass of a stream at that instant together.
    peak_data_burst: usize,
    // Drain intervals that passed before the receiver had everything.
    drains: u64,
    // The sender's retransmission timeouts, packets it resent on SACK
    // evidence, and times it entered recovery.
    rtos: u64,
    fast_resends: u64,
    // Sends the sender's own socket refused.
    refused: u64,
    recoveries: u64,
    max_cwnd: u64,
    // Data datagrams to the receiver lost at random on the way.
    lost_on_path: u64,
    // The stream ended before every byte arrived.
    died: bool,
}

// What the sender's retransmission timeouts came from, by cause.
#[derive(Debug, Default)]
struct Causes(BTreeMap<ExpiryCause, u64>);

// Data datagrams to the receiver lost on the way: a fixed pseudo-random draw,
// so every run loses the same ones.
struct Loss {
    state: u64,
    per_mille: u64,
}

impl Loss {
    fn lost(&mut self) -> bool {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 33) % 1000 < self.per_mille
    }
}

struct Endpoint {
    shallow: Option<Shallow>,
    // The bound on datagrams the socket holds before they go on the wire,
    // what it holds, and the send that waits for room.
    outbound: Option<Shallow>,
    out_queue: VecDeque<(SocketAddr, Vec<u8>)>,
    send_waker: Option<Waker>,
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
    next_out_drain: Option<Instant>,
    counters: Counters,
    // The instant and endpoint of the last data datagram sent, and how many
    // that endpoint sent then.
    burst: Option<(Instant, SocketAddr, usize)>,
    loss: Option<Loss>,
}

impl Model {
    fn next_event(&self) -> Option<Instant> {
        let delivery = self.in_flight.first_key_value().map(|((at, _), _)| *at);
        delivery
            .into_iter()
            .chain(self.next_drain)
            .chain(self.next_out_drain)
            .min()
    }

    fn put_on_wire(&mut self, at: Instant, source: SocketAddr, target: SocketAddr, data: Vec<u8>) {
        self.ordinal += 1;
        let key = (at + self.one_way, self.ordinal);
        self.in_flight.insert(key, (source, target, data));
        self.counters.peak_in_flight = self.counters.peak_in_flight.max(self.in_flight.len());
    }

    // A drain at an instant comes before deliveries at that instant: they
    // count toward the next interval.
    fn advance(&mut self, now: Instant) -> Vec<Waker> {
        let mut wakes = Vec::new();
        loop {
            if let Some(drain) = self.next_out_drain.filter(|&drain| drain <= now) {
                let mut every = None;
                let mut sent = Vec::new();
                for (address, endpoint) in &mut self.endpoints {
                    let Some(outbound) = endpoint.outbound else {
                        continue;
                    };
                    every = Some(outbound.drain_every);
                    for (target, data) in endpoint.out_queue.drain(..) {
                        sent.push((*address, target, data));
                    }
                    wakes.extend(endpoint.send_waker.take());
                }
                for (source, target, data) in sent {
                    self.put_on_wire(drain, source, target, data);
                }
                self.next_out_drain = every.map(|every| drain + every);
                continue;
            }
            let delivery = self.in_flight.first_key_value().map(|((at, _), _)| *at);
            match (self.next_drain, delivery) {
                (Some(drain), _) if drain <= now && delivery.is_none_or(|at| drain <= at) => {
                    let mut every = None;
                    self.counters.drains += 1;
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
                    let is_data =
                        data.first().map(|byte| byte >> 4) == Some(crate::raw::Type::Data as u8);
                    if endpoint.shallow.is_some()
                        && is_data
                        && self.loss.as_mut().is_some_and(Loss::lost)
                    {
                        self.counters.lost_on_path += 1;
                        continue;
                    }
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
    fn new(one_way: Duration, loss: Option<Loss>) -> Arc<Self> {
        Arc::new(Self {
            model: Mutex::new(Model {
                one_way,
                in_flight: BTreeMap::new(),
                ordinal: 0,
                endpoints: BTreeMap::new(),
                next_drain: None,
                next_out_drain: None,
                counters: Counters::default(),
                burst: None,
                loss,
            }),
            changed: Notify::new(),
        })
    }

    fn transport(
        self: &Arc<Self>,
        address: SocketAddr,
        shallow: Option<Shallow>,
        outbound: Option<Shallow>,
    ) -> Link {
        let mut model = self.model.lock();
        if let Some(shallow) = shallow {
            model.next_drain = Some(Instant::now() + shallow.drain_every);
        }
        if let Some(outbound) = outbound {
            model.next_out_drain = Some(Instant::now() + outbound.drain_every);
        }
        model.endpoints.insert(
            address,
            Endpoint {
                shallow,
                outbound,
                out_queue: VecDeque::new(),
                send_waker: None,
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
        match self.poll_send(None, source, target, || data) {
            Poll::Ready(sent) => sent,
            Poll::Pending => unreachable!("a send without a waker never waits"),
        }
    }

    // A send the source's socket has no room for waits for the next time
    // it puts its datagrams on the wire.
    fn poll_send(
        &self,
        cx: Option<&mut Context<'_>>,
        source: SocketAddr,
        target: SocketAddr,
        data: impl FnOnce() -> Vec<u8>,
    ) -> Poll<io::Result<usize>> {
        let (len, wakes) = {
            let mut model = self.model.lock();
            let now = Instant::now();
            let wakes = model.advance(now);
            let endpoint = model.endpoints.get_mut(&source).unwrap();
            let queued = match (endpoint.outbound, cx) {
                (Some(outbound), Some(cx)) => {
                    if endpoint.out_queue.len() >= outbound.queue_packets {
                        endpoint.send_waker = Some(cx.waker().clone());
                        drop(model);
                        for waker in wakes {
                            waker.wake();
                        }
                        return Poll::Pending;
                    }
                    true
                }
                _ => false,
            };
            let data = data();
            let len = data.len();
            if data.first().map(|byte| byte >> 4) == Some(crate::raw::Type::Data as u8) {
                model.counters.data_sent += 1;
                let (_, header_len) = crate::raw::UtpHeader::deserialize(&data).unwrap();
                let payload = (len - header_len) as u64;
                model.counters.data_payload += payload;
                if payload < PACKET_PAYLOAD as u64 {
                    model.counters.short_data += 1;
                }
                let burst = match model.burst {
                    Some((at, from, sent)) if at == now && from == source => sent + 1,
                    _ => 1,
                };
                model.burst = Some((now, source, burst));
                model.counters.peak_data_burst = model.counters.peak_data_burst.max(burst);
            }
            if queued {
                let endpoint = model.endpoints.get_mut(&source).unwrap();
                endpoint.out_queue.push_back((target, data));
            } else {
                model.put_on_wire(now, source, target, data);
            }
            (len, wakes)
        };
        for waker in wakes {
            waker.wake();
        }
        self.changed.notify_one();
        Poll::Ready(Ok(len))
    }

    // Brought up to now first, so drains that are due are counted.
    fn counters(&self) -> Counters {
        let (counters, wakes) = {
            let mut model = self.model.lock();
            let wakes = model.advance(Instant::now());
            (model.counters, wakes)
        };
        for waker in wakes {
            waker.wake();
        }
        counters
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
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.network
            .poll_send(Some(cx), self.address, target, || buf.to_vec())
    }

    fn bind_addr(&self) -> SocketAddr {
        self.address
    }

    fn link_mtu(&self) -> Option<u16> {
        Some(1500)
    }
}

impl PollSendToVectored for Link {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.network.poll_send(Some(cx), self.address, target, || {
            bufs.iter().flat_map(|buf| buf.iter().copied()).collect()
        })
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
async fn transfer(bytes: usize, one_way: Duration, shallow: Option<Shallow>) -> (Counters, Causes) {
    transfer_in_rounds(
        bytes,
        one_way,
        shallow,
        None,
        NonZeroUsize::new(1024 * 1024),
    )
    .await
}

// [`transfer`], with the writer handing the stream `per_round` bytes at the
// start of each round, as an upload rate limit grants a round's quota at
// once and the peer writes it in one go.
async fn transfer_in_rounds(
    bytes: usize,
    one_way: Duration,
    shallow: Option<Shallow>,
    rounds: Option<(usize, Duration)>,
    tx_buffer_initial: Option<NonZeroUsize>,
) -> (Counters, Causes) {
    let writes = match rounds {
        None => Writes::Whole,
        Some((per_round, round)) => Writes::Rounds(per_round, round),
    };
    transfer_with(
        bytes,
        one_way,
        shallow,
        Upload {
            writes,
            reads: None,
            tx_buffer_initial,
            send_queue: None,
            loss_per_mille: 0,
            loss_seed: 0,
        },
    )
    .await
}

// How the writer hands the stream its bytes.
#[derive(Clone, Copy)]
enum Writes {
    // In one write.
    Whole,
    // `.0` bytes at the start of each round of `.1`.
    Rounds(usize, Duration),
    // Writes of these sizes in turn, as a peer writes a message header and
    // then its block.
    Pieces(&'static [usize]),
}

#[derive(Clone, Copy)]
struct Upload {
    writes: Writes,
    // The receiving application reads `.0` bytes every `.1`, so the window
    // it advertises closes and opens by that much; otherwise it reads as
    // fast as the bytes come.
    reads: Option<(usize, Duration)>,
    tx_buffer_initial: Option<NonZeroUsize>,
    // The sender's socket holds this many datagrams and puts them on the
    // wire once an interval; otherwise each goes out as it is sent.
    send_queue: Option<Shallow>,
    // Data datagrams to a shallow receiver lost at random on the way, per
    // thousand, drawn from the seed.
    loss_per_mille: u64,
    loss_seed: u64,
}

async fn transfer_with(
    bytes: usize,
    one_way: Duration,
    shallow: Option<Shallow>,
    upload: Upload,
) -> (Counters, Causes) {
    let Upload {
        writes,
        reads,
        tx_buffer_initial,
        send_queue,
        loss_per_mille,
        loss_seed,
    } = upload;
    let network = Network::new(
        one_way,
        (loss_per_mille > 0).then_some(Loss {
            state: loss_seed,
            per_mille: loss_per_mille,
        }),
    );
    let stop = CancellationToken::new();
    let socket = |transport| {
        UtpSocket::new_with_opts(
            transport,
            DefaultUtpEnvironment {},
            SocketOpts {
                cancellation_token: stop.clone(),
                vsock_tx_bufsize_bytes_initial: tx_buffer_initial,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let sender = socket(network.transport(address(1), None, send_queue));
    let receiver = socket(network.transport(address(2), shallow, None));
    let driver = tokio::spawn(Arc::clone(&network).run(stop.clone()));

    let send = async {
        let mut stream = sender.connect(address(2)).await.unwrap();
        let data = payload(bytes);
        match writes {
            Writes::Whole => stream.write_all(&data).await?,
            Writes::Rounds(per_round, round) => {
                let mut rounds = tokio::time::interval(round);
                for lump in data.chunks(per_round) {
                    rounds.tick().await;
                    stream.write_all(lump).await?;
                }
            }
            Writes::Pieces(sizes) => {
                let mut rest = &data[..];
                for size in sizes.iter().cycle() {
                    if rest.is_empty() {
                        break;
                    }
                    let (piece, after) = rest.split_at((*size).min(rest.len()));
                    stream.write_all(piece).await?;
                    rest = after;
                }
            }
        }
        stream.flush().await?;
        io::Result::Ok(stream)
    };
    let receive = async {
        let mut stream = receiver.accept().await.unwrap();
        let mut received = vec![0; bytes];
        match reads {
            None => stream.read_exact(&mut received).await?,
            Some((per_read, every)) => {
                let mut reads = tokio::time::interval(every);
                for chunk in received.chunks_mut(per_read) {
                    reads.tick().await;
                    stream.read_exact(chunk).await?;
                }
                bytes
            }
        };
        let drains = network.counters().drains;
        assert!(received == payload(bytes), "the payload arrived intact");
        io::Result::Ok((stream, drains))
    };
    let (sent, received) = tokio::join!(send, receive);
    // Only a run with random loss may end in a dead stream; it is counted.
    let died = sent.is_err() || received.is_err();
    assert!(
        loss_per_mille > 0 || !died,
        "{:?} {:?}",
        sent.err(),
        received.as_ref().err()
    );
    let drains = received.map_or_else(|_| network.counters().drains, |(_, drains)| drains);
    let stats = sender.stats();
    let counters = Counters {
        died,
        drains,
        rtos: stats.rto_timeouts_count.load(Ordering::Relaxed),
        refused: stats.send_poll_pending.load(Ordering::Relaxed),
        fast_resends: stats
            .recovery_retransmitted_segments_count
            .load(Ordering::Relaxed),
        recoveries: stats.packets_lost.load(Ordering::Relaxed),
        max_cwnd: stats.max_congestion_window_bytes.load(Ordering::Relaxed),
        ..network.counters()
    };
    let mut causes = Causes::default();
    for cause in sender.state.expiry_causes.lock().iter() {
        *causes.0.entry(*cause).or_default() += 1;
    }
    sender.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
    stop.cancel();
    driver.await.unwrap();
    (counters, causes)
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
    let run = transfer(4 * 1024 * 1024, Duration::from_millis(50), Some(shallow)).await;
    report("50 ms", &run);
    let counters = run.0;
    assert!(counters.data_sent > 2800, "{counters:?}");
    // The window opened past what the queue holds.
    assert!(counters.peak_in_flight > 48, "{counters:?}");
    assert!(counters.peak_arrivals <= 48, "{counters:?}");
    assert_eq!(counters.dropped, 0, "{counters:?}");
}

// Payload bytes in one full data packet on the test links' 1500-byte MTU.
const PACKET_PAYLOAD: usize = 1452;

fn report(case: &str, (counters, causes): &(Counters, Causes)) {
    eprintln!(
        "{case}:{} lost {} of {} data packets ({:.1}%), {} resent on SACK evidence, {} timeouts, {} recoveries, window at most {} bytes; timeouts by cause {:?}; {counters:?}",
        if counters.died { " DIED," } else { "" },
        counters.dropped + counters.lost_on_path,
        counters.data_sent,
        (counters.dropped + counters.lost_on_path) as f64 * 100.0
            / counters.data_sent.max(1) as f64,
        counters.fast_resends,
        counters.rtos,
        counters.recoveries,
        counters.max_cwnd,
        causes.0,
    );
}

// The same receiver 1 ms and 0.1 ms away. The window overflows the queue
// and packets are lost, as they are for libtorrent there; every hole is
// resent on SACK evidence, so no retransmission timer fires, and the
// receiver gets at least half of what it can drain: 24 full packets for
// every millisecond it reads.
#[tokio::test(start_paused = true)]
async fn a_short_path_recovers_its_losses_without_timeouts() {
    const BYTES: usize = 4 * 1024 * 1024;
    let shallow = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(1),
    };
    for one_way in [Duration::from_micros(500), Duration::from_micros(50)] {
        let run = transfer(BYTES, one_way, Some(shallow)).await;
        report(&format!("one way {one_way:?}"), &run);
        let counters = run.0;
        assert!(counters.data_sent > 2800, "{counters:?}");
        assert_eq!(counters.rtos, 0, "{one_way:?}: {counters:?}");
        assert!(counters.fast_resends >= counters.dropped, "{counters:?}");
        assert!(
            BYTES / PACKET_PAYLOAD >= 24 * counters.drains as usize,
            "{one_way:?}: {counters:?}"
        );
    }
}

// An upload capped at 5 MB/s, 1 ms away: each half-second round hands the
// stream 2.5 MB at once after an idle gap. Holes inside a lump are resent
// on SACK evidence. A hole in the last packets of a lump has nothing sent
// after it to show it lost, so only the retransmission timer finds it, as
// it does for libtorrent: at most one timeout per lump.
#[tokio::test(start_paused = true)]
async fn a_capped_upload_times_out_only_on_the_tail_of_a_lump() {
    const BYTES: usize = 8 * 1024 * 1024;
    const LUMP: usize = 2_500_000;
    let shallow = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(1),
    };
    let rounds = Some((LUMP, Duration::from_millis(500)));
    let run = transfer_in_rounds(
        BYTES,
        Duration::from_micros(500),
        Some(shallow),
        rounds,
        NonZeroUsize::new(1024 * 1024),
    )
    .await;
    report("capped", &run);
    let counters = run.0;
    assert!(counters.data_sent > 5700, "{counters:?}");
    assert!(counters.rtos <= BYTES.div_ceil(LUMP) as u64, "{counters:?}");
}

// A receiver with an ample queue 1 ms and 0.1 ms away: the window, not the
// pacer, sets the rate. ACKs release what they report delivered, so the
// flight grows past anything one burst a wake allows.
#[tokio::test(start_paused = true)]
async fn a_deep_receiver_is_fed_at_the_rate_of_the_window() {
    const BYTES: usize = 64 * 1024 * 1024;
    for one_way in [Duration::from_micros(500), Duration::from_micros(50)] {
        let started = Instant::now();
        let (counters, _) = transfer(BYTES, one_way, None).await;
        let elapsed = started.elapsed();
        eprintln!(
            "deep, one way {one_way:?}: {counters:?}, {:.0} MB/s of the paused clock",
            BYTES as f64 / elapsed.as_secs_f64() / 1e6
        );
        assert_eq!(counters.dropped, 0, "{counters:?}");
        // More in flight at once than sixteen packets a millisecond allows
        // over the round trip.
        assert!(counters.peak_in_flight > 64, "{one_way:?}: {counters:?}");
    }
}

// The same deep receiver from a send buffer that starts at its default
// 32 KiB: the buffer grows with the window, so the flight reaches what the
// megabyte buffer allows.
#[tokio::test(start_paused = true)]
async fn the_default_send_buffer_grows_to_the_window() {
    const BYTES: usize = 64 * 1024 * 1024;
    let one_way = Duration::from_micros(500);
    let started = Instant::now();
    let (grown, _) = transfer_in_rounds(BYTES, one_way, None, None, None).await;
    eprintln!(
        "deep from the default buffer: {grown:?}, {:.0} MB/s of the paused clock",
        BYTES as f64 / started.elapsed().as_secs_f64() / 1e6
    );
    let (full, _) = transfer(BYTES, one_way, None).await;
    assert_eq!(grown.dropped, 0, "{grown:?}");
    assert!(
        grown.peak_in_flight * 10 >= full.peak_in_flight * 9,
        "{grown:?} against {full:?}"
    );
}

// A peer writes each block as a 13-byte message header and then 16 KiB of
// data, or in many small writes. Every data packet but the last carries a
// full packet's payload: a part-filled one waits for more while data is in
// flight.
#[tokio::test(start_paused = true)]
async fn a_writer_in_pieces_fills_every_packet() {
    const BYTES: usize = 16 * 1024 * 1024;
    let one_way = Duration::from_micros(500);
    let mut results = Vec::new();
    for (case, sizes, reads, tx_buffer_initial) in [
        ("header and block", &[13, 16384][..], None, None),
        (
            "header and block, megabyte buffer",
            &[13, 16384][..],
            None,
            NonZeroUsize::new(1024 * 1024),
        ),
        ("small writes", &[100][..], None, None),
        (
            "header and block, slow reader",
            &[13, 16384][..],
            Some((16384, Duration::from_micros(200))),
            None,
        ),
    ] {
        let started = Instant::now();
        let (counters, _) = transfer_with(
            BYTES,
            one_way,
            None,
            Upload {
                writes: Writes::Pieces(sizes),
                reads,
                tx_buffer_initial,
                send_queue: None,
                loss_per_mille: 0,
                loss_seed: 0,
            },
        )
        .await;
        eprintln!(
            "{case}: {} data packets, {} short, {:.0} payload bytes each, {:?}; {counters:?}",
            counters.data_sent,
            counters.short_data,
            counters.data_payload as f64 / counters.data_sent.max(1) as f64,
            started.elapsed(),
        );
        results.push((case, counters));
    }
    for (case, counters) in results {
        assert_eq!(counters.dropped, 0, "{case}: {counters:?}");
        // Short packets are the probes that find the path's size and the
        // last tail: one in a hundred at most.
        assert!(
            counters.short_data * 100 <= counters.data_sent,
            "{case}: {counters:?}"
        );
    }
}

// An upload capped at 20 MB/s, a second's quota written at once, to the
// same receiver 1 ms away: about one datagram in eight is lost. Through a
// sender socket that takes 48 datagrams every 2 ms, many sends are
// refused as well. The stream never dies and every byte arrives; only the
// tail of a lump waits for the retransmission timer.
#[tokio::test(start_paused = true)]
async fn a_capped_upload_through_a_full_socket_never_dies() {
    const BYTES: usize = 64 * 1024 * 1024;
    const LUMP: usize = 20_000_000;
    let shallow = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(1),
    };
    let full_socket = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(2),
    };
    for (case, send_queue) in [("open socket", None), ("full socket", Some(full_socket))] {
        let run = transfer_with(
            BYTES,
            Duration::from_micros(500),
            Some(shallow),
            Upload {
                writes: Writes::Rounds(LUMP, Duration::from_secs(1)),
                reads: None,
                tx_buffer_initial: None,
                send_queue,
                loss_per_mille: 0,
                loss_seed: 0,
            },
        )
        .await;
        report(case, &run);
        let counters = run.0;
        assert!(counters.data_sent > 46_000, "{case}: {counters:?}");
        assert!(
            counters.rtos <= BYTES.div_ceil(LUMP) as u64,
            "{case}: {counters:?}"
        );
        if send_queue.is_some() {
            assert!(counters.refused > 0, "{case}: {counters:?}");
        } else {
            assert!(
                counters.dropped * 10 > counters.data_sent,
                "{case}: {counters:?}"
            );
        }
    }
}

// The shallow receiver 1 ms away, and one data datagram in eight to it
// lost at random besides, as an upload capped at 20 MB/s writes a second's
// quota at once, for four loss draws. Every timeout is counted by its
// cause, and a stream that dies is counted, not failed: three sends of one
// packet lost in a row end a stream, as they do in libtorrent
// (utp_stream.cpp:3523), and with one in eight lost that is likely over
// 64 MiB.
#[tokio::test(start_paused = true)]
async fn timeouts_under_random_loss_by_cause() {
    const BYTES: usize = 64 * 1024 * 1024;
    const LUMP: usize = 20_000_000;
    let shallow = Shallow {
        queue_packets: 48,
        drain_every: Duration::from_millis(1),
    };
    for loss_seed in 1..=4 {
        let run = transfer_with(
            BYTES,
            Duration::from_micros(500),
            Some(shallow),
            Upload {
                writes: Writes::Rounds(LUMP, Duration::from_secs(1)),
                reads: None,
                tx_buffer_initial: None,
                send_queue: None,
                loss_per_mille: 120,
                loss_seed,
            },
        )
        .await;
        report(&format!("12% random loss, draw {loss_seed}"), &run);
        let (counters, _) = &run;
        assert!(
            counters.lost_on_path * 10 > counters.data_sent,
            "{counters:?}"
        );
    }
}

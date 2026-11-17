// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How an outgoing connection to a peer is opened: one dialer per
//! transport, raced in the order and with the delays a [`DialPlan`] gives.
//!
//! A dial is made once per connection attempt, never per message or
//! packet: the dialers are reached through `&dyn PeerDialer`, and each step
//! of the race is one boxed future for the attempt's life.

use std::{
    net::SocketAddr,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::Duration,
};

use futures::future::BoxFuture;
use tracing::debug;

use super::{ConnectionKind, Failed, Failure, Link, SingleStatAtomic, Transports};
use crate::{
    DialErrors, Error, Result,
    network::{SocketFactory, TcpRoom, TcpWant, UtpSocket},
};

/// The TCP want a torrent holds while it wants TCP room its route gives
/// others: the route counts the torrent pressing for the room for as long.
/// A look before a TCP dial that finds the route crowded out leaves one, and
/// a later such look replaces it. It is held, with no gap, until a TCP dial
/// is no longer blocked or no longer possible: a look that finds room or
/// none owed, a wait of the connect loop in which it can dial no one
/// (`idle`: at its peer limit, at the session's, wanting no peers, or with
/// no candidate at all), or a pause. Waiting for a candidate's reconnect
/// time is none of these: the torrent still wants the room then. A look
/// that lands while the loop is in such a wait, or after a pause, leaves no
/// want: the state it is checked against is read under the lock it is
/// stored under.
#[derive(Debug, Default)]
pub(crate) struct TcpWants {
    want: parking_lot::Mutex<Wanted>,
}

#[derive(Debug, Default)]
struct Wanted {
    want: Option<TcpWant>,
    // The torrent was paused: no look leaves a want again.
    closed: bool,
    // The connect loop waits where it can dial no one: no look leaves a
    // want until it is out of the wait.
    idle: bool,
}

impl TcpWants {
    // A look before a TCP dial found `room`. Only a crowded one leaves a
    // want, and none while the loop can dial no one or once the torrent is
    // paused.
    pub(crate) fn looked(&self, room: TcpRoom) {
        let old = {
            let mut wanted = self.want.lock();
            match room {
                TcpRoom::Crowded(want) if !wanted.closed && !wanted.idle => {
                    wanted.want.replace(want)
                }
                // Refused while idle or paused, the want goes with this look.
                TcpRoom::Crowded(_) | TcpRoom::Free | TcpRoom::Full => wanted.want.take(),
            }
        };
        drop(old);
    }

    /// The torrent is paused: it wants no TCP socket, and no dial still
    /// under way leaves one after.
    pub(crate) fn close(&self) {
        let old = {
            let mut wanted = self.want.lock();
            wanted.closed = true;
            wanted.want.take()
        };
        drop(old);
    }

    /// Whether the torrent holds a want.
    #[cfg(test)]
    pub(crate) fn holds(&self) -> bool {
        self.want.lock().want.is_some()
    }

    /// Waits out `wait`, a wait of the connect loop in which it can dial no
    /// one: the want goes on entry, and none is left while it lasts.
    pub(crate) async fn idle<T>(&self, wait: impl Future<Output = T>) -> T {
        let old = {
            let mut wanted = self.want.lock();
            wanted.idle = true;
            wanted.want.take()
        };
        drop(old);
        let _idle = Idle(self);
        wait.await
    }
}

// The connect loop is out of a wait in which it could dial no one, however
// the wait ended.
struct Idle<'a>(&'a TcpWants);

impl Drop for Idle<'_> {
    fn drop(&mut self) {
        self.0.want.lock().idle = false;
    }
}

/// Counts one transport's attempt: started when the connect starts, and
/// ended once, as it ended, or, when it is dropped unfinished, as timed out
/// when the dial's time ran out and as failed otherwise.
pub(crate) struct Tally<'a> {
    pub(super) stat: &'a SingleStatAtomic,
    pub(super) expired: &'a AtomicBool,
}

impl Tally<'_> {
    async fn count<R, E: Failed>(
        self,
        connect: impl Future<Output = std::result::Result<R, E>>,
    ) -> std::result::Result<R, E> {
        self.stat.attempts(1);
        let mut attempt = Attempt {
            tally: self,
            ended: false,
        };
        let result = connect.await;
        attempt.end(&result);
        result
    }
}

struct Attempt<'a> {
    tally: Tally<'a>,
    ended: bool,
}

impl Attempt<'_> {
    fn end<R, E: Failed>(&mut self, result: &std::result::Result<R, E>) {
        self.ended = true;
        let stat = self.tally.stat;
        match result {
            Ok(_) => stat.successes(1),
            Err(e) => match e.failure() {
                Failure::Refused => stat.refused(1),
                Failure::TimedOut => stat.timeouts(1),
                Failure::Unreachable => stat.unreachable(1),
                Failure::Other => stat.errors(1),
            },
        }
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        if self.tally.expired.load(Ordering::Relaxed) {
            self.tally.stat.timeouts(1);
        } else {
            self.tally.stat.errors(1);
        }
    }
}

/// Opens links to peers over one transport, with sockets from the
/// session's route only.
pub(crate) trait PeerDialer: Send + Sync {
    /// Whether a connection to `to` that may go over `transports` may go
    /// over this one, as the session set it up. It decides, not the peer
    /// index: a later transport may need more than the address.
    fn may_dial(&self, to: SocketAddr, transports: Transports) -> bool;

    /// Whether its socket is held now, and not only bound when a dial
    /// wants it.
    fn ready(&self) -> bool {
        true
    }

    /// Whether the route has a socket free for one more connection over it.
    fn room(&self) -> bool {
        true
    }

    /// What the route has for one more connection over it, and why not.
    fn look(&self) -> TcpRoom {
        if self.room() {
            TcpRoom::Free
        } else {
            TcpRoom::Full
        }
    }

    /// A link to `to`, its attempt counted by `tally`; `None` when no socket
    /// can be had for it now, and nothing was tried.
    fn dial<'a>(&'a self, to: SocketAddr, tally: Tally<'a>) -> BoxFuture<'a, Result<Option<Link>>>;
}

/// TCP, through the route's factory.
#[derive(Debug)]
pub(super) struct TcpDialer {
    pub enabled: bool,
    pub sockets: std::sync::Arc<dyn SocketFactory>,
}

impl PeerDialer for TcpDialer {
    fn may_dial(&self, _: SocketAddr, transports: Transports) -> bool {
        self.enabled && transports == Transports::Any
    }

    fn look(&self) -> TcpRoom {
        self.sockets.tcp_room()
    }

    fn dial<'a>(&'a self, to: SocketAddr, tally: Tally<'a>) -> BoxFuture<'a, Result<Option<Link>>> {
        Box::pin(async move {
            let (read, write) = tally
                .count(self.sockets.connect_tcp(to))
                .await
                .map_err(Error::TcpConnect)?;
            debug!(addr = ?to, "connected over TCP");
            Ok(Some(Link::new(ConnectionKind::Tcp, to, read, write, None)))
        })
    }
}

/// How the connector binds the session's outgoing uTP socket on a later
/// dial when the bind at the session's start failed, as it does on a tunnel
/// whose sockets were all held then.
#[derive(Clone)]
pub(crate) struct UtpRebind {
    pub address: SocketAddr,
    pub opts: weaver_utp::SocketOpts,
    pub receive_buffer: usize,
    pub connection_slots: std::sync::Arc<crate::connection_slots::ConnectionSlots>,
}

impl std::fmt::Debug for UtpRebind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpRebind")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// uTP, over the session's uTP socket.
#[derive(Debug)]
pub(super) struct UtpDialer {
    pub socket: tokio::sync::OnceCell<UtpSocket>,
    pub rebind: Option<UtpRebind>,
    pub sockets: std::sync::Arc<dyn SocketFactory>,
}

impl UtpDialer {
    /// The outgoing uTP socket a dial bound after the session started.
    pub fn rebound(&self) -> Option<&UtpSocket> {
        self.rebind.as_ref().and(self.socket.get())
    }

    // The outgoing uTP socket, bound now when the last bind failed and the
    // route has a datagram socket free. Dials that want it meanwhile wait
    // for the one bind and try again after it when it fails.
    async fn socket(&self) -> Option<&UtpSocket> {
        if let Some(socket) = self.socket.get() {
            return Some(socket);
        }
        let rebind = self.rebind.as_ref()?;
        if !self.sockets.datagram_room() {
            return None;
        }
        self.socket
            .get_or_try_init(|| {
                crate::listen::bind_utp(
                    self.sockets.as_ref(),
                    rebind.address,
                    rebind.opts.clone(),
                    rebind.receive_buffer,
                    Some(&rebind.connection_slots),
                )
            })
            .await
            .inspect_err(|e| debug!("error binding outgoing uTP socket again: {e:#}"))
            .ok()
    }
}

impl PeerDialer for UtpDialer {
    // A dial may go over uTP when the socket is bound, or a dial may bind it
    // again; a holepunched peer is dialed over it alone.
    fn may_dial(&self, _: SocketAddr, _: Transports) -> bool {
        self.ready() || self.rebind.is_some()
    }

    fn ready(&self) -> bool {
        self.socket.initialized()
    }

    fn dial<'a>(&'a self, to: SocketAddr, tally: Tally<'a>) -> BoxFuture<'a, Result<Option<Link>>> {
        Box::pin(async move {
            let Some(socket) = self.socket().await else {
                return Ok(None);
            };
            let stream = tally
                .count(socket.connect(to))
                .await
                .map_err(Error::UtpConnect)?;
            debug!(addr = ?to, "connected over uTP");
            let home = stream.home().cloned();
            let (read, write) = stream.split();
            Ok(Some(Link::new(
                ConnectionKind::Utp,
                to,
                Box::new(read),
                Box::new(write),
                home,
            )))
        })
    }
}

/// One transport of a [`DialPlan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DialStep {
    pub kind: ConnectionKind,
    /// The attempt this one gives way to, when that one is tried: this one
    /// starts once the other fails, or once `Behind::wait` has passed with
    /// the other still connecting. With none, or that one not tried, it
    /// starts at once.
    pub behind: Option<Behind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Behind {
    pub kind: ConnectionKind,
    pub wait: Duration,
}

/// The transports a connection to a peer is tried over, in order. The
/// first link to open wins and the other attempts are dropped; when every
/// attempt fails, the error says each one's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DialPlan<const N: usize> {
    pub steps: [DialStep; N],
}

impl DialPlan<2> {
    /// TCP first, as it is the more mature and the simpler, and uTP once
    /// TCP has not connected within a second, or at once when it fails. A
    /// route with no TCP socket free goes straight to uTP when its socket
    /// is held.
    pub const DEFAULT: Self = Self {
        steps: [
            DialStep {
                kind: ConnectionKind::Tcp,
                behind: None,
            },
            DialStep {
                kind: ConnectionKind::Utp,
                behind: Some(Behind {
                    kind: ConnectionKind::Tcp,
                    wait: Duration::from_secs(1),
                }),
            },
        ],
    };
}

// Which steps of `plan` a connection over `transports` tries: each whose
// dialer may dial, but one with no socket free on the route when another
// step's socket is held. What a look at TCP's room found goes to `wants`.
fn tried<'d, const N: usize>(
    plan: DialPlan<N>,
    dialers: impl Fn(ConnectionKind) -> &'d dyn PeerDialer,
    to: SocketAddr,
    transports: Transports,
    wants: Option<&TcpWants>,
) -> [bool; N] {
    let may = plan
        .steps
        .map(|step| dialers(step.kind).may_dial(to, transports));
    std::array::from_fn(|i| {
        may[i] && {
            let kind = plan.steps[i].kind;
            let room = dialers(kind).look();
            let free = room.is_free();
            if kind == ConnectionKind::Tcp
                && let Some(wants) = wants
            {
                wants.looked(room);
            }
            free || !plan
                .steps
                .iter()
                .enumerate()
                .any(|(j, other)| j != i && may[j] && dialers(other.kind).ready())
        }
    })
}

/// Races `plan`'s dialers to `to`. `tally` gives each attempt its counts.
/// The torrent's TCP want, in `wants`, is settled by the look at TCP's
/// room.
pub(super) async fn race<'d, const N: usize>(
    plan: DialPlan<N>,
    dialers: impl Fn(ConnectionKind) -> &'d dyn PeerDialer,
    tally: impl Fn(ConnectionKind) -> Tally<'d>,
    to: SocketAddr,
    transports: Transports,
    wants: Option<&TcpWants>,
) -> Result<Link> {
    let tried = tried(plan, &dialers, to, transports, wants);
    // Kept for an attempt behind another when that one fails before it
    // waits, as one does on its first poll.
    let failed: [tokio::sync::Notify; N] = std::array::from_fn(|_| tokio::sync::Notify::new());
    let index = |kind| plan.steps.iter().position(|step| step.kind == kind);
    let mut attempts: [Option<BoxFuture<'_, Result<Option<Link>>>>; N] = std::array::from_fn(|i| {
        let DialStep { kind, behind } = plan.steps[i];
        let wait = behind
            .filter(|behind| index(behind.kind).is_some_and(|j| tried[j]))
            .map(|behind| behind.wait);
        let attempt: BoxFuture<'_, _> = Box::pin(attempt(
            tried[i].then(|| dialers(kind)),
            tally(kind),
            wait.map(|wait| (wait, &failed[i])),
            to,
        ));
        Some(attempt)
    });
    let mut errors: [Option<Error>; N] = std::array::from_fn(|_| None);

    // Every attempt is polled in the plan's order, the earliest first, as
    // it has priority; after one ends, the polling starts again from the
    // first. An attempt that fails is told to those behind it before they
    // are polled again.
    std::future::poll_fn(|cx| {
        'polled: loop {
            for i in 0..attempts.len() {
                let Some(attempt) = attempts[i].as_mut() else {
                    continue;
                };
                let Poll::Ready(ended) = attempt.as_mut().poll(cx) else {
                    continue;
                };
                attempts[i] = None;
                match ended {
                    Ok(Some(link)) => return Poll::Ready(Ok(link)),
                    Ok(None) => {}
                    Err(e) => errors[i] = Some(e),
                }
                let kind = plan.steps[i].kind;
                for (j, behind) in plan.steps.iter().enumerate() {
                    if behind.behind.is_some_and(|behind| behind.kind == kind) {
                        failed[j].notify_one();
                    }
                }
                if attempts.iter().all(Option::is_none) {
                    return Poll::Ready(Err(failure(&plan, &mut errors)));
                }
                continue 'polled;
            }
            return Poll::Pending;
        }
    })
    .await
}

// One attempt of a dial over `dialer`, when it is tried: once `wait` has
// passed, or at once when the attempt it waits behind fails first.
async fn attempt<'a>(
    dialer: Option<&'a dyn PeerDialer>,
    tally: Tally<'a>,
    wait: Option<(Duration, &'a tokio::sync::Notify)>,
    to: SocketAddr,
) -> Result<Option<Link>> {
    let Some(dialer) = dialer else {
        return Ok(None);
    };
    if let Some((wait, failed)) = wait {
        tokio::select! {
            _ = failed.notified() => {},
            _ = tokio::time::sleep(wait) => {}
        }
    }
    dialer.dial(to, tally).await
}

// The error of a dial whose every attempt ended without a link: the one
// attempt's when one failed, every failed attempt's, in the plan's order,
// when more did, and none tried when none failed.
fn failure<const N: usize>(plan: &DialPlan<N>, errors: &mut [Option<Error>; N]) -> Error {
    let mut failed: Vec<_> = plan
        .steps
        .iter()
        .zip(errors)
        .filter_map(|(step, error)| Some((step.kind, error.take()?)))
        .collect();
    if failed.len() > 1 {
        return Error::Connect(DialErrors(failed));
    }
    failed
        .pop()
        .map_or(Error::ConnectDisabled, |(_, only)| only)
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Instant;

    use super::*;
    use crate::peer_info_reader::tests::halves;

    const TO: SocketAddr = SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 1)),
        6881,
    );

    // A dial the test answers: Ok connects it, an error fails it.
    type Answer = oneshot::Sender<std::io::Result<()>>;

    // A dial as it began: its kind, when, and its answer.
    type Began = (ConnectionKind, Instant, Answer);

    // A dialer whose every dial the test sees begin, and answers.
    struct Fake {
        kind: ConnectionKind,
        may: bool,
        ready: bool,
        room: bool,
        began: mpsc::UnboundedSender<Began>,
    }

    impl PeerDialer for Fake {
        // Asked about the peer dialed, never another address.
        fn may_dial(&self, to: SocketAddr, _: Transports) -> bool {
            assert_eq!(to, TO);
            self.may
        }

        fn ready(&self) -> bool {
            self.ready
        }

        fn room(&self) -> bool {
            self.room
        }

        fn dial<'a>(
            &'a self,
            to: SocketAddr,
            tally: Tally<'a>,
        ) -> BoxFuture<'a, Result<Option<Link>>> {
            Box::pin(async move {
                let kind = self.kind;
                let (answer, answered) = oneshot::channel();
                self.began.send((kind, Instant::now(), answer)).unwrap();
                tally
                    .count(async { answered.await.unwrap() })
                    .await
                    .map_err(|e| match kind {
                        ConnectionKind::Tcp => Error::TcpConnect(e),
                        ConnectionKind::Utp => {
                            Error::UtpConnect(weaver_utp::Error::StResetReceived)
                        }
                    })?;
                let (read, write) = halves(tokio::io::duplex(64).0);
                Ok(Some(Link::new(
                    kind,
                    to,
                    read,
                    write,
                    (kind == ConnectionKind::Utp).then(tokio::runtime::Handle::current),
                )))
            })
        }
    }

    // TCP and uTP fakes, each may dial, holds its socket and has room, and
    // the counts of each.
    struct Rig {
        tcp: Fake,
        utp: Fake,
        stats: [SingleStatAtomic; ConnectionKind::COUNT],
        expired: AtomicBool,
    }

    fn fakes() -> (Rig, mpsc::UnboundedReceiver<Began>) {
        let (began, dials) = mpsc::unbounded_channel();
        let fake = |kind| Fake {
            kind,
            may: true,
            ready: true,
            room: true,
            began: began.clone(),
        };
        let rig = Rig {
            tcp: fake(ConnectionKind::Tcp),
            utp: fake(ConnectionKind::Utp),
            stats: Default::default(),
            expired: AtomicBool::new(false),
        };
        (rig, dials)
    }

    impl Rig {
        fn race(&self, transports: Transports) -> impl Future<Output = Result<Link>> + '_ {
            self.race_by(DialPlan::DEFAULT, transports)
        }

        fn race_by<const N: usize>(
            &self,
            plan: DialPlan<N>,
            transports: Transports,
        ) -> impl Future<Output = Result<Link>> + '_ {
            super::race(
                plan,
                |kind| match kind {
                    ConnectionKind::Tcp => &self.tcp as &dyn PeerDialer,
                    ConnectionKind::Utp => &self.utp,
                },
                |kind| Tally {
                    stat: &self.stats[kind as usize],
                    expired: &self.expired,
                },
                TO,
                transports,
                None,
            )
        }

        fn stat(&self, kind: ConnectionKind) -> Counts {
            let s = self.stats[kind as usize].snapshot();
            (s.attempts, s.successes, s.refused, s.timeouts, s.errors)
        }
    }

    // (attempts, successes, refused, timeouts, errors)
    type Counts = (u64, u64, u64, u64, u64);

    // The next dial to begin while the race runs on.
    async fn began(
        race: std::pin::Pin<&mut impl Future<Output = Result<Link>>>,
        dials: &mut mpsc::UnboundedReceiver<Began>,
    ) -> Began {
        tokio::select! {
            biased;
            began = dials.recv() => began.unwrap(),
            ended = race => panic!("the race ended first: {:?}", ended.map(|link| link.kind)),
        }
    }

    fn refused() -> std::io::Error {
        std::io::ErrorKind::ConnectionRefused.into()
    }

    // TCP is dialed at once, and uTP a second later while TCP still
    // connects, not a moment before; the first to connect wins, and the
    // other attempt, dropped, counts as failed. Only uTP's link has a home.
    #[tokio::test(start_paused = true)]
    async fn tcp_is_dialed_at_once_and_utp_a_second_later() {
        let (rig, mut dials) = fakes();
        let start = Instant::now();
        let mut race = pin!(rig.race(Transports::Any));
        let (kind, at, _tcp) = began(race.as_mut(), &mut dials).await;
        assert_eq!((kind, at), (ConnectionKind::Tcp, start));
        let (kind, at, utp) = began(race.as_mut(), &mut dials).await;
        assert_eq!(
            (kind, at - start),
            (ConnectionKind::Utp, Duration::from_secs(1))
        );
        utp.send(Ok(())).unwrap();
        let link = race.await.unwrap();
        assert_eq!((link.kind, link.remote), (ConnectionKind::Utp, TO));
        assert!(link.home.is_some());
        assert_eq!(rig.stat(ConnectionKind::Tcp), (1, 0, 0, 0, 1));
        assert_eq!(rig.stat(ConnectionKind::Utp), (1, 1, 0, 0, 0));

        // TCP connecting within the second wins with no uTP dial, and its
        // link has no home.
        let (rig, mut dials) = fakes();
        let mut race = pin!(rig.race(Transports::Any));
        let (kind, _, tcp) = began(race.as_mut(), &mut dials).await;
        assert_eq!(kind, ConnectionKind::Tcp);
        tcp.send(Ok(())).unwrap();
        let link = race.await.unwrap();
        assert_eq!(link.kind, ConnectionKind::Tcp);
        assert!(link.home.is_none());
        assert!(dials.try_recv().is_err());
        assert_eq!(rig.stat(ConnectionKind::Tcp), (1, 1, 0, 0, 0));
        assert_eq!(rig.stat(ConnectionKind::Utp), (0, 0, 0, 0, 0));
    }

    // TCP failing first starts uTP at once, with no wait for the second;
    // both failing give the error that says each, as the TCP and uTP pair
    // did.
    #[tokio::test(start_paused = true)]
    async fn a_tcp_failure_starts_utp_at_once_and_both_failing_say_each() {
        let (rig, mut dials) = fakes();
        let start = Instant::now();
        let mut race = pin!(rig.race(Transports::Any));
        let (_, _, tcp) = began(race.as_mut(), &mut dials).await;
        tcp.send(Err(refused())).unwrap();
        let (kind, at, utp) = began(race.as_mut(), &mut dials).await;
        assert_eq!((kind, at), (ConnectionKind::Utp, start));
        utp.send(Err(std::io::Error::other("unused"))).unwrap();
        let error = race.await.err().unwrap();
        let said = format!(
            "error connecting: TCP={:#} uTP={:#}",
            refused(),
            weaver_utp::Error::StResetReceived
        );
        assert!(matches!(error, Error::Connect { .. }), "{error:?}");
        assert_eq!(error.to_string(), said);
        assert_eq!(rig.stat(ConnectionKind::Tcp), (1, 0, 1, 0, 0));
        assert_eq!(rig.stat(ConnectionKind::Utp), (1, 0, 0, 0, 1));
    }

    // One transport alone: its error as it was, uTP at once when TCP is not
    // tried, and no error but "disabled" when neither is.
    #[tokio::test(start_paused = true)]
    async fn a_lone_transport_fails_with_its_own_error() {
        let (mut rig, mut dials) = fakes();
        rig.utp.may = false;
        let mut race = pin!(rig.race(Transports::Any));
        let (_, _, tcp) = began(race.as_mut(), &mut dials).await;
        tcp.send(Err(refused())).unwrap();
        let error = race.await.err().unwrap();
        assert!(matches!(error, Error::TcpConnect(_)), "{error:?}");
        assert_eq!(
            error.to_string(),
            format!("error connecting over TCP: {:#}", refused())
        );

        let (mut rig, mut dials) = fakes();
        rig.tcp.may = false;
        let start = Instant::now();
        let mut race = pin!(rig.race(Transports::Any));
        let (kind, at, utp) = began(race.as_mut(), &mut dials).await;
        assert_eq!((kind, at), (ConnectionKind::Utp, start));
        utp.send(Err(refused())).unwrap();
        let error = race.await.err().unwrap();
        assert!(matches!(error, Error::UtpConnect(_)), "{error:?}");
        assert_eq!(rig.stat(ConnectionKind::Tcp), (0, 0, 0, 0, 0));

        let (mut rig, mut dials) = fakes();
        rig.tcp.may = false;
        rig.utp.may = false;
        let error = rig.race(Transports::Any).await.err().unwrap();
        assert!(matches!(error, Error::ConnectDisabled), "{error:?}");
        assert!(dials.try_recv().is_err());
    }

    // TCP with no socket free on the route is skipped when uTP holds its
    // socket, uTP then starting at once, and tried when uTP would have to
    // bind one first.
    #[tokio::test(start_paused = true)]
    async fn tcp_with_no_room_is_skipped_only_for_a_held_utp_socket() {
        let (mut rig, mut dials) = fakes();
        rig.tcp.room = false;
        let start = Instant::now();
        let mut race = pin!(rig.race(Transports::Any));
        let (kind, at, utp) = began(race.as_mut(), &mut dials).await;
        assert_eq!((kind, at), (ConnectionKind::Utp, start));
        utp.send(Ok(())).unwrap();
        assert_eq!(race.await.unwrap().kind, ConnectionKind::Utp);
        assert_eq!(rig.stat(ConnectionKind::Tcp), (0, 0, 0, 0, 0));

        let (mut rig, mut dials) = fakes();
        rig.tcp.room = false;
        rig.utp.ready = false;
        let mut race = pin!(rig.race(Transports::Any));
        let (kind, _, tcp) = began(race.as_mut(), &mut dials).await;
        assert_eq!(kind, ConnectionKind::Tcp);
        tcp.send(Ok(())).unwrap();
        assert_eq!(race.await.unwrap().kind, ConnectionKind::Tcp);
    }

    // An attempt still connecting when the dial's time runs out counts as
    // timed out once the dial is dropped.
    #[tokio::test(start_paused = true)]
    async fn an_attempt_dropped_after_the_time_ran_out_counts_as_timed_out() {
        let (rig, mut dials) = fakes();
        {
            let mut race = pin!(rig.race(Transports::Any));
            let _tcp = began(race.as_mut(), &mut dials).await;
            let _utp = began(race.as_mut(), &mut dials).await;
            rig.expired.store(true, Ordering::Relaxed);
        }
        assert_eq!(rig.stat(ConnectionKind::Tcp), (1, 0, 0, 1, 0));
        assert_eq!(rig.stat(ConnectionKind::Utp), (1, 0, 0, 1, 0));
    }

    // A plan of more steps than there are kinds runs every step, and when
    // each fails the error keeps each one's, in the plan's order.
    #[tokio::test(start_paused = true)]
    async fn every_step_of_a_longer_plan_runs_and_every_failure_is_kept() {
        let behind = |kind| {
            Some(Behind {
                kind,
                wait: Duration::from_secs(1),
            })
        };
        let plan = DialPlan {
            steps: [
                DialStep {
                    kind: ConnectionKind::Utp,
                    behind: None,
                },
                DialStep {
                    kind: ConnectionKind::Tcp,
                    behind: behind(ConnectionKind::Utp),
                },
                DialStep {
                    kind: ConnectionKind::Tcp,
                    behind: behind(ConnectionKind::Utp),
                },
            ],
        };
        let (rig, mut dials) = fakes();
        let mut race = pin!(rig.race_by(plan, Transports::Any));
        let (kind, _, utp) = began(race.as_mut(), &mut dials).await;
        assert_eq!(kind, ConnectionKind::Utp);
        utp.send(Err(refused())).unwrap();
        for _ in 0..2 {
            let (kind, _, tcp) = began(race.as_mut(), &mut dials).await;
            assert_eq!(kind, ConnectionKind::Tcp);
            tcp.send(Err(refused())).unwrap();
        }
        let error = race.await.err().unwrap();
        let Error::Connect(DialErrors(each)) = &error else {
            panic!("{error:?}");
        };
        let kinds: Vec<_> = each.iter().map(|(kind, _)| *kind).collect();
        assert_eq!(
            kinds,
            [
                ConnectionKind::Utp,
                ConnectionKind::Tcp,
                ConnectionKind::Tcp
            ]
        );
        assert_eq!(
            error.to_string(),
            format!(
                "error connecting: uTP={:#} TCP={:#} TCP={:#}",
                weaver_utp::Error::StResetReceived,
                refused(),
                refused()
            )
        );
        assert_eq!(rig.stat(ConnectionKind::Tcp), (2, 0, 2, 0, 0));
    }

    // A dialed link carries its kind's capabilities.
    #[tokio::test(start_paused = true)]
    async fn a_link_carries_its_kinds_capabilities() {
        let (rig, mut dials) = fakes();
        let mut race = pin!(rig.race(Transports::Any));
        let (_, _, tcp) = began(race.as_mut(), &mut dials).await;
        tcp.send(Ok(())).unwrap();
        let link = race.await.unwrap();
        assert_eq!(&link.caps, ConnectionKind::Tcp.capabilities());
    }
}

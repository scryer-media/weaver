// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use crate::core::{
    hash_id::{Id20, Id32, InfoHashes},
    net::{HttpClients, Resolver},
};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use url::Url;

use tokio::time::Instant;

use super::protocol::{AnnounceAsk, TrackerProtocol, Turn};
use super::reach_tests::{FixedResolver, Wired};
use super::upkeep_tests::table_once;
use super::{
    AnnounceErrorKind, AnnounceOk, MIN_ANNOUNCE_INTERVAL, Reachable, ReannounceDelay, Refused,
    StoppedAnnounces, SupportedTracker, ToldPorts, Tracker, TrackerAnnounce, TrackerComms,
    TrackerCommsStats, TrackerCommsStatsState, TrackerList, TrackerTask, Unreachable, may_complete,
    read_answer, retry_delay,
};
use super::{AnnounceEvent, TrackerKind, TrackerResult};
use super::{Announced, HttpAnnounces, TrackerNet, TrackerProtocols, TrackerSetup};
use crate::Identity;
use crate::tracker::tracker_comms_http::{MAX_INTERVAL, TrackerRequestEvent};
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

pub(super) fn http(name: &str) -> SupportedTracker {
    let url = Url::parse(&format!("http://{name}/announce")).unwrap();
    SupportedTracker::new(Tracker::operator(url), &inert()).unwrap()
}

/// The protocols of a test's tracker task, the real ones: HTTP over
/// `clients`, its names looked up by `udp_client`'s resolver as the
/// session's are, and UDP over `udp_client`.
pub(super) fn protocols(
    clients: Arc<dyn HttpClients>,
    reachable: Reachable,
    udp_client: UdpTrackerClient,
    http_announces: HttpAnnounces,
    allow_idna: bool,
) -> TrackerProtocols {
    TrackerProtocols::new(TrackerNet {
        http_clients: clients,
        reachable,
        udp_client,
        http_announces,
        allow_idna,
    })
}

/// A protocol a tracker list's own tests never speak: they only order,
/// count and compare trackers.
struct Inert(TrackerKind);

impl TrackerProtocol for Inert {
    fn kind(&self) -> TrackerKind {
        self.0
    }

    fn announce<'a>(&'a self, _: AnnounceAsk<'a>) -> BoxFuture<'a, anyhow::Result<Announced>> {
        unreachable!("an inert tracker is never announced to")
    }
}

/// The schemes of the real table, each of an inert protocol of its kind.
pub(super) fn inert() -> TrackerProtocols {
    let http: Arc<dyn TrackerProtocol> = Arc::new(Inert(TrackerKind::Http));
    TrackerProtocols::from_rows(vec![
        ("http", http.clone()),
        ("https", http),
        ("udp", Arc::new(Inert(TrackerKind::Udp))),
    ])
}

// Tiers of trackers the operator named: the test trackers listen on
// loopback.
pub(super) fn op(tiers: Vec<Vec<Url>>) -> Vec<Vec<Tracker>> {
    tiers
        .into_iter()
        .map(|tier| tier.into_iter().map(Tracker::operator).collect())
        .collect()
}

/// A private tracker's URL: a user, a passkey in the path, one in the
/// query.
const PRIVATE: &str = "https://member:pw@tracker.example:8443/0123passkey/announce?pk=s3cret";

/// Whether `text` names any part of [`PRIVATE`] past its host and port,
/// or of `url`'s path and query.
fn leaks(text: &str, url: &Url) -> bool {
    [
        "member",
        "pw@",
        "0123passkey",
        "s3cret",
        url.path(),
        url.query().unwrap_or("?"),
    ]
    .iter()
    .any(|secret| text.contains(secret))
}

#[test]
fn a_tracker_shows_only_its_scheme_host_and_port() {
    let url = Url::parse(PRIVATE).unwrap();
    let shown = format!(
        "{:?}",
        SupportedTracker::new(Tracker::operator(url.clone()), &inert()).unwrap()
    );
    assert_eq!(shown, "https://tracker.example:8443/…");
    assert!(!leaks(&shown, &url), "{shown}");
    let udp = Url::parse("udp://tracker.example:6969/0123passkey/announce").unwrap();
    assert_eq!(
        format!(
            "{:?}",
            SupportedTracker::new(Tracker::operator(udp), &inert()).unwrap()
        ),
        "udp://tracker.example:6969/…"
    );
    assert_eq!(
        super::redacted_tracker_url("http://[::1]/announce?pk=s3cret"),
        "http://[::1]:80/…"
    );
    assert_eq!(super::redacted_tracker_url("not a url"), "…");
}

// A URL right after a multibyte character is cut at that character's end,
// not inside it.
#[test]
fn tracker_text_redacts_a_url_after_a_multibyte_character() {
    assert_eq!(
        super::redacted_tracker_text("错误：https://tracker.example/announce?pk=s3cret gone"),
        "错误：https://tracker.example:443/… gone"
    );
}

// A tracker's scheme names the protocol it is spoken to over, and the
// protocol's kind's text is the metric label its counts carry.
#[tokio::test]
async fn a_trackers_scheme_names_its_kind() {
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let table = protocols(
        crate::test_support::loopback(),
        reach_all(),
        udp_client,
        Default::default(),
        false,
    );
    let kind = |url: &str| {
        SupportedTracker::new(Tracker::operator(Url::parse(url).unwrap()), &table)
            .map(|tracker| tracker.protocol.kind())
    };
    assert_eq!(kind("http://t.invalid/announce"), Some(TrackerKind::Http));
    assert_eq!(kind("https://t.invalid/announce"), Some(TrackerKind::Http));
    assert_eq!(kind("udp://t.invalid:6969"), Some(TrackerKind::Udp));
    assert_eq!(kind("wss://t.invalid/announce"), None);
    assert_eq!(
        TrackerKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>(),
        ["http", "udp"]
    );
}

/// Every log line written while it is the thread's subscriber.
#[derive(Clone, Default)]
struct Lines(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Lines {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Lines {
    fn capture(&self) -> tracing::subscriber::DefaultGuard {
        let lines = self.clone();
        tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::new("weaver_torrent=trace"))
                .with_ansi(false)
                .with_writer(move || lines.clone())
                .finish(),
        )
    }

    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// The URL of a loopback tracker with a passkey in its path and query.
async fn private_listener() -> (tokio::net::TcpListener, Url) {
    let (listener, url) = http_listener().await;
    let url = Url::parse(&format!("{url}/0123passkey?pk=s3cret")).unwrap();
    (listener, url)
}

#[tokio::test]
async fn an_answered_announce_logs_no_passkey() {
    let lines = Lines::default();
    let _capture = lines.capture();
    // A passkey path is never asked at loopback.
    let (listener, at) = private_listener().await;
    let url = off_loopback(&at);
    let stopped = StoppedAnnounces::default();
    let (task, token) = start_off_loopback(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
    // The row is set after the answer is logged.
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some()) => { r.unwrap(); }
    }
    let text = lines.text();
    assert!(text.contains("tracker answered"), "{text}");
    assert!(text.contains(&format!("{:?}", http_of(&url))), "{text}");
    assert!(!leaks(&text, &url), "{text}");
    token.cancel();
}

#[tokio::test]
async fn a_failed_announce_logs_and_keeps_no_passkey() {
    let lines = Lines::default();
    let _capture = lines.capture();
    // A passkey path is never asked at loopback.
    let (listener, at) = private_listener().await;
    let url = off_loopback(&at);
    let stopped = StoppedAnnounces::default();
    let (task, token) = start_off_loopback(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    // The tracker hangs up on the request: the client's error names
    // the URL it sent it to.
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = async {
            drop(read_announce(&listener).await);
            rx.wait_for(|a| a[0].last.is_some()).await.map(|_| ())
        } => r.unwrap(),
    }
    let error = status.announces()[0]
        .last
        .clone()
        .unwrap()
        .result
        .unwrap_err();
    assert!(!leaks(&error.message, &url), "{error}");
    let text = lines.text();
    assert!(text.contains("error calling tracker"), "{text}");
    assert!(!leaks(&text, &url), "{text}");
    token.cancel();
}

#[tokio::test]
async fn a_client_error_is_mapped_without_its_url() {
    let (listener, url) = private_listener().await;
    let hang_up = async {
        drop(read_request(&listener).await);
    };
    let (sent, ()) = tokio::join!(
        crate::test_support::build(reqwest::Client::builder().no_proxy())
            .unwrap()
            .get(url.clone())
            .send(),
        hang_up
    );
    let e = anyhow::Error::from(sent.unwrap_err());
    assert!(
        leaks(&format!("{e:#}"), &url),
        "the client's error names the URL"
    );
    let mapped = format!("{:#}", super::without_url(e));
    assert!(!leaks(&mapped, &url), "{mapped}");
}

fn http_of(url: &Url) -> SupportedTracker {
    SupportedTracker::new(Tracker::operator(url.clone()), &inert()).unwrap()
}

pub(super) fn live(downloaded: u64, left: u64, completed_during_run: bool) -> TrackerCommsStats {
    TrackerCommsStats {
        uploaded_bytes: 0,
        downloaded_bytes: downloaded,
        corrupt_bytes: 0,
        redundant_bytes: 0,
        left_bytes: Some(left),
        finished: left == 0,
        private: false,
        torrent_state: TrackerCommsStatsState::Live,
        completed_during_run,
        encryption: crate::EncryptionPolicy::Disabled,
    }
}

pub(super) fn list(tiers: &[&[&str]], now: Instant) -> TrackerList {
    let tiers = tiers
        .iter()
        .map(|tier| tier.iter().map(|name| http(name)).collect())
        .collect::<Vec<_>>();
    TrackerList::new(
        &tiers,
        now,
        &crate::tracker::AnnounceShape::WEAVER,
        short_of_a_seed(),
    )
}

/// The completion of a torrent its check found short of a seed.
pub(super) fn short_of_a_seed() -> crate::Completion {
    let completion = crate::Completion::default();
    completion.checked(false, false);
    completion
}

/// The completion of a torrent its check found a seed.
fn checked_a_seed() -> crate::Completion {
    let completion = crate::Completion::default();
    completion.checked(true, false);
    completion
}

pub(super) fn order(list: &TrackerList) -> Vec<SupportedTracker> {
    list.entries.iter().map(|e| e.tracker.clone()).collect()
}

/// A tracker's answer: re-announce in 30 minutes, no `min interval`.
pub(super) const ANSWERED: ReannounceDelay = ReannounceDelay {
    floor: Duration::ZERO,
    regular: Duration::from_secs(1800),
};

pub(super) const STARTED: Option<TrackerRequestEvent> = Some(TrackerRequestEvent::Started);
const COMPLETED: Option<TrackerRequestEvent> = Some(TrackerRequestEvent::Completed);

/// The leg the list's own tests announce over.
pub(super) const LEG: super::Leg = super::Leg::V4;

/// The announces due, each with its event over [`LEG`].
pub(super) fn due(
    list: &mut TrackerList,
    now: Instant,
    port: ToldPorts,
    stats: &TrackerCommsStats,
) -> Vec<(SupportedTracker, Option<TrackerRequestEvent>)> {
    list.start_due(now, port, stats)
        .into_iter()
        .map(|(tracker, events)| (tracker, events.over(LEG).flatten()))
        .collect()
}

/// A tracker accepted `event` over [`LEG`].
pub(super) fn answered(event: Option<TrackerRequestEvent>) -> super::Accepted {
    let mut accepted = super::Accepted::default();
    accepted.take(LEG, event);
    accepted
}

/// The trackers the run's end sends `stopped`.
fn stopping(list: &mut TrackerList) -> Vec<SupportedTracker> {
    list.stop()
        .owed
        .into_iter()
        .map(|(tracker, _)| tracker)
        .collect()
}

// Past the limit an announce waits; a turn given back goes to the
// announces with an event first, each kind in the order it came.
#[tokio::test]
async fn http_announces_past_the_limit_wait_their_turn_events_first() {
    use std::pin::pin;
    // Fused, as a waiter is polled again once it has its turn.
    let announces = super::HttpAnnounces::new(1);
    let first = announces.take(true).await;
    let mut plain_a = pin!(announces.take(false).fuse());
    let mut event_b = pin!(announces.take(true).fuse());
    let mut plain_c = pin!(announces.take(false).fuse());
    let mut event_d = pin!(announces.take(true).fuse());
    for waiter in [
        plain_a.as_mut().poll_unpin_once(),
        event_b.as_mut().poll_unpin_once(),
        plain_c.as_mut().poll_unpin_once(),
        event_d.as_mut().poll_unpin_once(),
    ] {
        assert!(waiter.is_none(), "a turn was taken past the limit");
    }
    assert_eq!(announces.out(), 1);

    let mut order = Vec::new();
    let mut held = first;
    for _ in 0..4 {
        drop(held);
        let mut next = None;
        for (name, waiter) in [
            ("a", plain_a.as_mut().poll_unpin_once()),
            ("b", event_b.as_mut().poll_unpin_once()),
            ("c", plain_c.as_mut().poll_unpin_once()),
            ("d", event_d.as_mut().poll_unpin_once()),
        ] {
            if let Some(turn) = waiter {
                assert!(next.is_none(), "two turns out past the limit");
                order.push(name);
                next = Some(turn);
            }
        }
        held = next.expect("a turn given back goes to a waiter");
        assert_eq!(announces.out(), 1);
    }
    assert_eq!(order, ["b", "d", "a", "c"]);
    drop(held);
    assert_eq!(announces.out(), 0);
}

// A waiter that gives up, before or after its turn is sent to it, holds
// no turn: the next waiter has it, or it is free again.
#[tokio::test]
async fn an_http_announce_that_gives_up_waiting_leaves_its_turn() {
    let announces = super::HttpAnnounces::new(1);
    let first = announces.take(false).await;
    let mut gone = Box::pin(announces.take(false));
    assert!(gone.as_mut().poll_unpin_once().is_none());
    drop(gone);
    let mut next = Box::pin(announces.take(false));
    assert!(next.as_mut().poll_unpin_once().is_none());
    drop(first);
    // The turn skipped the waiter that gave up.
    let second = next
        .as_mut()
        .poll_unpin_once()
        .expect("the next waiter's turn");
    assert_eq!(announces.out(), 1);

    // Sent its turn, then gone before it took it.
    let mut sent = Box::pin(announces.take(true));
    assert!(sent.as_mut().poll_unpin_once().is_none());
    drop(second);
    drop(sent);
    assert_eq!(announces.out(), 0);
    let _free = announces.take(false).now_or_never().expect("a free turn");
    assert_eq!(announces.out(), 1);
}

/// Polls a future once, with a waker that does nothing.
trait PollOnce: Future {
    fn poll_unpin_once(self: std::pin::Pin<&mut Self>) -> Option<Self::Output> {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match self.poll(&mut cx) {
            std::task::Poll::Ready(out) => Some(out),
            std::task::Poll::Pending => None,
        }
    }
}

impl<F: Future + ?Sized> PollOnce for F {}

#[test]
fn a_failing_tracker_backs_off_as_libtorrent_does() {
    // libtorrent announce_infohash::failed with tracker_backoff 250.
    let s = Duration::from_secs;
    let delays = (1..=5)
        .map(|fails| retry_delay(fails, Duration::ZERO))
        .collect::<Vec<_>>();
    assert_eq!(delays, [s(17), s(55), s(117), s(205), s(317)]);
    assert_eq!(retry_delay(16, Duration::ZERO), s(3205));
    assert_eq!(retry_delay(17, Duration::ZERO), s(3600));
    assert_eq!(retry_delay(127, Duration::ZERO), s(3600));
    // The interval of the failed answer holds the retry off longer.
    assert_eq!(retry_delay(1, s(1800)), s(1800));
    assert_eq!(retry_delay(16, s(300)), s(3205));
}

#[test]
fn a_refused_answer_is_retried_no_sooner_than_its_interval() {
    // libtorrent http_tracker_connection::on_response and
    // tracker_connection::fail_impl.
    let s = Duration::from_secs;
    let e = read_answer(b"d14:failure reason4:nopee").unwrap_err();
    assert!(format!("{e:#}").contains("nope"), "{e:#}");
    assert_eq!(Refused::retry_interval(&e), s(1800));
    let e = read_answer(b"d14:failure reason4:nope8:intervali3600ee").unwrap_err();
    assert_eq!(Refused::retry_interval(&e), s(3600));
    // Raised to the minimum announce interval.
    let e = read_answer(b"d14:failure reason4:nope8:intervali60ee").unwrap_err();
    assert_eq!(Refused::retry_interval(&e), MIN_ANNOUNCE_INTERVAL);
    // An answer that does not parse takes the default interval.
    let e = read_answer(b"not bencode").unwrap_err();
    assert_eq!(Refused::retry_interval(&e), s(1800));
    // A tracker that never answered gives no interval.
    let e = anyhow::anyhow!("connection refused");
    assert_eq!(Refused::retry_interval(&e), Duration::ZERO);
    assert!(read_answer(b"d8:intervali1800e5:peers0:e").is_ok());
}

#[test]
fn an_interval_too_long_to_reckon_with_is_the_longest_libtorrent_holds() {
    // libtorrent keeps a tracker's intervals as seconds32
    // (parse_tracker_response); torrent::tracker_response adds them to
    // the time of the answer.
    let t0 = Instant::now();
    for secs in [u64::MAX, i64::MAX as u64] {
        let answer = format!("d8:intervali{secs}e12:min intervali{secs}e5:peers0:e").into_bytes();
        let response = read_answer(&answer).unwrap();
        assert_eq!(
            (response.interval, response.min_interval),
            (MAX_INTERVAL, MAX_INTERVAL)
        );
        let mut list = list(&[&["a"]], t0);
        due(&mut list, t0, ToldPorts::default(), &live(0, 90, false));
        let delay = ReannounceDelay::new(response.interval, response.min_interval, None);
        list.on_success(&http("a"), answered(STARTED), t0, &delay);
        assert_eq!(list.next_due(), Some(t0 + MAX_INTERVAL));

        let refusal = format!("d14:failure reason4:nope8:intervali{secs}ee").into_bytes();
        let retry = Refused::retry_interval(&read_answer(&refusal).unwrap_err());
        assert_eq!(retry, MAX_INTERVAL);
        due(
            &mut list,
            t0 + MAX_INTERVAL,
            ToldPorts::default(),
            &live(0, 90, false),
        );
        list.on_failure(&http("a"), t0 + MAX_INTERVAL, retry);
        assert_eq!(list.next_due(), Some(t0 + MAX_INTERVAL + MAX_INTERVAL));
    }
}

#[test]
fn an_http_answer_naming_no_interval_takes_libtorrents_defaults() {
    // libtorrent parse_tracker_response: interval 1800 s, min interval
    // 30 s; an answer that names neither is still an answer.
    let s = Duration::from_secs;
    let response = read_answer(b"d5:peers0:e").unwrap();
    assert_eq!((response.interval, response.min_interval), (s(1800), s(30)));
    let response = read_answer(b"d8:intervali900e5:peers0:e").unwrap();
    assert_eq!((response.interval, response.min_interval), (s(900), s(30)));
    let response = read_answer(b"d12:min intervali0e5:peers0:e").unwrap();
    assert_eq!((response.interval, response.min_interval), (s(1800), s(0)));
}

#[test]
fn one_tracker_of_each_tier_is_announced_to_at_a_time() {
    // libtorrent torrent::announce_with_tracker and
    // update_tracker_timer with announce_to_all_tiers and not
    // announce_to_all_trackers, as qBittorrent sets them.
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"], &["c"]], t0);
    let stats = live(0, 90, false);

    // The first tracker of each tier is announced to, and nothing more
    // while they wait for their answers.
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("a"), STARTED), (http("c"), STARTED)]
    );
    assert_eq!(due(&mut list, t0, ToldPorts::default(), &stats), []);
    assert_eq!(list.next_due(), None);

    // a fails: it backs off to the end of its tier, b is tried at once.
    list.on_failure(&http("a"), t0, Duration::ZERO);
    assert_eq!(order(&list), [http("b"), http("a"), http("c")]);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("b"), STARTED)]
    );

    // b fails too: its tier has none left to try until the back-off is
    // up; c, answering, is not asked again.
    list.on_failure(&http("b"), t0, Duration::ZERO);
    assert_eq!(order(&list), [http("a"), http("b"), http("c")]);
    assert_eq!(due(&mut list, t0, ToldPorts::default(), &stats), []);
    list.on_success(&http("c"), answered(STARTED), t0, &ANSWERED);
    assert_eq!(due(&mut list, t0, ToldPorts::default(), &stats), []);

    // The failing trackers come due after their back-off; c, which
    // works, is not due yet.
    assert_eq!(list.next_due(), Some(t0 + s(17)));
    let t1 = t0 + s(17);
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &stats),
        [(http("a"), STARTED), (http("b"), STARTED)]
    );

    // a answers and b fails again, backing off longer. a works and comes
    // first in its tier, so b is not announced to while it keeps
    // working; c, in its own tier, keeps its own interval.
    list.on_success(&http("a"), answered(STARTED), t1, &ANSWERED);
    list.on_failure(&http("b"), t1, Duration::ZERO);
    assert_eq!(list.entries[1].next_announce, t1 + s(55));
    assert_eq!(list.next_due(), Some(t0 + s(1800)));
    assert_eq!(
        due(&mut list, t0 + s(1800), ToldPorts::default(), &stats),
        [(http("c"), None)]
    );
    list.on_success(&http("c"), answered(None), t0 + s(1800), &ANSWERED);
    assert_eq!(list.next_due(), Some(t1 + s(1800)));
    assert_eq!(
        due(&mut list, t1 + s(1800), ToldPorts::default(), &stats),
        [(http("a"), None)]
    );
}

#[test]
fn the_trackers_in_use_hear_completed_at_once_and_only_once() {
    // libtorrent tracker_list::completed, announce_infohash::can_announce
    // and torrent::send_tracker_request.
    let s = Duration::from_secs;
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"], &["c"]], t0);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &live(0, 90, false)),
        [(http("a"), STARTED), (http("c"), STARTED)]
    );
    let delay = ReannounceDelay {
        floor: s(900),
        regular: s(1800),
    };
    list.on_success(&http("a"), answered(STARTED), t0, &delay);
    list.on_success(&http("c"), answered(STARTED), t0, &delay);

    // The torrent becomes a seed: a and c are due at once, inside their
    // min interval, with `completed`.
    let seed = live(90, 0, true);
    let t1 = t0 + s(10);
    assert_eq!(due(&mut list, t1, ToldPorts::default(), &seed), []);
    list.completed(t1);
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &seed),
        [(http("a"), COMPLETED), (http("c"), COMPLETED)]
    );

    // a refuses it and backs off, and b, the next in its tier, is asked
    // at once; c accepts it.
    list.on_failure(&http("a"), t1, Duration::ZERO);
    list.on_success(&http("c"), answered(COMPLETED), t1, &delay);
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &seed),
        [(http("b"), STARTED)]
    );
    list.on_success(&http("b"), answered(STARTED), t1, &delay);

    // Once a tracker accepted it, no other is sent `completed`: b,
    // working, is the one of its tier announced to.
    assert_eq!(list.event(&list.entries[0], &seed).over(LEG), Some(None));
    assert_eq!(
        due(&mut list, t1 + s(1800), ToldPorts::default(), &seed),
        [(http("b"), None), (http("c"), None)]
    );
}

#[test]
fn a_torrent_with_its_selection_done_but_not_a_seed_announces_paused() {
    let t0 = Instant::now();
    let mut list = list(&[&["a"]], t0);
    let selection_done = TrackerCommsStats {
        finished: true,
        ..live(60, 30, false)
    };
    // `started` still goes first.
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &selection_done),
        [(http("a"), STARTED)]
    );
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);
    let t1 = t0 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &selection_done),
        [(http("a"), Some(TrackerRequestEvent::Paused))]
    );
    list.on_success(
        &http("a"),
        answered(Some(TrackerRequestEvent::Paused)),
        t1,
        &ANSWERED,
    );
    // A seed whose trackers heard it complete announces without an
    // event, and one still fetching too.
    list.completion.sent();
    let t2 = t1 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t2, ToldPorts::default(), &live(0, 0, false)),
        [(http("a"), None)]
    );
    list.on_success(&http("a"), answered(None), t2, &ANSWERED);
    let t3 = t2 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t3, ToldPorts::default(), &live(0, 30, false)),
        [(http("a"), None)]
    );
}

// As qBittorrent, a torrent with its selection done and not a seed
// says `paused` from its first announce, `started` never going ahead
// of it; once it wants pieces again a tracker not yet told hears
// `started`.
#[test]
fn as_qbittorrent_a_finished_torrent_says_paused_before_started() {
    let t0 = Instant::now();
    let mut list = TrackerList::new(
        &[vec![http("a")]],
        t0,
        &crate::tracker::qbittorrent::SHAPE,
        short_of_a_seed(),
    );
    let selection_done = TrackerCommsStats {
        finished: true,
        ..live(60, 30, false)
    };
    let paused = Some(TrackerRequestEvent::Paused);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &selection_done),
        [(http("a"), paused)]
    );
    list.on_success(&http("a"), answered(paused), t0, &ANSWERED);
    let t1 = t0 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &selection_done),
        [(http("a"), paused)]
    );
    list.on_success(&http("a"), answered(paused), t1, &ANSWERED);
    let t2 = t1 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t2, ToldPorts::default(), &live(60, 30, false)),
        [(http("a"), STARTED)]
    );
}

// A session with no listener announces port 1, as libtorrent does, and
// never 0; one with a listener announces its port.
#[test]
fn the_port_announced_without_a_listener_is_one() {
    assert_eq!(super::announced_port(None), 1);
    assert_eq!(super::announced_port(Some(6881)), 6881);
}

#[test]
fn a_torrent_complete_from_the_start_never_sends_completed() {
    // libtorrent torrent::files_checked presets m_complete_sent for a
    // torrent its check finds a seed.
    let t0 = Instant::now();
    let mut list = list(&[&["a"]], t0);
    list.completion = checked_a_seed();
    let seeding = live(0, 0, false);
    assert!(!may_complete(&seeding));
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &seeding),
        [(http("a"), STARTED)]
    );
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);
    let t1 = t0 + ANSWERED.regular;
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &seeding),
        [(http("a"), None)]
    );
}

#[test]
fn test_waits_for_completion_before_the_torrent_is_live() {
    // A magnet still resolving its metadata: nothing is known yet.
    assert!(may_complete(&TrackerCommsStats::default()));
    assert!(may_complete(&live(0, 90, false)));
    assert!(may_complete(&live(90, 0, true)));
}

#[test]
fn test_unknown_size_never_announces_as_complete() {
    let unknown = TrackerCommsStats::default();
    assert!(!unknown.is_completed());
    assert_eq!(unknown.announced_left(), 16 * 1024);
    let done = live(0, 0, false);
    assert!(done.is_completed());
    assert_eq!(done.announced_left(), 0);
}

#[test]
fn stopped_goes_to_the_trackers_announced_to_that_heard_started() {
    // libtorrent torrent::stop_announcing: the walk with event stopped
    // skips every tracker that never accepted `started`.
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"], &["c"]], t0);
    let stats = live(0, 90, false);
    assert_eq!(stopping(&mut list), []);

    due(&mut list, t0, ToldPorts::default(), &stats);
    // Not yet accepted: nothing is owed.
    assert_eq!(stopping(&mut list), []);
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);
    assert_eq!(stopping(&mut list), [http("a")]);
    list.on_success(&http("c"), answered(STARTED), t0, &ANSWERED);
    assert_eq!(stopping(&mut list), [http("a"), http("c")]);

    // a fails and b takes over: until b accepts `started`, a, failing,
    // is still owed `stopped`.
    list.on_failure(&http("a"), t0, Duration::ZERO);
    assert_eq!(
        due(&mut list, t0, ToldPorts::default(), &stats),
        [(http("b"), STARTED)]
    );
    assert_eq!(stopping(&mut list), [http("a"), http("c")]);
    // Then b, the tracker in use in its tier, is.
    list.on_success(&http("b"), answered(STARTED), t0, &ANSWERED);
    assert_eq!(stopping(&mut list), [http("b"), http("c")]);
}

#[test]
fn stopped_passes_over_a_tracker_whose_announce_waits_for_its_answer() {
    // libtorrent torrent::stop_announcing walks the trackers through
    // announce_infohash::can_announce, false while `updating`
    // (announce_entry.cpp:123-134, torrent.cpp:3308, v2.0.15).
    let t0 = Instant::now();
    let mut list = list(&[&["a", "b"], &["c"]], t0);
    let stats = live(0, 90, false);
    due(&mut list, t0, ToldPorts::default(), &stats);
    list.on_success(&http("a"), answered(STARTED), t0, &ANSWERED);
    list.on_success(&http("c"), answered(STARTED), t0, &ANSWERED);
    list.force(t0, None, true);
    let t1 = t0 + Duration::from_secs(1);
    assert_eq!(
        due(&mut list, t1, ToldPorts::default(), &stats),
        [(http("a"), None), (http("c"), None)]
    );
    // c answers, a does not: only c hears `stopped`.
    list.on_success(&http("c"), answered(None), t1, &ANSWERED);
    assert_eq!(stopping(&mut list), [http("c")]);
}

#[test]
fn a_tracker_named_twice_is_kept_where_first_named() {
    let list = list(&[&["a", "b"], &["a", "c"]], Instant::now());
    assert_eq!(order(&list), [http("a"), http("b"), http("c")]);
    assert_eq!(
        list.entries.iter().map(|e| e.tier).collect::<Vec<_>>(),
        [0, 0, 1]
    );
}

#[test]
fn test_min_interval_is_a_floor() {
    let s = Duration::from_secs;
    assert_eq!(
        ReannounceDelay::new(s(1800), s(900), None),
        ReannounceDelay {
            floor: s(900),
            regular: s(1800)
        }
    );
    // A forced interval below the tracker's min interval is raised to it.
    assert_eq!(
        ReannounceDelay::new(s(1800), s(900), Some(s(60))),
        ReannounceDelay {
            floor: s(900),
            regular: s(900)
        }
    );
    // An interval below min interval is raised too.
    assert_eq!(
        ReannounceDelay::new(s(600), s(900), None),
        ReannounceDelay {
            floor: s(900),
            regular: s(900)
        }
    );
    assert_eq!(
        ReannounceDelay::new(s(1800), Duration::ZERO, Some(s(60))),
        ReannounceDelay {
            floor: s(0),
            regular: s(60)
        }
    );
}

#[test]
fn a_tracker_interval_is_never_below_the_minimum_announce_interval() {
    // libtorrent min_announce_interval, for the interval of either
    // transport: 0 would be a tight loop.
    let s = Duration::from_secs;
    for interval in [s(0), s(5), s(299)] {
        assert_eq!(
            ReannounceDelay::new(interval, Duration::ZERO, None).regular,
            MIN_ANNOUNCE_INTERVAL
        );
    }
    assert_eq!(
        ReannounceDelay::new(s(301), Duration::ZERO, None).regular,
        s(301)
    );
    // The session's forced interval is its own choice, not the tracker's.
    assert_eq!(
        ReannounceDelay::new(s(0), Duration::ZERO, Some(s(60))).regular,
        s(60)
    );
}

#[tokio::test(start_paused = true)]
async fn a_tracker_answer_that_outlasts_the_completion_timeout_fails() {
    // A request that never ends: a tracker trickling bytes, each within
    // the receive timeout, or a UDP exchange retried without end.
    let start = tokio::time::Instant::now();
    let e = super::within_completion_timeout(std::future::pending::<anyhow::Result<()>>())
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("did not answer"), "{e:#}");
    assert!(start.elapsed() >= super::COMPLETION_TIMEOUT);
}

#[tokio::test(start_paused = true)]
async fn a_silent_http_tracker_fails_the_announce() {
    // Accepts the connection (the backlog does) and never answers.
    let (_listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some()) => r.unwrap(),
    };
    let error = status.announces()[0]
        .last
        .clone()
        .unwrap()
        .result
        .unwrap_err();
    assert!(error.message.contains("sent nothing"), "{error}");
    assert_eq!(error.kind, AnnounceErrorKind::Failed);
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

/// Takes no tracker's vote.
pub(super) fn no_vote() -> super::ExternalIpVote {
    Arc::new(|_, _| {})
}

/// The session reaches every address.
pub(super) fn reach_all() -> Reachable {
    Arc::new(|_| Ok(()))
}

/// The session's rule for one with `blocked` on its blocklist, and no
/// IPv6 when `ipv4_only`.
pub(super) fn reach_but(blocked: Vec<IpAddr>, ipv4_only: bool) -> Reachable {
    Arc::new(move |ip: IpAddr| {
        if ipv4_only && ip.is_ipv6() {
            Err(Unreachable::Family)
        } else if blocked.contains(&ip) {
            Err(Unreachable::Filtered)
        } else {
            Ok(())
        }
    })
}

pub(super) fn v1_hash() -> InfoHashes {
    InfoHashes {
        v1: Some(Id20::new([1; 20])),
        v2: None,
    }
}

#[tokio::test]
async fn a_hybrid_announces_under_v1_and_the_truncated_v2() {
    // libtorrent torrent::announce_with_tracker: one announce per
    // protocol_version the torrent has, each with its own event, and
    // torrent::tracker_key shared by both.
    let (listener, url) = http_listener().await;
    let hashes = InfoHashes {
        v1: Some(Id20::new([b'a'; 20])),
        v2: Some(Id32::new([b'b'; 32])),
    };
    let stopped = StoppedAnnounces::default();
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: hashes,
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;
    let mut announced = Vec::new();
    while announced.len() < 2 {
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => announced.push(q),
        }
    }
    let mut hashes_sent = announced
        .iter()
        .map(|q| query_param(q, "info_hash").unwrap().to_owned())
        .collect::<Vec<_>>();
    hashes_sent.sort();
    assert_eq!(hashes_sent, ["a".repeat(20), "b".repeat(20)]);
    for q in &announced {
        assert_eq!(query_param(q, "event"), Some("started"));
    }
    assert_eq!(
        query_param(&announced[0], "key"),
        query_param(&announced[1], "key")
    );
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn a_task_taken_over_stops_the_trackers_left_out_and_announces_under_every_hash() {
    let (a, a_url) = http_listener().await;
    let (b, b_url) = http_listener().await;
    let v1 = InfoHashes {
        v1: Some(Id20::new([b'a'; 20])),
        v2: None,
    };
    let hybrid = InfoHashes {
        v2: Some(Id32::new([b'b'; 32])),
        ..v1
    };
    let stopped = StoppedAnnounces::default();
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1,
        identity: identity(),
        tiers: op(vec![vec![a_url.clone()]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let (conn, started) = tokio::select! {
        _ = peers.next() => panic!("the tracker sent peers"),
        r = read_announce(&a) => r,
    };
    assert_eq!(query_param(&started, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

    // Taken over by a run that announces to b alone, under both hashes.
    let TrackerTask { mut peers, status } = TrackerTask { peers, status }
        .take_over(hybrid, op(vec![vec![b_url.clone()]]))
        .expect("the task announced under one of the run's hashes");
    let left_out = async {
        let (conn, q) = read_announce(&a).await;
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        q
    };
    let kept = async { (next_announce(&b).await, next_announce(&b).await) };
    let (stopped_a, (first, second)) = tokio::select! {
        _ = peers.next() => panic!("the tracker sent peers"),
        r = async { tokio::join!(left_out, kept) } => r,
    };
    // a, which took `started`, is sent `stopped`, under the same hash.
    assert_eq!(query_param(&stopped_a, "event"), Some("stopped"));
    assert_eq!(
        query_param(&stopped_a, "info_hash"),
        Some("a".repeat(20).as_str())
    );
    // b hears one torrent under both its hashes: one key, one peer id.
    let mut hashes = [&first, &second].map(|q| query_param(q, "info_hash").unwrap());
    hashes.sort();
    assert_eq!(hashes, ["a".repeat(20), "b".repeat(20)]);
    for q in [&stopped_a, &first, &second] {
        assert_eq!(query_param(q, "key"), query_param(&started, "key"));
        assert_eq!(query_param(q, "peer_id"), query_param(&started, "peer_id"));
    }
    for q in [&first, &second] {
        assert_eq!(query_param(q, "event"), Some("started"));
    }
    assert_eq!(
        status
            .announces()
            .iter()
            .map(|a| a.url.clone())
            .collect::<Vec<_>>(),
        [b_url.to_string()]
    );
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn a_task_another_torrents_run_takes_over_is_dropped_with_stopped() {
    let (a, a_url) = http_listener().await;
    let mine = InfoHashes {
        v1: Some(Id20::new([b'a'; 20])),
        v2: None,
    };
    let other = InfoHashes {
        v1: Some(Id20::new([b'c'; 20])),
        v2: None,
    };
    let stopped = StoppedAnnounces::default();
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: mine,
        identity: identity(),
        tiers: op(vec![vec![a_url.clone()]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let (conn, started) = tokio::select! {
        _ = peers.next() => panic!("the tracker sent peers"),
        r = read_announce(&a) => r,
    };
    assert_eq!(query_param(&started, "event"), Some("started"));
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

    // A run under hashes the task never announced under does not take
    // it: the task ends, and a hears it stop.
    let taken = TrackerTask { peers, status }.take_over(other, op(vec![vec![a_url.clone()]]));
    assert!(taken.is_none());
    let (conn, q) = read_announce(&a).await;
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert_eq!(query_param(&q, "info_hash"), Some("a".repeat(20).as_str()));
    assert_eq!(query_param(&q, "peer_id"), query_param(&started, "peer_id"));
    token.cancel();
}

/// Stats the test drives by hand.
pub(super) struct ManualStats {
    pub(super) stats: Mutex<TrackerCommsStats>,
    pub(super) completed: tokio::sync::watch::Sender<bool>,
}

impl super::TorrentStatsProvider for Arc<ManualStats> {
    fn get(&self) -> TrackerCommsStats {
        *self.stats.lock()
    }

    fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
        let mut rx = self.completed.subscribe();
        async move {
            let _ = rx.wait_for(|c| *c).await;
        }
        .boxed()
    }
}

/// A bound on the stopped announces far past any exchange a test runs.
const DAY: Duration = Duration::from_secs(86_400);

/// [`ManualStats`] whose run ends once the test opens `ended`; `waiting`
/// turns true once a stopped announce waits for it.
struct EndingStats {
    stats: Arc<ManualStats>,
    ended: tokio::sync::watch::Sender<bool>,
    waiting: tokio::sync::watch::Sender<bool>,
}

impl super::TorrentStatsProvider for EndingStats {
    fn get(&self) -> TrackerCommsStats {
        super::TorrentStatsProvider::get(&self.stats)
    }

    fn run_ended(&self) -> BoxFuture<'static, TrackerCommsStats> {
        let mut rx = self.ended.subscribe();
        let waiting = self.waiting.clone();
        let stats = self.stats.clone();
        async move {
            waiting.send_replace(true);
            let _ = rx.wait_for(|e| *e).await;
            super::TorrentStatsProvider::get(&stats)
        }
        .boxed()
    }
}

// libtorrent `torrent::do_pause` disconnects every connection, each
// counting the block it was cut in as redundant, before
// `stop_announcing` builds `stopped`: the stopped announce reports what
// the run's closing connections counted after its announcing stopped.
#[tokio::test]
async fn a_stopped_announce_reports_what_the_runs_closing_connections_counted() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let (ended, _) = tokio::sync::watch::channel(false);
    let (waiting, mut stopping) = tokio::sync::watch::channel(false);
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    // Bounds far past the exchange, so nothing here races them.
    let stopped = StoppedAnnounces::new(DAY).run_end_within(DAY);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(EndingStats {
            stats: stats.clone(),
            ended: ended.clone(),
            waiting,
        }),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));

    // The run ends: announcing stops first, then a closing connection
    // counts the 8192 bytes of the block it was cut in.
    drop(peers);
    stopping.wait_for(|w| *w).await.unwrap();
    *stats.stats.lock() = TrackerCommsStats {
        redundant_bytes: 8192,
        torrent_state: TrackerCommsStatsState::Paused,
        ..live(0, 90, false)
    };
    ended.send_replace(true);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert_eq!(query_param(&q, "redundant"), Some("8192"));

    stopped.wait_idle().await;
    token.cancel();
}

// The wait for a run's connections has its own bound apart from the
// announce's: a run that never ends is waited for no longer than that,
// and `stopped` still goes out, with the counters as they are, within
// its own bound whole.
#[tokio::test]
async fn a_run_slow_to_end_still_leaves_the_stopped_announce_its_bound() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let (ended, _) = tokio::sync::watch::channel(false);
    let (waiting, _) = tokio::sync::watch::channel(false);
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::new(DAY).run_end_within(Duration::ZERO);
    assert_eq!(stopped.whole(), DAY);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(EndingStats {
            stats: stats.clone(),
            ended,
            waiting,
        }),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));

    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert_eq!(query_param(&q, "redundant"), Some("0"));

    stopped.wait_idle().await;
    token.cancel();
}

/// The query string of the next HTTP request, answered with an empty peer list.
pub(super) async fn next_announce(listener: &tokio::net::TcpListener) -> String {
    let (conn, query) = read_announce(listener).await;
    respond(conn, "d8:intervali1800e5:peers0:e").await;
    query
}

/// The one peer [`answer_with_a_peer`] has the tracker name.
pub(super) fn the_peer() -> SocketAddr {
    (Ipv4Addr::LOCALHOST, 0x1a41).into()
}

/// Answers the next announce with `body`, which names [`the_peer`], and
/// waits for that peer on the stream: the answer has been taken in.
/// Returns the announce's query string.
pub(super) async fn answer_with_a_peer(
    listener: &tokio::net::TcpListener,
    peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
    body: &str,
) -> String {
    let (conn, query) = tokio::select! {
        _ = peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(listener) => r,
    };
    respond(conn, body).await;
    assert_eq!(peers.next().await, Some(the_peer()));
    query
}

/// The identity the tests' torrent names itself by.
pub(super) fn identity() -> Identity {
    Identity {
        peer_id: Id20::new([2; 20]),
        tracker_key: 0x5eed,
        version: "Test 1.0".into(),
        user_agent: "Test/1.0".into(),
        tracker_ids: Default::default(),
        completion: Default::default(),
        announcing: Default::default(),
        shape: &crate::tracker::AnnounceShape::WEAVER,
    }
}

/// The next HTTP request, unanswered, and its query string.
pub(super) async fn read_announce(
    listener: &tokio::net::TcpListener,
) -> (tokio::net::TcpStream, String) {
    let (conn, request) = read_request(listener).await;
    let path = request.split(' ').nth(1).unwrap().to_owned();
    (conn, path.split_once('?').unwrap().1.to_owned())
}

/// The next HTTP request, unanswered: its request line and headers.
pub(super) async fn read_request(
    listener: &tokio::net::TcpListener,
) -> (tokio::net::TcpStream, String) {
    let (conn, _) = listener.accept().await.unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
        conn.readable().await.unwrap();
        match conn.try_read(&mut chunk) {
            Ok(0) => panic!("connection closed before the request was read"),
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
    (conn, String::from_utf8(buf).unwrap())
}

/// Answers an HTTP request with `body`.
pub(super) async fn respond(conn: tokio::net::TcpStream, body: impl AsRef<[u8]>) {
    let body = body.as_ref();
    let mut response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    let mut response = response.as_slice();
    while !response.is_empty() {
        conn.writable().await.unwrap();
        match conn.try_write(response) {
            Ok(n) => response = &response[n..],
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
}

pub(super) fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

#[tokio::test]
async fn a_port_that_changes_is_announced_without_waiting_out_the_interval() {
    // libtorrent session_impl::queue_tracker_request reads the listen
    // port per request. The tracker's interval is 1800 s on the real
    // clock, so only the port change brings the second announce.
    let (listener, url) = http_listener().await;
    let (port, listen_port) = tokio::sync::watch::channel(ToldPorts::both(Some(6881)));
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: StoppedAnnounces::default(),
        counts: Default::default(),
    })
    .peers;
    let first = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        q = next_announce(&listener) => q,
    };
    assert_eq!(query_param(&first, "port"), Some("6881"));
    assert_eq!(query_param(&first, "event"), Some("started"));
    port.send_replace(ToldPorts::both(Some(40001)));
    let second = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        q = next_announce(&listener) => q,
    };
    assert_eq!(query_param(&second, "port"), Some("40001"));
    assert_eq!(query_param(&second, "event"), None);
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn an_http_announce_reports_the_bytes_that_failed_their_hash_and_the_bytes_we_did_not_need_since_started()
 {
    // The torrent had already counted some of each when the run began:
    // `started` reports none, and a later announce what grew since.
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(TrackerCommsStats {
            corrupt_bytes: 16384,
            redundant_bytes: 5,
            ..live(0, 90, false)
        }),
        completed: tokio::sync::watch::channel(false).0,
    });
    let (port, listen_port) = tokio::sync::watch::channel(ToldPorts::both(Some(6881)));
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: StoppedAnnounces::default(),
        counts: Default::default(),
    })
    .peers;
    let first = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        q = next_announce(&listener) => q,
    };
    assert_eq!(query_param(&first, "event"), Some("started"));
    assert_eq!(query_param(&first, "corrupt"), Some("0"));
    assert_eq!(query_param(&first, "redundant"), Some("0"));
    *stats.stats.lock() = TrackerCommsStats {
        corrupt_bytes: 32768,
        redundant_bytes: 21,
        ..live(0, 90, false)
    };
    port.send_replace(ToldPorts::both(Some(40001)));
    let second = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        q = next_announce(&listener) => q,
    };
    assert_eq!(query_param(&second, "corrupt"), Some("16384"));
    assert_eq!(query_param(&second, "redundant"), Some("16"));
    drop(peers);
    token.cancel();
}

#[tokio::test]
async fn test_http_tracker_hears_started_completed_and_stopped() {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let tracker = Url::parse(&format!(
        "http://{}/announce",
        listener.local_addr().unwrap()
    ))
    .unwrap();

    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::default();
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![tracker]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;

    // The torrent completes only once `started` is answered: an answer
    // that comes in after it completed puts the next announce, and with
    // it `completed`, an interval away (libtorrent
    // torrent::tracker_response).
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert_eq!(query_param(&q, "downloaded"), Some("0"));
    assert_eq!(query_param(&q, "left"), Some("90"));

    *stats.stats.lock() = TrackerCommsStats {
        uploaded_bytes: 7,
        ..live(90, 0, true)
    };
    stats.completed.send_replace(true);
    // Its answer is taken in before the run ends: a tracker still
    // waiting is owed no `stopped`.
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("completed"));
    assert_eq!(query_param(&q, "left"), Some("0"));

    *stats.stats.lock() = TrackerCommsStats {
        uploaded_bytes: 42,
        torrent_state: TrackerCommsStatsState::Paused,
        ..live(90, 0, false)
    };
    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert_eq!(query_param(&q, "uploaded"), Some("42"));
    assert_eq!(query_param(&q, "downloaded"), Some("90"));
    assert_eq!(query_param(&q, "left"), Some("0"));

    stopped.wait_idle().await;
    token.cancel();
}

// The run stops while its `completed` waits for the answer: the tracker
// is owed no `stopped` (see
// `stopped_passes_over_a_tracker_whose_announce_waits_for_its_answer`),
// but the `completed` already sent is not cut off. It is answered after
// the stop, within the stop's bound, and counted as sent (libtorrent
// `torrent::stop_announcing` aborts no request already out).
#[tokio::test]
async fn a_completed_in_flight_when_the_run_stops_still_reaches_its_tracker() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    // A bound no run reaches: the answer below ends the wait.
    let stopped = StoppedAnnounces::new(DAY);
    let identity = identity();
    let completion = identity.completion.clone();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity,
        tiers: op(vec![vec![url.clone()]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));

    *stats.stats.lock() = live(90, 0, true);
    stats.completed.send_replace(true);
    let (conn, q) = tokio::select! {
        _ = peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(&listener) => r,
    };
    assert_eq!(query_param(&q, "event"), Some("completed"));
    assert!(status.announces()[0].in_flight);

    // Stopped while the tracker has not answered.
    drop(peers);
    assert!(status.announces()[0].in_flight, "the announce was cut off");
    respond(conn, "d8:completei4e8:intervali1800e5:peers0:e").await;
    stopped.wait_idle().await;

    let row = status.announces()[0].clone();
    assert!(!row.in_flight);
    assert_eq!(row.next_announce, None);
    assert_eq!(
        row.last.unwrap().result.map(|a| a.seeders),
        Ok(Some(4)),
        "the row holds the completed answer"
    );
    assert!(completion.is_sent(), "the completed counts as sent");
    let counted = status.counted();
    let http = &counted.announces[TrackerKind::Http as usize];
    assert_eq!(
        http[AnnounceEvent::Completed as usize][TrackerResult::Ok as usize],
        1
    );
    assert_eq!(
        http[AnnounceEvent::Stopped as usize].iter().sum::<u64>(),
        0,
        "a tracker whose announce waited for its answer is owed no stopped"
    );
    token.cancel();
}

/// A run over two tiers, `a` then `b`, for the shutdown tests: `b` has
/// accepted `started` and `a`'s `started` waits for its answer, its
/// connection held. The stats provider is the test's.
async fn one_answered_one_in_flight(
    stopped: &StoppedAnnounces,
    token: &CancellationToken,
) -> (
    TrackerTask,
    (tokio::net::TcpListener, Url),
    (tokio::net::TcpListener, Url),
    tokio::net::TcpStream,
) {
    let (a, a_url) = http_listener().await;
    let (b, b_url) = http_listener().await;
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let mut task = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![a_url.clone()], vec![b_url.clone()]]),
        stats: Box::new(Arc::new(ManualStats {
            stats: Mutex::new(live(0, 90, false)),
            completed: tokio::sync::watch::channel(false).0,
        })),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let q = answer_with_a_peer(
        &b,
        &mut task.peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    let (conn, q) = tokio::select! {
        _ = task.peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(&a) => r,
    };
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert!(row(&task.status, &a_url).in_flight);
    (task, (a, a_url), (b, b_url), conn)
}

/// The table's row for the tracker at `url`.
fn row(status: &super::TrackerStatus, url: &Url) -> TrackerAnnounce {
    status
        .announces()
        .into_iter()
        .find(|a| a.url == url.as_str())
        .expect("the tracker has a row")
}

/// Resolves once the client closes `conn` without sending more.
async fn closed(conn: tokio::net::TcpStream) {
    let mut chunk = [0u8; 64];
    loop {
        conn.readable().await.unwrap();
        match conn.try_read(&mut chunk) {
            Ok(0) => return,
            Ok(n) => panic!("{n} more bytes came on a dropped announce"),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            // A reset closes it too.
            Err(_) => return,
        }
    }
}

// The session shuts down while a `started` waits for its answer: the
// announce is dropped at once, its connection closed, and the tracker
// that accepted `started` hears `stopped` without waiting on it
// (libtorrent `tracker_manager::abort_all_requests` at
// `session_impl::abort`, tracker_manager.cpp:520-527, cuts every request
// but a `stopped`). The bound is a day, so only the drop lets the stop
// end.
#[tokio::test]
async fn at_session_shutdown_an_announce_in_flight_is_dropped_and_stopped_still_goes() {
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::new(DAY);
    let (TrackerTask { peers, status }, (_a, a_url), (b, _), held) =
        one_answered_one_in_flight(&stopped, &token).await;

    stopped.shut_down();
    drop(peers);
    assert!(
        !row(&status, &a_url).in_flight,
        "the announce in flight was left to finish"
    );
    let q = next_announce(&b).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    closed(held).await;
    stopped.wait_idle().await;

    let counted = status.counted();
    let http = &counted.announces[TrackerKind::Http as usize];
    assert_eq!(
        http[AnnounceEvent::Started as usize][TrackerResult::Ok as usize],
        1,
        "only b answered started"
    );
    assert_eq!(
        http[AnnounceEvent::Stopped as usize][TrackerResult::Ok as usize],
        1,
        "b heard stopped"
    );
    token.cancel();
}

// The session shuts down while a `completed` waits for its answer from a
// tracker that accepted `started`: the `completed` is dropped, its
// connection closed, and as nothing more will reach that tracker it
// hears `stopped`, the event every tracker that answered `started` is
// owed at shutdown. The bound is a day, so only the drop lets the stop
// end.
#[tokio::test]
async fn at_session_shutdown_a_tracker_whose_completed_is_cut_hears_stopped() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::new(DAY);
    let identity = identity();
    let completion = identity.completion.clone();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity,
        tiers: op(vec![vec![url.clone()]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    *stats.stats.lock() = live(90, 0, true);
    stats.completed.send_replace(true);
    let (held, q) = tokio::select! {
        _ = peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(&listener) => r,
    };
    assert_eq!(query_param(&q, "event"), Some("completed"));

    stopped.shut_down();
    drop(peers);
    assert!(
        !row(&status, &url).in_flight,
        "the completed was left to finish"
    );
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    closed(held).await;
    stopped.wait_idle().await;

    assert!(
        !completion.is_sent(),
        "a completed never answered counts as sent"
    );
    let counted = status.counted();
    let http = &counted.announces[TrackerKind::Http as usize];
    assert_eq!(
        http[AnnounceEvent::Stopped as usize][TrackerResult::Ok as usize],
        1
    );
    token.cancel();
}

// A torrent is paused while its `completed` waits for its answer, so
// that announce is left to finish and its tracker owed no `stopped`; the
// session then shuts down before the answer. The `completed` is cut, and
// the tracker, which accepted `started`, hears `stopped` after all.
#[tokio::test]
async fn a_completed_left_to_finish_and_cut_at_shutdown_leaves_its_tracker_stopped() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::new(DAY);
    let identity = identity();
    let completion = identity.completion.clone();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity,
        tiers: op(vec![vec![url.clone()]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    *stats.stats.lock() = live(90, 0, true);
    stats.completed.send_replace(true);
    let (held, q) = tokio::select! {
        _ = peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(&listener) => r,
    };
    assert_eq!(query_param(&q, "event"), Some("completed"));

    // Paused while the session lives on: the completed is left to
    // finish, and its tracker, still waiting, is owed no stopped yet.
    drop(peers);
    assert!(
        row(&status, &url).in_flight,
        "a pause cut off the completed"
    );
    stopped.shut_down();
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    closed(held).await;
    stopped.wait_idle().await;

    assert!(
        !completion.is_sent(),
        "a completed never answered counts as sent"
    );
    let counted = status.counted();
    let http = &counted.announces[TrackerKind::Http as usize];
    assert_eq!(
        http[AnnounceEvent::Stopped as usize][TrackerResult::Ok as usize],
        1
    );
    token.cancel();
}

// A torrent is paused while its `completed` waits for its answer, and its
// route then goes down: the session withholds its `stopped` announces
// before it shuts down, as `sessions.rs` `hold` does. The `completed` is
// cut, and its tracker, though it accepted `started`, hears nothing more:
// the address it would go out from no longer carries this session.
#[tokio::test]
async fn a_completed_left_to_finish_and_cut_once_withheld_leaves_its_tracker_unsent() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::new(DAY);
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url.clone()]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    *stats.stats.lock() = live(90, 0, true);
    stats.completed.send_replace(true);
    let (held, q) = tokio::select! {
        _ = peers.next() => panic!("a peer came before the tracker was asked"),
        r = read_announce(&listener) => r,
    };
    assert_eq!(query_param(&q, "event"), Some("completed"));

    drop(peers);
    assert!(
        row(&status, &url).in_flight,
        "a pause cut off the completed"
    );
    stopped.withhold();
    stopped.shut_down();
    closed(held).await;
    // A `stopped` sent would dial the tracker before the stop could end.
    tokio::select! {
        () = stopped.wait_idle() => {}
        _ = listener.accept() => {
            panic!("the tracker was dialled after the stopped announces were withheld")
        }
    }

    assert!(
        listener.accept().now_or_never().is_none(),
        "the tracker was dialled after the stopped announces were withheld"
    );
    let counted = status.counted();
    let stopped_sent: u64 = counted
        .announces
        .iter()
        .flat_map(|kind| kind[AnnounceEvent::Stopped as usize])
        .sum();
    assert_eq!(stopped_sent, 0, "a withheld stopped was sent");
    token.cancel();
}

// A run stops while the session lives on, and its `started` in flight is
// left to finish, as when a torrent is paused; the session then shuts
// down before the tracker answers: the announce is dropped then, and the
// stop ends without it.
#[tokio::test]
async fn an_announce_left_to_finish_is_dropped_once_the_session_shuts_down() {
    let token = CancellationToken::new();
    let stopped = StoppedAnnounces::new(DAY);
    let (TrackerTask { peers, status }, (_a, a_url), (b, _), held) =
        one_answered_one_in_flight(&stopped, &token).await;

    drop(peers);
    let q = next_announce(&b).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert!(
        row(&status, &a_url).in_flight,
        "a torrent's own stop cut off its announce"
    );

    stopped.shut_down();
    closed(held).await;
    stopped.wait_idle().await;
    assert!(!row(&status, &a_url).in_flight);
    token.cancel();
}

#[tokio::test]
async fn a_zero_stopped_timeout_sends_no_stopped_announce() {
    // libtorrent torrent::announce_with_tracker: with
    // stop_tracker_timeout <= 0 the stopped event is not sent.
    let (listener, url) = http_listener().await;
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::new(Duration::ZERO);
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;

    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));

    // The stream's drop starts no `stopped` announce: the task it would
    // spawn holds the count, and none does.
    assert_eq!(stopped.running(), 1, "the tracker task is counted");
    drop(peers);
    assert_eq!(stopped.running(), 0, "a stopped announce was started");
    token.cancel();
}

// An announce names the torrent as its identity does, whatever the
// client's own User-Agent.
#[tokio::test]
async fn test_http_announce_carries_the_torrents_peer_id_and_user_agent() {
    let (listener, url) = http_listener().await;
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::default();
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            Arc::new(crate::test_support::Loopback::with(|| {
                reqwest::Client::builder()
                    .no_proxy()
                    .user_agent("Session/1.0")
            })),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;

    let request = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        request = async {
            let (conn, request) = read_request(&listener).await;
            respond(conn, "d8:intervali1800e5:peers0:e").await;
            request
        } => request,
    };
    let headers = request.to_ascii_lowercase();
    assert!(
        headers.contains("\r\nuser-agent: test/1.0\r\n"),
        "{request}"
    );
    assert!(!headers.contains("session/1.0"), "{request}");
    let query = request
        .split(' ')
        .nth(1)
        .unwrap()
        .split_once('?')
        .unwrap()
        .1;
    assert_eq!(
        query_param(query, "peer_id"),
        Some("%02".repeat(20).as_str())
    );

    drop(listener);
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test]
async fn test_tracker_id_is_sent_back_and_stopped_wants_no_peers() {
    let (listener, url) = http_listener().await;
    let stats = Arc::new(ManualStats {
        stats: Mutex::new(live(0, 90, false)),
        completed: tokio::sync::watch::channel(false).0,
    });
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::default();
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![url]]),
        stats: Box::new(stats.clone()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;

    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aA10:tracker id3:t-1e",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert_eq!(query_param(&q, "numwant"), Some("200"));
    assert_eq!(query_param(&q, "trackerid"), None);

    // The next announce carries the id back; an answer without one keeps it.
    *stats.stats.lock() = live(90, 0, true);
    stats.completed.send_replace(true);
    // Its answer is taken in before the run ends: a tracker still
    // waiting is owed no `stopped`.
    let q = answer_with_a_peer(
        &listener,
        &mut peers,
        "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("completed"));
    assert_eq!(query_param(&q, "numwant"), Some("200"));
    assert_eq!(query_param(&q, "trackerid"), Some("t-1"));

    // `stopped` asks for no peers, and still names the id.
    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    assert_eq!(query_param(&q, "numwant"), Some("0"));
    assert_eq!(query_param(&q, "trackerid"), Some("t-1"));

    stopped.wait_idle().await;
    token.cancel();
}

/// An announce asks for 200 peers, compact; an answer naming many of
/// both families hands every one on, in the order named, and the
/// tracker's row names them all with the swarm it gave.
#[tokio::test]
async fn an_answer_hands_on_every_peer_it_names_of_both_families() {
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;

    let v4: Vec<SocketAddr> = (1..=120u16)
        .map(|i| {
            (
                Ipv4Addr::new(10, 0, (i >> 8) as u8, u8::try_from(i & 0xff).unwrap()),
                6000 + i,
            )
                .into()
        })
        .collect();
    let v6: Vec<SocketAddr> = (1..=40u16)
        .map(|i| (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, i), 7000 + i).into())
        .collect();
    let compact = |peers: &[SocketAddr]| -> Vec<u8> {
        peers
            .iter()
            .flat_map(|peer| {
                let ip = match peer.ip() {
                    IpAddr::V4(ip) => ip.octets().to_vec(),
                    IpAddr::V6(ip) => ip.octets().to_vec(),
                };
                [ip, peer.port().to_be_bytes().to_vec()].concat()
            })
            .collect()
    };
    let (v4_bytes, v6_bytes) = (compact(&v4), compact(&v6));
    let mut body = b"d8:completei7e10:incompletei12e8:intervali1800e".to_vec();
    body.extend(format!("5:peers{}:", v4_bytes.len()).bytes());
    body.extend(&v4_bytes);
    body.extend(format!("6:peers6{}:", v6_bytes.len()).bytes());
    body.extend(&v6_bytes);
    body.push(b'e');

    let q = answer(&listener, &mut peers, body).await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    assert_eq!(query_param(&q, "compact"), Some("1"));
    assert_eq!(query_param(&q, "no_peer_id"), Some("0"));
    assert_eq!(query_param(&q, "numwant"), Some("200"));
    assert!(query_param(&q, "key").is_some(), "{q}");

    let expected: Vec<SocketAddr> = v4.iter().chain(&v6).copied().collect();
    let got: Vec<SocketAddr> = peers.by_ref().take(expected.len()).collect().await;
    assert_eq!(got, expected);

    let mut rx = status.subscribe();
    rx.wait_for(|announces| announces.iter().all(|a| !a.in_flight && a.last.is_some()))
        .await
        .unwrap();
    let announces = status.announces();
    assert_eq!(
        announce_of(&announces, &url).last.as_ref().unwrap().result,
        Ok(AnnounceOk {
            interval: Duration::from_secs(1800),
            min_interval: Duration::from_secs(30),
            seeders: Some(7),
            leechers: Some(12),
            downloaded: None,
            peers: expected,
            warning: None,
        })
    );

    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test]
async fn test_stream_dropped_before_any_announce_sends_nothing() {
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let stopped = StoppedAnnounces::default();
    let peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![
            Url::parse("http://tracker.invalid/announce").unwrap(),
        ]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(
            Arc::new(crate::test_support::Loopback::with(
                reqwest::Client::builder,
            )),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;
    drop(peers);
    // Never polled, so no tracker heard `started` and none is owed `stopped`.
    stopped.wait_idle().await;
    token.cancel();
}

pub(super) async fn http_listener() -> (tokio::net::TcpListener, Url) {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = Url::parse(&format!(
        "http://{}/announce",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    (listener, url)
}

/// `at`, a listener's URL, named by a name [`start_off_loopback`]
/// resolves to a documentation address instead of loopback, where a
/// tracker the operator named is asked at any path.
fn off_loopback(at: &Url) -> Url {
    let mut url = at.clone();
    url.set_host(Some("tracker.invalid")).unwrap();
    url
}

pub(super) async fn start(
    tiers: Vec<Vec<Url>>,
    stopped: &StoppedAnnounces,
) -> (TrackerTask, CancellationToken) {
    start_over(tiers, stopped, crate::test_support::loopback(), None).await
}

/// The tracker task for `tiers` named by [`off_loopback`], each reached
/// at the loopback listener on its URL's port by [`Wired`] clients.
async fn start_off_loopback(
    tiers: Vec<Vec<Url>>,
    stopped: &StoppedAnnounces,
) -> (TrackerTask, CancellationToken) {
    let listener = Url::parse("http://127.0.0.1:1/").unwrap();
    let resolver = Arc::new(FixedResolver {
        at: Ipv4Addr::new(192, 0, 2, 1).into(),
    });
    start_over(tiers, stopped, Wired::new(&listener), Some(resolver)).await
}

async fn start_over(
    tiers: Vec<Vec<Url>>,
    stopped: &StoppedAnnounces,
    clients: Arc<dyn HttpClients>,
    resolver: Option<Arc<dyn Resolver>>,
) -> (TrackerTask, CancellationToken) {
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, resolver);
    let task = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(tiers),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: protocols(clients, reach_all(), udp_client, Default::default(), false),
        stopped: stopped.clone(),
        counts: Default::default(),
    });
    (task, token)
}

pub(super) fn announce_of<'a>(announces: &'a [TrackerAnnounce], url: &Url) -> &'a TrackerAnnounce {
    announces
        .iter()
        .find(|a| a.url == url.as_str())
        .unwrap_or_else(|| panic!("{url} is not in the table"))
}

#[tokio::test]
async fn test_announce_table_records_in_flight_success_and_error() {
    let (ok, ok_url) = http_listener().await;
    let (err, err_url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![err_url.clone()], vec![ok_url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;

    // Every tracker is listed before any announce goes out.
    let never = |url: &Url| TrackerAnnounce {
        url: url.to_string(),
        ..Default::default()
    };
    assert_eq!(status.announces(), vec![never(&err_url), never(&ok_url)]);

    // Drive the peer stream while the trackers are called.
    macro_rules! driving {
        ($f:expr) => {
            tokio::select! {
                _ = peers.next() => panic!("the tracker sent no peers"),
                r = $f => r,
            }
        };
    }

    // Each tier's tracker is asked at once; the first refuses.
    let (err_conn, _) = driving!(read_announce(&err));
    let (ok_conn, _) = driving!(read_announce(&ok));
    let announces = status.announces();
    assert!(announce_of(&announces, &err_url).in_flight);
    let a = announce_of(&announces, &ok_url);
    assert!(a.in_flight);
    assert_eq!(a.last, None);
    respond(err_conn, "d14:failure reason4:nopee").await;
    respond(
        ok_conn,
        "d8:completei3e10:incompletei5e8:intervali1800e12:min intervali900e5:peers0:e",
    )
    .await;
    let mut rx = status.subscribe();
    driving!(rx.wait_for(|announces| announces.iter().all(|a| !a.in_flight && a.last.is_some())))
        .unwrap();

    let announces = status.announces();
    assert_eq!(
        announce_of(&announces, &ok_url)
            .last
            .as_ref()
            .unwrap()
            .result,
        Ok(AnnounceOk {
            interval: Duration::from_secs(1800),
            min_interval: Duration::from_secs(900),
            seeders: Some(3),
            leechers: Some(5),
            downloaded: None,
            peers: Vec::new(),
            warning: None,
        })
    );
    let error = announce_of(&announces, &err_url)
        .last
        .as_ref()
        .unwrap()
        .result
        .clone()
        .unwrap_err();
    assert!(error.message.contains("nope"), "{error}");
    assert_eq!(error.kind, AnnounceErrorKind::Refused);

    // The table outlives the task: the `stopped` answer is the last
    // outcome of the tracker in use, the refusal stays the other's, and
    // with the run over no tracker is due again.
    drop(peers);
    let q = next_announce(&ok).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    let after = status.announces();
    assert!(
        after
            .iter()
            .all(|a| !a.in_flight && a.next_announce.is_none()),
        "{after:?}"
    );
    assert_eq!(
        announce_of(&after, &err_url).last,
        announce_of(&announces, &err_url).last
    );
    assert_eq!(announce_of(&after, &err_url).fails, 1);
    let stopped_answer = announce_of(&after, &ok_url).last.as_ref().unwrap();
    assert_eq!(
        stopped_answer.result.as_ref().map(|a| a.seeders),
        Ok(None),
        "the row holds the stopped answer, which gave no counts"
    );
    token.cancel();
}

/// Answers the next HTTP request with `body`, driving `peers` meanwhile;
/// the request's query string.
async fn answer(
    listener: &tokio::net::TcpListener,
    peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
    body: impl AsRef<[u8]>,
) -> String {
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        q = async {
            let (conn, q) = read_announce(listener).await;
            respond(conn, body).await;
            q
        } => q,
    }
}

#[tokio::test]
async fn a_stopped_announce_is_the_rows_last_and_leaves_none_due_until_the_next_run() {
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();

    let q = answer(
        &listener,
        &mut peers,
        "d8:completei3e8:intervali1800e5:peers0:e",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
    }
    let started = status.announces()[0].clone();
    assert!(started.next_announce.is_some());
    let started_at = started.last.as_ref().unwrap().at;

    // Paused: `stopped` goes out and its answer is recorded as any is.
    drop(peers);
    let (conn, q) = read_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    // While it waits for its answer, the row says so, and no announce
    // is due any more.
    let waiting = status.announces()[0].clone();
    assert!(waiting.in_flight);
    assert_eq!(waiting.next_announce, None);
    assert_eq!(waiting.last, started.last);
    respond(conn, "d8:completei7e8:intervali1800e5:peers0:e").await;
    stopped.wait_idle().await;
    let row = status.announces()[0].clone();
    assert!(!row.in_flight);
    assert_eq!(row.next_announce, None);
    assert_eq!(row.fails, 0);
    let last = row.last.unwrap();
    assert!(last.at >= started_at);
    assert_eq!(last.result.map(|a| a.seeders), Ok(Some(7)));

    // Resumed: a new run, whose table records `started` again.
    let (task, token2) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    let q = answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
    }
    let row = status.announces()[0].clone();
    assert!(row.last.unwrap().result.is_ok());
    assert!(row.next_announce.is_some());
    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    token.cancel();
    token2.cancel();
}

#[tokio::test]
async fn a_refused_stopped_announce_counts_a_failure_and_leaves_none_due() {
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
    }

    drop(peers);
    let (conn, q) = read_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    respond(conn, "d14:failure reason4:nopee").await;
    stopped.wait_idle().await;
    let row = status.announces()[0].clone();
    assert_eq!(row.fails, 1);
    assert_eq!(row.next_announce, None);
    let error = row.last.unwrap().result.unwrap_err();
    assert_eq!(error.kind, AnnounceErrorKind::Refused);
    assert!(error.message.contains("nope"), "{error}");
    token.cancel();
}

// A tracker that echoes its announce URL, passkey and all, in why it
// refused or in a warning: the message a view, a log or an export reads
// shows the URL as scheme, host and port alone.
#[tokio::test]
async fn a_trackers_refusal_or_warning_naming_its_url_keeps_no_passkey() {
    let echoed = "http://t.example:2710/0123456789abcdef/announce?passkey=0123456789abcdef";
    let said = format!("passkey invalid for {echoed}, check it");
    let shown = "passkey invalid for http://t.example:2710/\u{2026} check it";

    // Refused, with the URL in why.
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("a refused tracker gave peers"),
        () = async {
            let (conn, _) = read_announce(&listener).await;
            let answer = format!("d14:failure reason{}:{said}e", said.len());
            respond(conn, answer.as_bytes()).await;
            rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight).await.unwrap();
        } => {}
    }
    let error = status.announces()[0]
        .clone()
        .last
        .unwrap()
        .result
        .unwrap_err();
    assert!(!error.message.contains("0123456789abcdef"), "{error}");
    assert!(error.message.contains(shown), "{error}");
    drop(peers);
    stopped.wait_idle().await;
    token.cancel();

    // Answered, with the URL in a warning.
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        () = async {
            let (conn, _) = read_announce(&listener).await;
            let answer = format!(
                "d8:intervali1800e5:peers0:15:warning message{}:{said}e",
                said.len()
            );
            respond(conn, answer.as_bytes()).await;
            rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight).await.unwrap();
        } => {}
    }
    let answered = status.announces()[0].clone().last.unwrap().result.unwrap();
    assert_eq!(answered.warning.as_deref(), Some(shown));
    drop(peers);
    let (conn, q) = read_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    respond(conn, b"d8:intervali1800e5:peers0:e").await;
    stopped.wait_idle().await;
    token.cancel();
}

#[tokio::test]
async fn each_announce_is_counted_under_its_event_and_how_it_ended() {
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::default();
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    assert_eq!(status.counted(), Default::default());

    // `started` is accepted, with one peer.
    let (named, ()) = tokio::join!(peers.next(), async {
        let (conn, q) = read_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        respond(conn, b"d8:intervali1800e5:peers6:\x0a\x00\x00\x01\x1a\xe1e").await;
    });
    assert_eq!(named, Some("10.0.0.1:6881".parse().unwrap()));
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent one peer"),
        r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
    }
    // `stopped` is refused.
    drop(peers);
    let (conn, q) = read_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    respond(conn, "d14:failure reason4:nopee").await;
    stopped.wait_idle().await;

    let counted = status.counted();
    let http = &counted.announces[TrackerKind::Http as usize];
    assert_eq!(
        http[AnnounceEvent::Started as usize][TrackerResult::Ok as usize],
        1
    );
    assert_eq!(
        http[AnnounceEvent::Stopped as usize][TrackerResult::Refused as usize],
        1
    );
    // Nothing else was counted, and the peer was.
    assert_eq!(
        counted
            .announces
            .as_flattened()
            .as_flattened()
            .iter()
            .sum::<u64>(),
        2
    );
    assert_eq!(counted.peers, [1, 0]);
    token.cancel();
}

#[tokio::test]
async fn a_zero_stopped_timeout_still_leaves_no_announce_due() {
    let (listener, url) = http_listener().await;
    let stopped = StoppedAnnounces::new(Duration::ZERO);
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;
    let mut rx = status.subscribe();
    answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
    tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
    }
    let answered = status.announces()[0].clone();
    assert!(answered.next_announce.is_some());

    // No `stopped` goes out, and the run is over all the same.
    drop(peers);
    let row = status.announces()[0].clone();
    assert_eq!(row.next_announce, None);
    assert_eq!(row.last, answered.last);
    token.cancel();
}

#[tokio::test]
async fn an_accepted_answers_external_ip_is_the_trackers_vote() {
    let (refusing, refusing_url) = http_listener().await;
    let (listener, url) = http_listener().await;
    let token = CancellationToken::new();
    let udp_sock = crate::test_support::loopback_udp().await;
    let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
    let (votes, mut voted) = tokio::sync::mpsc::unbounded_channel();
    let stopped = StoppedAnnounces::default();
    let mut peers = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![vec![refusing_url], vec![url]]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: Arc::new(move |ours, by| votes.send((ours, by)).unwrap()),
        protocols: protocols(
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            Default::default(),
            false,
        ),
        stopped: stopped.clone(),
        counts: Default::default(),
    })
    .peers;

    // The first tier's tracker refuses, naming an address (65.65.65.65):
    // a refusal casts no vote.
    let q = answer(
        &refusing,
        &mut peers,
        "d11:external ip4:AAAA14:failure reason4:nopee",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    // The next tier's accepts, naming 88.89.90.91: its vote, at the
    // address its answer came from.
    let q = answer(
        &listener,
        &mut peers,
        "d11:external ip4:XYZ[8:intervali1800e5:peers0:e",
    )
    .await;
    assert_eq!(query_param(&q, "event"), Some("started"));
    let vote = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        v = voted.recv() => v.unwrap(),
    };
    assert_eq!(
        vote,
        (
            IpAddr::from(Ipv4Addr::new(88, 89, 90, 91)),
            IpAddr::from(Ipv4Addr::LOCALHOST)
        )
    );
    drop(peers);
    let q = next_announce(&listener).await;
    assert_eq!(query_param(&q, "event"), Some("stopped"));
    stopped.wait_idle().await;
    assert!(voted.try_recv().is_err(), "an answer without one voted");
    token.cancel();
}

#[tokio::test]
async fn test_announce_abandoned_in_flight_leaves_no_outcome() {
    let (listener, url) = http_listener().await;
    // A stop with no time for its announces abandons the one in flight;
    // with time, it finishes (see
    // `a_completed_in_flight_when_the_run_stops_still_reaches_its_tracker`).
    let stopped = StoppedAnnounces::new(Duration::ZERO);
    let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
    let TrackerTask { mut peers, status } = task;

    let _conn = tokio::select! {
        _ = peers.next() => panic!("the tracker sent no peers"),
        r = read_announce(&listener) => r,
    };
    assert!(status.announces()[0].in_flight);

    // Dropping the stream drops the task mid-announce.
    drop(peers);
    assert_eq!(
        status.announces(),
        vec![TrackerAnnounce {
            url: url.to_string(),
            ..Default::default()
        }]
    );
    // `started` was never accepted, so no `stopped` is owed.
    stopped.wait_idle().await;
    token.cancel();
}

/// A protocol for a scheme no session speaks, which records what it is
/// asked, in order, and answers from memory: no socket, no HTTP client.
#[derive(Default)]
struct Recording {
    asked: Mutex<Vec<String>>,
}

/// The refusal [`Recording`] fails an announce to `refused.invalid` with,
/// which only it can classify.
#[derive(Debug)]
struct FakeRefusal;

impl std::fmt::Display for FakeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the fake tracker refused")
    }
}

impl std::error::Error for FakeRefusal {}

impl TrackerProtocol for Recording {
    fn kind(&self) -> TrackerKind {
        TrackerKind::Udp
    }

    fn gate(&self, event: bool) -> Option<BoxFuture<'_, Turn>> {
        self.asked.lock().push(format!("gate event={event}"));
        Some(async { Box::new(()) as Turn }.boxed())
    }

    fn announce<'a>(&'a self, ask: AnnounceAsk<'a>) -> BoxFuture<'a, anyhow::Result<Announced>> {
        self.asked.lock().push(format!(
            "announce {:?} {:?} to {:?}",
            ask.info_hash,
            ask.events.counted_as(),
            ask.tracker
        ));
        let announced = Announced {
            peers: vec![the_peer()],
            interval: Duration::from_secs(1800),
            min_interval: Duration::from_secs(60),
            seeders: Some(3),
            leechers: Some(4),
            downloaded: None,
            warning: None,
            seen_as: None,
            body_len: None,
            accepted: Default::default(),
        };
        if ask.tracker.url.host_str() == Some("refused.invalid") {
            return futures::future::ready(Err(anyhow::Error::new(FakeRefusal))).boxed();
        }
        let leg = super::Leg::Unbound;
        let answers = ask
            .events
            .over(leg)
            .map(|event| (leg, event, Ok(announced)));
        futures::future::ready(super::merged(answers)).boxed()
    }

    fn classify(&self, cause: &(dyn std::error::Error + 'static)) -> Option<TrackerResult> {
        cause.is::<FakeRefusal>().then_some(TrackerResult::Refused)
    }
}

// The tracker task speaks to a tracker only through the protocol its
// scheme's row names: a made-up scheme, registered alone, is announced to
// after its gate is asked, and its answers and failures are counted as it
// classifies them, under its kind, with no HTTP or UDP code in the table.
#[tokio::test]
async fn the_tracker_task_announces_through_the_scheme_table() {
    let recording = Arc::new(Recording::default());
    let table = TrackerProtocols::from_rows(vec![(
        "fake",
        recording.clone() as Arc<dyn TrackerProtocol>,
    )]);
    let url = |host: &str| Url::parse(&format!("fake://{host}:1/0123passkey/announce")).unwrap();
    let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(TrackerSetup {
        info_hashes: v1_hash(),
        identity: identity(),
        tiers: op(vec![
            vec![url("tracker.invalid")],
            vec![url("refused.invalid")],
        ]),
        stats: Box::new(()),
        force_interval: None,
        listen_port: tokio::sync::watch::channel(ToldPorts::both(Some(6881))).1,
        vote: no_vote(),
        protocols: table,
        stopped: StoppedAnnounces::default(),
        counts: Default::default(),
    });
    assert_eq!(peers.next().await, Some(the_peer()));
    let mut rx = status.subscribe();
    tokio::select! {
        _ = peers.next() => panic!("one tracker names one peer"),
        r = rx.wait_for(|a| a.iter().all(|a| a.last.is_some())) => { r.unwrap(); }
    }

    let hash = Id20::new([1; 20]);
    let mut asked = recording.asked.lock().clone();
    asked.sort();
    assert_eq!(
        asked,
        [
            format!("announce {hash:?} Started to fake://refused.invalid:1/…"),
            format!("announce {hash:?} Started to fake://tracker.invalid:1/…"),
            "gate event=true".to_owned(),
            "gate event=true".to_owned(),
        ]
    );
    let counted = status.counted();
    let started = &counted.announces[TrackerKind::Udp as usize][AnnounceEvent::Started as usize];
    assert_eq!(started[TrackerResult::Ok as usize], 1);
    assert_eq!(started[TrackerResult::Refused as usize], 1);
    assert_eq!(started.iter().sum::<u64>(), 2);
    // A scheme the table has no row for is no tracker of the torrent.
    assert!(!status.reannounce(
        Duration::ZERO,
        Some(&Url::parse("udp://tracker.invalid:6969").unwrap()),
        false
    ));
    drop(peers);
}

// The rows found through the map of first rows are those a search of the
// table for each entry finds: the same rows change, to the same values, and
// the same answer is given on whether one did, over seeded tables with
// repeated URLs, entries with no row and entries that repeat a URL.
#[test]
fn rows_written_through_a_map_are_those_a_search_writes() {
    use std::time::{Duration as Wait, SystemTime};

    use super::{TrackerAnnounce, write_rows};

    // The search as it was before the map.
    fn searched(
        rows: &mut [TrackerAnnounce],
        entries: &[(String, u8, Option<SystemTime>)],
    ) -> bool {
        let mut modified = false;
        for (url, fails, next) in entries {
            let Some(row) = rows.iter_mut().find(|a| a.url == *url) else {
                continue;
            };
            if (row.fails, row.next_announce) != (*fails, *next) {
                (row.fails, row.next_announce) = (*fails, *next);
                modified = true;
            }
        }
        modified
    }

    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let url = |n: u64| format!("http://tracker{}.test/announce", n % 12);
    let value = |n: u64| {
        let fails = u8::try_from(n % 3).unwrap();
        let next = (n & 0x10 != 0).then(|| SystemTime::UNIX_EPOCH + Wait::from_secs((n >> 8) % 4));
        (fails, next)
    };
    for run in 0..500 {
        let rows: Vec<TrackerAnnounce> = (0..next() % 10)
            .map(|_| {
                let n = next();
                let (fails, next_announce) = value(n >> 4);
                TrackerAnnounce {
                    url: url(n),
                    fails,
                    next_announce,
                    ..TrackerAnnounce::default()
                }
            })
            .collect();
        let entries: Vec<(String, u8, Option<SystemTime>)> = (0..next() % 10)
            .map(|_| {
                let n = next();
                let (fails, next) = value(n >> 4);
                (url(n), fails, next)
            })
            .collect();
        let (mut mapped, mut reference) = (rows.clone(), rows);
        let changed = write_rows(&mut mapped, &entries, |e| e.0.as_str(), |e| (e.1, e.2));
        assert_eq!(changed, searched(&mut reference, &entries), "run {run}");
        assert_eq!(mapped, reference, "run {run}");
    }
}

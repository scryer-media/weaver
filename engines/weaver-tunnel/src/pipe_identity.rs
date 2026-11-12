// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One live WireGuard session per identity.
//
// A WireGuard server keeps one session, and one endpoint, for each peer key.
// It moves the endpoint to wherever the last authenticated packet came from,
// and answers with its newest session. While an older session with the same
// key pair still sends, a newer session's first handshake goes unanswered
// until the newer one retries after the rekey timeout; and a newer device
// that shakes hands takes the session from the older one, whose streams then
// stall until it shakes hands again. So before a session brings its tunnel
// up, it retires every other idle session with its identity, and first the
// idle sessions that ride on those, since a session carried by another holds
// the one beneath busy.
//
// A busy session stays up: its streams are someone's, and only an idle
// session is retired. The same session reused is never its own rival, so a
// hop kept across a rebuild keeps its tunnel.
//
// A probe session never brings up a second device for an identity another
// session holds: it is lent that session's tunnel, and asks through it, and
// it never retires that session. A probe whose identity no session holds
// brings up its own tunnel and holds the claim while it runs; a session
// that comes up meanwhile waits for the probe to go idle, then retires it.
use std::sync::{
    Arc, Mutex, Weak,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock};

use super::SessionHop;
use crate::{
    TunnelProvider,
    wireguard::{WireGuardIdentity, WireGuardTunnelProvider},
};

// A session's budget permit, whether the session holds its identity's
// claim, and whether it is a probe's.
#[derive(Default)]
pub(super) struct Held {
    pub(super) permit: Mutex<Option<Arc<OwnedSemaphorePermit>>>,
    claimed: AtomicBool,
    pub(super) probe: bool,
}

impl Held {
    pub(super) fn probe() -> Self {
        Self {
            probe: true,
            ..Default::default()
        }
    }
    // Gives back the permit and the claim. The next bring-up claims again.
    pub(super) fn release(&self) {
        self.permit.lock().expect("session capacity").take();
        self.claimed.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
struct Claim {
    identity: WireGuardIdentity,
    activity: Weak<RwLock<()>>,
    provider: Weak<dyn TunnelProvider>,
    resolver: Weak<WireGuardTunnelProvider>,
    held: Weak<Held>,
    // The activity of the session this one rides on, when it is carried.
    carrier: Option<Weak<RwLock<()>>>,
    probe: bool,
}

// The tunnel of the session that holds a probe's identity, lent to the
// probe. The guard holds that session busy while the probe uses it, so it is
// not retired under the probe.
pub(super) struct Lent {
    pub(super) provider: Arc<dyn TunnelProvider>,
    pub(super) resolver: Arc<WireGuardTunnelProvider>,
    pub(super) activity: OwnedRwLockReadGuard<()>,
}

// Every WireGuard session that has claimed its identity and not given the
// claim back. Each holds a session budget permit, so the list is as long as
// the budget allows; a session dropped while claimed is pruned on the next
// claim.
static CLAIMS: Mutex<Vec<Claim>> = Mutex::new(Vec::new());

// Claims `hop`'s identity before it brings its tunnel up, retiring the idle
// sessions that hold the same identity. A hop that already holds its claim
// returns at once, so this costs one atomic load on every later use. A probe
// is lent the tunnel of a session that holds its identity instead.
pub(super) async fn claim(hop: &SessionHop) -> Option<Lent> {
    let resolver = hop.resolver.as_ref()?;
    let probe = hop.capacity.held.probe;
    if !probe && hop.capacity.held.claimed.load(Ordering::Acquire) {
        return None;
    }
    let mine = Arc::downgrade(&hop.activity);
    let identity = resolver.identity().clone();
    let carrier = hop
        .is_carried()
        .then(|| hop.inner.session_activity())
        .flatten()
        .map(|activity| Arc::downgrade(&activity));
    // Decided under the lock, waited on after it: a retirement awaits the
    // tunnel's teardown, and a loan awaits the lender's guard.
    // A lender's tunnel, nothing to do, or the sessions to retire.
    type Lender = (
        Arc<RwLock<()>>,
        Arc<dyn TunnelProvider>,
        Arc<WireGuardTunnelProvider>,
    );
    let next: Result<Vec<Claim>, Option<Lender>> = {
        let mut claims = CLAIMS.lock().expect("wireguard claims");
        claims.retain(|claim| claim.activity.strong_count() > 0);
        let rivals: Vec<&Claim> = claims
            .iter()
            .filter(|claim| {
                claim.identity == identity
                    && !claim.activity.ptr_eq(&mine)
                    // A session this one rides on, or one riding on it, is
                    // part of the same path, not a rival for the server's
                    // session.
                    && !carrier.as_ref().is_some_and(|carrier| {
                        carrier.ptr_eq(&claim.activity)
                            || rides_on(&claims, carrier, &claim.activity)
                    })
                    && !rides_on(&claims, &claim.activity, &mine)
            })
            .collect();
        let lender = probe
            .then(|| {
                rivals.iter().find_map(|claim| {
                    Some((
                        claim.activity.upgrade()?,
                        claim.provider.upgrade()?,
                        claim.resolver.upgrade()?,
                    ))
                })
            })
            .flatten();
        if lender.is_some() {
            Err(lender)
        } else if hop.capacity.held.claimed.swap(true, Ordering::AcqRel) {
            Err(None)
        } else {
            let mut order = Vec::new();
            for rival in rivals {
                carried_first(&claims, rival, &mut order);
            }
            claims.retain(|claim| !claim.activity.ptr_eq(&mine));
            claims.push(Claim {
                identity,
                activity: mine,
                provider: Arc::downgrade(&hop.provider),
                resolver: Arc::downgrade(resolver),
                held: Arc::downgrade(&hop.capacity.held),
                carrier,
                probe,
            });
            Ok(order)
        }
    };
    match next {
        Ok(order) => {
            for claim in order {
                claim.retire().await;
            }
            None
        }
        Err(None) => None,
        Err(Some((activity, provider, resolver))) => Some(Lent {
            provider,
            resolver,
            activity: activity.read_owned().await,
        }),
    }
}

// Gives back `hop`'s claim once its tunnel is down.
pub(super) fn release(hop: &SessionHop) {
    let mine = Arc::downgrade(&hop.activity);
    CLAIMS
        .lock()
        .expect("wireguard claims")
        .retain(|claim| !claim.activity.ptr_eq(&mine));
}

// Whether the session `upper` rides, through any number of carriers, on
// `lower`.
fn rides_on(claims: &[Claim], upper: &Weak<RwLock<()>>, lower: &Weak<RwLock<()>>) -> bool {
    let mut at = upper.clone();
    // A path is as deep as the sessions claimed, and no deeper.
    for _ in 0..claims.len() {
        let Some(carrier) = claims
            .iter()
            .find(|claim| claim.activity.ptr_eq(&at))
            .and_then(|claim| claim.carrier.clone())
        else {
            return false;
        };
        if carrier.ptr_eq(lower) {
            return true;
        }
        at = carrier;
    }
    false
}

// Appends `claim` to `order` after every session carried on it, deepest
// first.
fn carried_first(claims: &[Claim], claim: &Claim, order: &mut Vec<Claim>) {
    if order
        .iter()
        .any(|done| done.activity.ptr_eq(&claim.activity))
    {
        return;
    }
    for carried in claims.iter().filter(|carried| {
        carried
            .carrier
            .as_ref()
            .is_some_and(|carrier| carrier.ptr_eq(&claim.activity))
    }) {
        carried_first(claims, carried, order);
    }
    order.push(claim.clone());
}

impl Claim {
    // Stops the session's tunnel when nothing uses the session. A probe's
    // runs for a bounded time, so it is waited out; anyone else's busy
    // session is left up.
    async fn retire(&self) {
        let Some(activity) = self.activity.upgrade() else {
            return;
        };
        let _idle = if self.probe {
            activity.write_owned().await
        } else {
            match activity.try_write_owned() {
                Ok(idle) => idle,
                Err(_) => return,
            }
        };
        let Some(provider) = self.provider.upgrade() else {
            return;
        };
        if let Some(kind) = provider.kind() {
            crate::metrics::record_session_retirement(kind);
        }
        provider.retire().await;
        if let Some(held) = self.held.upgrade() {
            held.release();
        }
        CLAIMS
            .lock()
            .expect("wireguard claims")
            .retain(|claim| !claim.activity.ptr_eq(&self.activity));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::OwnedRwLockReadGuard;

    use super::super::{
        DialError, DialPath, Dialed, Dialer, Naming, SessionCapacity, SessionHop, Target,
    };
    use super::*;
    use crate::{TunnelError, TunnelStream, wireguard::WireGuardSpec};

    type Events = Arc<Mutex<Vec<String>>>;

    // A session's tunnel that records when it is brought up and when it is
    // stopped. A carried one holds the session beneath busy while it is up,
    // as a tunnel's socket inside that session does.
    struct Recorded {
        name: &'static str,
        events: Events,
        carrier: Option<Arc<RwLock<()>>>,
        riding: Mutex<Option<OwnedRwLockReadGuard<()>>>,
    }

    #[async_trait::async_trait]
    impl TunnelProvider for Recorded {
        async fn prepare(&self) -> Result<(), TunnelError> {
            if let Some(carrier) = &self.carrier {
                let guard = carrier.clone().read_owned().await;
                *self.riding.lock().unwrap() = Some(guard);
            }
            self.events
                .lock()
                .unwrap()
                .push(format!("up {}", self.name));
            Ok(())
        }
        async fn retire(&self) {
            self.riding.lock().unwrap().take();
            self.events
                .lock()
                .unwrap()
                .push(format!("down {}", self.name));
        }
        async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
            Err(TunnelError::Engine("nothing to dial".into()))
        }
        fn describe(&self) -> String {
            self.name.into()
        }
    }

    // The stage beneath a first hop.
    struct Beneath;

    #[async_trait::async_trait]
    impl Dialer for Beneath {
        async fn dial(&self, _: &Target) -> Result<Dialed, DialError> {
            Err(DialError::Skipped("beneath".into()))
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(30)
        }
        fn describe(&self) -> String {
            "beneath".into()
        }
    }

    // A spec whose identity is our key `own` toward the peer key `peer`.
    fn spec(own: u8, peer: u8) -> WireGuardSpec {
        WireGuardSpec {
            proxy_config_id: format!("{own}-{peer}"),
            proxy_name: "test".into(),
            revision: "1".into(),
            endpoint_host: "127.0.0.1".into(),
            endpoint_port: 9,
            private_key: [own; 32],
            peer_public_key: [peer; 32],
            preshared_key: None,
            addresses: vec![crate::IpCidr::host("10.0.0.1".parse().unwrap())],
            dns_servers: Vec::new(),
            mtu: crate::DEFAULT_WIREGUARD_MTU,
            persistent_keepalive: None,
            request_timeout: Duration::from_secs(10),
            max_sockets: 64,
        }
    }

    // A WireGuard session named `name` with identity (`own`, `peer`), on
    // `carrier` when given.
    fn session(
        name: &'static str,
        own: u8,
        peer: u8,
        carrier: Option<&Arc<SessionHop>>,
        events: &Events,
    ) -> Arc<SessionHop> {
        hop(name, own, peer, carrier, events, false)
    }

    // A session built to probe identity (`own`, `peer`).
    fn probe(name: &'static str, own: u8, peer: u8, events: &Events) -> Arc<SessionHop> {
        hop(name, own, peer, None, events, true)
    }

    fn hop(
        name: &'static str,
        own: u8,
        peer: u8,
        carrier: Option<&Arc<SessionHop>>,
        events: &Events,
        probing: bool,
    ) -> Arc<SessionHop> {
        let capacity = SessionCapacity::new(Arc::new(tokio::sync::Semaphore::new(1)));
        let capacity = if probing {
            capacity.probing()
        } else {
            capacity
        };
        let resolver = Arc::new(crate::WireGuardTunnelProvider::new(
            spec(own, peer),
            Arc::new(crate::NoopTunnelObserver),
        ));
        Arc::new(SessionHop {
            capacity,
            sockets: Default::default(),
            activity: Default::default(),
            id: own as u32,
            provider: Arc::new(Recorded {
                name,
                events: events.clone(),
                carrier: carrier.map(|carrier| carrier.activity.clone()),
                riding: Mutex::new(None),
            }),
            inner: match carrier {
                Some(carrier) => carrier.clone(),
                None => Arc::new(Beneath),
            },
            transport: None,
            path: DialPath {
                egress: 4,
                proxies: carrier.map(|carrier| vec![carrier.id]).unwrap_or_default(),
                ..Default::default()
            },
            endpoint: None,
            timeout: Duration::from_secs(30),
            naming: Naming::proxy_servers(Vec::new()),
            resolver: Some(resolver),
            forwarding: Default::default(),
            failure: Default::default(),
        })
    }

    fn taken(events: &Events) -> Vec<String> {
        std::mem::take(&mut *events.lock().unwrap())
    }

    // A rebuilt session stops the idle one with its identity before its own
    // tunnel comes up, and leaves a session with another identity alone.
    #[tokio::test]
    async fn a_rebuilt_session_stops_the_old_one_with_its_identity_before_it_comes_up() {
        let events = Events::default();
        let old = session("old", 1, 2, None, &events);
        let other = session("other", 1, 3, None, &events);
        let new = session("new", 1, 2, None, &events);
        old.prepare().await.unwrap();
        other.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up old", "up other"]);

        new.prepare().await.unwrap();
        assert_eq!(taken(&events), ["down old", "up new"]);

        // The same session used again holds its claim and stops nothing.
        new.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up new"]);
    }

    // A session with streams open is someone's: a rival leaves it up.
    #[tokio::test]
    async fn a_busy_session_with_the_same_identity_stays_up() {
        let events = Events::default();
        let old = session("old", 1, 2, None, &events);
        let new = session("new", 1, 2, None, &events);
        old.prepare().await.unwrap();
        let stream = old.activity.clone().read_owned().await;
        new.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up old", "up new"]);
        drop(stream);
    }

    // A session the old one carries holds it busy, so the carried session is
    // stopped first, then the old one, and only then does the new one come
    // up. Its own carried session then finds nothing left to stop.
    #[tokio::test]
    async fn a_rebuilt_carrier_stops_the_old_chain_above_it_first() {
        let events = Events::default();
        let old_carrier = session("old carrier", 1, 2, None, &events);
        let old_carried = session("old carried", 5, 6, Some(&old_carrier), &events);
        old_carried.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up old carrier", "up old carried"]);

        let new_carrier = session("new carrier", 1, 2, None, &events);
        new_carrier.prepare().await.unwrap();
        assert_eq!(
            taken(&events),
            ["down old carried", "down old carrier", "up new carrier"]
        );

        let new_carried = session("new carried", 5, 6, Some(&new_carrier), &events);
        new_carried.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up new carrier", "up new carried"]);
    }

    // A session that rides on one with its own identity is on the same path,
    // not a rival: neither stops the other.
    #[tokio::test]
    async fn sessions_on_one_path_never_stop_each_other() {
        let events = Events::default();
        let lower = session("lower", 1, 2, None, &events);
        let upper = session("upper", 1, 2, Some(&lower), &events);
        upper.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up lower", "up upper"]);
    }

    // A session that retired itself gives its claim back, so it is no rival
    // and comes up again as itself.
    #[tokio::test]
    async fn a_retired_session_gives_its_claim_back() {
        let events = Events::default();
        let old = session("old", 1, 2, None, &events);
        let new = session("new", 1, 2, None, &events);
        old.prepare().await.unwrap();
        assert!(old.retire_idle().await);
        new.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up old", "down old", "up new"]);
        // The old session comes back as the newer claim, and stops the new
        // one, which is idle.
        old.prepare().await.unwrap();
        assert_eq!(taken(&events), ["down new", "up old"]);
    }

    // A probe for an identity a live session holds is lent the live
    // session's tunnel: it brings up no tunnel of its own, holds the live
    // session busy while it asks, and stops nothing.
    #[tokio::test]
    async fn a_probe_asks_through_the_live_session_with_its_identity() {
        let events = Events::default();
        let live = session("live", 1, 2, None, &events);
        let probe = probe("probe", 1, 2, &events);
        live.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up live"]);

        let lent = claim(&probe)
            .await
            .expect("a probe is lent the live tunnel");
        assert!(Arc::ptr_eq(&lent.provider, &live.provider));
        assert!(Arc::ptr_eq(&lent.resolver, live.resolver.as_ref().unwrap()));
        assert!(
            live.activity.try_write().is_err(),
            "the lender is held busy"
        );
        drop(lent);
        assert!(live.activity.try_write().is_ok());

        probe.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up live"]);
        assert!(!probe.capacity.held.claimed.load(Ordering::Acquire));

        // The live session still holds the claim: a rebuilt one stops it.
        let rebuilt = session("rebuilt", 1, 2, None, &events);
        rebuilt.prepare().await.unwrap();
        assert_eq!(taken(&events), ["down live", "up rebuilt"]);
    }

    // A probe whose identity no session holds comes up as itself and holds
    // the claim, so a second probe asks through it; a session that comes up
    // afterwards retires the idle probe first.
    #[tokio::test]
    async fn a_probe_with_no_live_session_comes_up_and_holds_the_claim() {
        let events = Events::default();
        let first = probe("first probe", 1, 2, &events);
        let second = probe("second probe", 1, 2, &events);
        let live = session("live", 1, 2, None, &events);
        first.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up first probe"]);
        second.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up first probe"]);

        live.prepare().await.unwrap();
        assert_eq!(taken(&events), ["down first probe", "up live"]);
    }

    // A session that comes up while a probe with its identity is busy waits
    // until the probe is idle, retires it, and only then comes up, so the
    // two tunnels never run together.
    #[tokio::test]
    async fn a_session_waits_for_a_busy_probe_then_retires_it_before_coming_up() {
        let events = Events::default();
        let probe = probe("probe", 1, 2, &events);
        let live = session("live", 1, 2, None, &events);
        probe.prepare().await.unwrap();
        assert_eq!(taken(&events), ["up probe"]);
        let asking = probe.activity.clone().read_owned().await;

        let coming_up = tokio::spawn({
            let live = live.clone();
            async move { live.prepare().await }
        });
        // The current-thread runtime runs the bring-up until it waits; once
        // it holds the claim, it waits on the probe.
        while !live.capacity.held.claimed.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(
            taken(&events).is_empty(),
            "nothing moves while the probe asks"
        );

        drop(asking);
        coming_up.await.unwrap().unwrap();
        assert_eq!(taken(&events), ["down probe", "up live"]);
    }
}

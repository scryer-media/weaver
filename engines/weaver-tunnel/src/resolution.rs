// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Answers to name lookups, held for as long as their TTL allows.
//
// One cache serves one resolver, because an answer holds only for the
// resolver and the path that gave it: the host's own DNS has one cache for
// the whole process, and each proxy hop that asks DNS servers has its own,
// built and dropped with the hop. A caller asking for a name another caller
// is already looking up waits for that lookup instead of starting its own.
use std::{
    collections::HashMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use tokio::{net::TcpStream, sync::watch, time::Instant};

use crate::{
    TunnelError,
    metrics::{self, CacheOutcome, Resolver},
    pipe::ResolutionFailed,
};

#[cfg(test)]
#[path = "resolution_tests.rs"]
mod tests;

#[path = "resolution_record.rs"]
pub mod record;

// How long a lookup's answer holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ttl {
    // The answer's own TTL, in seconds.
    Seconds(u32),
    // The resolver gives none, as the host's own DNS and a WireGuard
    // tunnel's do not.
    Unknown,
}

// What a lookup found. No addresses with a TTL is a negative answer: the
// name has no address, for that long.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lookup {
    pub addresses: Vec<IpAddr>,
    pub ttl: Ttl,
    // For a negative answer, whether it said the name does not exist
    // rather than that it has no address.
    pub nonexistent: bool,
}

// How long answers are held.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    // No answer is held longer than this.
    pub ceiling: Duration,
    // How long an answer without a TTL is held.
    pub untimed: Duration,
    // No negative answer is held longer than this.
    pub negative: Duration,
    // How many names one cache holds at once.
    pub capacity: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            ceiling: Duration::from_secs(60 * 60),
            untimed: Duration::from_secs(60),
            negative: Duration::from_secs(60),
            capacity: 1024,
        }
    }
}

impl Policy {
    // How long `lookup` may be held, or `None` when it may not be: a TTL of
    // zero, or a negative answer that says for how long.
    fn hold(&self, lookup: &Lookup) -> Option<Duration> {
        let held = match (lookup.ttl, lookup.addresses.is_empty()) {
            (Ttl::Unknown, true) => return None,
            (Ttl::Unknown, false) => self.untimed,
            (Ttl::Seconds(seconds), true) => Duration::from_secs(seconds.into()).min(self.negative),
            (Ttl::Seconds(seconds), false) => Duration::from_secs(seconds.into()),
        };
        Some(held.min(self.ceiling)).filter(|held| !held.is_zero())
    }
}

// A lookup's failure goes to every caller that waited on it, and failures
// are not `Clone`, so each waiter gets a replica of the same kind and
// message.
pub trait Replicate {
    fn replicate(&self) -> Self;
}

impl Replicate for io::Error {
    fn replicate(&self) -> Self {
        let source = self.get_ref();
        if source.is_some_and(|source| source.is::<ResolutionFailed>()) {
            return io::Error::new(self.kind(), ResolutionFailed);
        }
        if let Some(source) = source.and_then(|source| source.downcast_ref::<TunnelError>()) {
            return io::Error::new(self.kind(), source.clone());
        }
        io::Error::new(self.kind(), self.to_string())
    }
}

// An answer as the cache holds it.
#[derive(Clone)]
struct Answer {
    addresses: Arc<[IpAddr]>,
    nonexistent: bool,
}

impl Answer {
    fn of(lookup: &Lookup) -> Self {
        Self {
            addresses: lookup.addresses.as_slice().into(),
            nonexistent: lookup.nonexistent,
        }
    }
    // The addresses, after noting the answer in the test recording the
    // caller runs in, if any. `held` says it was held from before.
    fn noted(&self, name: &str, held: bool) -> Vec<IpAddr> {
        record::cached(name, held);
        if self.addresses.is_empty() {
            let (failure, said) = if self.nonexistent {
                (record::LookupFailure::NoSuchName, "NXDOMAIN")
            } else {
                (record::LookupFailure::NoAddress, "NODATA")
            };
            record::failed(name, failure, || said.to_owned());
        }
        self.addresses.to_vec()
    }
}

// What a lookup came to, once it lands.
type Landed<E> = Option<Result<Answer, Arc<E>>>;

enum Slot<E> {
    Held {
        answer: Answer,
        until: Instant,
    },
    Asking {
        flight: u64,
        landed: watch::Receiver<Landed<E>>,
        waiting: usize,
    },
}

struct Slots<E> {
    names: HashMap<String, Slot<E>>,
    flights: u64,
}

impl<E> Slots<E> {
    // Keeps at most `capacity` names: answers past their time go first,
    // then the ones closest to running out. A lookup in flight stays.
    fn trim(&mut self, now: Instant, capacity: usize) {
        if self.names.len() <= capacity {
            return;
        }
        self.names
            .retain(|_, slot| !matches!(slot, Slot::Held { until, .. } if *until <= now));
        while self.names.len() > capacity {
            let soonest = self
                .names
                .iter()
                .filter_map(|(name, slot)| match slot {
                    Slot::Held { until, .. } => Some((*until, name)),
                    Slot::Asking { .. } => None,
                })
                .min()
                .map(|(_, name)| name.clone());
            let Some(soonest) = soonest else {
                break;
            };
            self.names.remove(&soonest);
        }
    }
}

pub struct ResolutionCache<E> {
    policy: Policy,
    slots: Mutex<Slots<E>>,
}

impl<E: Replicate> Default for ResolutionCache<E> {
    fn default() -> Self {
        Self::new(Policy::default())
    }
}

enum Turn<E> {
    Wait(watch::Receiver<Landed<E>>),
    Ask(u64, watch::Sender<Landed<E>>),
}

impl<E: Replicate> ResolutionCache<E> {
    pub fn new(policy: Policy) -> Self {
        Self {
            policy,
            slots: Mutex::new(Slots {
                names: HashMap::new(),
                flights: 0,
            }),
        }
    }

    // The addresses `name` resolves to: the answer held for it while that
    // lasts, else what `lookup` finds, held then for as long as its TTL
    // allows. `resolver` labels how the cache answered. A negative answer
    // comes back as no addresses.
    pub async fn resolve<F, Fut>(
        &self,
        resolver: Resolver,
        name: &str,
        lookup: F,
    ) -> Result<Vec<IpAddr>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Lookup, E>>,
    {
        if let Ok(ip) = name.parse() {
            return Ok(vec![ip]);
        }
        let key = key(name);
        let mut lookup = Some(lookup);
        loop {
            let turn = {
                let mut slots = self.slots.lock().expect("resolution cache");
                let slots = &mut *slots;
                match slots.names.get_mut(&key) {
                    Some(Slot::Held { answer, until }) if *until > Instant::now() => {
                        metrics::record_cache(resolver, CacheOutcome::Hit);
                        return Ok(answer.noted(name, true));
                    }
                    Some(Slot::Asking {
                        landed, waiting, ..
                    }) => {
                        *waiting += 1;
                        Turn::Wait(landed.clone())
                    }
                    _ => {
                        slots.flights += 1;
                        let flight = slots.flights;
                        let (sender, landed) = watch::channel(None);
                        slots.names.insert(
                            key.clone(),
                            Slot::Asking {
                                flight,
                                landed,
                                waiting: 0,
                            },
                        );
                        Turn::Ask(flight, sender)
                    }
                }
            };
            match turn {
                Turn::Wait(mut landed) => {
                    // A lookup dropped before it landed answers nobody: ask
                    // again, and the first to do so asks for everyone.
                    let Ok(answer) = landed.wait_for(Option::is_some).await else {
                        continue;
                    };
                    metrics::record_cache(resolver, CacheOutcome::Joined);
                    return match answer.as_ref().expect("a landed lookup") {
                        Ok(answer) => Ok(answer.noted(name, false)),
                        Err(error) => {
                            record::cached(name, false);
                            Err(error.replicate())
                        }
                    };
                }
                Turn::Ask(flight, sender) => {
                    metrics::record_cache(resolver, CacheOutcome::Miss);
                    let flight = Flight {
                        cache: self,
                        key: &key,
                        flight,
                        sender,
                        landed: false,
                    };
                    let lookup = lookup.take().expect("a caller asks at most once");
                    record::cached(name, false);
                    return flight.land(name, lookup().await);
                }
            }
        }
    }

    // `resolve` for a thread that may block. It does not wait on a lookup
    // another caller is making for the name; it asks for itself.
    pub fn resolve_blocking(
        &self,
        resolver: Resolver,
        name: &str,
        lookup: impl FnOnce() -> Result<Lookup, E>,
    ) -> Result<Vec<IpAddr>, E> {
        if let Ok(ip) = name.parse() {
            return Ok(vec![ip]);
        }
        let key = key(name);
        {
            let slots = self.slots.lock().expect("resolution cache");
            if let Some(Slot::Held { answer, until }) = slots.names.get(&key)
                && *until > Instant::now()
            {
                metrics::record_cache(resolver, CacheOutcome::Hit);
                return Ok(answer.addresses.to_vec());
            }
        }
        metrics::record_cache(resolver, CacheOutcome::Miss);
        let lookup = lookup()?;
        if let Some(held) = self.policy.hold(&lookup) {
            let now = Instant::now();
            let mut slots = self.slots.lock().expect("resolution cache");
            // A lookup in flight lands its own answer.
            if !matches!(slots.names.get(&key), Some(Slot::Asking { .. })) {
                slots.names.insert(
                    key,
                    Slot::Held {
                        answer: Answer::of(&lookup),
                        until: now + held,
                    },
                );
                slots.trim(now, self.policy.capacity);
            }
        }
        Ok(lookup.addresses)
    }

    // Drops the answer held for `name`, so the next caller asks afresh: for
    // when none of the addresses it gave could be reached.
    pub fn forget(&self, name: &str) {
        let key = key(name);
        let mut slots = self.slots.lock().expect("resolution cache");
        if matches!(slots.names.get(&key), Some(Slot::Held { .. })) {
            slots.names.remove(&key);
        }
    }

    // How many callers wait on the lookup in flight for `name`, or `None`
    // when none is.
    #[cfg(test)]
    fn waiting(&self, name: &str) -> Option<usize> {
        match self
            .slots
            .lock()
            .expect("resolution cache")
            .names
            .get(&key(name))
        {
            Some(Slot::Asking { waiting, .. }) => Some(*waiting),
            _ => None,
        }
    }

    // Whether an answer is held for `name`, current or not.
    #[cfg(test)]
    fn held(&self, name: &str) -> bool {
        matches!(
            self.slots
                .lock()
                .expect("resolution cache")
                .names
                .get(&key(name)),
            Some(Slot::Held { .. })
        )
    }
}

fn key(name: &str) -> String {
    name.trim_end_matches('.').to_ascii_lowercase()
}

// A lookup the cache is waiting on. Dropped before it lands, as when the
// dial that asked gives up, it lets the next caller ask instead.
struct Flight<'a, E> {
    cache: &'a ResolutionCache<E>,
    key: &'a str,
    flight: u64,
    sender: watch::Sender<Landed<E>>,
    landed: bool,
}

impl<E: Replicate> Flight<'_, E> {
    fn land(mut self, name: &str, found: Result<Lookup, E>) -> Result<Vec<IpAddr>, E> {
        let now = Instant::now();
        let mut slots = self.cache.slots.lock().expect("resolution cache");
        let (shared, result) = match found {
            Ok(lookup) => {
                let answer = Answer::of(&lookup);
                answer.noted(name, false);
                match self.cache.policy.hold(&lookup) {
                    Some(held) => {
                        slots.names.insert(
                            self.key.to_owned(),
                            Slot::Held {
                                answer: answer.clone(),
                                until: now + held,
                            },
                        );
                        slots.trim(now, self.cache.policy.capacity);
                    }
                    None => {
                        slots.names.remove(self.key);
                    }
                }
                (Ok(answer), Ok(lookup.addresses))
            }
            Err(error) => {
                slots.names.remove(self.key);
                (Err(Arc::new(error.replicate())), Err(error))
            }
        };
        self.sender.send_replace(Some(shared));
        self.landed = true;
        result
    }
}

impl<E> Drop for Flight<'_, E> {
    fn drop(&mut self) {
        if self.landed {
            return;
        }
        let Ok(mut slots) = self.cache.slots.lock() else {
            return;
        };
        if matches!(
            slots.names.get(self.key),
            Some(Slot::Asking { flight, .. }) if *flight == self.flight
        ) {
            slots.names.remove(self.key);
        }
    }
}

// The host's own DNS. Every dial that resolves with it shares this cache.
static HOST: LazyLock<ResolutionCache<io::Error>> = LazyLock::new(ResolutionCache::default);

// The addresses the host's own DNS gives for `host`, in the order it gives
// them, without repeats.
#[expect(
    clippy::disallowed_methods,
    reason = "the host resolver every Host-resolved leg shares"
)]
pub async fn host_lookup(host: &str) -> io::Result<Vec<IpAddr>> {
    if host.parse::<IpAddr>().is_err() {
        record::asking(host, record::Asker::host);
    }
    HOST.resolve(Resolver::Host, host, || async {
        let found = tokio::net::lookup_host((host, 0)).await;
        metrics::record_resolution(Resolver::Host, found.is_ok());
        Ok(untimed(found?.map(|peer| peer.ip())))
    })
    .await
    .inspect(|addresses| record::found(host, addresses))
    .inspect_err(|error| record::failed_saying(host, error))
}

// `host_lookup` for a thread that may block.
#[expect(
    clippy::disallowed_methods,
    reason = "the host resolver every Host-resolved leg shares"
)]
pub fn host_lookup_blocking(host: &str) -> io::Result<Vec<IpAddr>> {
    HOST.resolve_blocking(Resolver::Host, host, || {
        let found = std::net::ToSocketAddrs::to_socket_addrs(&(host, 0));
        metrics::record_resolution(Resolver::Host, found.is_ok());
        Ok(untimed(found?.map(|peer| peer.ip())))
    })
}

// Drops what the host's own DNS gave for `host`.
pub fn forget_host(host: &str) {
    HOST.forget(host);
}

// Connects to the first of `host`'s addresses that answers, as
// `TcpStream::connect` does, resolving with the host's own DNS. When none
// answers, the name is asked afresh next time.
#[expect(
    clippy::disallowed_methods,
    reason = "a leg that dials directly with the host resolver"
)]
pub async fn connect_host(host: &str, port: u16) -> io::Result<TcpStream> {
    let mut last = None;
    for ip in host_lookup(host).await? {
        match TcpStream::connect(SocketAddr::new(ip, port)).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last = Some(error),
        }
    }
    forget_host(host);
    Err(last.unwrap_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, NO_ADDRESS)))
}

// What `connect_host` says of a name with no address, as std says it; `fault`
// reads it back.
pub(crate) const NO_ADDRESS: &str = "could not resolve to any address";

fn untimed(found: impl Iterator<Item = IpAddr>) -> Lookup {
    let mut addresses = Vec::new();
    for ip in found {
        if !addresses.contains(&ip) {
            addresses.push(ip);
        }
    }
    Lookup {
        addresses,
        ttl: Ttl::Unknown,
        nonexistent: false,
    }
}

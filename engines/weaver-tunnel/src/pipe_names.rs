// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Who answers the names a route dials, carried with the dial rather than
// built into its stages. Hops, pools and sessions are shared between routes
// whose DNS settings differ, so each stage asks the route that is dialling
// through it how a name is to be resolved. A change of a route's DNS setting
// replaces the names it dials with, and the answers held for the old setting
// go with them, as do the DNS connections their lookups kept open between
// lookups; no stage is rebuilt, and no connection a dial made ends.
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, RwLock},
};

use super::{DialError, Dialer, Naming};
use crate::{
    dns::{DnsServer, DnsSessions, Trust, TrustRoots},
    resolution::ResolutionCache,
};

tokio::task_local! {
    static NAMES: Names;
}

// Who answers a route's names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DnsChoice {
    // The host's own resolver.
    System,
    // The proxy that carries each dial: a stream proxy is handed the name, a
    // WireGuard tunnel's own servers are asked through it. A stage with no
    // proxy is answered by the host when `system_fallback` is set, and fails
    // its lookups when it is not.
    Proxy {
        trust: Trust,
        system_fallback: bool,
    },
    // These servers, asked in order, through `transit`.
    Weaver {
        servers: Vec<DnsServer>,
        trust: Trust,
        transit: DnsTransit,
    },
    // Nothing may answer: the route's setting cannot be read or cannot be
    // met. Every lookup fails, saying why.
    Refused(String),
}

// Which way a route's listed servers are reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DnsTransit {
    // Straight out of the leg's egress.
    Egress,
    // Through the last hop of the leg's active proxy, or its egress on a
    // stage with no proxy.
    Proxy,
}

#[derive(Clone)]
struct Held {
    answers: Arc<ResolutionCache<DialError>>,
    sessions: Arc<DnsSessions>,
}

impl Held {
    fn new(roots: &Option<Arc<TrustRoots>>) -> Self {
        Self {
            answers: Arc::default(),
            sessions: Arc::new(DnsSessions::with_roots(roots.clone())),
        }
    }
}

struct Inner {
    choice: DnsChoice,
    // The leg's egress, which listed servers are asked from.
    egress: Option<Arc<dyn Dialer>>,
    // What each path's TLS sessions and validator trust: None for the
    // built-in roots, built only when a lookup first needs them.
    roots: Option<Arc<TrustRoots>>,
    // The answers, TLS sessions, kept DNS connections and DNSSEC validator
    // of each path names are asked on, kept apart so nothing is shared
    // between legs on different paths.
    held: Mutex<HashMap<String, Held>>,
}

// One route's DNS setting on one egress, and every answer held under it.
#[derive(Clone)]
pub struct Names {
    inner: Arc<Inner>,
    // Whether the name dialled is a proxy's or tunnel's own endpoint.
    endpoint: bool,
}

impl Names {
    pub fn new(choice: DnsChoice, egress: Option<Arc<dyn Dialer>>) -> Self {
        Self::rooted(choice, egress, None)
    }
    // The same, trusting a test's own roots.
    #[cfg(test)]
    pub(crate) fn with_roots(
        choice: DnsChoice,
        egress: Option<Arc<dyn Dialer>>,
        roots: Arc<TrustRoots>,
    ) -> Self {
        Self::rooted(choice, egress, Some(roots))
    }
    fn rooted(
        choice: DnsChoice,
        egress: Option<Arc<dyn Dialer>>,
        roots: Option<Arc<TrustRoots>>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                choice,
                egress,
                roots,
                held: Mutex::new(HashMap::new()),
            }),
            endpoint: false,
        }
    }
    pub fn choice(&self) -> &DnsChoice {
        &self.inner.choice
    }
    // Runs `work` with `names` answering every name it dials, or as it is
    // when there are none.
    pub async fn scope<F: Future>(names: Option<Names>, work: F) -> F::Output {
        match names {
            Some(names) => NAMES.scope(names, work).await,
            None => work.await,
        }
    }
    // The same, for work that runs on a blocking thread.
    pub fn sync_scope<R>(names: Option<Names>, work: impl FnOnce() -> R) -> R {
        match names {
            Some(names) => NAMES.sync_scope(names, work),
            None => work(),
        }
    }
    // The names of the dial this task is making, if any.
    pub fn current() -> Option<Names> {
        NAMES.try_with(Clone::clone).ok()
    }
    // These names, for a proxy's or tunnel's own endpoint.
    pub fn for_endpoint(&self) -> Names {
        Self {
            inner: self.inner.clone(),
            endpoint: true,
        }
    }
    fn held(&self, path: &str) -> Held {
        self.inner
            .held
            .lock()
            .expect("held names")
            .entry(path.to_owned())
            .or_insert_with(|| Held::new(&self.inner.roots))
            .clone()
    }
    fn servers_at_egress(&self, servers: &[DnsServer], trust: Trust) -> Naming {
        let Some(egress) = &self.inner.egress else {
            return Naming::Refused(
                "the route's DNS servers have no egress to be asked from".into(),
            );
        };
        let held = self.held("egress");
        Naming::Servers {
            servers: servers.to_vec(),
            trust,
            egress: egress.clone(),
            answers: held.answers,
            sessions: held.sessions,
        }
    }
    // How a name dialled straight out of the egress is resolved. A proxy's
    // own endpoint is named below the proxy: by the listed servers when
    // Weaver answers, else by the host.
    pub(crate) fn at_egress(&self, endpoint: bool) -> Naming {
        let pick = NamingPolicy(&self.inner.choice).pick(Stage::Egress, endpoint || self.endpoint);
        self.named(pick)
    }
    // How a name dialled through the hop on `path` is resolved. `tunnel` is
    // the DNS servers of a WireGuard hop's tunnel, None for any other kind.
    pub(crate) fn at_hop(&self, path: &str, tunnel: Option<&[DnsServer]>) -> Naming {
        let pick =
            NamingPolicy(&self.inner.choice).pick(Stage::Hop { path, tunnel }, self.endpoint);
        self.named(pick)
    }
    // The naming `pick` says, holding the answers and TLS sessions of the
    // path it asks on: the hop's path for servers asked through the hop,
    // the egress's own for servers asked from it.
    fn named(&self, pick: Pick<'_>) -> Naming {
        match pick {
            Pick::Host => Naming::Host,
            Pick::Proxy => Naming::Proxy,
            Pick::Refused(reason) => Naming::Refused(reason),
            Pick::Listed {
                servers,
                trust,
                via: Via::Egress,
            } => self.servers_at_egress(servers, trust),
            Pick::Listed {
                servers,
                trust,
                via: Via::Hop { path },
            } => {
                let held = self.held(path);
                Naming::ProxyServers {
                    servers: servers.to_vec(),
                    trust,
                    answers: held.answers,
                    sessions: held.sessions,
                }
            }
        }
    }
}

// The stage a name is dialled from.
#[derive(Clone, Copy)]
enum Stage<'a> {
    // Straight out of the leg's egress.
    Egress,
    // Through the proxy hop on `path`. `tunnel` is the DNS servers of a
    // WireGuard hop's tunnel, None for any other kind.
    Hop {
        path: &'a str,
        tunnel: Option<&'a [DnsServer]>,
    },
}

// Where listed servers are asked from. Only a hop stage has a path to
// give, so servers asked through a hop are never held in the egress's slot.
#[derive(Clone, Copy)]
enum Via<'a> {
    // Straight out of the leg's egress, held in the egress's own slot.
    Egress,
    // Through the hop on `path`, held in that path's slot.
    Hop { path: &'a str },
}

// Which resolver answers a name, with what it needs.
enum Pick<'a> {
    // The host's own resolver.
    Host,
    // The proxy the dial goes through, handed the name.
    Proxy,
    // These servers, asked in order and judged by `trust`.
    Listed {
        servers: &'a [DnsServer],
        trust: Trust,
        via: Via<'a>,
    },
    // No one: every lookup fails, saying why.
    Refused(String),
}

// A route's DNS choice, read as one table: for each stage a name is dialled
// from, and whether the name is a proxy's or tunnel's own endpoint, which
// resolver answers it.
struct NamingPolicy<'a>(&'a DnsChoice);

impl<'a> NamingPolicy<'a> {
    fn pick(&self, stage: Stage<'a>, endpoint: bool) -> Pick<'a> {
        match (self.0, stage, endpoint) {
            (DnsChoice::Refused(reason), _, _) => Pick::Refused(reason.clone()),
            (DnsChoice::System, Stage::Egress, _) => Pick::Host,
            (DnsChoice::System, Stage::Hop { .. }, false) => Pick::Host,
            // A proxy's own address on the host's profile, carried by this
            // hop, is resolved by the hop: a proxy is handed the name, a
            // WireGuard tunnel's servers are asked through it, and the host
            // is never asked.
            (DnsChoice::System, Stage::Hop { path, tunnel }, true) => through_tunnel(
                path,
                tunnel,
                Trust::AsSent,
                "the WireGuard proxy carrying this proxy lists no DNS servers",
            ),
            // Below the proxy, its own endpoint is named by the host.
            (DnsChoice::Proxy { .. }, Stage::Egress, true) => Pick::Host,
            (
                DnsChoice::Proxy {
                    system_fallback: true,
                    ..
                },
                Stage::Egress,
                false,
            ) => Pick::Host,
            (DnsChoice::Proxy { .. }, Stage::Egress, false) => Pick::Refused(
                "the route's DNS is answered by its proxy, and this stage has no proxy".into(),
            ),
            (DnsChoice::Proxy { trust, .. }, Stage::Hop { path, tunnel }, _) => through_tunnel(
                path,
                tunnel,
                *trust,
                "the WireGuard proxy lists no DNS servers",
            ),
            (DnsChoice::Weaver { servers, trust, .. }, Stage::Egress, _) => Pick::Listed {
                servers,
                trust: *trust,
                via: Via::Egress,
            },
            // A proxy's own address named by listed servers, dialled through
            // the hop beneath it, asks them on the path below that hop.
            (DnsChoice::Weaver { servers, trust, .. }, Stage::Hop { path, .. }, true) => {
                Pick::Listed {
                    servers,
                    trust: *trust,
                    via: Via::Hop { path },
                }
            }
            (
                DnsChoice::Weaver {
                    servers,
                    trust,
                    transit,
                },
                Stage::Hop { path, .. },
                false,
            ) => Pick::Listed {
                servers,
                trust: *trust,
                via: match transit {
                    DnsTransit::Egress => Via::Egress,
                    DnsTransit::Proxy => Via::Hop { path },
                },
            },
        }
    }
}

// The hop resolves the name itself: a WireGuard tunnel's own servers are
// asked through it, judged by `trust`, and any other proxy is handed the
// name, which only answers taken as sent allow. A tunnel that lists no
// servers refuses, saying `none_listed`. The tunnel's servers are held in
// the slot of the hop's `path`.
fn through_tunnel<'a>(
    path: &'a str,
    tunnel: Option<&'a [DnsServer]>,
    trust: Trust,
    none_listed: &str,
) -> Pick<'a> {
    match tunnel {
        Some([]) => Pick::Refused(none_listed.into()),
        Some(servers) => Pick::Listed {
            servers,
            trust,
            via: Via::Hop { path },
        },
        None if trust == Trust::AsSent => Pick::Proxy,
        None => Pick::Refused("only a WireGuard proxy's answers can be validated here".into()),
    }
}

// The names a leg dials with, which a reload replaces in place: a leg, and a
// session built on it, keep the slot and read the names from it at each dial.
#[derive(Default)]
pub struct NamesSlot(RwLock<Option<Names>>);

impl NamesSlot {
    pub fn new(names: Names) -> Self {
        Self(RwLock::new(Some(names)))
    }
    pub fn get(&self) -> Option<Names> {
        self.0.read().expect("names slot").clone()
    }
    pub fn set(&self, names: Names) {
        *self.0.write().expect("names slot") = Some(names);
    }
}

#[cfg(test)]
#[path = "pipe_names_tests.rs"]
mod tests;

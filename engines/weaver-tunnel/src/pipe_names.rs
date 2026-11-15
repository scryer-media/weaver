// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Who answers the names a route dials, carried with the dial rather than
// built into its stages. Hops, pools and sessions are shared between routes
// whose DNS settings differ, so each stage asks the route that is dialling
// through it how a name is to be resolved. A change of a route's DNS setting
// replaces the names it dials with, and the answers held for the old setting
// go with them; no stage is rebuilt and no connection ends.
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex, RwLock},
};

use super::{DialError, Dialer, Naming};
use crate::{
    dns::{DnsServer, DnsSessions, Trust},
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

#[derive(Clone, Default)]
struct Held {
    answers: Arc<ResolutionCache<DialError>>,
    sessions: Arc<DnsSessions>,
}

struct Inner {
    choice: DnsChoice,
    // The leg's egress, which listed servers are asked from.
    egress: Option<Arc<dyn Dialer>>,
    // The answers and TLS sessions of each path names are asked on, kept
    // apart so no answer is shared between legs on different paths.
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
        Self {
            inner: Arc::new(Inner {
                choice,
                egress,
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
            .or_default()
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
        let endpoint = endpoint || self.endpoint;
        match &self.inner.choice {
            DnsChoice::Refused(reason) => Naming::Refused(reason.clone()),
            DnsChoice::System => Naming::Host,
            DnsChoice::Proxy { .. } if endpoint => Naming::Host,
            DnsChoice::Proxy {
                system_fallback: true,
                ..
            } => Naming::Host,
            DnsChoice::Proxy { .. } => Naming::Refused(
                "the route's DNS is answered by its proxy, and this stage has no proxy".into(),
            ),
            DnsChoice::Weaver { servers, trust, .. } => self.servers_at_egress(servers, *trust),
        }
    }
    // How a name dialled through the hop on `path` is resolved. `tunnel` is
    // the DNS servers of a WireGuard hop's tunnel, None for any other kind.
    pub(crate) fn at_hop(&self, path: &str, tunnel: Option<&[DnsServer]>) -> Naming {
        match &self.inner.choice {
            DnsChoice::Refused(reason) => Naming::Refused(reason.clone()),
            // A proxy's own address on the host's profile, carried by this
            // hop, is resolved by the hop: a proxy is handed the name, a
            // WireGuard tunnel's servers are asked through it, and the host
            // is never asked.
            DnsChoice::System if self.endpoint => match tunnel {
                Some([]) => Naming::Refused(
                    "the WireGuard proxy carrying this proxy lists no DNS servers".into(),
                ),
                Some(servers) => {
                    let held = self.held(path);
                    Naming::ProxyServers {
                        servers: servers.to_vec(),
                        trust: Trust::AsSent,
                        answers: held.answers,
                        sessions: held.sessions,
                    }
                }
                None => Naming::Proxy,
            },
            DnsChoice::System => Naming::Host,
            DnsChoice::Proxy { trust, .. } => match tunnel {
                Some([]) => Naming::Refused("the WireGuard proxy lists no DNS servers".into()),
                Some(servers) => {
                    let held = self.held(path);
                    Naming::ProxyServers {
                        servers: servers.to_vec(),
                        trust: *trust,
                        answers: held.answers,
                        sessions: held.sessions,
                    }
                }
                None if *trust == Trust::AsSent => Naming::Proxy,
                None => {
                    Naming::Refused("only a WireGuard proxy's answers can be validated here".into())
                }
            },
            // A proxy's own address named by listed servers, dialled through
            // the hop beneath it, asks them on the path below that hop.
            DnsChoice::Weaver { servers, trust, .. } if self.endpoint => {
                let held = self.held(path);
                Naming::ProxyServers {
                    servers: servers.clone(),
                    trust: *trust,
                    answers: held.answers,
                    sessions: held.sessions,
                }
            }
            DnsChoice::Weaver {
                servers,
                trust,
                transit: DnsTransit::Egress,
            } => self.servers_at_egress(servers, *trust),
            DnsChoice::Weaver {
                servers,
                trust,
                transit: DnsTransit::Proxy,
            } => {
                let held = self.held(path);
                Naming::ProxyServers {
                    servers: servers.clone(),
                    trust: *trust,
                    answers: held.answers,
                    sessions: held.sessions,
                }
            }
        }
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

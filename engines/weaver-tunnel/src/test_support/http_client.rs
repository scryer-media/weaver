// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// HTTP clients on the host's own egress, for a consumer's tests that need a
// finished client and have no proxy runtime to take one from. Every
// connection one opens is dialed by the system egress, through a bridge the
// process starts the first time such a client is asked for. A client looks
// no name up itself: it hands each name to its bridge.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use crate::bridge::Bridge;
use crate::egress::SocketEgress;
use crate::error::TunnelError;
use crate::pipe::{Dialer, Egress, Purpose, Target};
use crate::provider::{TunnelProvider, TunnelStream};

// A host, its port, and the addresses a client dials it at.
type Pin = (String, u16, Vec<IpAddr>);

// The system egress, as the tunnel a bridge dials through: a pinned host at
// its addresses alone, and only it; with no pin, any host, a name resolved
// by the egress.
struct SystemEgress {
    egress: Egress,
    pin: Option<Pin>,
}

#[async_trait::async_trait]
impl TunnelProvider for SystemEgress {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let host = host.trim_matches(['[', ']']);
        let addresses = match &self.pin {
            None => Vec::new(),
            Some((pinned, at, addresses)) if pinned == host && *at == port => addresses.clone(),
            Some((pinned, at, _)) => {
                return Err(TunnelError::Engine(format!(
                    "{host}:{port}: the client is held to {pinned}:{at}"
                )));
            }
        };
        let target = Target {
            host: host.into(),
            port,
            purpose: Purpose::Outbound,
            addresses,
        };
        let dialed = self
            .egress
            .dial(&target)
            .await
            .map_err(|error| TunnelError::Engine(error.to_string()))?;
        Ok(dialed.into_observed_stream())
    }

    fn describe(&self) -> String {
        "the system egress".into()
    }
}

// The runtime the bridges serve on, for as long as the process runs,
// whichever runtime a client is used on.
fn runtime() -> &'static tokio::runtime::Handle {
    static RUNTIME: OnceLock<tokio::runtime::Handle> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("the system egress bridges' runtime");
        let handle = runtime.handle().clone();
        std::thread::spawn(move || runtime.block_on(std::future::pending::<()>()));
        handle
    })
}

// The bridge to the system egress for `pin`, started the first time it is
// asked for and kept while the process runs.
fn bridge(pin: Option<Pin>) -> Arc<Bridge> {
    static BRIDGES: OnceLock<Mutex<HashMap<Option<Pin>, Arc<Bridge>>>> = OnceLock::new();
    let mut bridges = BRIDGES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let bridge = bridges.entry(pin.clone()).or_insert_with(|| {
        Bridge::start(
            runtime(),
            Arc::new(SystemEgress {
                egress: system_egress(),
                pin,
            }),
            Duration::MAX,
            ("direct".into(), "client".into()),
        )
        .expect("a system egress bridge")
    });
    Arc::clone(bridge)
}

/// The host's own egress, alone, as a route with nothing saved leaves, and
/// with no connect deadline: a test waits on what it checks.
pub fn system_dialer() -> Arc<dyn Dialer> {
    Arc::new(system_egress())
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test's own route: the host's egress with nothing saved"
)]
fn system_egress() -> Egress {
    Egress::new(0, SocketEgress::System, Duration::MAX)
}

#[expect(
    clippy::disallowed_methods,
    reason = "a test's client, every connection of it through the system egress bridge"
)]
fn through(builder: reqwest::ClientBuilder, bridge: &Bridge) -> reqwest::Result<reqwest::Client> {
    let addr = bridge.addr().expect("a system egress bridge's address");
    let (user, password) = bridge.credentials();
    builder
        .proxy(reqwest::Proxy::all(format!("socks5h://{addr}"))?.basic_auth(user, password))
        .build()
}

/// The client `builder` makes, every connection of it dialed by the
/// system egress, as a route with nothing saved leaves: a name is resolved
/// there.
pub fn direct_client(builder: reqwest::ClientBuilder) -> reqwest::Result<reqwest::Client> {
    through(builder, &bridge(None))
}

/// [`direct_client`], held to `host` on `port` at `addresses`: it dials
/// them alone, and no other host.
pub fn pinned_client(
    builder: reqwest::ClientBuilder,
    host: &str,
    port: u16,
    addresses: &[IpAddr],
) -> reqwest::Result<reqwest::Client> {
    let pin = (host.to_owned(), port, addresses.to_vec());
    through(builder, &bridge(Some(pin)))
}

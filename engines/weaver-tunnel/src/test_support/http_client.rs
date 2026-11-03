// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// HTTP clients on the host's own egress, for a consumer's tests that need a
// finished client and have no proxy runtime to take one from. Every
// connection one opens is dialed by the system egress, through a bridge
// started when a client is asked for and kept while a client holds it. A
// client looks no name up itself: it hands each name to its bridge.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
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

// The runtime the bridges serve on, whichever runtime a client is used on.
// Its thread runs while a bridge does, and ends with the last of them.
struct BridgeRuntime {
    handle: tokio::runtime::Handle,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

#[cfg(test)]
static RUNTIME_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

impl BridgeRuntime {
    fn start() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("the system egress bridges' runtime");
        let handle = runtime.handle().clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        #[cfg(test)]
        RUNTIME_THREADS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let thread = std::thread::spawn(move || {
            let _ = runtime.block_on(stopped);
            drop(runtime);
            #[cfg(test)]
            RUNTIME_THREADS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        });
        Self {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        }
    }
}

impl Drop for BridgeRuntime {
    fn drop(&mut self) {
        drop(self.stop.take());
        // Waited for only outside any runtime: a runtime's thread that
        // dropped the last client could be one the stop waits on.
        if let Some(thread) = self.thread.take()
            && tokio::runtime::Handle::try_current().is_err()
        {
            let _ = thread.join();
        }
    }
}

// A bridge, with the runtime it serves on. Each client of it holds it, so
// it stops when the last of them is dropped, and the runtime with the last
// bridge.
struct Running {
    bridge: Arc<Bridge>,
    _runtime: Arc<BridgeRuntime>,
}

#[derive(Default)]
struct Bridges {
    running: HashMap<Option<Pin>, Weak<Running>>,
    runtime: Weak<BridgeRuntime>,
}

// The bridge to the system egress for `pin`: the one a live client already
// uses, else one started now.
fn bridge(pin: Option<Pin>) -> Arc<Running> {
    static BRIDGES: OnceLock<Mutex<Bridges>> = OnceLock::new();
    let mut bridges = BRIDGES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    bridges
        .running
        .retain(|_, running| running.strong_count() > 0);
    if let Some(running) = bridges.running.get(&pin).and_then(Weak::upgrade) {
        return running;
    }
    let runtime = bridges.runtime.upgrade().unwrap_or_else(|| {
        let runtime = Arc::new(BridgeRuntime::start());
        bridges.runtime = Arc::downgrade(&runtime);
        runtime
    });
    let bridge = Bridge::start(
        &runtime.handle,
        Arc::new(SystemEgress {
            egress: system_egress(),
            pin: pin.clone(),
        }),
        Duration::MAX,
        ("direct".into(), "client".into()),
    )
    .expect("a system egress bridge");
    let running = Arc::new(Running {
        bridge,
        _runtime: runtime,
    });
    bridges.running.insert(pin, Arc::downgrade(&running));
    running
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
fn through(
    builder: reqwest::ClientBuilder,
    running: Arc<Running>,
) -> reqwest::Result<reqwest::Client> {
    let addr = running
        .bridge
        .addr()
        .expect("a system egress bridge's address");
    let proxy = reqwest::Url::parse(&format!("socks5h://{addr}")).expect("a bridge's proxy URL");
    let (user, password) = running.bridge.credentials();
    let (user, password) = (user.to_owned(), password.to_owned());
    // The client holds its bridge for as long as it, or a clone, lives.
    let every_request = reqwest::Proxy::custom(move |_| {
        let _ = &running;
        Some(proxy.clone())
    });
    builder
        .proxy(every_request.basic_auth(&user, &password))
        .build()
}

/// The client `builder` makes, every connection of it dialed by the
/// system egress, as a route with nothing saved leaves: a name is resolved
/// there.
pub fn direct_client(builder: reqwest::ClientBuilder) -> reqwest::Result<reqwest::Client> {
    through(builder, bridge(None))
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
    through(builder, bridge(Some(pin)))
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::sync::atomic::Ordering;

    use super::{RUNTIME_THREADS, direct_client, pinned_client};

    // The bridges' runtime thread runs while a client holds a bridge, and
    // ends when the last client goes.
    #[test]
    fn the_bridges_runtime_ends_with_its_last_client() {
        let direct = direct_client(reqwest::Client::builder()).unwrap();
        let pinned = pinned_client(
            reqwest::Client::builder(),
            "seed.test",
            80,
            &[IpAddr::from([127, 0, 0, 1])],
        )
        .unwrap();
        let clone = direct.clone();
        assert_eq!(RUNTIME_THREADS.load(Ordering::SeqCst), 1);
        drop(direct);
        drop(pinned);
        assert_eq!(RUNTIME_THREADS.load(Ordering::SeqCst), 1);
        drop(clone);
        assert_eq!(RUNTIME_THREADS.load(Ordering::SeqCst), 0);

        // A client asked for later starts the runtime again.
        let again = direct_client(reqwest::Client::builder()).unwrap();
        assert_eq!(RUNTIME_THREADS.load(Ordering::SeqCst), 1);
        drop(again);
        assert_eq!(RUNTIME_THREADS.load(Ordering::SeqCst), 0);
    }
}

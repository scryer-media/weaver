// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The sockets and HTTP clients the engine's tests open.
//!
//! Sockets come from [`KernelSockets`], the factory a session on an
//! own-socket leg is handed. Clients come from [`build`], the one place the
//! engine's tests build one; an embedder builds a session's clients, so
//! nothing outside the tests here does.

use std::{
    collections::HashMap,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use crate::{
    core::net::{BoxFuture, ClientShape, DatagramSocket, Egress, HttpClients, HttpTarget},
    network::{KernelSockets, SocketFactory},
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
};

/// The kernel sockets of a session bound to no device.
pub(crate) fn sockets() -> KernelSockets {
    KernelSockets::new(None).expect("kernel sockets on no device")
}

/// The kernel factory, shared.
pub(crate) fn socket_factory() -> std::sync::Arc<dyn crate::network::SocketFactory> {
    std::sync::Arc::new(sockets())
}

/// A session's default options, over the kernel's sockets.
pub(crate) fn session_options() -> crate::SessionOptions {
    crate::SessionOptions::new(socket_factory())
}

/// A datagram socket bound at `addr`.
pub(crate) async fn udp(addr: impl Into<SocketAddr>) -> Arc<dyn DatagramSocket> {
    let addr = addr.into();
    sockets()
        .bind_udp(addr)
        .await
        .unwrap_or_else(|e| panic!("binding a datagram socket at {addr}: {e}"))
}

/// A datagram socket bound at `addr`, or why it is not.
pub(crate) async fn udp_at(addr: SocketAddr) -> io::Result<Arc<dyn DatagramSocket>> {
    sockets().bind_udp(addr).await
}

/// A datagram socket on an ephemeral loopback port.
pub(crate) async fn loopback_udp() -> Arc<dyn DatagramSocket> {
    udp((Ipv4Addr::LOCALHOST, 0)).await
}

/// [`loopback_udp`], as the kernel socket it is, for a test that reads
/// only what has already arrived.
pub(crate) fn loopback_kernel_udp() -> Arc<crate::network::KernelUdp> {
    sockets()
        .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
        .expect("binding a loopback datagram socket")
}

/// [`loopback_udp`], from a caller that cannot wait: the kernel factory
/// binds at once, inside the caller's runtime.
pub(crate) fn loopback_udp_now() -> Arc<dyn DatagramSocket> {
    use futures::FutureExt;
    sockets()
        .bind_udp((Ipv4Addr::LOCALHOST, 0).into())
        .now_or_never()
        .expect("the kernel factory binds at once")
        .expect("binding a loopback datagram socket")
}

/// A stream connection to `addr`, as its two halves.
pub(crate) async fn tcp(
    addr: impl Into<SocketAddr>,
) -> io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)> {
    sockets().connect_tcp(addr.into()).await
}

/// The client `builder` makes, from the routing layer's direct egress.
pub(crate) fn build(builder: reqwest::ClientBuilder) -> anyhow::Result<reqwest::Client> {
    Ok(weaver_tunnel::test_support::http_client::direct_client(
        builder,
    )?)
}

/// The client of `shape` for `target`, from `builder`: one held to a
/// target follows no redirect, and dials the target's host at the target's
/// addresses only.
pub(crate) fn client(
    builder: reqwest::ClientBuilder,
    target: Option<HttpTarget<'_>>,
    shape: &ClientShape,
) -> anyhow::Result<reqwest::Client> {
    let Some(target) = target else {
        return build(shape.apply(builder)?);
    };
    let builder = shape.apply(builder.redirect(reqwest::redirect::Policy::none()))?;
    // An address in the URL is dialed as it is.
    if target.host.parse::<IpAddr>().is_ok() || target.addrs.is_empty() {
        return build(builder);
    }
    Ok(weaver_tunnel::test_support::http_client::pinned_client(
        builder,
        target.host,
        target.port,
        target.addrs,
    )?)
}

/// Where a request to `to` leaves on a loopback route: a loopback address
/// from itself; anything else through a proxy, since the route reaches no
/// other.
pub(crate) fn loopback_egress(to: SocketAddr) -> Egress {
    if to.ip().is_loopback() {
        Egress::From(to.ip())
    } else {
        Egress::Proxied
    }
}

type Key = (String, u16, Vec<IpAddr>, ClientShape);

/// A session's clients on a loopback route, each from `base`: one per
/// target and shape, built once and handed back each time it is asked
/// again; its port mapper's socket from the kernel sockets.
pub(crate) struct Loopback<F = fn() -> reqwest::ClientBuilder> {
    base: F,
    kept: Mutex<HashMap<Key, reqwest::Client>>,
}

fn no_proxy() -> reqwest::ClientBuilder {
    reqwest::Client::builder().no_proxy()
}

impl Default for Loopback {
    fn default() -> Self {
        Self::with(no_proxy as fn() -> reqwest::ClientBuilder)
    }
}

impl<F> Loopback<F>
where
    F: Fn() -> reqwest::ClientBuilder + Send + Sync + 'static,
{
    pub(crate) fn with(base: F) -> Self {
        Self {
            base,
            kept: Mutex::default(),
        }
    }
}

/// [`Loopback::default`], shared.
pub(crate) fn loopback() -> Arc<dyn HttpClients> {
    Arc::new(Loopback::default())
}

impl<F> HttpClients for Loopback<F>
where
    F: Fn() -> reqwest::ClientBuilder + Send + Sync + 'static,
{
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        let key = target.map_or_else(
            || (String::new(), 0, Vec::new(), shape.clone()),
            |t| (t.host.to_owned(), t.port, t.addrs.to_vec(), shape.clone()),
        );
        let mut kept = self.kept.lock().unwrap();
        if let Some(client) = kept.get(&key) {
            return Ok(client.clone());
        }
        let built = client((self.base)(), target, shape)?;
        kept.insert(key, built.clone());
        Ok(built)
    }

    fn egress(&self, to: SocketAddr) -> Egress {
        loopback_egress(to)
    }

    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move { sockets().bind_udp(local).await })
    }
}

/// Open and drop a stream connection to `addr` from outside any runtime:
/// what wakes a blocking accept loop a test serves from.
pub(crate) fn knock(addr: SocketAddr) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime to knock from");
        let _ = runtime.block_on(tcp(addr));
    })
    .join()
    .expect("the knock");
}

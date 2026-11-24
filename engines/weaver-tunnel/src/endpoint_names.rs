// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A tunnel's own endpoint name, asked of the leg the tunnel rides on, so the
// answer goes through that leg's cache, test-via record, metric and address
// family filter.
//
// A proxy's own address is named by the DNS profile the proxy selects, never
// by a route's. Each session is given one slot holding that profile's names,
// and every lookup of its endpoint asks with them: whichever route dials,
// and from the session's own tasks alike. A tunnel stacked on another asks
// through the tunnel beneath, with the same names. A slot that holds no
// names answers nothing.
use crate::pipe::{Dialer, DnsChoice, Names, NamesSlot, Resolution};
use std::{io, net::IpAddr, sync::Arc};

pub struct EndpointNames(pub Arc<dyn Dialer>, pub Arc<NamesSlot>);

#[async_trait::async_trait]
impl crate::endpoint::EndpointResolver for EndpointNames {
    async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        match Names::scope(Some(of_endpoint(&self.1)), self.0.resolve(host)).await {
            Ok(Resolution::Addresses(addresses)) => Ok(addresses),
            Ok(Resolution::Unresolved) => Err(io::Error::other(format!(
                "{host}: {} hands names on and resolves none",
                self.0.describe()
            ))),
            Err(error) => Err(io::Error::other(format!("{host}: {error}"))),
        }
    }
}

// The names a proxy's own address is asked with: its slot's, as an
// endpoint. An empty slot refuses the lookup.
pub(crate) fn of_endpoint(slot: &NamesSlot) -> Names {
    slot.get()
        .unwrap_or_else(|| {
            Names::new(
                DnsChoice::Refused("the proxy's DNS profile is not set".into()),
                None,
            )
        })
        .for_endpoint()
}

// The datagrams of the tunnel a stacked tunnel rides, asked its endpoint
// name with the stacked session's own slot: the name is resolved through
// the tunnel beneath, as those names say.
pub struct EndpointDatagrams(
    pub Arc<dyn crate::endpoint::DatagramTransport>,
    pub Arc<NamesSlot>,
);

impl EndpointDatagrams {
    fn names(&self) -> Option<Names> {
        Some(of_endpoint(&self.1))
    }
}

#[async_trait::async_trait]
impl crate::endpoint::DatagramTransport for EndpointDatagrams {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, crate::TunnelError> {
        Names::scope(self.names(), self.0.resolve(host)).await
    }
    async fn bind(
        &self,
        peer: std::net::SocketAddr,
    ) -> Result<Arc<dyn crate::endpoint::DatagramSocket>, crate::TunnelError> {
        Names::scope(self.names(), self.0.bind(peer)).await
    }
}

#[cfg(test)]
mod tests;

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A tunnel's own endpoint name, asked of the leg the tunnel rides on, so the
// answer goes through that leg's cache, test-via record, metric and address
// family filter.
use crate::pipe::{Dialer, Resolution};
use std::{io, net::IpAddr, sync::Arc};

pub struct EndpointNames(pub Arc<dyn Dialer>);

#[async_trait::async_trait]
impl crate::endpoint::EndpointResolver for EndpointNames {
    async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        match self.0.resolve(host).await {
            Ok(Resolution::Addresses(addresses)) => Ok(addresses),
            Ok(Resolution::Unresolved) => Err(io::Error::other(format!(
                "{host}: {} hands names on and resolves none",
                self.0.describe()
            ))),
            Err(error) => Err(io::Error::other(format!("{host}: {error}"))),
        }
    }
}

#[cfg(test)]
mod tests;

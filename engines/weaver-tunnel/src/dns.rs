// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Bounded DNS-over-TCP to listed servers, asked through a pipe stage: the
// leg's egress or a proxy hop.
#[cfg(test)]
use crate::{
    TunnelError,
    pipe::{DialError, Dialer, Target},
    resolution::Lookup,
};
#[cfg(test)]
use exchange::{Session, Wires};
#[cfg(test)]
use failure::Unanswered;
use failure::{Exchanged, Failed};
#[cfg(test)]
use judge::validating::Dnssec;
pub(crate) use list::ListResolver;
#[cfg(test)]
use list::resolve_judged;
pub use list::resolve_through;
#[cfg(test)]
use message::{parse_response, read_name, u16_at};
use model::{Carrier, FAMILIES, Found, SERVER_TIMEOUT, Stage};
pub use model::{DnsServer, Trust, Wire, WireKind, unresolved};
pub(crate) use model::{INVALID, NOT_VALIDATED, UNRESOLVED};
pub(crate) use resolver::{Answer, ExitResolver, Refusing, Resolver};
pub(crate) use roots::TrustRoots;
pub use sessions::DnsSessions;
#[cfg(test)]
use std::net::{IpAddr, SocketAddr};
#[cfg(test)]
use std::sync::atomic::Ordering;

mod exchange;
mod failure;
mod judge;
mod list;
mod message;
mod model;
mod probe;
mod resolver;
mod roots;
mod sessions;
pub use probe::{Probed, ServerProbe, probe_server};

// Every server plain and every answer taken as sent, as most tests ask.
#[cfg(test)]
async fn resolve_name(
    provider: &dyn Carrier,
    servers: &[DnsServer],
    host: &str,
) -> Result<Lookup, Unanswered> {
    let roots = TrustRoots::built_in();
    resolve(provider, roots.wires(), servers, Trust::AsSent, host).await
}

#[cfg(test)]
async fn resolve(
    provider: &dyn Carrier,
    wires: &Wires,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> Result<Lookup, Unanswered> {
    let dnssec = Dnssec::new(None);
    resolve_judged(provider, wires, &dnssec, servers, trust, host)
        .await
        .0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resolution::{
        Ttl,
        record::{self, LookupFailure, TrustOutcome},
    };
    mod families;
    mod trust;
    mod ttl;
    mod wires;
    #[test]
    fn compression_cycles_and_truncation_are_rejected() {
        assert!(read_name(&[0xc0, 0], &mut 0).is_err());
        assert!(read_name(&[3, b'a'], &mut 0).is_err());
        assert!(parse_response(&[0; 12], 1, "test", 1).is_err());
    }
}

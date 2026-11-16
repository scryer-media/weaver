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
use exchange::{IDLE, REUSED_WAIT, Session, Wires};
#[cfg(test)]
use failure::Unanswered;
use failure::{Exchanged, Failed};
#[cfg(test)]
use judge::validating::Dnssec;
pub(crate) use list::ListResolver;
pub use list::resolve_through;
#[cfg(test)]
use list::{resolve_judged, resolve_unkept};
#[cfg(test)]
use message::u16_at;
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
    // A naming of its own, as a cold lookup has: it keeps what it leaves
    // for no later lookup here.
    let sessions = DnsSessions::of(wires.clone(), Dnssec::new(None));
    resolve_judged(provider, &sessions, servers, trust, host)
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
    mod reading;
    mod trust;
    mod ttl;
    mod wires;
    // Replies are read by the DNSSEC crate's parser, on both paths.
    #[test]
    fn compression_cycles_and_truncation_are_rejected() {
        use weaver_dnssec::{Name, Query, RecordType, Reply};
        let query = Query::new(Name::from_text("test.").unwrap(), RecordType::A);
        let header = [0, 1, 0x81, 0x80, 0, 1, 0, 0, 0, 0, 0, 0];
        // A question name that points at itself.
        let mut reply = header.to_vec();
        reply.extend_from_slice(&[0xc0, 12, 0, 1, 0, 1]);
        assert!(Reply::read(&query, &reply).is_err());
        // One whose label runs past the end of the message.
        let mut reply = header.to_vec();
        reply.extend_from_slice(&[4, b't', b'e', b's']);
        assert!(Reply::read(&query, &reply).is_err());
        assert!(Reply::read(&query, &[0; 12]).is_err());
    }
}

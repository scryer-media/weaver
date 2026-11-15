// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one DNS server can do: whether it answers in plain, over TLS and
// over HTTPS, and whether it hands back the records a DNSSEC check needs.
// Each is asked once, through the stage given, with TLS sessions and a
// validator of the probe's own, so nothing a route's naming holds is used
// or kept. Nothing here is counted in the DNS metrics.
use super::{
    Carrier, DnsServer, FAMILY_QUERY_TIMEOUT, Failed, NEXT_ID, Stage, Why, Wire, WireKind, dnssec,
    refusal, u16_at,
    wire::{DnsSessions, Session, Wires},
};
use crate::{pipe::Dialer, resolution::record::LookupError};
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

// The root's SOA: a question every recursive server answers, and one whose
// check needs the root's DNSKEY set, proven by the built-in root anchors.
const SOA: u16 = 6;

// What one probe came to: whether it worked, how long it took, and why not
// when it did not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Probed {
    pub supported: bool,
    pub elapsed: Duration,
    pub error: Option<LookupError>,
}

// One server's probes. `plain`, `tls` and `https` ask the server's address
// on that wire, at the server's own port, TLS name or path when it is
// listed on that wire and at the wire's usual one otherwise (53, 853, and
// 443 at /dns-query). `dnssec` asks on the server's own wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerProbe {
    pub plain: Probed,
    pub tls: Probed,
    pub https: Probed,
    pub dnssec: Probed,
}

// Probes `server` through `dialer`, every probe at once.
pub async fn probe_server(dialer: &dyn Dialer, server: &DnsServer) -> ServerProbe {
    let stage = Stage {
        dialer,
        failure: Mutex::new(None),
    };
    probe_on(&stage, &DnsSessions::default(), server).await
}

// The server's address on `kind`, as the probe of that wire asks it.
fn on(server: &DnsServer, kind: WireKind) -> DnsServer {
    if server.wire.kind() == kind {
        return server.clone();
    }
    let wire = match kind {
        WireKind::Plain => Wire::Plain,
        WireKind::Tls => Wire::Tls { name: None },
        WireKind::Https => Wire::Https {
            path: "/dns-query".into(),
        },
    };
    DnsServer {
        address: std::net::SocketAddr::new(server.address.ip(), kind.default_port()),
        wire,
    }
}

pub(super) async fn probe_on(
    carrier: &dyn Carrier,
    sessions: &DnsSessions,
    server: &DnsServer,
) -> ServerProbe {
    let wires = sessions.wires();
    let (plain, tls, https) = (
        on(server, WireKind::Plain),
        on(server, WireKind::Tls),
        on(server, WireKind::Https),
    );
    let (plain, tls, https, dnssec) = tokio::join!(
        timed(&plain, answers(carrier, wires, &plain)),
        timed(&tls, answers(carrier, wires, &tls)),
        timed(&https, answers(carrier, wires, &https)),
        timed(server, signed(carrier, wires, sessions.dnssec(), server)),
    );
    ServerProbe {
        plain,
        tls,
        https,
        dnssec,
    }
}

// Runs one probe within the ceiling a family's query has, and says why it
// failed in the words a lookup would use.
async fn timed(
    server: &DnsServer,
    probe: impl std::future::Future<Output = Result<(), Failed>>,
) -> Probed {
    let started = tokio::time::Instant::now();
    let answered = tokio::time::timeout(FAMILY_QUERY_TIMEOUT, probe).await;
    let elapsed = started.elapsed();
    let mut why = Why::default();
    match answered {
        Ok(Ok(())) => {
            return Probed {
                supported: true,
                elapsed,
                error: None,
            };
        }
        Ok(Err(failed)) => why.query(server, &Ok::<_, ()>(Err(failed))),
        Err(elapsed) => why.query(server, &Err::<Result<super::Found, Failed>, _>(elapsed)),
    }
    Probed {
        supported: false,
        elapsed,
        error: why
            .0
            .map(|(failure, detail)| LookupError { failure, detail }),
    }
}

// Whether `server` answers the root's SOA on its wire: any reply to the
// query, whatever its records, so long as it is not an error code.
async fn answers(carrier: &dyn Carrier, wires: &Wires, server: &DnsServer) -> Result<(), Failed> {
    let id = match server.wire {
        Wire::Https { .. } => 0,
        Wire::Plain | Wire::Tls { .. } => NEXT_ID.fetch_add(1, Ordering::Relaxed),
    };
    let mut packet = Vec::from(id.to_be_bytes());
    // Recursion desired; one question: the root, SOA, IN.
    packet.extend_from_slice(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
    packet.extend_from_slice(&SOA.to_be_bytes());
    packet.extend_from_slice(&[0, 1]);
    let mut session = Session::new(carrier, wires, server);
    let bytes = session.ask(&packet, &mut false).await?;
    if let Some(rcode) = refusal(&bytes, id) {
        return Err(Failed::Rcode(rcode));
    }
    let flags = u16_at(&bytes, 2).map_err(|_| Failed::Invalid)?;
    if u16_at(&bytes, 0).ok() != Some(id) || flags & 0x8000 == 0 {
        return Err(Failed::Invalid);
    }
    Ok(())
}

// Whether `server` gives the root's SOA with the signatures and keys that
// prove it from the root anchors: the root's DNSKEY set asked with DO set,
// and checked as a route's lookup under `Validate` checks it.
async fn signed(
    carrier: &dyn Carrier,
    wires: &Wires,
    dnssec: &dnssec::Dnssec,
    server: &DnsServer,
) -> Result<(), Failed> {
    let mut session = Session::new(carrier, wires, server);
    let found = dnssec::validated(&mut session, dnssec, "", SOA, &mut false).await?;
    match found.insecure {
        None => Ok(()),
        // The root anchors prove the root signed, so this only comes of a
        // server whose answer leaves the proof out.
        Some(_) => Err(Failed::Stripped(Vec::new())),
    }
}

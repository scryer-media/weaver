// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A server's probes, against fixture servers that each speak one wire.
use super::*;
use crate::dns::probe::probe_on;

// Fixture servers for one address, one per port: each probe reaches the
// one listening on the port it dials, and a port nobody listens on fails
// the dial.
struct ByPort(Vec<(u16, Servers)>);

#[async_trait::async_trait]
impl Carrier for ByPort {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        match self
            .0
            .iter()
            .find(|(port, _)| *port == server.address.port())
        {
            Some((_, servers)) => servers.open(server).await,
            None => Err(TunnelError::Engine(format!(
                "nothing listens at {}",
                server.address
            ))),
        }
    }
}

impl ByPort {
    fn dials(&self) -> Vec<SocketAddr> {
        self.0
            .iter()
            .flat_map(|(_, servers)| servers.dials())
            .collect()
    }
}

// Sessions of a probe's own that trust `authority` and the fixture's root
// keys.
fn probing(authority: &Authority, resolver: &FakeResolver) -> DnsSessions {
    let sessions = DnsSessions::default();
    sessions.trust(&authority.wires());
    sessions
        .dnssec()
        .anchored(resolver.zones()[0].anchors().unwrap(), NOW);
    sessions
}

// Plain on 53, TLS on `tls_port` with a certificate for `tls_names`, and
// HTTPS over HTTP/2 on 443, all at 192.0.2.53 and answering as `serves`.
fn every_port(
    authority: &Authority,
    resolver: &Arc<FakeResolver>,
    serves: &'static [(&'static str, Serves)],
    tls_port: u16,
    tls_names: &[&str],
) -> ByPort {
    let answer = serving(resolver, serves);
    let plain =
        Servers::new(Speaks::Dot { close_after: None }, answer.clone()).plain_at("192.0.2.53");
    let tls = Servers::new(Speaks::Dot { close_after: None }, answer.clone())
        .at("192.0.2.53", authority.server(tls_names, b"dot"));
    let https = Servers::new(
        Speaks::H2 {
            respond: Arc::new(h2_message),
            goaway_after: None,
            misstep: H2Misstep::None,
        },
        answer,
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"h2"));
    ByPort(vec![(53, plain), (tls_port, tls), (443, https)])
}

#[tokio::test(start_paused = true)]
async fn a_server_on_every_wire_is_probed_on_each_and_checks_dnssec() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = every_port(&authority, &resolver, &[], 853, &["192.0.2.53"]);
    let sessions = probing(&authority, &resolver);
    let before = counted();
    let sent = crate::metrics::dnssec_queries();
    let probe = probe_on(&carrier, &sessions, &first()).await;
    for (wire, probed) in [
        ("plain", &probe.plain),
        ("tls", &probe.tls),
        ("https", &probe.https),
        ("dnssec", &probe.dnssec),
    ] {
        assert!(probed.supported, "{wire}: {probed:?}");
        assert_eq!(probed.error, None, "{wire}");
    }
    // Each wire was asked at its own port.
    let dials = carrier.dials();
    for port in [53, 853, 443] {
        assert!(
            dials.iter().any(|dial| dial.port() == port),
            "{port}: {dials:?}"
        );
    }
    // Nothing the probe asked is counted as a lookup's.
    assert_eq!(crate::metrics::dnssec_queries(), sent);
    assert!(since(&before).is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_listed_wire_is_probed_at_its_own_port_and_name() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    // The certificate names only dns.example, so a probe that checked it
    // against the address would fail.
    let carrier = every_port(&authority, &resolver, &[], 8853, &["dns.example"]);
    let sessions = probing(&authority, &resolver);
    let listed = DnsServer {
        address: "192.0.2.53:8853".parse().unwrap(),
        wire: Wire::Tls {
            name: Some("dns.example".into()),
        },
    };
    let probe = probe_on(&carrier, &sessions, &listed).await;
    assert!(probe.tls.supported, "{:?}", probe.tls);
    assert!(probe.plain.supported, "{:?}", probe.plain);
    assert!(probe.https.supported, "{:?}", probe.https);
    // The DNSSEC check rides the listed wire.
    assert!(probe.dnssec.supported, "{:?}", probe.dnssec);
    assert!(!carrier.dials().iter().any(|dial| dial.port() == 853));
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_that_strips_signatures_fails_the_other_probes_in_a_lookups_words() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let answer = serving(&resolver, &[("192.0.2.53", Serves::Stripped)]);
    let carrier = ByPort(vec![(
        53,
        Servers::new(Speaks::Dot { close_after: None }, answer).plain_at("192.0.2.53"),
    )]);
    let sessions = probing(&authority, &resolver);
    let probe = probe_on(&carrier, &sessions, &first()).await;
    assert!(probe.plain.supported, "{:?}", probe.plain);
    for (wire, probed, port) in [("tls", &probe.tls, 853), ("https", &probe.https, 443)] {
        assert!(!probed.supported, "{wire}");
        assert_eq!(
            probed.error,
            Some(failure(
                LookupFailure::NoReply,
                &format!("nothing listens at 192.0.2.53:{port}")
            )),
            "{wire}"
        );
    }
    assert!(!probe.dnssec.supported);
    assert_eq!(
        probe.dnssec.error.as_ref().map(|error| error.failure),
        Some(LookupFailure::DnssecUnavailable)
    );
}

#[tokio::test(start_paused = true)]
async fn a_server_that_never_replies_fails_each_probe_as_timed_out() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let carrier = ByPort(vec![(
        53,
        Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
            .silent_at("192.0.2.53"),
    )]);
    let sessions = probing(&authority, &resolver);
    let probe = probe_on(&carrier, &sessions, &first()).await;
    for probed in [&probe.plain, &probe.dnssec] {
        assert!(!probed.supported);
        assert_eq!(
            probed.error.as_ref().map(|error| error.failure),
            Some(LookupFailure::TimedOut)
        );
        assert_eq!(probed.elapsed, FAMILY_QUERY_TIMEOUT);
    }
}

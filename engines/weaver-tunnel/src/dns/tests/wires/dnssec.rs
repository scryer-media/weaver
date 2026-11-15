// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNSSEC checked here, on every wire, against zones signed in process.
use super::*;
use weaver_dnssec::testing::{FakeResolver, KeyAlgorithm, Zone as Signed};
use weaver_dnssec::{Name, Query, RecordType};

// The time the fixture's signatures hold at: inside their window.
const NOW: u64 = 1_100_000;
// Well past the window's end, beyond any allowance for skew.
const LATE: u64 = 3_000_000;

// The first server listed: plain, at 192.0.2.53.
fn first() -> DnsServer {
    DnsServer::plain("192.0.2.53".parse().unwrap())
}

fn name(text: &str) -> Name {
    Name::from_text(text).unwrap()
}

// The root, com and example.com signed, with www.example.com's address;
// plain.com beneath com and home.arpa beneath the root, both unsigned.
fn tree(algorithm: KeyAlgorithm) -> Arc<FakeResolver> {
    let root = Signed::new(Name::root(), algorithm).unwrap();
    let com = Signed::new(name("com."), algorithm).unwrap();
    let mut example = Signed::new(name("example.com."), algorithm).unwrap();
    example.add_record(name("www.example.com."), RecordType::A, vec![192, 0, 2, 7]);
    let mut plain = Signed::new(name("plain.com."), algorithm).unwrap();
    plain.unsigned = true;
    plain.add_record(name("www.plain.com."), RecordType::A, vec![192, 0, 2, 8]);
    let mut home = Signed::new(name("home.arpa."), algorithm).unwrap();
    home.unsigned = true;
    home.add_record(name("nas.home.arpa."), RecordType::A, vec![192, 0, 2, 9]);
    Arc::new(FakeResolver::new(vec![root, com, example, plain, home]).unwrap())
}

// A naming's validator that trusts only the fixture's root keys, at `now`.
fn trusting(resolver: &FakeResolver, now: u64) -> Dnssec {
    let dnssec = Dnssec::default();
    dnssec.anchored(resolver.zones()[0].anchors().unwrap(), now);
    dnssec
}

// The question of a query as it went out, and where it ends.
fn question(query: &[u8]) -> (Query, usize) {
    let mut at = 12;
    while query[at] != 0 {
        at += 1 + query[at] as usize;
    }
    let name = Name::from_wire(&query[12..=at]).unwrap();
    let rtype = RecordType(u16::from_be_bytes([query[at + 1], query[at + 2]]));
    (name_query(name, rtype), at + 5)
}

fn name_query(name: Name, rtype: RecordType) -> Query {
    Query { name, rtype }
}

// How a fixture server answers.
#[derive(Clone, Copy, PartialEq)]
enum Serves {
    // As the signed zones say.
    Signed,
    // With every signature left out.
    Stripped,
    // With www.example.com's address changed after it was signed.
    Tampered,
}

// The reply to `query`, with its id, as a server that `serves` gives it.
fn reply(resolver: &FakeResolver, serves: Serves, query: &[u8]) -> Vec<u8> {
    let (asked, end) = question(query);
    let made = match serves {
        Serves::Signed => resolver.answer(&asked),
        Serves::Stripped | Serves::Tampered => resolver.reply(&asked).ok().and_then(|mut reply| {
            if serves == Serves::Stripped {
                reply.strip_signatures();
            } else {
                for record in &mut reply.answer {
                    if record.rtype == RecordType::A {
                        record.rdata = vec![192, 0, 2, 66];
                    }
                }
            }
            reply.encode(&asked).ok()
        }),
    };
    let mut reply = made.unwrap_or_else(|| {
        // SERVFAIL, with the question alone.
        let mut failed = query[..end].to_vec();
        failed[2..4].copy_from_slice(&[0x81, 0x82]);
        failed[6..12].fill(0);
        failed
    });
    reply[..2].copy_from_slice(&query[..2]);
    reply
}

// Every server answers as `serves` says for its address, else signed.
fn serving(
    resolver: &Arc<FakeResolver>,
    serves: &'static [(&'static str, Serves)],
) -> impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + Clone + 'static {
    let resolver = resolver.clone();
    move |asked, _, query| {
        let how = serves
            .iter()
            .find(|(ip, _)| ip.parse::<IpAddr>().unwrap() == asked)
            .map_or(Serves::Signed, |(_, how)| *how);
        reply(&resolver, how, query)
    }
}

// What a test recording says of a validated lookup through `servers`.
async fn checked(
    carrier: &Servers,
    wires: &Wires,
    dnssec: &Dnssec,
    servers: &[DnsServer],
    host: &str,
) -> record::HostResolution {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            record::asking(host, || record::Asker::servers(servers, Trust::Validate));
            let (resolved, _) =
                resolve_judged(carrier, wires, dnssec, servers, Trust::Validate, host).await;
            if let Ok(lookup) = resolved {
                record::found(host, &lookup.addresses);
            }
        })
        .await;
    recording.finish(host, 80, None).expect("a resolution")
}

// One server at 192.0.2.53 on each wire, answering as `answer` says.
fn every_wire(
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + Clone + 'static,
) -> Vec<(Servers, Wires, DnsServer)> {
    let plain =
        Servers::new(Speaks::Dot { close_after: None }, answer.clone()).plain_at("192.0.2.53");
    let mut wires = vec![(
        plain,
        Authority::new().wires(),
        DnsServer::plain("192.0.2.53".parse().unwrap()),
    )];
    for (speaks, alpn, listed) in [
        (
            Speaks::Dot { close_after: None },
            &b"dot"[..],
            dot("192.0.2.53", None),
        ),
        (
            doh_fixture(plain_reply, false),
            &b"http/1.1"[..],
            doh("192.0.2.53:443"),
        ),
        (
            Speaks::H2 {
                respond: Arc::new(h2_message),
                goaway_after: None,
                misstep: H2Misstep::None,
            },
            &b"h2"[..],
            doh("192.0.2.53:443"),
        ),
    ] {
        let authority = Authority::new();
        let servers = Servers::new(speaks, answer.clone())
            .at("192.0.2.53", authority.server(&["192.0.2.53"], alpn));
        wires.push((servers, authority.wires(), listed));
    }
    wires
}

fn bogus(server: &str, words: &str) -> Option<LookupError> {
    Some(failure(
        LookupFailure::Bogus,
        &format!("{server}: DNSSEC validation failed: {words}"),
    ))
}

#[tokio::test(start_paused = true)]
async fn a_signed_answer_validates_on_every_wire() {
    let resolver = tree(KeyAlgorithm::P256);
    for (servers, wires, listed) in every_wire(serving(&resolver, &[])) {
        let dnssec = trusting(&resolver, NOW);
        let before = counted();
        let sent = crate::metrics::dnssec_queries();
        let resolution = checked(
            &servers,
            &wires,
            &dnssec,
            std::slice::from_ref(&listed),
            "www.example.com",
        )
        .await;
        assert_eq!(resolution.addresses, seven(), "{listed:?}");
        assert_eq!(
            resolution.trust,
            Some(TrustOutcome::Validated),
            "{listed:?}"
        );
        assert_eq!(resolution.error, None, "{listed:?}");
        assert_eq!(since(&before), [TrustOutcome::Validated], "{listed:?}");
        // Every query the check needed went to the one server, and each was
        // counted.
        let answered: usize = servers.answered().iter().sum();
        assert!(answered > 2, "{listed:?}");
        assert_eq!(
            crate::metrics::dnssec_queries() - sent,
            answered as u64,
            "{listed:?}"
        );
        assert!(
            servers.dials().iter().all(|dial| *dial == listed.address),
            "{listed:?}"
        );
        match listed.wire {
            // A stream per query.
            Wire::Plain => assert_eq!(servers.dials().len(), answered, "{listed:?}"),
            // One connection for the whole check.
            Wire::Tls { .. } | Wire::Https { .. } => {
                assert_eq!(servers.dials().len(), 1, "{listed:?}")
            }
        }
    }
}

// www.example.com's address, checked against a plain fixture server of its
// own. The check is driven without the ceiling on a family's query, so a
// runtime whose clock cannot be paused sets no deadline.
async fn validated_seven() -> Vec<IpAddr> {
    let resolver = tree(KeyAlgorithm::P256);
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .plain_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let listed = first();
    let mut session = Session::new(&servers, &wires, &listed);
    let mut reached = false;
    let found = query(
        &mut session,
        &dnssec,
        "www.example.com",
        1,
        Trust::Validate,
        &mut reached,
    )
    .await;
    found.map_or_else(|_| Vec::new(), |found| found.addresses)
}

// On a worker of a runtime with several, each step of the check runs in
// place of the worker's other tasks, as in the product.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_answer_validates_on_a_runtime_with_several_workers() {
    let before = crate::dns::dnssec::steps_in_place();
    let found = tokio::spawn(validated_seven()).await.unwrap();
    assert_eq!(found, seven());
    assert!(crate::dns::dnssec::steps_in_place() > before);
}

// Inside a LocalSet on the thread that blocks on a runtime with several
// workers, tokio forbids blocking in place: the steps run inline there,
// in the set's own future and in a task spawned on it.
#[test]
fn a_signed_answer_validates_inside_a_local_set_on_a_runtime_with_several_workers() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    let (inline, spawned) = local.block_on(&runtime, async {
        let inline = validated_seven().await;
        let spawned = tokio::task::spawn_local(validated_seven()).await.unwrap();
        (inline, spawned)
    });
    assert_eq!(inline, seven());
    assert_eq!(spawned, seven());
}

// A runtime with one thread runs the steps inline, in a LocalSet or not.
#[tokio::test(start_paused = true)]
async fn a_signed_answer_validates_on_a_runtime_with_one_thread() {
    let resolver = tree(KeyAlgorithm::P256);
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .plain_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let resolution = checked(&servers, &wires, &dnssec, &[first()], "www.example.com").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    let in_a_set = tokio::task::LocalSet::new()
        .run_until(checked(
            &servers,
            &wires,
            &dnssec,
            &[first()],
            "www.example.com",
        ))
        .await;
    assert_eq!(in_a_set.addresses, seven());
    assert_eq!(in_a_set.trust, Some(TrustOutcome::Validated));
}

// The root, com and example.com signed, with www.example.com's address in
// both families.
fn dual() -> Arc<FakeResolver> {
    let root = Signed::new(Name::root(), KeyAlgorithm::P256).unwrap();
    let com = Signed::new(name("com."), KeyAlgorithm::P256).unwrap();
    let mut example = Signed::new(name("example.com."), KeyAlgorithm::P256).unwrap();
    example.add_record(name("www.example.com."), RecordType::A, vec![192, 0, 2, 7]);
    let six: std::net::Ipv6Addr = "2001:db8::7".parse().unwrap();
    example.add_record(
        name("www.example.com."),
        RecordType::AAAA,
        six.octets().to_vec(),
    );
    Arc::new(FakeResolver::new(vec![root, com, example]).unwrap())
}

// One family's answer tampered with, the other's signed: the lookup answers
// with the family that validated, judged validated. The family that failed
// is counted bogus beside it, once; the lookup's own outcome is counted once
// too, as for any lookup.
#[tokio::test(start_paused = true)]
async fn a_family_that_fails_the_check_is_counted_bogus_when_the_other_answers() {
    let resolver = dual();
    let answered = resolver.clone();
    let answer = move |_: IpAddr, _: u16, query: &[u8]| {
        let (asked, _) = question(query);
        if asked.rtype != RecordType::AAAA {
            return reply(&answered, Serves::Signed, query);
        }
        let mut made = answered.reply(&asked).unwrap();
        for record in &mut made.answer {
            if record.rtype == RecordType::AAAA {
                record.rdata[15] ^= 1;
            }
        }
        let mut bytes = made.encode(&asked).unwrap();
        bytes[..2].copy_from_slice(&query[..2]);
        bytes
    };
    let servers = Servers::new(Speaks::Dot { close_after: None }, answer).plain_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let before = counted();
    let resolution = checked(&servers, &wires, &dnssec, &[first()], "www.example.com").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    assert_eq!(resolution.error, None);
    let mut outcomes = since(&before);
    outcomes.sort_by_key(|outcome| *outcome as usize);
    let mut expected = vec![TrustOutcome::Validated, TrustOutcome::Bogus];
    expected.sort_by_key(|outcome| *outcome as usize);
    assert_eq!(outcomes, expected);
}

// Each DNSSEC query is counted as it goes out: one the ceiling cuts off
// with no reply, and one sent again on a new connection after an HTTP/2
// GOAWAY.
#[tokio::test(start_paused = true)]
async fn each_dnssec_query_is_counted_as_it_goes_out() {
    let resolver = tree(KeyAlgorithm::P256);
    // A plain server that takes each stream and never says anything: each
    // family's first batch waits out the ceiling. Each plain stream carries
    // one query, so every dial is a query sent.
    let silent = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .silent_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let sent = crate::metrics::dnssec_queries();
    let resolution = checked(&silent, &wires, &dnssec, &[first()], "www.example.com").await;
    assert!(resolution.addresses.is_empty());
    let dials = silent.dials().len();
    assert!(dials >= 2);
    assert_eq!(crate::metrics::dnssec_queries() - sent, dials as u64);

    // An HTTP/2 server that sends each connection away after two requests.
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::H2 {
            respond: Arc::new(h2_message),
            goaway_after: Some(2),
            misstep: H2Misstep::None,
        },
        serving(&resolver, &[]),
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"h2"));
    let wires = authority.wires();
    let dnssec = trusting(&resolver, NOW);
    let sent = crate::metrics::dnssec_queries();
    let listed = doh("192.0.2.53:443");
    let resolution = checked(
        &servers,
        &wires,
        &dnssec,
        std::slice::from_ref(&listed),
        "www.example.com",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    let answered: usize = servers.answered().iter().sum();
    assert!(servers.dials().len() > 1);
    assert!(crate::metrics::dnssec_queries() - sent > answered as u64);
}

// DNS over TLS writes a batch at once; a server that closes after every
// reply has the rest of the batch sent again on a new connection, one per
// reply, and the check still validates.
#[tokio::test(start_paused = true)]
async fn dot_validates_through_a_server_that_closes_after_every_reply() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot {
            close_after: Some(1),
        },
        serving(&resolver, &[]),
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"));
    let wires = authority.wires();
    let dnssec = trusting(&resolver, NOW);
    let listed = dot("192.0.2.53", None);
    let resolution = checked(
        &servers,
        &wires,
        &dnssec,
        std::slice::from_ref(&listed),
        "www.example.com",
    )
    .await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    assert!(servers.answered().iter().all(|answered| *answered <= 1));
}

#[tokio::test(start_paused = true)]
async fn each_reply_carries_the_id_its_query_went_out_with() {
    let resolver = tree(KeyAlgorithm::P256);
    // A server that answers every query with an id one off.
    let answered = resolver.clone();
    let off = move |_: IpAddr, _: u16, query: &[u8]| {
        let mut sent = reply(&answered, Serves::Signed, query);
        let id = u16::from_be_bytes([sent[0], sent[1]]).wrapping_add(1);
        sent[..2].copy_from_slice(&id.to_be_bytes());
        sent
    };
    for (servers, wires, listed) in every_wire(off) {
        let dnssec = trusting(&resolver, NOW);
        let resolution = checked(
            &servers,
            &wires,
            &dnssec,
            std::slice::from_ref(&listed),
            "www.example.com",
        )
        .await;
        assert!(resolution.addresses.is_empty(), "{listed:?}");
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(LookupFailure::Other),
            "{listed:?}"
        );
        assert_eq!(resolution.trust, None, "{listed:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn an_unsigned_zone_resolves_unsigned() {
    let resolver = tree(KeyAlgorithm::P256);
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .plain_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let before = counted();
    for (host, address) in [
        ("www.plain.com", "192.0.2.8"),
        ("nas.home.arpa", "192.0.2.9"),
    ] {
        let resolution = checked(&servers, &wires, &dnssec, &[first()], host).await;
        assert_eq!(
            resolution.addresses,
            [address.parse::<IpAddr>().unwrap()],
            "{host}"
        );
        assert_eq!(resolution.trust, Some(TrustOutcome::Unsigned), "{host}");
        assert_eq!(resolution.error, None, "{host}");
    }
    assert_eq!(
        since(&before),
        [TrustOutcome::Unsigned, TrustOutcome::Unsigned]
    );
}

#[tokio::test(start_paused = true)]
async fn a_tampered_answer_is_bogus_and_resolves_nothing_on_every_wire() {
    let resolver = tree(KeyAlgorithm::P256);
    for (servers, wires, listed) in
        every_wire(serving(&resolver, &[("192.0.2.53", Serves::Tampered)]))
    {
        let dnssec = trusting(&resolver, NOW);
        let before = counted();
        let resolution = checked(
            &servers,
            &wires,
            &dnssec,
            std::slice::from_ref(&listed),
            "www.example.com",
        )
        .await;
        assert!(resolution.addresses.is_empty(), "{listed:?}");
        assert_eq!(
            resolution.error,
            bogus("192.0.2.53", "the signatures do not match the records"),
            "{listed:?}"
        );
        assert_eq!(resolution.trust, Some(TrustOutcome::Bogus), "{listed:?}");
        assert_eq!(since(&before), [TrustOutcome::Bogus], "{listed:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_server_that_strips_signatures_leaves_dnssec_unavailable() {
    let resolver = tree(KeyAlgorithm::P256);
    for (servers, wires, listed) in
        every_wire(serving(&resolver, &[("192.0.2.53", Serves::Stripped)]))
    {
        let dnssec = trusting(&resolver, NOW);
        let resolution = checked(
            &servers,
            &wires,
            &dnssec,
            std::slice::from_ref(&listed),
            "www.example.com",
        )
        .await;
        assert!(resolution.addresses.is_empty(), "{listed:?}");
        assert_eq!(
            resolution.error,
            Some(failure(
                LookupFailure::DnssecUnavailable,
                "192.0.2.53: the server returned no DNSSEC records"
            )),
            "{listed:?}"
        );
        assert_eq!(resolution.trust, Some(TrustOutcome::Bogus), "{listed:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_next_server_is_asked_after_one_that_stripped_its_answer() {
    let resolver = tree(KeyAlgorithm::P256);
    let servers = Servers::new(
        Speaks::Dot { close_after: None },
        serving(&resolver, &[("192.0.2.53", Serves::Stripped)]),
    )
    .plain_at("192.0.2.53")
    .plain_at("192.0.2.54");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let listed = [first(), DnsServer::plain("192.0.2.54".parse().unwrap())];
    let resolution = checked(&servers, &wires, &dnssec, &listed, "www.example.com").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    assert!(
        servers
            .dials()
            .iter()
            .any(|dial| dial.ip() == listed[1].address.ip())
    );
}

// The validator holds a name and type it proved bogus for a minute against
// the server that gave it. The next server is still asked, and its good
// answer resolves the name. Asked again inside the minute, the first server
// is not asked for that name and type: the held failure answers.
#[tokio::test(start_paused = true)]
async fn a_name_proved_bogus_is_held_only_against_the_server_that_gave_it() {
    let resolver = tree(KeyAlgorithm::P256);
    let asked: Arc<Mutex<Vec<(IpAddr, Query)>>> = Arc::default();
    let answer = {
        let resolver = resolver.clone();
        let asked = asked.clone();
        move |ip: IpAddr, _: u16, query: &[u8]| {
            asked.lock().unwrap().push((ip, question(query).0));
            let tampered = ip == "192.0.2.53".parse::<IpAddr>().unwrap();
            let how = if tampered {
                Serves::Tampered
            } else {
                Serves::Signed
            };
            reply(&resolver, how, query)
        }
    };
    let servers = Servers::new(Speaks::Dot { close_after: None }, answer)
        .plain_at("192.0.2.53")
        .plain_at("192.0.2.54");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, NOW);
    let listed = [first(), DnsServer::plain("192.0.2.54".parse().unwrap())];
    let address = name_query(name("www.example.com."), RecordType::A);
    let before = counted();
    let resolution = checked(&servers, &wires, &dnssec, &listed, "www.example.com").await;
    assert_eq!(resolution.addresses, seven());
    assert_eq!(resolution.trust, Some(TrustOutcome::Validated));
    assert_eq!(since(&before), [TrustOutcome::Validated]);
    let second = listed[1].address.ip();
    assert!(
        asked
            .lock()
            .unwrap()
            .iter()
            .any(|(ip, query)| *ip == second && *query == address)
    );
    // The first server again, inside the hold.
    let mark = asked.lock().unwrap().len();
    let resolution = checked(&servers, &wires, &dnssec, &[first()], "www.example.com").await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        bogus("192.0.2.53", "the signatures do not match the records")
    );
    assert_eq!(resolution.trust, Some(TrustOutcome::Bogus));
    assert!(
        !asked.lock().unwrap()[mark..]
            .iter()
            .any(|(_, query)| *query == address)
    );
}

#[tokio::test(start_paused = true)]
async fn an_expired_signature_says_the_clock_may_be_wrong() {
    let resolver = tree(KeyAlgorithm::P256);
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .plain_at("192.0.2.53");
    let wires = Authority::new().wires();
    let dnssec = trusting(&resolver, LATE);
    let resolution = checked(&servers, &wires, &dnssec, &[first()], "www.example.com").await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error,
        bogus(
            "192.0.2.53",
            "the signatures have expired; the host clock may be wrong"
        )
    );
}

#[tokio::test(start_paused = true)]
async fn a_plain_server_after_an_encrypted_one_is_never_asked_under_validate() {
    let resolver = tree(KeyAlgorithm::P256);
    let stranger = Authority::new();
    let trusted = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .at("192.0.2.53", stranger.server(&["192.0.2.53"], b"dot"))
        .plain_at("192.0.2.54");
    let wires = trusted.wires();
    let dnssec = trusting(&resolver, NOW);
    let listed = [
        dot("192.0.2.53", None),
        DnsServer::plain("192.0.2.54".parse().unwrap()),
    ];
    let resolution = checked(&servers, &wires, &dnssec, &listed, "www.example.com").await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(
        resolution.error.map(|error| error.failure),
        Some(LookupFailure::TlsFailed)
    );
    assert!(servers.dials().iter().all(|dial| dial.port() == 853));
    // No query left in the clear.
    assert!(
        servers
            .sent()
            .iter()
            .all(|sent| !sent.windows(7).any(|bytes| bytes == b"\x07example"))
    );
}

// Through a naming: the answer is held for the validator's TTL and found
// again as validated; a naming that takes answers as sent holds its own,
// which a validating naming never sees; nothing asks the host's resolver.
#[tokio::test(start_paused = true)]
async fn a_naming_holds_validated_answers_apart_and_never_asks_the_host() {
    let resolver = tree(KeyAlgorithm::P256);
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::Dot { close_after: None },
        serving(&resolver, &[("192.0.2.54", Serves::Tampered)]),
    )
    .at("192.0.2.53", authority.server(&["192.0.2.53"], b"dot"))
    .at("192.0.2.54", authority.server(&["192.0.2.54"], b"dot"));
    let wires = authority.wires();
    let host_asked = || {
        crate::metrics::snapshot()
            .resolutions
            .into_iter()
            .find(|(resolver, ..)| *resolver == crate::metrics::Resolver::Host)
            .map(|(_, ok, failed)| ok + failed)
    };
    let asked_before = host_asked();

    let validating = Naming::proxy_servers(vec![dot("192.0.2.53", None)], Trust::Validate);
    sessions(&validating).trust(&wires);
    sessions(&validating)
        .dnssec()
        .anchored(resolver.zones()[0].anchors().unwrap(), NOW);
    let first = through_naming(&validating, &servers, "www.example.com").await;
    assert_eq!(first.addresses, seven());
    assert_eq!(first.trust, Some(TrustOutcome::Validated));
    assert!(!first.cached);
    let dials = servers.dials().len();
    let again = through_naming(&validating, &servers, "www.example.com").await;
    assert_eq!(again.addresses, seven());
    assert_eq!(again.trust, Some(TrustOutcome::Validated));
    assert!(again.cached);
    assert_eq!(servers.dials().len(), dials);

    // A tampered server: taken as sent by one naming, refused by the other.
    let as_sent = Naming::proxy_servers(vec![dot("192.0.2.54", None)], Trust::AsSent);
    sessions(&as_sent).trust(&wires);
    let taken = through_naming(&as_sent, &servers, "www.example.com").await;
    assert_eq!(taken.addresses, ["192.0.2.66".parse::<IpAddr>().unwrap()]);
    let strict = Naming::proxy_servers(vec![dot("192.0.2.54", None)], Trust::Validate);
    sessions(&strict).trust(&wires);
    sessions(&strict)
        .dnssec()
        .anchored(resolver.zones()[0].anchors().unwrap(), NOW);
    let refused = through_naming(&strict, &servers, "www.example.com").await;
    assert!(refused.addresses.is_empty());
    assert!(!refused.cached);
    assert_eq!(refused.trust, Some(TrustOutcome::Bogus));
    // The naming's answers never hold a failure. The validator's own
    // minute-long hold of what it proved bogus answers the next lookup.
    let asked_again = through_naming(&strict, &servers, "www.example.com").await;
    assert!(!asked_again.cached);
    assert_eq!(asked_again.trust, Some(TrustOutcome::Bogus));

    assert_eq!(host_asked(), asked_before);
    for resolution in [first, again, taken, refused, asked_again] {
        assert_ne!(resolution.by, record::ResolvedBy::Host);
    }
}

// A cold check of a three-level chain through a route with a 300 ms round
// trip, on the paused clock: each dial costs one round trip, and each reply
// the client waits on one more. The address family is checked cold, then
// the other family with the keys it proved. The six queries of the cold
// check come in three batches. Plain dials each batch's streams together
// and DNS over TLS writes each batch at once, so each costs a batch's
// round trips, not a query's. HTTP/1.1 asks in turn.
#[tokio::test(start_paused = true)]
async fn a_cold_check_takes_these_round_trips_on_each_wire() {
    const RTT: std::time::Duration = std::time::Duration::from_millis(300);
    let resolver = tree(KeyAlgorithm::P256);
    // Per wire: for each family, the queries sent, the round trips waited
    // in all, and those one after another, which the time is.
    let expected = [
        ("plain", [(6, 12, 6), (1, 2, 2)]),
        ("tls", [(6, 5, 5), (1, 1, 1)]),
        ("https/1.1", [(6, 8, 8), (1, 1, 1)]),
        ("https/2", [(6, 6, 6), (1, 1, 1)]),
    ];
    for ((servers, wires, listed), (wire, families)) in every_wire(serving(&resolver, &[]))
        .into_iter()
        .zip(expected)
    {
        let servers = servers.delayed(RTT);
        let dnssec = trusting(&resolver, NOW);
        let mut session = Session::new(&servers, &wires, &listed);
        for (qtype, (queries, waits, serial)) in [1u16, 28].into_iter().zip(families) {
            let (waited, answered) = (servers.waits(), servers.answered().iter().sum::<usize>());
            let mut reached = false;
            let started = tokio::time::Instant::now();
            let found = query(
                &mut session,
                &dnssec,
                "www.example.com",
                qtype,
                Trust::Validate,
                &mut reached,
            )
            .await;
            let spent = started.elapsed();
            assert!(found.is_ok(), "{wire} {qtype}");
            assert_eq!(
                (
                    servers.answered().iter().sum::<usize>() - answered,
                    servers.waits() - waited,
                    spent,
                ),
                (queries, waits, RTT * serial),
                "{wire} {qtype}"
            );
        }
    }
}

// A cold check's CPU time per algorithm, from the root down to an address,
// with nothing held. Printed, not asserted.
#[test]
#[ignore = "prints timings only and signs slowly; run on request"]
fn a_cold_check_costs_this_much() {
    for algorithm in [
        KeyAlgorithm::P256,
        KeyAlgorithm::Ed25519,
        KeyAlgorithm::Rsa2048,
        KeyAlgorithm::MlDsa44,
    ] {
        let resolver = tree(algorithm);
        let anchors = resolver.zones()[0].anchors().unwrap();
        let question =
            weaver_dnssec::Question::new(name("www.example.com."), RecordType::A).unwrap();
        // The fixture signs as it answers, so the replies are made first.
        let validator = weaver_dnssec::Validator::new(anchors.clone(), Default::default()).unwrap();
        let (_, queries) =
            weaver_dnssec::testing::drive(&validator, &resolver, question.clone(), NOW);
        let replies: Vec<_> = queries
            .iter()
            .map(|query| (query.clone(), resolver.answer(query).unwrap()))
            .collect();
        let validator = weaver_dnssec::Validator::new(anchors, Default::default()).unwrap();
        let mut validation = validator.begin(question, NOW);
        let started = std::time::Instant::now();
        let verdict = loop {
            match validation.step() {
                weaver_dnssec::Step::Done(verdict) => break verdict,
                weaver_dnssec::Step::Need(query) => {
                    let (_, bytes) = replies.iter().find(|(asked, _)| *asked == query).unwrap();
                    validation.supply(&query, bytes).unwrap();
                }
            }
        };
        let spent = started.elapsed();
        assert!(
            matches!(verdict, weaver_dnssec::Verdict::Secure(_)),
            "{algorithm:?}"
        );
        println!(
            "{algorithm:?}: {} queries, {} signature checks, {spent:?}",
            queries.len(),
            weaver_dnssec::testing::signature_checks(&validation),
        );
    }
}

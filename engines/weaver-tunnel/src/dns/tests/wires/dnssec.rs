// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNSSEC checked here, on every wire, against zones signed in process.
use super::*;
use weaver_dnssec::testing::{FakeResolver, KeyAlgorithm, Zone as Signed};
use weaver_dnssec::{Name, Query, RecordType};

mod probe;

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
    // The connections each wire makes for the whole check: plain dials a
    // stream per query, DNS over TLS and HTTP/2 keep one, and HTTP/1.1 one
    // per family.
    let connections = [None, Some(1), Some(2), Some(1)];
    for ((servers, wires, listed), connections) in every_wire(serving(&resolver, &[]))
        .into_iter()
        .zip(connections)
    {
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
        assert_eq!(
            servers.dials().len(),
            connections.unwrap_or(answered),
            "{listed:?}"
        );
    }
}

// www.example.com's address, checked against a plain fixture server of its
// own. The check is driven without the time limit a server has, so a
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
    let found =
        crate::dns::dnssec::validated(&mut session, &dnssec, "www.example.com", 1, &mut reached)
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

// The root, com and example.com signed, with the addresses of
// www.example.com and api.example.com in both families.
fn dual() -> Arc<FakeResolver> {
    let root = Signed::new(Name::root(), KeyAlgorithm::P256).unwrap();
    let com = Signed::new(name("com."), KeyAlgorithm::P256).unwrap();
    let mut example = Signed::new(name("example.com."), KeyAlgorithm::P256).unwrap();
    for (host, last) in [("www.example.com.", 7u8), ("api.example.com.", 8)] {
        example.add_record(name(host), RecordType::A, vec![192, 0, 2, last]);
        let six = std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, last.into());
        example.add_record(name(host), RecordType::AAAA, six.octets().to_vec());
    }
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
    // A plain server that takes each stream and never says anything: every
    // query sent waits out the server's time limit, and no check gets past
    // its first step. Each plain stream carries one query, so every dial is
    // a query sent.
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

    // An HTTP/2 server that sends its first connection away at its first
    // stream.
    let authority = Authority::new();
    let servers = Servers::new(
        Speaks::H2 {
            respond: Arc::new(h2_message),
            goaway_after: Some(1),
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
// reply has the rest of the batch sent again on new connections, one query
// out at a time, and the check still validates.
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
    let sent = crate::metrics::dnssec_queries();
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
    // The server read each query it answered once, on a connection of its
    // own. After the first batch the talk is wary and writes one query at a
    // time, so each query goes out at most three times: in the batch, on a
    // connection that closed after its last reply, and answered on the next.
    // Seven queries on seven connections, seventeen writes in all.
    assert_eq!(servers.answered(), [1; 7]);
    assert_eq!(crate::metrics::dnssec_queries() - sent, 17);
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
        // DNS over TLS has every query out on the one connection by id, so
        // a reply under an id it did not send is no reply to any of them:
        // it is passed over, and the queries run out the server's time.
        // Elsewhere each reply has its own stream, and one under the wrong
        // id fails its query.
        let failure = match listed.wire {
            Wire::Tls { .. } => LookupFailure::TimedOut,
            _ => LookupFailure::Other,
        };
        assert_eq!(
            resolution.error.map(|error| error.failure),
            Some(failure),
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

// A validated lookup of both families of a name three labels down, through
// a route with a 300 ms round trip, on the paused clock: each dial costs
// one round trip, and each reply the client waits on one more. Cold, then a
// second name in the same zone, with the keys and delegations the first
// proved. Both families are checked side by side, each stepping as its own
// replies come, and a query both families need goes once: seven queries
// cold, two for the second name. Plain dials the first stream alone, then
// the rest as they are asked; DNS over TLS writes what is asked at once and
// HTTP/2 sends it on streams of the one connection, so queries asked
// together cost one round trip, not one each. Waits that overlap are each
// counted, so the round trips in all may pass those one after another.
// HTTP/1.1 asks each family's chain in turn on a connection of the family's
// own: the first connection's handshake says the server speaks HTTP/1.1,
// and the other family's connection opens once the first is busy, so it
// costs a dial and a handshake of its own, two connections per lookup.
// DNS over TLS and HTTP/2 make one connection, so one TLS handshake, per
// lookup.
#[tokio::test(start_paused = true)]
async fn a_validated_lookup_takes_these_round_trips_on_each_wire() {
    const RTT: std::time::Duration = std::time::Duration::from_millis(300);
    let resolver = dual();
    // Per wire: for the cold lookup, then the second, the queries sent, the
    // dials made, the round trips waited in all, and those one after
    // another, which the time is.
    let expected = [
        ("plain", [(7, 7, 14, 6), (2, 2, 4, 3)]),
        ("tls", [(7, 1, 5, 5), (2, 1, 3, 3)]),
        ("https/1.1", [(7, 2, 11, 7), (2, 2, 6, 5)]),
        ("https/2", [(7, 1, 5, 5), (2, 1, 4, 3)]),
    ];
    for ((servers, wires, listed), (wire, lookups)) in every_wire(serving(&resolver, &[]))
        .into_iter()
        .zip(expected)
    {
        let servers = servers.delayed(RTT);
        let dnssec = trusting(&resolver, NOW);
        for (host, (queries, dials, waits, serial)) in ["www.example.com", "api.example.com"]
            .into_iter()
            .zip(lookups)
        {
            let answered = servers.answered().iter().sum::<usize>();
            let (dialled, waited) = (servers.dials().len(), servers.waits());
            let started = tokio::time::Instant::now();
            let (resolved, _) = resolve_judged(
                &servers,
                &wires,
                &dnssec,
                std::slice::from_ref(&listed),
                Trust::Validate,
                host,
            )
            .await;
            let spent = started.elapsed();
            assert_eq!(resolved.unwrap().addresses.len(), 2, "{wire} {host}");
            assert_eq!(
                (
                    servers.answered().iter().sum::<usize>() - answered,
                    servers.dials().len() - dialled,
                    servers.waits() - waited,
                    spent,
                ),
                (queries, dials, waits, RTT * serial),
                "{wire} {host}"
            );
        }
    }
}

// The servers asked in the tests below, over TLS: 192.0.2.53 and .54
// trusted, .55 with a certificate the client does not trust.
fn three_over_tls(
    answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static,
) -> (Servers, Wires) {
    let trusted = Authority::new();
    let stranger = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, answer)
        .at("192.0.2.53", trusted.server(&["192.0.2.53"], b"dot"))
        .at("192.0.2.54", trusted.server(&["192.0.2.54"], b"dot"))
        .at("192.0.2.55", stranger.server(&["192.0.2.55"], b"dot"));
    (servers, trusted.wires())
}

// Under `Validate`, a first server that takes the dial and never answers
// spends its own time limit only. The next server's cold check, five round
// trips of 900 ms, runs on a whole limit of its own.
#[tokio::test(start_paused = true)]
async fn a_cold_check_at_the_next_server_has_a_whole_time_limit_of_its_own() {
    const RTT: std::time::Duration = std::time::Duration::from_millis(900);
    let resolver = dual();
    let authority = Authority::new();
    let servers = Servers::new(Speaks::Dot { close_after: None }, serving(&resolver, &[]))
        .silent_at("192.0.2.53")
        .at("192.0.2.54", authority.server(&["192.0.2.54"], b"dot"))
        .delayed(RTT);
    let dnssec = trusting(&resolver, NOW);
    let started = tokio::time::Instant::now();
    let (resolved, account) = resolve_judged(
        &servers,
        &authority.wires(),
        &dnssec,
        &[dot("192.0.2.53", None), dot("192.0.2.54", None)],
        Trust::Validate,
        "www.example.com",
    )
    .await;
    assert_eq!(resolved.unwrap().addresses.len(), 2);
    assert_eq!(account.judged, Some(TrustOutcome::Validated));
    assert_eq!(
        account.from.map(|from| from.address),
        Some("192.0.2.54".parse::<IpAddr>().unwrap())
    );
    // One connection to each: the first took nothing it answered, the
    // second the whole cold check.
    let dials: Vec<String> = servers
        .dials()
        .iter()
        .map(|dial| dial.ip().to_string())
        .collect();
    assert_eq!(dials, ["192.0.2.53", "192.0.2.54"]);
    assert_eq!(servers.answered(), [0, 7]);
    assert_eq!(started.elapsed(), SERVER_TIMEOUT + RTT * 5);
}

// A server that answers every query of www.example.com's chain but its
// query for the `silent` family: an empty reply is no reply.
fn silent_for(
    resolver: &Arc<FakeResolver>,
    silent: RecordType,
) -> impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + Clone + 'static {
    let resolver = resolver.clone();
    move |_, _, query| {
        if question(query).0 == name_query(name("www.example.com."), silent) {
            return Vec::new();
        }
        reply(&resolver, Serves::Signed, query)
    }
}

// The address of the family that is not `silent`.
fn the_other(silent: RecordType) -> Vec<IpAddr> {
    let [seven, six] = <[IpAddr; 2]>::try_from(seven_and_six()).unwrap();
    vec![if silent == RecordType::A { six } else { seven }]
}

// A resolver that drops one family's query and answers the rest, under
// either trust: the other family's answer, or under `Validate` its check,
// comes while the silent query is still out, and stands when the server's
// time limit ends the silent one. Each family silent in turn, on every wire.
#[tokio::test(start_paused = true)]
async fn a_silent_family_leaves_the_others_answer_on_every_wire_under_either_trust() {
    let resolver = dual();
    for trust in [Trust::AsSent, Trust::Validate] {
        for silent in [RecordType::A, RecordType::AAAA] {
            for (servers, wires, listed) in every_wire(silent_for(&resolver, silent)) {
                let dnssec = trusting(&resolver, NOW);
                let started = tokio::time::Instant::now();
                let (resolved, account) = resolve_judged(
                    &servers,
                    &wires,
                    &dnssec,
                    std::slice::from_ref(&listed),
                    trust,
                    "www.example.com",
                )
                .await;
                let case = format!("{listed} {trust:?} {silent:?} silent");
                let lookup = resolved.expect(&case);
                assert_eq!(lookup.addresses, the_other(silent), "{case}");
                let judged = match trust {
                    Trust::AsSent => TrustOutcome::AsSent,
                    Trust::Validate => TrustOutcome::Validated,
                };
                assert_eq!(account.judged, Some(judged), "{case}");
                // The silent query ran out the server's limit, and no more.
                assert_eq!(started.elapsed(), SERVER_TIMEOUT, "{case}");
            }
        }
    }
}

// Under `AsSent`, one family's query names an alias still to ask for while
// the other family's query stays silent: the alias is followed at once and
// its address kept when the time limit ends the silent query. Each family
// silent in turn, on every wire.
#[tokio::test(start_paused = true)]
async fn an_alias_is_followed_while_the_other_family_stays_silent_on_every_wire() {
    for silent in [1, 28] {
        let answer = move |_: IpAddr, qtype: u16, query: &[u8]| {
            let target = query.windows(6).any(|name| name == b"target");
            match (qtype == silent, target, qtype) {
                (true, _, _) => Vec::new(),
                (false, false, _) => {
                    respond(query, 0, &[rr(&ASKED, 5, 60, &wire("target.test"))], &[])
                }
                (false, true, 1) => v4(query, 60),
                (false, true, _) => respond(query, 0, &[rr(&ASKED, 28, 60, &SIX)], &[]),
            }
        };
        let kept = the_other(if silent == 1 {
            RecordType::A
        } else {
            RecordType::AAAA
        });
        for (servers, wires, listed) in every_wire(answer) {
            let started = tokio::time::Instant::now();
            let lookup = resolve(
                &servers,
                &wires,
                std::slice::from_ref(&listed),
                Trust::AsSent,
                "alias.test",
            )
            .await
            .unwrap();
            assert_eq!(lookup.addresses, kept, "{listed} {silent}");
            // The silent query was read too.
            assert_eq!(
                servers.answered().iter().sum::<usize>(),
                3,
                "{listed} {silent}"
            );
            assert_eq!(started.elapsed(), SERVER_TIMEOUT, "{listed} {silent}");
        }
    }
}

// SERVFAIL, with the question alone.
fn servfail(query: &[u8]) -> Vec<u8> {
    let (_, end) = question(query);
    let mut failed = query[..end].to_vec();
    failed[2..4].copy_from_slice(&[0x81, 0x82]);
    failed[6..12].fill(0);
    failed
}

// Both address queries failed with SERVFAIL and every DNSKEY query silent:
// both checks have their verdicts from the first replies, and the lookup
// ends then, with nothing waited on the silent queries. Over HTTP/1.1,
// where the queries the checks also asked for wait for their lane, none of
// them goes out after the verdicts.
#[tokio::test(start_paused = true)]
async fn a_lookup_ends_once_both_checks_have_their_verdicts_on_every_wire() {
    let resolver = dual();
    let signed = resolver.clone();
    let answer = move |_: IpAddr, _: u16, query: &[u8]| {
        let (asked, _) = question(query);
        if asked.name == name("www.example.com.")
            && matches!(asked.rtype, RecordType::A | RecordType::AAAA)
        {
            return servfail(query);
        }
        if asked.rtype == RecordType::DNSKEY {
            return Vec::new();
        }
        reply(&signed, Serves::Signed, query)
    };
    // The wires in turn: plain, DNS over TLS, HTTP/1.1, HTTP/2.
    for (wire, (servers, wires, listed)) in every_wire(answer).into_iter().enumerate() {
        let dnssec = trusting(&resolver, NOW);
        let started = tokio::time::Instant::now();
        let resolution = checked(
            &servers,
            &wires,
            &dnssec,
            std::slice::from_ref(&listed),
            "www.example.com",
        )
        .await;
        assert!(resolution.addresses.is_empty(), "{listed:?}");
        assert_eq!(started.elapsed(), std::time::Duration::ZERO, "{listed:?}");
        // Over HTTP/1.1 each family's connection carried its address query
        // alone.
        if wire == 2 {
            assert_eq!(servers.answered(), [1, 1], "{listed:?}");
        }
    }
}

// Over HTTP/1.1, the IPv4 check ends at a refused DS query while the IPv6
// check still waits on its silent address query: the queries only the IPv4
// check asked ahead, still waiting for their turn, never go out.
#[tokio::test(start_paused = true)]
async fn a_finished_check_leaves_its_unsent_queries_unsent_while_the_other_runs() {
    let resolver = dual();
    let signed = resolver.clone();
    let answer = move |_: IpAddr, _: u16, query: &[u8]| {
        let (asked, _) = question(query);
        if asked.rtype == RecordType::AAAA {
            return Vec::new();
        }
        if asked.rtype == RecordType::DS {
            // REFUSED, with the question alone.
            let mut refused = servfail(query);
            refused[3] = 0x85;
            return refused;
        }
        reply(&signed, Serves::Signed, query)
    };
    // The wires in turn: plain, DNS over TLS, HTTP/1.1, HTTP/2.
    let (servers, wires, listed) = every_wire(answer).swap_remove(2);
    let dnssec = trusting(&resolver, NOW);
    let started = tokio::time::Instant::now();
    let resolution = checked(
        &servers,
        &wires,
        &dnssec,
        std::slice::from_ref(&listed),
        "www.example.com",
    )
    .await;
    assert!(resolution.addresses.is_empty());
    assert_eq!(started.elapsed(), SERVER_TIMEOUT);
    let asked: Vec<RecordType> = servers
        .log
        .lock()
        .unwrap()
        .requests
        .iter()
        .map(|seen| question(&seen.body).0.rtype)
        .collect();
    assert_eq!(
        asked,
        [
            RecordType::A,
            RecordType::AAAA,
            RecordType::DNSKEY,
            RecordType::DS
        ]
    );
}

// A check whose chain fails part way at the first server, or whose TLS
// check fails there, is made again at the next server, which proves it.
#[tokio::test(start_paused = true)]
async fn a_check_that_fails_part_way_or_at_tls_moves_to_the_next_server() {
    let resolver = dual();
    let signed = resolver.clone();
    // 192.0.2.53 answers the addresses but fails every DNSKEY query.
    let (servers, wires) = three_over_tls(move |asked, _, query| {
        if asked.to_string() == "192.0.2.53" && question(query).0.rtype == RecordType::DNSKEY {
            servfail(query)
        } else {
            reply(&signed, Serves::Signed, query)
        }
    });
    for first in ["192.0.2.53", "192.0.2.55"] {
        let dnssec = trusting(&resolver, NOW);
        let dialled = servers.dials().len();
        let (resolved, account) = resolve_judged(
            &servers,
            &wires,
            &dnssec,
            &[dot(first, None), dot("192.0.2.54", None)],
            Trust::Validate,
            "www.example.com",
        )
        .await;
        assert_eq!(resolved.unwrap().addresses.len(), 2, "{first}");
        assert_eq!(account.judged, Some(TrustOutcome::Validated), "{first}");
        assert_eq!(
            account.from.map(|from| from.address),
            Some("192.0.2.54".parse::<IpAddr>().unwrap()),
            "{first}"
        );
        let dials: Vec<String> = servers.dials()[dialled..]
            .iter()
            .map(|dial| dial.ip().to_string())
            .collect();
        assert_eq!(dials, [first, "192.0.2.54"], "{first}");
    }
}

// Keys and delegations proven from one server's good answer are held, and
// the next lookup at another server sends only its two address queries.
// The held chain proves nothing about an answer: the A record that server
// tampered with still fails the check, and only the AAAA answer it left
// alone stands. Asked again, the A record is refused by the validator's
// hold on what that server was seen to fail, without being asked for.
#[tokio::test(start_paused = true)]
async fn a_held_chain_never_makes_a_tampered_answer_pass() {
    let resolver = dual();
    let (servers, wires) = three_over_tls(serving(&resolver, &[("192.0.2.54", Serves::Tampered)]));
    let dnssec = trusting(&resolver, NOW);
    let (carrier, wires, dnssec) = (&servers, &wires, &dnssec);
    let lookup = |server: &'static str| async move {
        let listed = [dot(server, None)];
        resolve_judged(
            carrier,
            wires,
            dnssec,
            &listed,
            Trust::Validate,
            "www.example.com",
        )
        .await
    };
    let (good, _) = lookup("192.0.2.53").await;
    assert_eq!(good.unwrap().addresses.len(), 2);
    for sent in [2, 1] {
        let answered = servers.answered().iter().sum::<usize>();
        let before = counted();
        let (tampered, account) = lookup("192.0.2.54").await;
        assert_eq!(servers.answered().iter().sum::<usize>() - answered, sent);
        let six: IpAddr = "2001:db8::7".parse().unwrap();
        assert_eq!(tampered.unwrap().addresses, [six]);
        assert_eq!(account.judged, Some(TrustOutcome::Validated));
        assert_eq!(
            since(&before),
            [TrustOutcome::Bogus, TrustOutcome::Validated]
        );
    }
}

// A change of the route's DNS setting makes its names anew, and the chain
// the old setting proved goes with them: the same zone is checked cold
// again. Under one setting, a second lookup in the zone uses what the first
// proved.
#[tokio::test(start_paused = true)]
async fn a_dns_setting_change_empties_the_held_chain() {
    let resolver = dual();
    let (servers, wires) = three_over_tls(serving(&resolver, &[]));
    let tunnel = [dot("192.0.2.53", None)];
    let choice = crate::pipe::DnsChoice::Proxy {
        trust: Trust::Validate,
        system_fallback: false,
    };
    let fresh = |names: &crate::pipe::Names| {
        let naming = names.at_hop("hop", Some(&tunnel));
        sessions(&naming).trust(&wires);
        sessions(&naming)
            .dnssec()
            .anchored(resolver.zones()[0].anchors().unwrap(), NOW);
        naming
    };
    let asked = |servers: &Servers| servers.answered().iter().sum::<usize>();
    let names = crate::pipe::Names::new(choice.clone(), None);
    let naming = fresh(&names);
    let before = asked(&servers);
    let first = through_naming(&naming, &servers, "www.example.com").await;
    assert_eq!(first.trust, Some(TrustOutcome::Validated));
    assert_eq!(asked(&servers) - before, 7);
    // The same setting: the zone's chain is held.
    let again = names.at_hop("hop", Some(&tunnel));
    let before = asked(&servers);
    let second = through_naming(&again, &servers, "api.example.com").await;
    assert_eq!(second.trust, Some(TrustOutcome::Validated));
    assert_eq!(asked(&servers) - before, 2);
    // The setting changes.
    let changed = crate::pipe::Names::new(choice, None);
    let naming = fresh(&changed);
    let before = asked(&servers);
    let cold = through_naming(&naming, &servers, "api.example.com").await;
    assert_eq!(cold.trust, Some(TrustOutcome::Validated));
    assert_eq!(asked(&servers) - before, 7);
}

// What a negative reply's authority section keeps of its SOA.
#[derive(Clone, Copy, Debug)]
enum Soa {
    Kept,
    // The SOA and its signatures left out.
    Dropped,
    // The SOA said to be another zone's.
    Elsewhere,
}

// Under `Validate`, a negative answer holds as dnsmasq holds one: by its
// SOA, signed or not, and for no time when the reply has no SOA owned by
// the name or a zone above it. It is still the answer.
#[tokio::test(start_paused = true)]
async fn a_validated_negative_answer_holds_by_its_soa_and_for_no_time_without_one() {
    let resolver = tree(KeyAlgorithm::P256);
    for (host, soa, held, judged) in [
        (
            "missing.example.com",
            Soa::Kept,
            300,
            TrustOutcome::Validated,
        ),
        (
            "missing.example.com",
            Soa::Dropped,
            0,
            TrustOutcome::Validated,
        ),
        ("missing.plain.com", Soa::Kept, 300, TrustOutcome::Unsigned),
        (
            "missing.plain.com",
            Soa::Elsewhere,
            0,
            TrustOutcome::Unsigned,
        ),
        ("missing.plain.com", Soa::Dropped, 0, TrustOutcome::Unsigned),
    ] {
        let signed = resolver.clone();
        let servers = Servers::new(Speaks::Dot { close_after: None }, move |_, _, query| {
            let (asked, _) = question(query);
            let Ok(mut reply) = signed.reply(&asked) else {
                return servfail(query);
            };
            if asked.name == name(&format!("{host}.")) {
                let is_soa = |record: &weaver_dnssec::testing::Record| {
                    record.rtype == RecordType::SOA
                        || (record.rtype == RecordType::RRSIG
                            && record.rdata.get(..2) == Some(&RecordType::SOA.0.to_be_bytes()[..]))
                };
                match soa {
                    Soa::Kept => {}
                    Soa::Dropped => reply.authority.retain(|record| !is_soa(record)),
                    Soa::Elsewhere => {
                        for record in reply.authority.iter_mut().filter(|record| is_soa(record)) {
                            record.owner = name("elsewhere.");
                        }
                    }
                }
            }
            let mut bytes = reply.encode(&asked).unwrap();
            bytes[..2].copy_from_slice(&query[..2]);
            bytes
        })
        .plain_at("192.0.2.53");
        let dnssec = trusting(&resolver, NOW);
        let (resolved, account) = resolve_judged(
            &servers,
            &Authority::new().wires(),
            &dnssec,
            &[first()],
            Trust::Validate,
            host,
        )
        .await;
        let lookup = resolved.unwrap();
        assert!(lookup.addresses.is_empty(), "{host} {soa:?}");
        assert!(lookup.nonexistent, "{host} {soa:?}");
        assert_eq!(lookup.ttl, Ttl::Seconds(held), "{host} {soa:?}");
        assert_eq!(account.judged, Some(judged), "{host} {soa:?}");
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

use super::*;
use crate::metrics::Resolver;
use crate::resolution::{Lookup, ResolutionCache, Ttl};

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

#[test]
fn each_resolver_says_why_in_its_own_words() {
    for (said, failure) in [
        (
            "failed to lookup address information: nodename nor servname provided, or not known",
            LookupFailure::NoSuchName,
        ),
        (
            "failed to lookup address information: Name or service not known",
            LookupFailure::NoSuchName,
        ),
        (
            "No such host is known. (os error 11001)",
            LookupFailure::NoSuchName,
        ),
        (
            "the tunnel's DNS server does not know this name",
            LookupFailure::NoSuchName,
        ),
        (
            "failed to lookup address information: No address associated with hostname",
            LookupFailure::NoAddress,
        ),
        (
            "the tunnel's DNS server returned no address for this name",
            LookupFailure::NoAddress,
        ),
        (crate::pipe::NO_ADDRESS_RESOLVED, LookupFailure::NoAddress),
        (
            "failed to lookup address information: Temporary failure in name resolution",
            LookupFailure::ServerFailure,
        ),
        (
            "the tunnel's DNS server did not answer in time",
            LookupFailure::TimedOut,
        ),
        (crate::pipe::RESOLVE_TIMED_OUT, LookupFailure::TimedOut),
        (crate::dns::INVALID, LookupFailure::Other),
    ] {
        assert_eq!(classify(said), failure, "{said}");
    }
}

#[tokio::test]
async fn nothing_is_noted_outside_a_recording() {
    asking("outside.test", Asker::host);
    found("outside.test", &[ip("192.0.2.1")]);
    let recording = Arc::new(Recording::default());
    assert_eq!(recording.finish("outside.test", 80, None), None);
}

#[tokio::test]
async fn the_first_note_of_each_kind_stands() {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            asking("Name.Test.", || Asker::servers(&[ip("192.0.2.53")]));
            asking("name.test", Asker::host);
            cached("name.test", false);
            cached("name.test", true);
            found("name.test", &[ip("192.0.2.1"), ip("192.0.2.2")]);
            failed("name.test", LookupFailure::Other, || "later".into());
            connected("name.test", ip("192.0.2.2"));
        })
        .await;
    assert_eq!(
        recording.finish("name.test", 80, None),
        Some(HostResolution {
            host: "name.test".into(),
            by: ResolvedBy::Servers,
            proxy: None,
            servers: vec![ip("192.0.2.53")],
            addresses: vec![ip("192.0.2.1"), ip("192.0.2.2")],
            cached: false,
            connected: Some(ip("192.0.2.2")),
            error: None,
        })
    );
}

#[tokio::test]
async fn a_recording_is_carried_and_adopts_notes_without_overriding_its_own() {
    let recording = Arc::new(Recording::default());
    assert!(Recording::current().is_none());
    let carried = recording.scope(async { Recording::current() }).await;
    assert!(carried.is_some_and(|carried| Arc::ptr_eq(&carried, &recording)));
    recording.scope(async { cached("name.test", true) }).await;
    let member = Arc::new(Recording::default());
    member
        .scope(async {
            asking("name.test", || Asker::proxy(7));
            cached("name.test", false);
            connected("name.test", ip("192.0.2.7"));
            bound(SocketAddr::new(ip("192.0.2.8"), 51820), false);
        })
        .await;
    recording.adopt(&member);
    assert_eq!(
        recording.finish("name.test", 443, None),
        Some(HostResolution {
            host: "name.test".into(),
            by: ResolvedBy::Proxy,
            proxy: Some(7),
            servers: Vec::new(),
            addresses: Vec::new(),
            cached: true,
            connected: Some(ip("192.0.2.7")),
            error: None,
        })
    );
    assert_eq!(
        recording
            .finish("192.0.2.8", 51820, None)
            .unwrap()
            .connected,
        Some(ip("192.0.2.8"))
    );
}

#[tokio::test]
async fn a_lookup_still_out_when_the_dial_ended_ran_out_of_time() {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async { asking("slow.test", || Asker::proxy_servers(4, &[ip("192.0.2.53")])) })
        .await;
    let resolution = recording.finish("slow.test", 80, None).unwrap();
    assert_eq!(resolution.by, ResolvedBy::ProxyServers);
    assert_eq!(resolution.proxy, Some(4));
    assert_eq!(
        resolution.error.map(|error| error.failure),
        Some(LookupFailure::TimedOut)
    );
}

#[tokio::test]
async fn a_name_handed_to_a_proxy_has_no_addresses() {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async { asking("far.test", || Asker::proxy(3)) })
        .await;
    let resolution = recording.finish("far.test", 443, None).unwrap();
    assert_eq!(resolution.by, ResolvedBy::Proxy);
    assert_eq!(resolution.proxy, Some(3));
    assert!(resolution.addresses.is_empty());
    assert_eq!(resolution.error, None);
}

#[tokio::test]
async fn an_address_is_its_own_answer() {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async { bound(SocketAddr::new(ip("192.0.2.9"), 51820), true) })
        .await;
    let resolution = recording.finish("192.0.2.9", 51820, None).unwrap();
    assert_eq!(resolution.by, ResolvedBy::Literal);
    assert_eq!(resolution.addresses, [ip("192.0.2.9")]);
    assert_eq!(resolution.connected, Some(ip("192.0.2.9")));
}

#[tokio::test]
async fn a_tunnel_engine_that_resolves_its_own_endpoint_is_read_from_its_socket_or_its_error() {
    let recording = Arc::new(Recording::default());
    recording
        .scope(async {
            bound(SocketAddr::new(ip("192.0.2.40"), 51820), true);
            bound(SocketAddr::new(ip("192.0.2.41"), 51820), false);
        })
        .await;
    let resolution = recording.finish("vpn.test", 51820, None).unwrap();
    assert_eq!(resolution.by, ResolvedBy::Host);
    assert_eq!(resolution.addresses, [ip("192.0.2.41")]);
    assert_eq!(resolution.connected, Some(ip("192.0.2.41")));

    let recording = Arc::new(Recording::default());
    let said = "proxy 2: could not establish a WireGuard tunnel to vpn.test:51820: the endpoint \
                name could not be resolved: failed to lookup address information: nodename nor \
                servname provided, or not known";
    let resolution = recording.finish("vpn.test", 51820, Some(said)).unwrap();
    assert_eq!(resolution.by, ResolvedBy::Host);
    let error = resolution.error.unwrap();
    assert_eq!(error.failure, LookupFailure::NoSuchName);
    assert!(
        error
            .detail
            .starts_with("failed to lookup address information")
    );
    // Any other failure says nothing of a lookup.
    assert_eq!(
        recording.finish("vpn.test", 51820, Some("connection refused")),
        None
    );
}

#[tokio::test]
async fn a_held_answer_is_noted_as_held_and_a_held_negative_says_what_kind() {
    let cache = ResolutionCache::<std::io::Error>::default();
    let negative = || async {
        Ok(Lookup {
            addresses: Vec::new(),
            ttl: Ttl::Seconds(30),
            nonexistent: true,
        })
    };
    let first = Arc::new(Recording::default());
    first
        .scope(cache.resolve(Resolver::Routed, "gone.test", negative))
        .await
        .unwrap();
    let second = Arc::new(Recording::default());
    second
        .scope(async {
            asking("gone.test", || Asker::servers(&[ip("192.0.2.53")]));
            cache
                .resolve(Resolver::Routed, "gone.test", negative)
                .await
                .unwrap()
        })
        .await;
    let resolution = second.finish("gone.test", 80, None).unwrap();
    assert!(resolution.cached);
    assert_eq!(
        resolution.error,
        Some(LookupError {
            failure: LookupFailure::NoSuchName,
            detail: "NXDOMAIN".into(),
        })
    );
}

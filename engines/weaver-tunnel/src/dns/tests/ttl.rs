use super::*;
use std::sync::Arc;

type Answer = dyn Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync;

// Servers that answer each query as the test says.
struct Zone(Arc<Answer>);

impl Zone {
    fn new(answer: impl Fn(IpAddr, u16, &[u8]) -> Vec<u8> + Send + Sync + 'static) -> Self {
        Self(Arc::new(answer))
    }
}

#[async_trait::async_trait]
impl Carrier for Zone {
    async fn open(&self, server: IpAddr) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        let (client, mut stream) = tokio::io::duplex(4096);
        let answer = self.0.clone();
        tokio::spawn(async move {
            let size = stream.read_u16().await.unwrap() as usize;
            let mut query = vec![0; size];
            stream.read_exact(&mut query).await.unwrap();
            let reply = answer(server, u16_at(&query, size - 4).unwrap(), &query);
            stream.write_u16(reply.len() as u16).await.unwrap();
            stream.write_all(&reply).await.unwrap();
        });
        Ok(Box::new(client))
    }
}

// The name asked, by pointer to the question.
const ASKED: [u8; 2] = [0xc0, 0x0c];

fn wire(name: &str) -> Vec<u8> {
    let mut wire = Vec::new();
    for label in name.split('.') {
        wire.push(label.len() as u8);
        wire.extend_from_slice(label.as_bytes());
    }
    wire.push(0);
    wire
}

fn rr(owner: &[u8], kind: u16, ttl: u32, data: &[u8]) -> Vec<u8> {
    let mut record = owner.to_vec();
    record.extend_from_slice(&kind.to_be_bytes());
    record.extend_from_slice(&[0, 1]);
    record.extend_from_slice(&ttl.to_be_bytes());
    record.extend_from_slice(&(data.len() as u16).to_be_bytes());
    record.extend_from_slice(data);
    record
}

fn soa(ttl: u32, minimum: u32) -> Vec<u8> {
    let mut data = wire("ns.test");
    data.extend(wire("hostmaster.test"));
    for field in [1u32, 7200, 900, 1_209_600, minimum] {
        data.extend_from_slice(&field.to_be_bytes());
    }
    rr(&wire("test"), 6, ttl, &data)
}

fn respond(query: &[u8], rcode: u8, answers: &[Vec<u8>], authority: &[Vec<u8>]) -> Vec<u8> {
    let mut reply = query.to_vec();
    reply[2] = 0x81;
    reply[3] = 0x80 | rcode;
    reply[6..8].copy_from_slice(&(answers.len() as u16).to_be_bytes());
    reply[8..10].copy_from_slice(&(authority.len() as u16).to_be_bytes());
    for record in answers.iter().chain(authority) {
        reply.extend_from_slice(record);
    }
    reply
}

fn v4(query: &[u8], ttl: u32) -> Vec<u8> {
    respond(query, 0, &[rr(&ASKED, 1, ttl, &[192, 0, 2, 7])], &[])
}

fn server() -> IpAddr {
    "192.0.2.53".parse().unwrap()
}

#[tokio::test]
async fn an_answer_holds_for_the_shortest_ttl_either_family_gave() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => respond(query, 0, &[rr(&ASKED, 28, 60, &[0x20; 16])], &[]),
    });
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses.len(), 2);
    assert_eq!(lookup.ttl, Ttl::Seconds(60));
}

#[tokio::test]
async fn an_alias_holds_no_longer_than_its_shortest_link() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => respond(
            query,
            0,
            &[
                rr(&ASKED, 5, 30, &wire("target.test")),
                rr(&wire("target.test"), 1, 300, &[192, 0, 2, 7]),
            ],
            &[],
        ),
        // No address of this family, and nothing said about how long.
        _ => respond(query, 0, &[], &[]),
    });
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
    assert_eq!(lookup.ttl, Ttl::Seconds(30));
}

#[tokio::test]
async fn a_family_with_no_address_holds_the_answer_no_longer_than_its_negative_ttl() {
    let zone = Zone::new(|_, qtype, query| match qtype {
        1 => v4(query, 300),
        _ => respond(query, 0, &[], &[soa(600, 45)]),
    });
    let lookup = resolve_name(&zone, &[server()], "fixture.test")
        .await
        .unwrap();
    assert_eq!(lookup.addresses.len(), 1);
    assert_eq!(lookup.ttl, Ttl::Seconds(45));
}

#[tokio::test]
async fn a_name_that_does_not_exist_is_a_negative_answer_for_the_soa_bound() {
    for (soa_ttl, minimum, held) in [(900, 120, 120), (20, 120, 20)] {
        let zone = Zone::new(move |_, _, query| respond(query, 3, &[], &[soa(soa_ttl, minimum)]));
        let lookup = resolve_name(&zone, &[server()], "missing.test")
            .await
            .unwrap();
        assert!(lookup.addresses.is_empty());
        assert_eq!(lookup.ttl, Ttl::Seconds(held));
    }
}

#[tokio::test]
async fn an_alias_to_a_name_that_does_not_exist_is_a_negative_answer() {
    let zone = Zone::new(|_, _, query| {
        respond(
            query,
            3,
            &[rr(&ASKED, 5, 30, &wire("gone.test"))],
            &[soa(900, 120)],
        )
    });
    let lookup = resolve_name(&zone, &[server()], "alias.test")
        .await
        .unwrap();
    assert!(lookup.addresses.is_empty());
    assert_eq!(lookup.ttl, Ttl::Seconds(30));
}

#[tokio::test]
async fn no_address_without_an_soa_stays_unanswered() {
    let zone = Zone::new(|_, _, query| respond(query, 3, &[], &[]));
    let unanswered = resolve_name(&zone, &[server()], "missing.test")
        .await
        .unwrap_err();
    assert!(unanswered.reached);
}

#[tokio::test]
async fn a_negative_answer_needs_every_server_asked_to_give_one() {
    let other: IpAddr = "192.0.2.54".parse().unwrap();
    let zone = Zone::new(move |asked, _, query| {
        if asked == other {
            respond(query, 3, &[], &[])
        } else {
            respond(query, 3, &[], &[soa(900, 120)])
        }
    });
    let unanswered = resolve_name(&zone, &[server(), other], "missing.test")
        .await
        .unwrap_err();
    assert!(unanswered.reached);
    let zone = Zone::new(|_, _, query| respond(query, 3, &[], &[soa(900, 120)]));
    let both = [server(), other];
    let lookup = resolve_name(&zone, &both, "missing.test").await.unwrap();
    assert_eq!(lookup.ttl, Ttl::Seconds(120));
}

#[test]
fn a_ttl_with_the_top_bit_set_is_read_as_zero() {
    let mut query = vec![0, 9, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    query.extend(wire("fixture.test"));
    query.extend_from_slice(&[0, 1, 0, 1]);
    let reply = respond(
        &query,
        0,
        &[rr(&ASKED, 1, 0x8000_0001, &[192, 0, 2, 7])],
        &[],
    );
    let parsed = parse_response(&reply, 9, "fixture.test", 1).unwrap();
    assert_eq!(parsed.ttl, 0);
}

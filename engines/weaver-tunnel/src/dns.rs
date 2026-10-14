// Bounded DNS-over-TCP to listed servers, asked through a pipe stage: the
// leg's egress or a proxy hop.
use crate::{
    TunnelError, TunnelStream,
    pipe::{DialError, Dialer, Purpose, Target},
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn invalid() -> TunnelError {
    TunnelError::Engine("routed DNS returned an invalid response".into())
}
static NEXT_ID: AtomicU16 = AtomicU16::new(1);

// Ceiling on one address-family query, dial and CNAME walk included.
const FAMILY_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// What a query is asked over.
#[async_trait::async_trait]
trait Carrier: Send + Sync {
    async fn open(&self, server: IpAddr) -> Result<Box<dyn TunnelStream>, TunnelError>;
}

// Why no server gave an answer. `reached` says whether any server could be
// dialled at all.
#[derive(Debug)]
struct Unanswered {
    error: TunnelError,
    reached: bool,
}

// A pipe stage carrying the queries. It keeps why it could not reach a
// server, so a lookup that reached none fails as the stage failed.
struct Stage<'a> {
    dialer: &'a dyn Dialer,
    failure: Mutex<Option<DialError>>,
}

#[async_trait::async_trait]
impl Carrier for Stage<'_> {
    async fn open(&self, server: IpAddr) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let target = Target {
            host: server.to_string(),
            port: 53,
            purpose: Purpose::Dns,
            addresses: vec![server],
        };
        match self.dialer.dial(&target).await {
            Ok(dialed) => Ok(dialed.into_observed_stream()),
            Err(error) => {
                let said = TunnelError::Engine(error.to_string());
                *self.failure.lock().expect("dns stage failure") = Some(error);
                Err(said)
            }
        }
    }
}

// Resolves `host` by asking `servers` in order through `dialer`. When no
// server could be reached at all, the error is the stage's own, so a dead
// path counts against the path and not against the name.
pub async fn resolve_through(
    dialer: &dyn Dialer,
    servers: &[IpAddr],
    host: &str,
) -> Result<Vec<IpAddr>, DialError> {
    if let Ok(ip) = host.parse() {
        return Ok(vec![ip]);
    }
    let stage = Stage {
        dialer,
        failure: Mutex::new(None),
    };
    let resolved = resolve_name(&stage, servers, host).await;
    crate::metrics::record_resolution(crate::metrics::Resolver::Routed, resolved.is_ok());
    resolved.map_err(|unanswered| {
        let failure = stage.failure.into_inner().expect("dns stage failure");
        match failure {
            Some(failure) if !unanswered.reached => failure,
            _ => DialError::Destination(std::io::Error::other(unanswered.error)),
        }
    })
}

async fn resolve_name(
    provider: &dyn Carrier,
    servers: &[IpAddr],
    host: &str,
) -> Result<Vec<IpAddr>, Unanswered> {
    if servers.is_empty() {
        return Err(Unanswered {
            error: TunnelError::Configuration("no DNS servers to ask".into()),
            reached: false,
        });
    }
    let mut reached_any = false;
    for server in servers {
        let mut addresses = Vec::new();
        for qtype in [1u16, 28] {
            // A resolver may answer for only one family. A failing or silent
            // peer query must neither erase the answers already collected nor
            // stop the other family from being asked, and its own wait is
            // bounded so it cannot spend the caller's whole deadline.
            //
            // A route that cannot reach the server at all is different: the
            // other family would only dial the same dead route again, and the
            // caller's ladder moves on sooner for being told once.
            // `query` marks `reached` as soon as the server answers the
            // dial, so a query cut off by the ceiling still says so.
            let mut reached = false;
            let answered = tokio::time::timeout(
                FAMILY_QUERY_TIMEOUT,
                query(provider, *server, host, qtype, &mut reached),
            )
            .await;
            reached_any |= reached;
            match answered {
                Ok(Ok(found)) => addresses.extend(found),
                _ if !reached => break,
                _ => {}
            }
        }
        if !addresses.is_empty() {
            addresses.sort();
            addresses.dedup();
            return Ok(addresses);
        }
    }
    Err(Unanswered {
        error: TunnelError::Engine("routed DNS could not resolve the destination".into()),
        reached: reached_any,
    })
}

async fn query(
    provider: &dyn Carrier,
    server: IpAddr,
    host: &str,
    qtype: u16,
    reached: &mut bool,
) -> Result<Vec<IpAddr>, TunnelError> {
    let mut name = host.trim_end_matches('.').to_ascii_lowercase();
    for _ in 0..8 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let mut packet = Vec::from(id.to_be_bytes());
        packet.extend_from_slice(&[1, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
        if name.is_empty() || name.len() > 253 {
            return Err(invalid());
        }
        for label in name.split('.') {
            if label.is_empty() || label.len() > 63 || !label.is_ascii() {
                return Err(invalid());
            }
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
        packet.extend_from_slice(&qtype.to_be_bytes());
        packet.extend_from_slice(&[0, 1]);
        let mut stream = provider.open(server).await?;
        *reached = true;
        stream
            .write_u16(packet.len() as u16)
            .await
            .map_err(|_| invalid())?;
        stream.write_all(&packet).await.map_err(|_| invalid())?;
        let size = stream.read_u16().await.map_err(|_| invalid())? as usize;
        if !(12..=16384).contains(&size) {
            return Err(invalid());
        }
        let mut reply = vec![0; size];
        stream.read_exact(&mut reply).await.map_err(|_| invalid())?;
        let (addresses, alias) = parse_response(&reply, id, &name, qtype)?;
        if !addresses.is_empty() {
            return Ok(addresses);
        }
        match alias {
            Some(alias) => name = alias,
            None => return Ok(Vec::new()),
        }
    }
    Err(invalid())
}

fn u16_at(bytes: &[u8], at: usize) -> Result<u16, TunnelError> {
    Ok(u16::from_be_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}

fn read_name(bytes: &[u8], cursor: &mut usize) -> Result<String, TunnelError> {
    let mut at = *cursor;
    let mut jumped = false;
    let mut labels = Vec::new();
    let mut length = 0;
    for _ in 0..128 {
        let head = *bytes.get(at).ok_or_else(invalid)?;
        if head & 0xc0 == 0xc0 {
            let target = (u16_at(bytes, at)? & 0x3fff) as usize;
            if target >= at {
                return Err(invalid());
            }
            if !jumped {
                *cursor = at + 2;
                jumped = true;
            }
            at = target;
            continue;
        }
        if head & 0xc0 != 0 {
            return Err(invalid());
        }
        at += 1;
        if head == 0 {
            if !jumped {
                *cursor = at;
            }
            return Ok(labels.join("."));
        }
        let label = bytes.get(at..at + head as usize).ok_or_else(invalid)?;
        if !label.is_ascii() {
            return Err(invalid());
        }
        length += label.len() + 1;
        if length > 254 {
            return Err(invalid());
        }
        labels.push(
            std::str::from_utf8(label)
                .map_err(|_| invalid())?
                .to_ascii_lowercase(),
        );
        at += head as usize;
    }
    Err(invalid())
}

fn parse_response(
    bytes: &[u8],
    id: u16,
    host: &str,
    qtype: u16,
) -> Result<(Vec<IpAddr>, Option<String>), TunnelError> {
    if u16_at(bytes, 0)? != id || u16_at(bytes, 2)? & 0xfa0f != 0x8000 || u16_at(bytes, 4)? != 1 {
        return Err(invalid());
    }
    let count = u16_at(bytes, 6)? as usize;
    if count > 128 {
        return Err(invalid());
    }
    let mut at = 12;
    if read_name(bytes, &mut at)? != host
        || u16_at(bytes, at)? != qtype
        || u16_at(bytes, at + 2)? != 1
    {
        return Err(invalid());
    }
    at += 4;
    let mut records = Vec::new();
    let mut aliases = Vec::new();
    for _ in 0..count {
        let owner = read_name(bytes, &mut at)?;
        let kind = u16_at(bytes, at)?;
        let class = u16_at(bytes, at + 2)?;
        let size = u16_at(bytes, at + 8)? as usize;
        at += 10;
        let data = bytes.get(at..at + size).ok_or_else(invalid)?;
        if class == 1 {
            match (kind, size) {
                (1, 4) => records.push((
                    owner,
                    IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                )),
                (28, 16) => records.push((
                    owner,
                    IpAddr::V6(Ipv6Addr::from(
                        <[u8; 16]>::try_from(data).map_err(|_| invalid())?,
                    )),
                )),
                (5, _) => {
                    let mut cursor = at;
                    let alias = read_name(bytes, &mut cursor)?;
                    if cursor != at + size {
                        return Err(invalid());
                    }
                    aliases.push((owner, alias));
                }
                _ => {}
            }
        }
        at += size;
    }
    let mut canonical = host.to_owned();
    for _ in 0..8 {
        match aliases.iter().find(|(owner, _)| owner == &canonical) {
            Some((_, alias)) => canonical = alias.clone(),
            None => break,
        }
    }
    if aliases.iter().any(|(owner, _)| owner == &canonical) {
        return Err(invalid());
    }
    let found = records
        .into_iter()
        .filter(|(owner, ip)| owner == &canonical && (ip.is_ipv4() == (qtype == 1)))
        .map(|(_, ip)| ip)
        .collect();
    Ok((found, (canonical != host).then_some(canonical)))
}

#[cfg(test)]
mod tests {
    use super::*;
    mod families;
    #[test]
    fn compression_cycles_and_truncation_are_rejected() {
        assert!(read_name(&[0xc0, 0], &mut 0).is_err());
        assert!(read_name(&[3, b'a'], &mut 0).is_err());
        assert!(parse_response(&[0; 12], 1, "test", 1).is_err());
    }
}

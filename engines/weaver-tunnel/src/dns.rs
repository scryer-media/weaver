//! Bounded DNS-over-TCP through a selected proxy. Never uses the host resolver.
use crate::{TunnelError, TunnelProvider};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn invalid() -> TunnelError {
    TunnelError::Engine("routed DNS returned an invalid response".into())
}
static NEXT_ID: AtomicU16 = AtomicU16::new(1);

pub async fn resolve(
    provider: &dyn TunnelProvider,
    servers: &[IpAddr],
    host: &str,
) -> Result<Vec<IpAddr>, TunnelError> {
    if let Ok(ip) = host.parse() {
        return Ok(vec![ip]);
    }
    if servers.is_empty() {
        return Err(TunnelError::Configuration(
            "this proxy needs a routed DNS server for hostname resolution".into(),
        ));
    }
    for server in servers {
        let mut addresses = Vec::new();
        let mut failed = false;
        for qtype in [1u16, 28] {
            match query(provider, *server, host, qtype).await {
                Ok(found) => addresses.extend(found),
                Err(_) => {
                    failed = true;
                    break;
                }
            }
        }
        if !failed && !addresses.is_empty() {
            addresses.sort();
            addresses.dedup();
            return Ok(addresses);
        }
    }
    Err(TunnelError::Engine(
        "routed DNS could not resolve the destination".into(),
    ))
}

async fn query(
    provider: &dyn TunnelProvider,
    server: IpAddr,
    host: &str,
    qtype: u16,
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
        let mut stream = provider.dial(&server.to_string(), 53).await?;
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
    #[test]
    fn compression_cycles_and_truncation_are_rejected() {
        assert!(read_name(&[0xc0, 0], &mut 0).is_err());
        assert!(read_name(&[3, b'a'], &mut 0).is_err());
        assert!(parse_response(&[0; 12], 1, "test", 1).is_err());
    }
}

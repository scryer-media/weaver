use crate::{NntpError, ServerConfig, tls::NntpTransport};
use std::{
    io::{Read, Write},
    net::TcpStream,
};

pub async fn connect(
    config: &ServerConfig,
) -> Result<
    (
        NntpTransport,
        std::sync::Arc<weaver_tunnel::bridge::ConnectionOutcome>,
    ),
    NntpError,
> {
    let bridge = config
        .proxy
        .as_ref()
        .ok_or_else(|| NntpError::Io(std::io::Error::other("proxy transport missing")))?;
    let mut tcp = tokio::net::TcpStream::connect(bridge.addr()?).await?;
    let outcome = bridge.register_connection(tcp.local_addr()?)?;
    tcp.set_nodelay(true)?;
    weaver_tunnel::transport::socks_connect(
        &mut tcp,
        &config.host,
        config.port,
        Some(bridge.credentials()),
    )
    .await
    .map_err(|e| NntpError::Io(std::io::Error::other(e)))?;
    let transport = NntpTransport::Plain {
        inner: tcp,
        remote_addr: None,
    };
    if config.tls {
        crate::tls::upgrade_starttls(
            transport,
            &config.host,
            config.tls_ca_cert.as_deref(),
            config.tls_name_mismatch_certificate_der.as_deref(),
            config.tls_cipher_preference,
        )
        .await
        .map(|transport| (transport, outcome))
    } else {
        Ok((transport, outcome))
    }
}

pub fn connect_blocking(
    config: &ServerConfig,
) -> Result<
    (
        TcpStream,
        std::sync::Arc<weaver_tunnel::bridge::ConnectionOutcome>,
    ),
    NntpError,
> {
    let bridge = config
        .proxy
        .as_ref()
        .ok_or_else(|| NntpError::Io(std::io::Error::other("proxy transport missing")))?;
    let mut tcp = TcpStream::connect_timeout(&bridge.addr()?, config.connect_timeout)?;
    let outcome = bridge.register_connection(tcp.local_addr()?)?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(bridge.connect_timeout))?;
    tcp.set_write_timeout(Some(config.connect_timeout))?;
    tcp.write_all(&[5, 1, 2])?;
    let mut greeting = [0; 2];
    tcp.read_exact(&mut greeting)?;
    if greeting != [5, 2] {
        return Err(NntpError::Io(std::io::Error::other(
            "proxy bridge refused negotiation",
        )));
    }
    let (user, password) = bridge.credentials();
    let mut auth = vec![1, user.len() as u8];
    auth.extend_from_slice(user.as_bytes());
    auth.push(password.len() as u8);
    auth.extend_from_slice(password.as_bytes());
    tcp.write_all(&auth)?;
    tcp.read_exact(&mut greeting)?;
    if greeting != [1, 0] {
        return Err(NntpError::Io(std::io::Error::other(
            "proxy bridge authentication failed",
        )));
    }
    tcp.write_all(
        &weaver_tunnel::transport::socks_request(&config.host, config.port)
            .map_err(|e| NntpError::Io(std::io::Error::other(e)))?,
    )?;
    let mut reply = [0; 4];
    tcp.read_exact(&mut reply)?;
    if reply[..3] != [5, 0, 0] {
        return Err(NntpError::Io(std::io::Error::other(
            "proxy routing ladder exhausted",
        )));
    }
    let length = match reply[3] {
        1 => 4,
        4 => 16,
        3 => {
            let mut len = [0];
            tcp.read_exact(&mut len)?;
            len[0] as usize
        }
        _ => {
            return Err(NntpError::Io(std::io::Error::other(
                "invalid proxy bridge reply",
            )));
        }
    };
    tcp.read_exact(&mut vec![0; length + 2])?;
    tcp.set_read_timeout(Some(config.command_timeout))?;
    tcp.set_write_timeout(Some(config.command_timeout))?;
    Ok((tcp, outcome))
}

use crate::route_stream::BlockingSocket;
use crate::{NntpError, ServerConfig, tls::NntpTransport};

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
    let (tcp, outcome) = bridge.dial(&config.host, config.port).await?;
    let transport = NntpTransport::Plain {
        inner: tcp.into(),
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
        BlockingSocket,
        std::sync::Arc<weaver_tunnel::bridge::ConnectionOutcome>,
    ),
    NntpError,
> {
    let bridge = config
        .proxy
        .as_ref()
        .ok_or_else(|| NntpError::Io(std::io::Error::other("proxy transport missing")))?;
    let (stream, outcome) = bridge
        .runtime()
        .block_on(bridge.dial(&config.host, config.port))?;
    Ok((
        BlockingSocket::tunnel(stream, bridge.runtime().clone(), config.command_timeout),
        outcome,
    ))
}

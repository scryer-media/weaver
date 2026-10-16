// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Why a proxy failed, as a stable code and the values its sentence names.
// The web words each code itself; `Display` is the English for logs and for
// a web build that has no sentence for the code.
//
// The tunnel error type carries most of what went wrong as text: a stream
// proxy's failures travel as engine text, and the SSH, WireGuard and HTTP/3
// engines describe what failed in their own words. `classify` reads that
// text back: Weaver's own wording through the code that writes it, the
// engines' wording through the phrases below, and the operating system's
// through the error number it appends.
use std::{fmt, io, time::Duration};

use crate::{
    ED25519_ONLY_PRIVATE_KEY_MESSAGE, TunnelError, WIREGUARD_KEY_MESSAGE,
    pipe::{self, DialError, ResolutionFailed},
    transport::{self, Reply, Said},
};

#[cfg(test)]
#[path = "fault_tests.rs"]
mod tests;

// A value a fault's sentence may name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Param {
    Text(String),
    Number(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyFault {
    // The endpoint, and the network on the way to it.
    NameNotResolved,
    ConnectionRefused,
    HostUnreachable,
    NetworkUnreachable,
    NoRouteVia { via: String },
    ConnectionClosed,
    TimedOut { seconds: u64 },
    UdpTimedOut { seconds: u64 },
    EgressUnusable { detail: String },

    // What a proxy answered.
    NotAProxy { protocol: &'static str },
    AuthRequired,
    AuthUnsupported,
    CredentialsRejected,
    CredentialsInvalid,
    ClosedDuringSignIn,
    ConnectRefused { status: u16 },
    DestinationRefused { target: String },
    DestinationUnreachable { target: String, reply: String },
    DestinationTimedOut { target: String },
    DestinationForbidden { target: String },

    // SSH.
    SshKeyMissing,
    SshKeyPassphrase,
    SshKeyNotEd25519 { algorithm: Option<String> },
    SshKeyUnreadable { detail: String },
    SshHostKeyChanged { expected: String, actual: String },
    SshKeyRejected { username: String },
    SshHandshakeFailed { detail: String },
    SshForwardingRefused { target: String },

    // WireGuard.
    WireGuardKeyInvalid,
    WireGuardPeerIsSelf,
    WireGuardNoDns,
    WireGuardNoHandshake { seconds: u64, public_key: String },
    WireGuardFailed { detail: String },

    // HTTP/3.
    Http3Tls,
    Http3HandshakeFailed,
    Http3Failed { detail: String },

    // Weaver's own routing.
    ProxyUnavailable,
    StreamFailed,
    TestNeedsDns,

    // Anything else, in the words it came in.
    Other { detail: String },
}

impl ProxyFault {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NameNotResolved => "NAME_NOT_RESOLVED",
            Self::ConnectionRefused => "CONNECTION_REFUSED",
            Self::HostUnreachable => "HOST_UNREACHABLE",
            Self::NetworkUnreachable => "NETWORK_UNREACHABLE",
            Self::NoRouteVia { .. } => "NO_ROUTE_VIA",
            Self::ConnectionClosed => "CONNECTION_CLOSED",
            Self::TimedOut { .. } => "TIMED_OUT",
            Self::UdpTimedOut { .. } => "UDP_TIMED_OUT",
            Self::EgressUnusable { .. } => "EGRESS_UNUSABLE",
            Self::NotAProxy { .. } => "NOT_A_PROXY",
            Self::AuthRequired => "AUTH_REQUIRED",
            Self::AuthUnsupported => "AUTH_UNSUPPORTED",
            Self::CredentialsRejected => "CREDENTIALS_REJECTED",
            Self::CredentialsInvalid => "CREDENTIALS_INVALID",
            Self::ClosedDuringSignIn => "CLOSED_DURING_SIGN_IN",
            Self::ConnectRefused { .. } => "CONNECT_REFUSED",
            Self::DestinationRefused { .. } => "DESTINATION_REFUSED",
            Self::DestinationUnreachable { .. } => "DESTINATION_UNREACHABLE",
            Self::DestinationTimedOut { .. } => "DESTINATION_TIMED_OUT",
            Self::DestinationForbidden { .. } => "DESTINATION_FORBIDDEN",
            Self::SshKeyMissing => "SSH_KEY_MISSING",
            Self::SshKeyPassphrase => "SSH_KEY_PASSPHRASE",
            Self::SshKeyNotEd25519 { .. } => "SSH_KEY_NOT_ED25519",
            Self::SshKeyUnreadable { .. } => "SSH_KEY_UNREADABLE",
            Self::SshHostKeyChanged { .. } => "SSH_HOST_KEY_CHANGED",
            Self::SshKeyRejected { .. } => "SSH_KEY_REJECTED",
            Self::SshHandshakeFailed { .. } => "SSH_HANDSHAKE_FAILED",
            Self::SshForwardingRefused { .. } => "SSH_FORWARDING_REFUSED",
            Self::WireGuardKeyInvalid => "WIREGUARD_KEY_INVALID",
            Self::WireGuardPeerIsSelf => "WIREGUARD_PEER_IS_SELF",
            Self::WireGuardNoDns => "WIREGUARD_NO_DNS",
            Self::WireGuardNoHandshake { .. } => "WIREGUARD_NO_HANDSHAKE",
            Self::WireGuardFailed { .. } => "WIREGUARD_FAILED",
            Self::Http3Tls => "HTTP3_TLS",
            Self::Http3HandshakeFailed => "HTTP3_HANDSHAKE_FAILED",
            Self::Http3Failed { .. } => "HTTP3_FAILED",
            Self::ProxyUnavailable => "PROXY_UNAVAILABLE",
            Self::StreamFailed => "STREAM_FAILED",
            Self::TestNeedsDns => "TEST_NEEDS_DNS",
            Self::Other { .. } => "OTHER",
        }
    }

    // The values the sentence for the code may name, by the names the web's
    // sentences use.
    pub fn params(&self) -> Vec<(&'static str, Param)> {
        let text = |value: &str| Param::Text(value.to_owned());
        match self {
            Self::NoRouteVia { via } => vec![("via", text(via))],
            Self::TimedOut { seconds } | Self::UdpTimedOut { seconds } => {
                vec![("seconds", Param::Number(*seconds))]
            }
            Self::EgressUnusable { detail }
            | Self::SshKeyUnreadable { detail }
            | Self::SshHandshakeFailed { detail }
            | Self::WireGuardFailed { detail }
            | Self::Http3Failed { detail }
            | Self::Other { detail } => vec![("detail", text(detail))],
            Self::NotAProxy { protocol } => vec![("protocol", text(protocol))],
            Self::ConnectRefused { status } => vec![("status", Param::Number(u64::from(*status)))],
            Self::DestinationRefused { target }
            | Self::DestinationTimedOut { target }
            | Self::DestinationForbidden { target }
            | Self::SshForwardingRefused { target } => vec![("target", text(target))],
            Self::DestinationUnreachable { target, reply } => {
                vec![("target", text(target)), ("reply", text(reply))]
            }
            Self::SshKeyNotEd25519 { algorithm } => algorithm
                .iter()
                .map(|algorithm| ("algorithm", text(algorithm)))
                .collect(),
            Self::SshHostKeyChanged { expected, actual } => {
                vec![("expected", text(expected)), ("actual", text(actual))]
            }
            Self::SshKeyRejected { username } => vec![("username", text(username))],
            Self::WireGuardNoHandshake {
                seconds,
                public_key,
            } => vec![
                ("seconds", Param::Number(*seconds)),
                ("publicKey", text(public_key)),
            ],
            Self::NameNotResolved
            | Self::ConnectionRefused
            | Self::HostUnreachable
            | Self::NetworkUnreachable
            | Self::ConnectionClosed
            | Self::AuthRequired
            | Self::AuthUnsupported
            | Self::CredentialsRejected
            | Self::CredentialsInvalid
            | Self::ClosedDuringSignIn
            | Self::SshKeyMissing
            | Self::SshKeyPassphrase
            | Self::WireGuardKeyInvalid
            | Self::WireGuardPeerIsSelf
            | Self::WireGuardNoDns
            | Self::Http3Tls
            | Self::Http3HandshakeFailed
            | Self::ProxyUnavailable
            | Self::StreamFailed
            | Self::TestNeedsDns => Vec::new(),
        }
    }
}

// Each sentence leaves out what the place showing it already names: a
// proxy's row shows its host and port, a hop on the flow shows its name.
impl fmt::Display for ProxyFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameNotResolved => {
                f.write_str("The host name did not resolve. Check its spelling and the DNS servers.")
            }
            Self::ConnectionRefused => f.write_str(
                "The connection was refused. Check the port and that the proxy is running.",
            ),
            Self::HostUnreachable => f.write_str(
                "The host is unreachable. Check the address and the network path to it.",
            ),
            Self::NetworkUnreachable => f.write_str(
                "The network is unreachable. Check this machine's network connection.",
            ),
            Self::NoRouteVia { via } => write!(
                f,
                "No route through {via}. Check that interface's network connection."
            ),
            Self::ConnectionClosed => f.write_str(
                "The connection closed before the proxy answered. Check the proxy type and port.",
            ),
            Self::TimedOut { seconds } => write!(
                f,
                "No answer within {seconds} seconds. Check the address, the port and any firewall."
            ),
            Self::UdpTimedOut { seconds } => write!(
                f,
                "No answer over UDP within {seconds} seconds. Check the address, the port and that the firewall lets UDP through."
            ),
            Self::EgressUnusable { .. } => f.write_str(
                "The egress interface could not be used. Check that it is up and has an address.",
            ),
            Self::NotAProxy { protocol } => write!(
                f,
                "The server did not answer with the {protocol} proxy protocol. Check the proxy type and port."
            ),
            Self::AuthRequired => f.write_str("The proxy requires a username and password."),
            Self::AuthUnsupported => {
                f.write_str("The proxy does not accept a username and password. Remove them.")
            }
            Self::CredentialsRejected => {
                f.write_str("The proxy rejected the username or password.")
            }
            Self::CredentialsInvalid => f.write_str(
                "SOCKS5 needs a username of 1 to 255 bytes and a password of at most 255 bytes.",
            ),
            Self::ClosedDuringSignIn => f.write_str(
                "The proxy closed the connection during sign-in. Check the username and password.",
            ),
            Self::ConnectRefused { status } => {
                write!(f, "The proxy refused the connection with HTTP {status}.")
            }
            Self::DestinationRefused { target } => write!(f, "{target} refused the connection."),
            Self::DestinationUnreachable { target, reply } => {
                write!(f, "{target} could not be reached ({reply}).")
            }
            Self::DestinationTimedOut { target } => write!(f, "{target} did not answer in time."),
            Self::DestinationForbidden { target } => write!(
                f,
                "The proxy's rules do not allow connections to {target}."
            ),
            Self::SshKeyMissing => f.write_str("SSH needs an Ed25519 private key."),
            Self::SshKeyPassphrase => {
                f.write_str("The private key is passphrase-protected. Enter its passphrase.")
            }
            Self::SshKeyNotEd25519 { .. } => f.write_str(
                "The private key is not Ed25519. Generate one with ssh-keygen -t ed25519.",
            ),
            Self::SshKeyUnreadable { .. } => f.write_str(
                "The private key could not be read. Paste the whole key, including its BEGIN and END lines.",
            ),
            Self::SshHostKeyChanged { .. } => f.write_str(
                "The server's host key changed. Verify the server, then forget the old host key.",
            ),
            Self::SshKeyRejected { username } => write!(
                f,
                "The server did not accept the private key for {username}. Check its authorized_keys."
            ),
            Self::SshHandshakeFailed { detail } => write!(f, "The SSH handshake failed: {detail}"),
            Self::SshForwardingRefused { target } => write!(
                f,
                "The SSH server refused to forward to {target}. Allow TCP forwarding for this user."
            ),
            Self::WireGuardKeyInvalid => f.write_str(
                "A WireGuard key is not valid. Paste each key exactly as wg prints it.",
            ),
            Self::WireGuardPeerIsSelf => f.write_str(
                "The peer public key is this tunnel's own. Use the server's public key.",
            ),
            Self::WireGuardNoDns => f.write_str(
                "The tunnel has no DNS server. Add the DNS line from its configuration, or use IP addresses.",
            ),
            Self::WireGuardNoHandshake {
                seconds,
                public_key,
            } => write!(
                f,
                "No handshake within {seconds} seconds. Check the endpoint, and that the server's peer list has public key {public_key}."
            ),
            Self::WireGuardFailed { detail } => {
                write!(f, "The WireGuard tunnel could not start: {detail}")
            }
            Self::Http3Tls => f.write_str(
                "TLS with the proxy failed. Check the host name and the proxy's certificate.",
            ),
            Self::Http3HandshakeFailed => f.write_str(
                "The QUIC handshake failed. Check the port and that the proxy serves HTTP/3.",
            ),
            Self::Http3Failed { detail } => write!(f, "HTTP/3 to the proxy failed: {detail}"),
            Self::ProxyUnavailable => f.write_str("The proxy is disabled or could not be set up."),
            Self::StreamFailed => {
                f.write_str("The connection through the proxy failed after it opened.")
            }
            Self::TestNeedsDns => f.write_str(
                "Testing an HTTP/3 proxy needs a host and port, or DNS servers it resolves with.",
            ),
            Self::Other { detail } => f.write_str(detail),
        }
    }
}

// A host and port as a person types them: an IPv6 address goes in brackets.
pub fn authority(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

// Whole seconds, rounded up, so a wait is never reported shorter than it was.
pub fn seconds(wait: Duration) -> u64 {
    (wait.as_secs() + u64::from(wait.subsec_nanos() > 0)).max(1)
}

// What the SSH engine says.
const SSH_KEY_REQUIRED: &str = "SSH tunnels require an Ed25519 private key";
const SSH_KEY_PASSPHRASE: &str = "the private key is passphrase-protected";
const SSH_KEY_UNREADABLE: &str = "the private key could not be read: ";
const SSH_KEY_ALGORITHM: &str = " (this key is ";

// What the WireGuard engine says.
const WIREGUARD_NO_HANDSHAKE: &str = "the peer did not complete a handshake within ";
const WIREGUARD_PUBLIC_KEY: &str = "public key (";
const WIREGUARD_CARRIER: &str = "the tunnel beneath could not carry this one: ";
const WIREGUARD_PEER_IS_SELF: &str = "the peer public key is this tunnel's own public key";
const WIREGUARD_NO_DNS: &str = "the tunnel has no DNS server configured";
const WIREGUARD_REFUSED: &str = "the far side refused the connection";

// What the HTTP/3 engine says.
const HTTP3_NO_NAME: [&str; 2] = [
    "proxy endpoint DNS lookup failed",
    "proxy endpoint has no addresses",
];
const HTTP3_DEADLINE: [&str; 2] = [
    "HTTP/3 dial deadline exceeded",
    "QUIC address attempt deadline exceeded",
];
const HTTP3_HANDSHAKE: [&str; 2] = [
    "QUIC handshake failed",
    "HTTP/3 initialization failed or timed out",
];
const HTTP3_RESPONSE: &str = "invalid or interrupted CONNECT response";

// Every way a name lookup says it found nothing: the system's resolver,
// Weaver's own lookups, and the engines' lookups of an endpoint or through
// a tunnel.
const NAME_PHRASES: [&str; 13] = [
    "failed to lookup address information",
    crate::resolution::NO_ADDRESS,
    pipe::NO_ADDRESS_RESOLVED,
    pipe::RESOLVE_TIMED_OUT,
    crate::dns::UNRESOLVED,
    crate::dns::INVALID,
    "the endpoint name",
    "the tunnel's DNS server",
    "tunnel DNS slot",
    "the tunnel resolver",
    "the name resolved to no usable address",
    HTTP3_NO_NAME[0],
    HTTP3_NO_NAME[1],
];

// Every way a wait says it ran out.
const TIMEOUT_PHRASES: [&str; 3] = ["timed out", "in time", "deadline exceeded"];

// Windows sockets' name lookup errors, which have no `io::ErrorKind`.
const WINDOWS_NAME_ERRORS: std::ops::RangeInclusive<i32> = 11001..=11004;

// What `error` came to, for a hop or a proxy given `wait` to answer.
pub fn classify(error: &TunnelError, wait: Duration) -> ProxyFault {
    match error {
        TunnelError::Configuration(text) => configuration(text, wait),
        TunnelError::HostKeyMismatch {
            expected, actual, ..
        } => ProxyFault::SshHostKeyChanged {
            expected: expected.clone(),
            actual: actual.clone(),
        },
        TunnelError::Connect { detail, .. } => {
            from_text(detail, wait).unwrap_or_else(|| ProxyFault::SshHandshakeFailed {
                detail: detail.clone(),
            })
        }
        TunnelError::Auth { username, .. } => ProxyFault::SshKeyRejected {
            username: username.clone(),
        },
        TunnelError::Dial { host, port, detail } => dial(authority(host, *port), detail, wait),
        TunnelError::WireGuardConnect { detail, .. } => wireguard(detail, wait),
        TunnelError::Http3Connect { detail, .. } => http3(detail, wait),
        TunnelError::Http3Tls { .. } => ProxyFault::Http3Tls,
        TunnelError::Http3ProxyStatus { status: 407 } => ProxyFault::CredentialsRejected,
        TunnelError::Http3ProxyStatus { status } => ProxyFault::ConnectRefused { status: *status },
        TunnelError::Engine(text) => from_text(text, wait).unwrap_or_else(|| ProxyFault::Other {
            detail: text.clone(),
        }),
    }
}

// What an I/O failure came to: a tunnel error it carries, else what its
// words or its kind say.
pub fn from_io(error: &io::Error, wait: Duration) -> ProxyFault {
    if let Some(inner) = error.get_ref() {
        if let Some(tunnel) = inner.downcast_ref::<TunnelError>() {
            return classify(tunnel, wait);
        }
        if inner.is::<ResolutionFailed>() {
            return ProxyFault::NameNotResolved;
        }
    }
    let text = error.to_string();
    from_text(&text, wait)
        .or_else(|| from_kind(error.kind(), wait))
        .unwrap_or(ProxyFault::Other { detail: text })
}

impl DialError {
    // What this failure came to, for a stage given `wait` to answer.
    pub fn fault(&self, wait: Duration) -> ProxyFault {
        match self {
            Self::Skipped(reason) => ProxyFault::Other {
                detail: reason.clone(),
            },
            Self::AtCapacity(_) => ProxyFault::Other {
                detail: self.to_string(),
            },
            Self::Bind(error) => ProxyFault::EgressUnusable {
                detail: error.to_string(),
            },
            Self::Egress(error) | Self::Destination(error) | Self::Refused(error) => {
                from_io(error, wait)
            }
            Self::Hop { source, .. } | Self::Fatal(source) => classify(source, wait),
            Self::Timeout { .. } => ProxyFault::TimedOut {
                seconds: seconds(wait),
            },
        }
    }
}

// What a text says, wherever in it and however it was wrapped: Weaver's own
// words first, then a lookup that found nothing, then the system's error
// number, then a wait that ran out.
pub fn from_text(text: &str, wait: Duration) -> Option<ProxyFault> {
    if let Some(fault) = weaver(text) {
        return Some(fault);
    }
    if NAME_PHRASES.iter().any(|phrase| text.contains(phrase)) {
        return Some(ProxyFault::NameNotResolved);
    }
    if let Some(fault) = os_error(text).and_then(|code| from_os_error(code, wait)) {
        return Some(fault);
    }
    TIMEOUT_PHRASES
        .iter()
        .any(|phrase| text.contains(phrase))
        .then(|| ProxyFault::TimedOut {
            seconds: seconds(wait),
        })
}

// Weaver's own words, as the stream proxy adapters and the egress write them.
fn weaver(text: &str) -> Option<ProxyFault> {
    if let Some(rest) = after(text, transport::HOP_BEFORE) {
        return reached(rest);
    }
    if let Some(rest) = after(text, transport::UNREACHABLE) {
        return reached(rest);
    }
    if let Some(rest) = after(text, transport::REJECTED) {
        let (target, status) = rest.rsplit_once(transport::REJECTED_STATUS)?;
        let digits = status.len()
            - status
                .trim_start_matches(|c: char| c.is_ascii_digit())
                .len();
        let status = status[..digits].parse().ok()?;
        return Some(if status == 403 {
            ProxyFault::DestinationForbidden {
                target: target.into(),
            }
        } else {
            ProxyFault::ConnectRefused { status }
        });
    }
    if let Some(said) = Said::find(text) {
        return Some(match said {
            Said::SocksClosed | Said::SocksConnectClosed | Said::HttpConnectClosed => {
                ProxyFault::ConnectionClosed
            }
            Said::NotSocks | Said::SocksReplyInvalid => {
                ProxyFault::NotAProxy { protocol: "SOCKS5" }
            }
            Said::HttpHeadersTooLarge | Said::HttpResponseInvalid => {
                ProxyFault::NotAProxy { protocol: "HTTP" }
            }
            Said::SocksAuthRequired | Said::HttpAuthRequired => ProxyFault::AuthRequired,
            Said::SocksMethodRejected => ProxyFault::AuthUnsupported,
            Said::SocksCredentialLength => ProxyFault::CredentialsInvalid,
            Said::SocksAuthClosed => ProxyFault::ClosedDuringSignIn,
            Said::SocksCredentialsRejected | Said::HttpCredentialsRejected => {
                ProxyFault::CredentialsRejected
            }
            Said::InvalidDestination | Said::InvalidConnectDestination | Said::SocketSetup => {
                ProxyFault::Other {
                    detail: said.text().into(),
                }
            }
        });
    }
    let via = after(text, pipe::NO_ROUTE_VIA)?;
    Some(ProxyFault::NoRouteVia {
        via: via.split_once(": ").map_or(via, |(via, _)| via).into(),
    })
}

// What a proxy's answer about reaching `target` says, from the text after
// its prefix: "<target>: <reply>".
fn reached(rest: &str) -> Option<ProxyFault> {
    let (target, reply) = rest.rsplit_once(": ")?;
    let target = target.to_owned();
    Some(match Reply::read(reply)? {
        Reply::Socks(5) => ProxyFault::DestinationRefused { target },
        Reply::Socks(2) => ProxyFault::DestinationForbidden { target },
        Reply::Http(504) | Reply::Silent => ProxyFault::DestinationTimedOut { target },
        reply => ProxyFault::DestinationUnreachable {
            target,
            reply: reply.to_string(),
        },
    })
}

fn after<'a>(text: &'a str, marker: &str) -> Option<&'a str> {
    text.find(marker).map(|at| &text[at + marker.len()..])
}

fn configuration(text: &str, wait: Duration) -> ProxyFault {
    if text.starts_with(SSH_KEY_REQUIRED) {
        ProxyFault::SshKeyMissing
    } else if text.starts_with(SSH_KEY_PASSPHRASE) {
        ProxyFault::SshKeyPassphrase
    } else if let Some(rest) = text.strip_prefix(ED25519_ONLY_PRIVATE_KEY_MESSAGE) {
        ProxyFault::SshKeyNotEd25519 {
            algorithm: rest
                .strip_prefix(SSH_KEY_ALGORITHM)
                .and_then(|rest| rest.strip_suffix(')'))
                .map(Into::into),
        }
    } else if let Some(detail) = text.strip_prefix(SSH_KEY_UNREADABLE) {
        ProxyFault::SshKeyUnreadable {
            detail: detail.into(),
        }
    } else if text.contains(WIREGUARD_KEY_MESSAGE) {
        ProxyFault::WireGuardKeyInvalid
    } else if text.starts_with(WIREGUARD_PEER_IS_SELF) {
        ProxyFault::WireGuardPeerIsSelf
    } else if text.starts_with(WIREGUARD_NO_DNS) {
        ProxyFault::WireGuardNoDns
    } else {
        from_text(text, wait).unwrap_or_else(|| ProxyFault::Other {
            detail: text.into(),
        })
    }
}

// A tunnel that was up could not open a stream to `target`.
fn dial(target: String, detail: &str, wait: Duration) -> ProxyFault {
    if pipe::is_forwarding_refusal(detail) {
        ProxyFault::SshForwardingRefused { target }
    } else if NAME_PHRASES.iter().any(|phrase| detail.contains(phrase)) {
        ProxyFault::NameNotResolved
    } else if detail.contains(WIREGUARD_REFUSED) {
        ProxyFault::DestinationRefused { target }
    } else if TIMEOUT_PHRASES.iter().any(|phrase| detail.contains(phrase)) {
        ProxyFault::DestinationTimedOut { target }
    } else {
        from_text(detail, wait).unwrap_or_else(|| ProxyFault::DestinationUnreachable {
            target,
            reply: detail.into(),
        })
    }
}

fn wireguard(detail: &str, wait: Duration) -> ProxyFault {
    if let Some(rest) = detail.strip_prefix(WIREGUARD_NO_HANDSHAKE) {
        return ProxyFault::WireGuardNoHandshake {
            seconds: rest
                .split_once('s')
                .and_then(|(seconds, _)| seconds.parse().ok())
                .unwrap_or_else(|| seconds(wait)),
            public_key: rest
                .rsplit_once(WIREGUARD_PUBLIC_KEY)
                .and_then(|(_, key)| key.strip_suffix(")."))
                .unwrap_or_default()
                .into(),
        };
    }
    // A carrier's failure, in its own words.
    let said = detail.strip_prefix(WIREGUARD_CARRIER).unwrap_or(detail);
    from_text(said, wait).unwrap_or_else(|| ProxyFault::WireGuardFailed {
        detail: detail.into(),
    })
}

fn http3(detail: &str, wait: Duration) -> ProxyFault {
    if HTTP3_NO_NAME.contains(&detail) {
        ProxyFault::NameNotResolved
    } else if HTTP3_DEADLINE.contains(&detail) {
        ProxyFault::UdpTimedOut {
            seconds: seconds(wait),
        }
    } else if HTTP3_HANDSHAKE.contains(&detail) {
        ProxyFault::Http3HandshakeFailed
    } else if detail == HTTP3_RESPONSE {
        ProxyFault::NotAProxy { protocol: "HTTP/3" }
    } else {
        ProxyFault::Http3Failed {
            detail: detail.into(),
        }
    }
}

// The error number the system's words end with, as std writes it.
fn os_error(text: &str) -> Option<i32> {
    let (_, number) = text.rsplit_once("(os error ")?;
    number.split_once(')')?.0.parse().ok()
}

fn from_os_error(code: i32, wait: Duration) -> Option<ProxyFault> {
    if WINDOWS_NAME_ERRORS.contains(&code) {
        return Some(ProxyFault::NameNotResolved);
    }
    from_kind(io::Error::from_raw_os_error(code).kind(), wait)
}

fn from_kind(kind: io::ErrorKind, wait: Duration) -> Option<ProxyFault> {
    use io::ErrorKind as Kind;
    Some(match kind {
        Kind::ConnectionRefused => ProxyFault::ConnectionRefused,
        Kind::HostUnreachable => ProxyFault::HostUnreachable,
        Kind::NetworkUnreachable | Kind::NetworkDown => ProxyFault::NetworkUnreachable,
        Kind::TimedOut => ProxyFault::TimedOut {
            seconds: seconds(wait),
        },
        Kind::ConnectionReset
        | Kind::ConnectionAborted
        | Kind::BrokenPipe
        | Kind::UnexpectedEof => ProxyFault::ConnectionClosed,
        _ => return None,
    })
}

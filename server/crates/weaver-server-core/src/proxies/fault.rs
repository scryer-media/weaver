// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Why a proxy failed, and what a proxy test that passed showed, as stable
// codes and the values their sentences name. The web words each code; the
// English here is for logs and for a web build with no sentence for it.
use std::fmt;

pub use weaver_tunnel::fault::{Param, ProxyFault};

// The values a fault's sentence may name, as the API sends them.
pub fn fault_params(fault: &ProxyFault) -> serde_json::Map<String, serde_json::Value> {
    fault
        .params()
        .into_iter()
        .map(|(name, value)| {
            let value = match value {
                Param::Text(text) => serde_json::Value::String(text),
                Param::Number(number) => serde_json::Value::from(number),
            };
            (name.to_owned(), value)
        })
        .collect()
}

// How far a proxy test that passed got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyTestPass {
    // The tunnel came up.
    Connected,
    // The proxy carried a connection to a DNS server it resolves with.
    Forwarded,
    // The proxy's endpoint answered; nothing was asked of it.
    Reachable,
}

impl ProxyTestPass {
    pub fn code(self) -> &'static str {
        match self {
            Self::Connected => "CONNECTED",
            Self::Forwarded => "FORWARDED",
            Self::Reachable => "REACHABLE",
        }
    }
}

impl fmt::Display for ProxyTestPass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connected => "Connected.",
            Self::Forwarded => "Connected and forwarded through the proxy.",
            Self::Reachable => "Reachable. Give a host and port to check forwarding.",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_keep_their_names_and_types() {
        let fault = ProxyFault::WireGuardNoHandshake {
            seconds: 12,
            public_key: "KEY".into(),
        };
        assert_eq!(
            serde_json::Value::Object(fault_params(&fault)),
            serde_json::json!({ "seconds": 12, "publicKey": "KEY" })
        );
        assert!(fault_params(&ProxyFault::AuthRequired).is_empty());
    }

    // A WireGuard profile's private and preshared keys are in no text Weaver
    // makes from it: not its `Debug`, not the tunnel's, not a refusal, not a
    // tunnel error, not the fault a failed handshake comes to, and not that
    // fault's API values. The public key a failed handshake names still is.
    #[test]
    fn no_wireguard_text_holds_the_private_or_preshared_key() {
        use std::time::Duration;

        use base64::Engine as _;
        use base64::engine::general_purpose::STANDARD;
        use weaver_tunnel::TunnelError;

        use crate::proxies::{ProxyKind, ProxyProfile, ProxySecrets, Resolver};

        let private = STANDARD.encode([7u8; 32]);
        let preshared = STANDARD.encode([5u8; 32]);
        let profile = ProxyProfile {
            id: 1,
            name: "Frankfurt".into(),
            kind: ProxyKind::WireGuard,
            enabled: true,
            host: "fra.vpn.example".into(),
            port: 51820,
            resolver: Resolver::Proxy,
            dns_servers: vec!["10.2.0.1".parse().unwrap()],
            tunnel_addresses: vec!["10.2.0.2/32".into()],
            peer_public_key: Some(STANDARD.encode([9u8; 32])),
            mtu: 1420,
            keepalive_seconds: Some(25),
            timeout_seconds: 30,
            host_key_fingerprint: None,
            revision: 1,
            natpmp: false,
            forwarded_port: None,
            secrets: ProxySecrets {
                private_key: Some(private.clone()),
                preshared_key: Some(preshared.clone()),
                ..ProxySecrets::default()
            },
        };
        let spec = profile.wireguard_spec().expect("spec");
        let own_key = spec.public_key();

        let mut texts = vec![
            format!("{profile:?}"),
            format!("{:?}", profile.secrets),
            format!("{spec:?}"),
            spec.describe(),
            spec.endpoint_description(),
        ];
        // Every refusal the profile's keys can earn, at save and at bring-up.
        let refused = |change: &dyn Fn(&mut ProxyProfile)| {
            let mut broken = profile.clone();
            change(&mut broken);
            broken.validate().expect_err("refused")
        };
        let refusals = [
            refused(&|p| p.secrets.private_key = Some(format!("{private}x"))),
            refused(&|p| p.secrets.preshared_key = Some(format!("{preshared}x"))),
            refused(&|p| p.secrets.preshared_key = Some(preshared[..40].to_string())),
            refused(&|p| p.peer_public_key = Some(own_key.clone())),
            refused(&|p| p.dns_servers.clear()),
        ];
        for refusal in &refusals {
            texts.push(format!("{refusal} {refusal:?}"));
            texts.push(serde_json::Value::Object(refusal.params()).to_string());
        }
        let key_errors = [
            weaver_tunnel::validate_wireguard_keys(&format!("{private}x"), &own_key, None),
            weaver_tunnel::validate_wireguard_keys(
                &private,
                &STANDARD.encode([9u8; 32]),
                Some(&format!("{preshared}x")),
            ),
            weaver_tunnel::validate_wireguard_keys(&private, &own_key, Some(&preshared)),
        ];
        // The engine's own words for a handshake that never came, and a fault
        // for every way a WireGuard bring-up can fail.
        let no_handshake = format!(
            "the peer did not complete a handshake within 30s. WireGuard answers nothing it \
             cannot authenticate, so this is what a wrong private key, a wrong peer public key, \
             a wrong or missing preshared key and an unreachable endpoint all look like. Check \
             that the server's `[Peer]` section lists this tunnel's public key ({own_key})."
        );
        let mut errors: Vec<TunnelError> = key_errors
            .into_iter()
            .map(|result| result.expect_err("refused"))
            .collect();
        errors.extend(
            [
                no_handshake.as_str(),
                "the tunnel's network stack stopped before the handshake completed",
            ]
            .map(|detail| TunnelError::WireGuardConnect {
                host: spec.endpoint_host.clone(),
                port: spec.endpoint_port,
                detail: detail.to_string(),
            }),
        );
        let mut handshake = None;
        for error in &errors {
            let fault = weaver_tunnel::fault::classify(error, Duration::from_secs(30));
            texts.push(format!("{error} {error:?}"));
            texts.push(format!("{fault} {fault:?} {}", fault.code()));
            texts.push(serde_json::Value::Object(fault_params(&fault)).to_string());
            if matches!(fault, ProxyFault::WireGuardNoHandshake { .. }) {
                handshake = Some(fault.to_string());
            }
        }

        let (seven, five) = ("7, 7, 7", "5, 5, 5");
        for text in &texts {
            for secret in [
                private.as_str(),
                preshared.as_str(),
                &preshared[..40],
                seven,
                five,
            ] {
                assert!(!text.contains(secret), "{secret} in {text}");
            }
        }
        // The public key the server's peer list must hold is still named.
        let handshake = handshake.expect("a handshake fault");
        assert!(handshake.contains(&own_key), "{handshake}");
    }
}

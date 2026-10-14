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
}

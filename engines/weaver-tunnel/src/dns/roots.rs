// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a naming trusts: the TLS roots its encrypted wires check servers
// against, the DNSSEC trust anchors and policy its validator is built from,
// and the clock signatures are checked by. A product build has one set, the
// built-in one, never written after it is built. Each naming's TLS
// sessions and validator are its own, built from these.
use super::exchange::Wires;
use std::sync::{Arc, LazyLock};
use tokio_rustls::rustls::RootCertStore;
use weaver_dnssec::{AnchorError, Policy, TrustAnchor, Validator};

pub(crate) struct TrustRoots {
    wires: Wires,
    anchors: Vec<TrustAnchor>,
    policy: Policy,
    clock: Clock,
}

// The clock signatures are checked against.
#[derive(Clone, Copy)]
pub(super) enum Clock {
    // Seconds since 1970 by the host clock, within the validator's
    // allowance for skew.
    Host,
    // A fixed time, for a test's own signed zones.
    #[cfg(test)]
    Fixed(u64),
}

// The web PKI roots, the IANA root keys, the default policy and the host
// clock: the only trust a product build has. Built at its first use: the
// first lookup of a host name through listed servers, on any wire, as that
// lookup takes the session's wires, or the first probe of a server.
static BUILT_IN: LazyLock<Arc<TrustRoots>> = LazyLock::new(|| {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Arc::new(
        TrustRoots::new(
            roots,
            TrustAnchor::iana_root(),
            Policy::default(),
            Clock::Host,
        )
        .expect("the IANA root anchors"),
    )
});

// The roots `roots` holds, or the built-in ones when it holds none. Only
// here are the built-in roots built, so nothing builds them before a lookup
// needs them.
pub(super) fn or_built_in(roots: &Option<Arc<TrustRoots>>) -> &TrustRoots {
    roots.as_deref().unwrap_or(&BUILT_IN)
}

impl TrustRoots {
    // The built-in trust, shared by every naming.
    #[cfg(test)]
    pub(crate) fn built_in() -> Arc<Self> {
        BUILT_IN.clone()
    }

    // Trusts `tls_roots` for the encrypted wires, and `anchors` under
    // `policy` at `clock`. The anchors are checked here, so a validator
    // built from them later cannot fail.
    pub(super) fn new(
        tls_roots: RootCertStore,
        anchors: Vec<TrustAnchor>,
        policy: Policy,
        clock: Clock,
    ) -> Result<Self, AnchorError> {
        Validator::new(anchors.clone(), policy)?;
        Ok(Self {
            wires: Wires::trusting(tls_roots),
            anchors,
            policy,
            clock,
        })
    }

    // The TLS settings every naming takes its own copy of.
    pub(super) fn wires(&self) -> &Wires {
        &self.wires
    }

    // A validator of its own, for one naming, with nothing yet proven.
    pub(super) fn validator(&self) -> Validator {
        Validator::new(self.anchors.clone(), self.policy)
            .expect("trust anchors checked when the roots were built")
    }

    // The time signatures are checked against now.
    pub(super) fn now(&self) -> u64 {
        match self.clock {
            Clock::Host => std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_secs()),
            #[cfg(test)]
            Clock::Fixed(now) => now,
        }
    }
}

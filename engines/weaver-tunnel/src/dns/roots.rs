// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a naming trusts: the TLS roots its encrypted wires check servers
// against, the DNSSEC trust anchors and policy its validator is built from,
// and the clock signatures are checked by. A product build has one set, the
// built-in one, never written after it is built: its TLS roots are the
// compiled-in list and the host's store, its trust anchors compiled in
// alone. Each naming's TLS
// sessions and validator are its own, built from these.
use super::exchange::Wires;
use std::sync::{Arc, OnceLock};
use tokio_rustls::rustls::{RootCertStore, pki_types::CertificateDer};
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

// The TLS roots compiled in and those the host's store holds, the IANA root
// keys, the default policy and the host clock: the only trust a product
// build has. Built once per process, at its first use, off the async
// runtime (`ready`): the first lookup of a host name through listed
// servers, on any wire, or the first probe of a server.
static BUILT_IN: OnceLock<Arc<TrustRoots>> = OnceLock::new();

fn built_in() -> &'static Arc<TrustRoots> {
    BUILT_IN.get_or_init(|| {
        Arc::new(
            TrustRoots::new(
                host_tls_roots(),
                TrustAnchor::iana_root(),
                Policy::default(),
                Clock::Host,
            )
            .expect("the IANA root anchors"),
        )
    })
}

// Set once `BUILT_IN` is built by `ready`.
static READY: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

// Builds the built-in roots, when `roots` stands for them, on one blocking
// thread: reading the host's store touches files or the OS keychain, which
// never runs on the async runtime. Each async way in to the roots awaits
// this first. The first callers all await the one build, and every later
// one pays one check.
pub(super) async fn ready(roots: &Option<Arc<TrustRoots>>) {
    if roots.is_some() {
        return;
    }
    let built = READY
        .get_or_try_init(|| async {
            tokio::task::spawn_blocking(|| {
                built_in();
            })
            .await
        })
        .await;
    match built {
        Ok(()) => {}
        // Only the compiled-in IANA anchors can fail the build, as the
        // host's store is read under `catch_unwind`.
        Err(failed) if failed.is_panic() => std::panic::resume_unwind(failed.into_panic()),
        // Cancelled: the runtime is shutting down, and drops the task that
        // awaits this before the blocking pool, so nothing goes on to read
        // the roots inline on the runtime.
        Err(_) => {}
    }
}

// The roots `roots` holds, or the built-in ones when it holds none, built
// by `ready` before any lookup or probe gets here. A test that takes them
// without `ready` builds them where it stands.
pub(super) fn or_built_in(roots: &Option<Arc<TrustRoots>>) -> &TrustRoots {
    roots.as_deref().unwrap_or_else(|| built_in())
}

// The TLS roots of a product build: the compiled-in web PKI list and every
// certificate the host's store yields, so an operator can add a root to the
// host's store. On every platform, when `SSL_CERT_FILE` or `SSL_CERT_DIR`
// is set, the files they name are that store, and the platform's own store
// is not read. Read once per process.
fn host_tls_roots() -> RootCertStore {
    let (certs, errors) = read_host_store(|| {
        let found = rustls_native_certs::load_native_certs();
        let errors = found.errors.iter().map(ToString::to_string).collect();
        (found.certs, errors)
    });
    tls_roots(certs, &errors)
}

// The certificates `load` reads from the host's store, and why it could
// not read them all. A read that panics reads nothing and says so, so the
// host's store never fails the roots' build.
pub(super) fn read_host_store(
    load: impl FnOnce() -> (Vec<CertificateDer<'static>>, Vec<String>) + std::panic::UnwindSafe,
) -> (Vec<CertificateDer<'static>>, Vec<String>) {
    std::panic::catch_unwind(load).unwrap_or_else(|panicked| {
        let why = panicked
            .downcast_ref::<&str>()
            .map(|why| (*why).to_owned())
            .or_else(|| panicked.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        (
            Vec::new(),
            vec![format!("reading the store panicked: {why}")],
        )
    })
}

// The compiled-in web PKI roots and `host`'s certificates. A certificate
// that does not parse as a root is skipped. `errors` say why the host's
// store could not be read, in whole or in part: what was read is still
// trusted, the compiled-in roots always, and DNS never fails for it.
pub(super) fn tls_roots(host: Vec<CertificateDer<'static>>, errors: &[String]) -> RootCertStore {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let (added, skipped) = roots.add_parsable_certificates(host);
    if !errors.is_empty() {
        tracing::warn!(
            errors = %errors.join("; "),
            "the host's certificate store could not be read in full; DNS over TLS and HTTPS \
             trust the built-in roots and what was read"
        );
    }
    tracing::debug!(added, skipped, "DNS TLS roots taken from the host's store");
    roots
}

impl TrustRoots {
    // The built-in trust, shared by every naming.
    #[cfg(test)]
    pub(crate) fn built_in() -> Arc<Self> {
        built_in().clone()
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

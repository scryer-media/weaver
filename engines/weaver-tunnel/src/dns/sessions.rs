// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What one naming keeps across its lookups: the TLS sessions it resumes, the
// connections it keeps open, and its DNSSEC validator.
use super::{
    exchange::{Kept, Wires},
    judge::validating::Dnssec,
    roots::{self, TrustRoots},
};
use std::sync::{Arc, OnceLock};

// The TLS sessions one naming resumes. A ticket from a server asked on one
// route is never offered to it on another, which would tie the two routes
// together in the clear ClientHello. Built on first use, kept with the
// naming's answers, and dropped with them when the hop is rebuilt. The
// encrypted connections its lookups leave open are kept beside them, apart
// in the same way, and close when they are dropped. The naming's DNSSEC
// validator lives beside them, for the same span. Both are built from the
// naming's trust roots, the built-in ones unless a test gives its own (None
// is the built-in ones, built at their first use).
pub struct DnsSessions {
    roots: Option<Arc<TrustRoots>>,
    wires: OnceLock<Wires>,
    kept: Kept,
    dnssec: Dnssec,
}

impl Default for DnsSessions {
    fn default() -> Self {
        Self::with_roots(None)
    }
}

impl DnsSessions {
    pub(crate) fn with_roots(roots: Option<Arc<TrustRoots>>) -> Self {
        Self {
            dnssec: Dnssec::new(roots.clone()),
            roots,
            wires: OnceLock::new(),
            kept: Kept::default(),
        }
    }
    // A naming's sessions on `wires` as they are, with `dnssec`, for a
    // test that sets its own TLS trust or validator.
    #[cfg(test)]
    pub(super) fn of(wires: Wires, dnssec: Dnssec) -> Self {
        Self {
            roots: None,
            wires: OnceLock::from(wires),
            kept: Kept::default(),
            dnssec,
        }
    }
    pub(super) fn wires(&self) -> &Wires {
        self.wires
            .get_or_init(|| roots::or_built_in(&self.roots).wires().apart())
    }
    pub(super) fn kept(&self) -> &Kept {
        &self.kept
    }
    pub(super) fn dnssec(&self) -> &Dnssec {
        &self.dnssec
    }
}

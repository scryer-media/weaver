// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Who answers a name a dial needs: the host's own resolver, the proxy the
// dial goes through, the servers a route lists, or no one. A route's naming
// policy picks one for each stage, and the stage's naming builds it for one
// lookup (`Naming::resolver` in `pipe`). Only `pipe` and its submodules can
// build the host's resolver. The others are visible to the whole crate, since
// `pipe` builds them; the listed servers' constructors are called only by
// `Naming::resolver` today. A WireGuard tunnel's own resolver is asked by
// `SessionHop::resolve` directly, not through this trait.
use crate::{
    pipe::DialError,
    resolution::record::{self, Asker, LookupFailure},
};
use std::{future::Future, io, net::IpAddr};

// What a resolver gives for a name: its addresses, or None when the proxy
// the dial goes through is handed the name to resolve itself.
pub(crate) type Answer = Result<Option<Vec<IpAddr>>, DialError>;

pub(crate) trait Resolver {
    // Fails a lookup before anyone is asked, for a resolver that refuses
    // every name. Every other resolver lets it go on.
    fn refuse(&self, _host: &str) -> Result<(), DialError> {
        Ok(())
    }

    // Who the lookup is asked of, for a dial through proxy `proxy`.
    fn asker(&self, proxy: u32) -> Asker;

    // What `host` resolves to.
    fn lookup(&self, host: &str) -> impl Future<Output = Answer> + Send;

    // Drops what the resolver holds for `host`, after none of the addresses
    // it gave could be reached. A resolver that holds nothing does nothing.
    fn forget(&self, _host: &str) {}
}

// The proxy the dial goes through is handed the name and resolves it.
// Weaver asks no one.
pub(crate) struct ExitResolver;

impl Resolver for ExitResolver {
    fn asker(&self, proxy: u32) -> Asker {
        Asker::proxy(proxy)
    }

    async fn lookup(&self, _host: &str) -> Answer {
        Ok(None)
    }
}

// Nothing may answer: every lookup fails, saying why, before anyone is
// asked.
pub(crate) struct Refusing<'a>(pub(crate) &'a str);

impl Resolver for Refusing<'_> {
    fn refuse(&self, host: &str) -> Result<(), DialError> {
        let said = format!("{host}: {}", self.0);
        record::failed(host, LookupFailure::Other, || said.clone());
        Err(DialError::Destination(io::Error::other(said)))
    }

    fn asker(&self, _proxy: u32) -> Asker {
        unreachable!("a refused naming asks no one")
    }

    async fn lookup(&self, _host: &str) -> Answer {
        unreachable!("a refused naming asks no one")
    }
}

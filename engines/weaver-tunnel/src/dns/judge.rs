// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a lookup asks one server, and what the replies mean, apart from how
// the queries travel. A judge says which queries it wants and takes each
// reply; one loop, `run`, carries them over the server's talk on any wire,
// for every trust setting.
use super::{
    Exchanged, FAMILIES, Found, SERVER_TIMEOUT, Trust,
    exchange::{Reply, Session, Talk},
};
use as_sent::AsSent;
use std::future::Future;
use validating::Dnssec;

pub(super) mod as_sent;
pub(super) mod validating;

// What to ask one server next, and what its replies come to. Each query has
// a key of the judge's own, one per query out at once, and a lane, one chain
// of queries such as one family's, which the wire uses to take turns.
pub(super) trait Judge {
    // Gives `asking` the queries wanted now: those due since the last reply,
    // or every first query when nothing is asked yet. A query the judge no
    // longer wants may be taken back while it waits to go out.
    fn want(&mut self, asking: &mut Asking<'_, '_, '_>) -> impl Future<Output = ()> + Send;

    // Takes the reply to the query given under `key`, or why it got none.
    fn supply(&mut self, key: usize, reply: Reply);

    // Whether every family has its outcome, so nothing more is asked.
    fn done(&self) -> bool;

    // The talk ended with `failed`, a server that could not be reached or
    // failed TLS: every family still under way ends with it.
    fn fail(&mut self, failed: Exchanged);
}

// The part of a talk a judge uses: queries given and taken back. Only the
// loop sends and reads.
pub(super) struct Asking<'t, 's, 'a>(&'t mut Talk<'s, 'a>);

impl Asking<'_, '_, '_> {
    // The id the next query given goes out with, as the talk's wire sends it.
    pub(super) fn id(&self) -> u16 {
        self.0.id()
    }

    // A query the lane cannot go on without.
    pub(super) fn send(&mut self, key: usize, lane: usize, packet: Vec<u8>) {
        self.0.send(key, lane, packet);
    }

    // A query asked ahead of its need.
    pub(super) fn ahead(&mut self, key: usize, lane: usize, packet: Vec<u8>) {
        self.0.ahead(key, lane, packet);
    }

    // Another lane wants the query given under `key`, and says whether it
    // waits on it.
    pub(super) fn wanted_by(&mut self, key: usize, lane: usize, awaited: bool) {
        self.0.wanted_by(key, lane, awaited);
    }

    // Takes back the query given under `key` if it has not gone out yet.
    // Whether it was taken back.
    pub(super) fn withdraw(&mut self, key: usize) -> bool {
        self.0.withdraw(key)
    }
}

// Asks the session's server what `judge` wants, on a talk of its own, and
// gives the judge each reply as it comes, until the judge is done or the
// talk has nothing given or out. `reached` is set once the server answers a
// dial. A talk that fails ends every family still under way with that
// failure. The talk's connections close when this returns or is dropped,
// but for one it leaves healthy to the naming that keeps it, when the
// session is a naming's own lookup.
pub(super) async fn run(session: &mut Session<'_>, reached: &mut bool, judge: &mut impl Judge) {
    let mut talk = session.talk(reached);
    loop {
        judge.want(&mut Asking(&mut talk)).await;
        if judge.done() {
            return;
        }
        match talk.next().await {
            Ok(Some((key, reply))) => judge.supply(key, reply),
            Ok(None) => return,
            Err(failed) => {
                judge.fail(failed);
                return;
            }
        }
    }
}

// What one server says of each of `host`'s families, A then AAAA, within
// the server's time limit: each family's outcome, or None for one the limit
// cut off. Both families are asked together on the one session, and each
// sends its next query as soon as the reply it waits on comes, so neither
// waits on the other's. A resolver may answer for one family only, so a
// family that fails or stays silent leaves the other's answer standing. A
// server that cannot be reached, or fails TLS, fails both at once, so a dead
// route is dialled once.
pub(super) async fn asked(
    session: &mut Session<'_>,
    dnssec: &Dnssec,
    host: &str,
    trust: Trust,
    reached: &mut bool,
) -> [Option<Result<Found, Exchanged>>; 2] {
    let name = host.trim_end_matches('.').to_ascii_lowercase();
    match trust {
        Trust::AsSent => {
            let mut walks = AsSent::new(&name);
            let run = run(session, reached, &mut walks);
            let _ = tokio::time::timeout(SERVER_TIMEOUT, run).await;
            walks.found()
        }
        // The validator asks for the name, its aliases and what proves
        // them, by the same session.
        Trust::Validate => {
            // A backslash would be read as an escape in the validator's name.
            if !host_name(&name) || name.contains('\\') {
                return FAMILIES.map(|_| Some(Err(Exchanged::Invalid)));
            }
            session.counting();
            let mut found = FAMILIES.map(|_| None);
            let checks =
                validating::validated_all(session, dnssec, &name, &FAMILIES, &mut found, reached);
            let _ = tokio::time::timeout(SERVER_TIMEOUT, checks).await;
            found
        }
    }
}

// A name a query may ask for: ASCII labels of 1 to 63 bytes, 253 bytes in
// all.
fn host_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 253
        && name
            .split('.')
            .all(|label| !label.is_empty() && label.len() <= 63 && label.is_ascii())
}

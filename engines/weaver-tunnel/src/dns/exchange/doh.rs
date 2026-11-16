// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNS over HTTPS (RFC 8484): HTTP/2 streams on one connection, or HTTP/1.1
// queries one at a time per connection, as the server's handshake says.
use super::{
    Asked, Exchange, Link, Next, Queue, REUSED_WAIT, Reply, SENDS, Wires, handshake, sent, turns,
};
use crate::dns::{Carrier, DnsServer, Exchanged, Wire};
pub(super) use https::Connection;
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::Poll;

mod https;

// A new connection to `server` over DNS over HTTPS, as `dot` makes one.
async fn doh(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    reached: impl FnOnce(),
) -> Result<Box<Connection>, Exchanged> {
    let tls = handshake(provider, wires, server, reached).await?;
    Ok(Box::new(Connection::new(tls).await?))
}

// One HTTP/1.1 query out, ending with its reply and the connection, when it
// stays open for the next; or with the failure of the dial for it, the
// query not sent.
type OneCall<'a> =
    Pin<Box<dyn Future<Output = Result<(Option<Box<Connection>>, Reply), Exchanged>> + Send + 'a>>;

// A new connection being made, ending with it or the failure, and whether
// the server answered the dial.
type Dialling<'a> =
    Pin<Box<dyn Future<Output = (Result<Box<Connection>, Exchanged>, bool)> + Send + 'a>>;

// DNS over HTTPS. The first connection's handshake says which HTTP the
// server speaks.
#[derive(Default)]
pub(super) struct Doh<'a> {
    // HTTP/2: the one connection, and its streams out by key. `spent` once a
    // stream found the connection gone: it takes no new stream, and goes
    // once none is out. `wary` once a stream was dropped or refused: from
    // then on each lane has at most one stream it waits on out, and one
    // asked ahead, so a server that takes fewer streams than were sent
    // answers each lane in turn, and a stream that stays silent holds up
    // only what waits on it. `old` is a spent connection whose streams still
    // out run on beside `two`, and beside `dialling`, the new connection
    // being made, kept across calls.
    // `kept` while `two` is one a naming kept and nothing has answered on it
    // in this talk: found gone, silent for `REUSED_WAIT`, or holding more
    // than one exchange may read, its streams go again on a new connection,
    // as if never sent, and it is dropped unread. Over HTTP/1.1, `kept`
    // until the connection a naming kept is taken for a query.
    two: Option<Box<Connection>>,
    old: Option<Box<Connection>>,
    dialling: Option<Dialling<'a>>,
    spent: bool,
    wary: bool,
    kept: bool,
    out: BTreeMap<usize, Asked>,
    // HTTP/1.1, once a connection said so. Each lane asks in turn, one
    // query out at a time, on a connection that is free, or on a new one
    // when none is: so a query that stays silent holds up only its own
    // lane, and the talk opens at most one connection per lane. Each free
    // connection says whether it has answered before; a kept one has. Until
    // the server has answered the talk, as on a kept connection, only one
    // query is out. `undialled` once a new connection could not be made:
    // from then on a query waits for one of the talk's connections to come
    // free, and a new one is dialled only when the talk has none out.
    one: bool,
    idle: Vec<(Box<Connection>, bool)>,
    calls: Vec<(Asked, OneCall<'a>)>,
    undialled: bool,
}

impl Doh<'_> {
    // A talk on the connection a naming kept, when it kept one, over the
    // HTTP it speaks; or on a new one dialled for the first query.
    pub(super) fn on(kept: Option<Link>) -> Self {
        match kept {
            Some(Link::Doh(connection)) if connection.multiplexes() => Self {
                two: Some(connection),
                kept: true,
                ..Self::default()
            },
            Some(Link::Doh(connection)) => Self {
                one: true,
                idle: vec![(connection, true)],
                kept: true,
                ..Self::default()
            },
            _ => Self::default(),
        }
    }
}

impl<'a> Exchange<'a> for Doh<'a> {
    // DNS over HTTPS sends id 0, so a cache between can serve it (RFC 8484
    // §4.1).
    fn id() -> u16 {
        0
    }

    // Over the HTTP the first connection's handshake picks.
    async fn next(&mut self, queue: &mut Queue<'_, 'a>) -> Next {
        loop {
            if let Some(ended) = queue.ready.pop_front() {
                return Ok(Some(ended));
            }
            if self.one {
                return self.http1(queue).await;
            }
            if self.two.is_some() || self.old.is_some() {
                if let Some(ended) = self.http2(queue).await {
                    return Ok(ended);
                }
                if self.two.is_some() || queue.waiting.is_empty() {
                    continue;
                }
            }
            if queue.waiting.is_empty() {
                return Ok(None);
            }
            // The new connection is made while the old one's streams still
            // out are read: a reply that comes on it meanwhile is handed
            // back at once, and the dial goes on at the next call.
            let (provider, wires, server) = (
                queue.session.provider,
                queue.session.wires,
                queue.session.server,
            );
            let Self { dialling, old, .. } = &mut *self;
            let dialling = dialling.get_or_insert_with(|| {
                Box::pin(async move {
                    let mut answered = false;
                    let link = doh(provider, wires, server, || answered = true).await;
                    (link, answered)
                })
            });
            let came = match old.as_mut().filter(|old| old.carrying()) {
                Some(old) => tokio::select! {
                    dialled = dialling.as_mut() => Ok(dialled),
                    ended = old.next() => Err(ended),
                },
                None => Ok(dialling.as_mut().await),
            };
            let (link, answered) = match came {
                Ok(dialled) => dialled,
                Err(ended) => {
                    if let Some((key, reply)) = ended
                        && let Some(ended) = self.landed(queue, key, reply, false)
                    {
                        return Ok(ended);
                    }
                    continue;
                }
            };
            self.dialling = None;
            let first = !*queue.reached;
            *queue.reached |= answered;
            match queue.dialled(first, link)? {
                Some(connection) if connection.multiplexes() => {
                    self.two = Some(connection);
                    self.spent = false;
                    self.kept = false;
                }
                Some(connection) => {
                    self.one = true;
                    self.idle.push((connection, false));
                    // Streams still out on a spent HTTP/2 connection go
                    // again over HTTP/1.1, as a stream that found its
                    // connection gone goes again on a new one.
                    self.old = None;
                    let mut again = VecDeque::new();
                    for asked in std::mem::take(&mut self.out).into_values() {
                        if asked.sends < SENDS {
                            again.push_back(asked);
                        } else {
                            queue.ready.push_back((asked.key, Err(Exchanged::Closed)));
                        }
                    }
                    again.append(&mut queue.waiting);
                    queue.waiting = again;
                }
                None => {}
            }
        }
    }

    // HTTP/2: the one connection, when it still runs, the server has not
    // sent it away, and no stream is out on it; never a spent connection
    // or one of a wary talk, nor while another is dialled or drains.
    // Streams are apart, and none is out, so nothing later lookups read on
    // it is this one's. HTTP/1.1: a free connection. One is free only when
    // the response to its last query was read whole and the server keeps it
    // alive, and HTTP/1.1 answers one request at a time on a connection, so
    // it owes nothing.
    fn leave(&mut self) -> Option<Link> {
        if self.one {
            return self.idle.pop().map(|(connection, _)| Link::Doh(connection));
        }
        let healthy = self.old.is_none()
            && self.dialling.is_none()
            && self.out.is_empty()
            && !self.spent
            && !self.wary
            && self
                .two
                .as_ref()
                .is_some_and(|two| two.open() && !two.carrying() && !two.overran());
        if !healthy {
            return None;
        }
        self.two.take().map(Link::Doh)
    }
}

impl<'a> Doh<'a> {
    // HTTP/2: each query goes out on a stream of its own as it is given,
    // beside those still out. A stream the server refused is sent again on
    // the connection; one that found the connection gone, on a new one,
    // opened as soon as a query waits while the old one's streams still out
    // run on beside it: at most two connections at once. Either is sent at
    // most `SENDS` times in all, so a server that refuses or drops every
    // stream ends the query. None when a new connection is wanted or the
    // talk is to look again, and Some(None) once nothing is given or out.
    async fn http2(&mut self, queue: &mut Queue<'_, 'a>) -> Option<Option<(usize, Reply)>> {
        if self
            .old
            .as_ref()
            .is_some_and(|connection| !connection.carrying())
        {
            self.old = None;
        }
        // A spent connection goes once its streams have ended, or drains
        // beside a new one once a query waits for it.
        if let Some(connection) = self.two.as_ref()
            && (self.spent || !connection.open())
        {
            if !connection.carrying() {
                self.two = None;
                self.kept = false;
            } else if self.old.is_none() && !queue.waiting.is_empty() {
                self.old = self.two.take();
                self.kept = false;
            }
        }
        // A kept connection reads what the server sent while it was kept
        // before its first stream: one that read too much, or was sent away
        // meanwhile, is dropped as one found gone.
        if self.kept && self.out.is_empty() && self.two.as_mut().is_some_and(|two| !two.settle()) {
            self.gone(queue);
        }
        if self.two.is_none() && !queue.waiting.is_empty() {
            return None;
        }
        let server = queue.session.server;
        let Wire::Https { path } = &server.wire else {
            unreachable!("an HTTPS link is made only for an HTTPS server")
        };
        if let Some(connection) = self.two.as_mut()
            && connection.open()
            && !self.spent
        {
            let given = if self.wary {
                let out = &self.out;
                let lane_out = |lane| {
                    out.values()
                        .filter(|asked| asked.lane == lane)
                        .map(|asked| asked.awaited)
                        .reduce(|one, other| one || other)
                };
                turns(&mut queue.waiting, lane_out, true)
            } else {
                queue.waiting.drain(..).collect()
            };
            for mut asked in given {
                match connection.start(server, path, asked.key, &asked.packet) {
                    Ok(()) => {
                        sent(queue.session.counts, 1);
                        asked.sends += 1;
                        self.out.insert(asked.key, asked);
                    }
                    Err(failed) => queue.ready.push_back((asked.key, Err(failed))),
                }
            }
        }
        let carrying = |connection: &Option<Box<Connection>>| {
            connection
                .as_ref()
                .is_some_and(|connection| connection.carrying())
        };
        let (key, reply, on_two) = match (carrying(&self.two), carrying(&self.old)) {
            (false, false) => {
                return match queue.ready.pop_front() {
                    Some(ended) => Some(Some(ended)),
                    None if queue.waiting.is_empty() => Some(None),
                    None => None,
                };
            }
            (true, false) => {
                let two = self.two.as_mut()?;
                // A kept connection that has not answered yet gets a short
                // wait of its own: silent past it, it is taken as gone.
                let ended = if self.kept {
                    match tokio::time::timeout(REUSED_WAIT, two.next()).await {
                        Ok(ended) => ended,
                        Err(_) => {
                            self.gone(queue);
                            return None;
                        }
                    }
                } else {
                    two.next().await
                };
                let (key, reply) = ended?;
                (key, reply, true)
            }
            (false, true) => {
                let (key, reply) = self.old.as_mut()?.next().await?;
                (key, reply, false)
            }
            (true, true) => {
                let (two, old) = (self.two.as_mut()?, self.old.as_mut()?);
                tokio::select! {
                    ended = two.next() => {
                        let (key, reply) = ended?;
                        (key, reply, true)
                    }
                    ended = old.next() => {
                        let (key, reply) = ended?;
                        (key, reply, false)
                    }
                }
            }
        };
        self.landed(queue, key, reply, on_two)
    }

    // The stream under `key` ended with `reply`, on `two` when `on_two`, on
    // `old` otherwise. None when its query goes out again, or is not out.
    fn landed(
        &mut self,
        queue: &mut Queue<'_, 'a>,
        key: usize,
        reply: Reply,
        on_two: bool,
    ) -> Option<Option<(usize, Reply)>> {
        if on_two && self.kept {
            // A kept connection found gone before anything answered on it:
            // every stream out was on it, and each goes again on a new
            // connection, its send not counted. The talk goes on as one
            // that dialled nothing yet.
            if matches!(reply, Err(Exchanged::Closed)) {
                self.gone(queue);
                return None;
            }
            self.kept = false;
            *queue.reached = true;
        }
        if on_two {
            // A stream that found the connection gone may end before this
            // side sees the connection end.
            self.spent |= self.two.as_ref().is_some_and(|two| !two.open());
        }
        let asked = self.out.remove(&key)?;
        match reply {
            Err(Exchanged::Refused) if asked.sends < SENDS => {
                self.wary = true;
                queue.waiting.push_front(asked);
                None
            }
            Err(Exchanged::Closed) if asked.sends < SENDS => {
                self.wary = true;
                self.spent |= on_two;
                queue.waiting.push_front(asked);
                None
            }
            Err(Exchanged::Closed) => {
                self.spent |= on_two;
                Some(Some((key, Err(Exchanged::Closed))))
            }
            reply => Some(Some((key, reply))),
        }
    }

    // The kept HTTP/2 connection, found gone before anything answered on it,
    // is dropped unread. Every stream out was on it, and each goes again on
    // a new connection, its send not counted. The talk goes on as one that
    // dialled nothing yet.
    fn gone(&mut self, queue: &mut Queue<'_, 'a>) {
        self.two = None;
        self.kept = false;
        for mut asked in std::mem::take(&mut self.out).into_values().rev() {
            asked.sends -= 1;
            queue.waiting.push_front(asked);
        }
    }

    // HTTP/1.1: each lane's next query goes out as soon as the lane has
    // none out, on a free connection or a new one. A query asked ahead goes
    // only once a lane waits on it: out ahead, a silent one would hold its
    // lane's next query back, and a second query out for the lane would
    // take a second connection. A query whose new connection could not be
    // made waits for one the talk has; it fails with the dial only when the
    // talk has no other.
    async fn http1(&mut self, queue: &mut Queue<'_, 'a>) -> Next {
        let (provider, wires, server, counts) = (
            queue.session.provider,
            queue.session.wires,
            queue.session.server,
            queue.session.counts,
        );
        let https = self;
        loop {
            let calls = &https.calls;
            let lane_out = |lane| {
                calls
                    .iter()
                    .any(|(out, _)| out.lane == lane)
                    .then_some(true)
            };
            let given = turns(&mut queue.waiting, lane_out, false);
            let mut held = Vec::new();
            for asked in given {
                let taken = https.idle.pop();
                // No new connection while one could not be made, or the
                // server has not answered the talk, and another is still
                // out: the query waits for that one to come free.
                if taken.is_none()
                    && (https.undialled || !*queue.reached)
                    && !https.calls.is_empty()
                {
                    held.push(asked);
                    continue;
                }
                let kept = taken.is_some() && std::mem::take(&mut https.kept);
                let call: OneCall<'a> = Box::pin(one_call(
                    provider,
                    wires,
                    server,
                    (counts, kept),
                    taken,
                    asked.packet.clone(),
                ));
                https.calls.push((asked, call));
            }
            for asked in held.into_iter().rev() {
                queue.waiting.push_front(asked);
            }
            if https.calls.is_empty() {
                return Ok(None);
            }
            let (asked, ended) = poll_fn(|cx| {
                for at in 0..https.calls.len() {
                    if let Poll::Ready(ended) = https.calls[at].1.as_mut().poll(cx) {
                        let (asked, _) = https.calls.swap_remove(at);
                        return Poll::Ready((asked, ended));
                    }
                }
                Poll::Pending
            })
            .await;
            match ended {
                Ok((connection, reply)) => {
                    *queue.reached = true;
                    if let Some(connection) = connection {
                        https.idle.push((connection, true));
                    }
                    return Ok(Some((asked.key, reply)));
                }
                // The talk's first dial, after the kept connection it began
                // on was found gone, failed: as a talk's first dial does, it
                // ends the talk.
                Err(failed) if !*queue.reached => return Err(failed),
                // A dial that failed while another connection is out or
                // free: the query waits for one of those.
                Err(_) if !https.calls.is_empty() || !https.idle.is_empty() => {
                    https.undialled = true;
                    queue.waiting.push_front(asked);
                }
                Err(failed) => return Ok(Some((asked.key, Err(failed)))),
            }
        }
    }
}

// One query over HTTP/1.1, on the free connection `taken` when there is one,
// else on a new one. A connection that answered before and is found closed
// before any byte of the reply is replaced once. The connection a naming
// kept, when `kept`, is replaced once on any failure of its first exchange,
// whatever it is (found closed, a bad status, a reply that does not read),
// or when it stays silent for `REUSED_WAIT`: what it said while kept (a 408
// written before the server closed it) is not this query's answer, and it
// is dropped. The query then goes once more on a new connection, whose
// failure is the call's. The connection comes back with the reply while it
// stays open. A dial that fails ends the call with its failure, the query
// not sent on it. Each write of the query counts as one sent, when
// `counts`.
async fn one_call(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    (counts, mut kept): (bool, bool),
    mut taken: Option<(Box<Connection>, bool)>,
    packet: Vec<u8>,
) -> Result<(Option<Box<Connection>>, Reply), Exchanged> {
    let Wire::Https { path } = &server.wire else {
        unreachable!("an HTTPS query goes only to an HTTPS server")
    };
    loop {
        let (mut connection, used) = match taken.take() {
            Some(taken) => taken,
            None => (doh(provider, wires, server, || {}).await?, false),
        };
        sent(counts, 1);
        let exchanged = connection.exchange(server, path, &packet);
        let reused = std::mem::take(&mut kept);
        let outcome = if reused {
            tokio::time::timeout(REUSED_WAIT, exchanged)
                .await
                .unwrap_or(Err(Exchanged::Closed))
        } else {
            exchanged.await
        };
        match outcome {
            Ok((reply, open)) => return Ok((open.then_some(connection), Ok(reply))),
            Err(_) if reused => {}
            Err(Exchanged::Closed) if used => {}
            Err(failed) => return Ok((None, Err(failed))),
        }
    }
}

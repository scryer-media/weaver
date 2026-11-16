// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Plain DNS over TCP: a stream of its own for each query.
use super::{Exchange, NEXT_ID, Next, Queue, Reply, Session, framed, io_reason, sent};
use crate::dns::Exchanged;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

// Plain DNS streams dialled at once for one server. A lookup's DNSSEC check
// can need some forty queries, each its own TCP stream through the route;
// eight in flight keeps one lookup well inside the TCP children a resolver
// serves at once (dnsmasq allows 20, Pi-hole's build 60) while still
// carrying both families' widest round together.
const PLAIN_DIALS: usize = 8;

type PlainCall<'a> = Pin<Box<dyn Future<Output = Reply> + Send + 'a>>;

// Plain DNS: a stream per query, each with the caller's key. `dialled` is
// set by the first dial the server answers.
pub(super) struct PlainTcp<'a> {
    pub(super) calls: Vec<(usize, PlainCall<'a>)>,
    pub(super) dialled: Arc<AtomicBool>,
}

impl<'a> Exchange<'a> for PlainTcp<'a> {
    fn id() -> u16 {
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    async fn ask(session: &Session<'a>, packet: &[u8], reached: &mut bool) -> Reply {
        let mut stream = session
            .provider
            .open(session.server)
            .await
            .map_err(Exchanged::Unreached)?;
        *reached = true;
        session.sent(1);
        framed(&mut stream, packet, plain_failed).await
    }

    // A stream dialled for each query, at most `PLAIN_DIALS` at once. Until
    // the server has answered a dial only one is dialled, so a route that
    // reaches nothing is dialled once and fails the server. A dial that
    // fails after one was answered fails only its own query.
    async fn next(&mut self, queue: &mut Queue<'_, 'a>) -> Next {
        let Self { calls, dialled } = self;
        let (provider, server, counts) = (
            queue.session.provider,
            queue.session.server,
            queue.session.counts,
        );
        loop {
            while calls.len() < PLAIN_DIALS && (calls.is_empty() || *queue.reached) {
                let Some(asked) = queue.waiting.pop_front() else {
                    break;
                };
                let dialled = dialled.clone();
                let call: PlainCall<'a> = Box::pin(async move {
                    let mut stream = provider.open(server).await.map_err(Exchanged::Unreached)?;
                    dialled.store(true, Ordering::Relaxed);
                    sent(counts, 1);
                    framed(&mut stream, &asked.packet, plain_failed).await
                });
                calls.push((asked.key, call));
            }
            if calls.is_empty() {
                return Ok(None);
            }
            let before = *queue.reached;
            let ended = poll_fn(|cx| {
                for at in 0..calls.len() {
                    if let Poll::Ready(reply) = calls[at].1.as_mut().poll(cx) {
                        let (key, _) = calls.swap_remove(at);
                        return Poll::Ready(Some((key, reply)));
                    }
                }
                // The first dial was answered: start the rest.
                if !before && dialled.load(Ordering::Relaxed) {
                    return Poll::Ready(None);
                }
                Poll::Pending
            })
            .await;
            *queue.reached |= dialled.load(Ordering::Relaxed);
            match ended {
                None => continue,
                Some((_, Err(failed @ Exchanged::Unreached(_)))) if !*queue.reached => {
                    return Err(failed);
                }
                ended => return Ok(ended),
            }
        }
    }
}

fn plain_failed(error: std::io::Error) -> Exchanged {
    Exchanged::Io(io_reason(error.kind()))
}

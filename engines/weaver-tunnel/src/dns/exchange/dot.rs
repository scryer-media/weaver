// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// DNS over TLS (RFC 7858): one connection kept for a lookup's queries.
use super::{Asked, Exchange, NEXT_ID, Next, Queue, SENDS, Tls, Wires, handshake, reply};
use crate::dns::{Carrier, DnsServer, Exchanged};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use tokio::io::AsyncWriteExt;

// A new connection to `server` over DNS over TLS. `reached` is called once
// the server answers the dial, before the handshake.
async fn dot(
    provider: &dyn Carrier,
    wires: &Wires,
    server: &DnsServer,
    reached: impl FnOnce(),
) -> Result<Box<Tls>, Exchanged> {
    Ok(Box::new(handshake(provider, wires, server, reached).await?))
}

// DNS over TLS: one connection, the queries written on it by id, and the
// replies read in whatever order they come. `answered` counts the replies
// the connection has given. `wary` once a connection that answered closed
// with queries out: from then on one query is out at a time.
pub(super) struct Dot {
    pub(super) stream: Option<Box<Tls>>,
    pub(super) answered: usize,
    pub(super) out: BTreeMap<[u8; 2], Asked>,
    pub(super) wary: bool,
}

impl<'a> Exchange<'a> for Dot {
    fn id() -> u16 {
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    }

    // Each query written on the connection as it is given, and the replies
    // read one at a time, each matched to its query by id and question. A
    // query waits while another with its id is out. A reply with no query's
    // id is passed over, as plain DNS and HTTPS never see it; one with a
    // query's id but another question fails that query, as it would there.
    // A connection that closes with queries out after it answered something
    // is replaced for them, so a talk opens at most one connection per
    // reply, and one more. From then on the talk is wary: each connection
    // has one query out at a time, and only one a lane waits on, as over
    // HTTP/1.1, so on a server that closes after every reply a query goes
    // out at most twice. Each query is written at most `SENDS` times; one
    // the connection went with that often fails as closed.
    async fn next(&mut self, queue: &mut Queue<'_, 'a>) -> Next {
        loop {
            if let Some(ended) = queue.ready.pop_front() {
                return Ok(Some(ended));
            }
            // Wary, a query asked ahead goes only once a lane waits on it, so
            // `next` can end with it still given.
            let wary = self.wary;
            if self.out.is_empty() && !queue.waiting.iter().any(|asked| asked.awaited || !wary) {
                return Ok(None);
            }
            let Some(connection) = self.stream.as_mut() else {
                // A new connection for the queries waiting.
                let first = !*queue.reached;
                let (provider, wires, server) = (
                    queue.session.provider,
                    queue.session.wires,
                    queue.session.server,
                );
                let reached = &mut *queue.reached;
                let link = dot(provider, wires, server, || *reached = true).await;
                if let Some(connection) = queue.dialled(first, link)? {
                    self.stream = Some(connection);
                    self.answered = 0;
                }
                continue;
            };
            let mut given = std::mem::take(&mut queue.waiting);
            if self.wary {
                // One query out at a time, and only one a lane waits on: out
                // ahead, a silent one would hold back the query a lane waits
                // on.
                let next = given
                    .iter()
                    .position(|asked| asked.awaited)
                    .filter(|_| self.out.is_empty())
                    .and_then(|at| given.remove(at));
                queue.waiting = given;
                given = next.into_iter().collect();
            }
            let mut framed = Vec::new();
            let mut sending = 0;
            for mut asked in given {
                let id = asked.packet.get(..2).map_or([0; 2], |id| [id[0], id[1]]);
                if self.out.contains_key(&id) {
                    queue.waiting.push_back(asked);
                    continue;
                }
                framed.extend_from_slice(&(asked.packet.len() as u16).to_be_bytes());
                framed.extend_from_slice(&asked.packet);
                sending += 1;
                asked.sends += 1;
                self.out.insert(id, asked);
            }
            queue.session.sent(sending);
            let wrote = framed.is_empty()
                || (connection.write_all(&framed).await.is_ok()
                    && connection.flush().await.is_ok());
            let read = if wrote {
                reply(connection.as_mut()).await
            } else {
                Err(Exchanged::Closed)
            };
            match read {
                Ok(bytes) => {
                    let Some(asked) = self.out.remove(&[bytes[0], bytes[1]]) else {
                        continue;
                    };
                    self.answered += 1;
                    let reply = if asks(&asked.packet, &bytes) {
                        Ok(bytes)
                    } else {
                        Err(Exchanged::Invalid)
                    };
                    return Ok(Some((asked.key, reply)));
                }
                Err(Exchanged::Closed) if self.answered > 0 => {
                    self.stream = None;
                    self.wary |= !self.out.is_empty();
                    for asked in std::mem::take(&mut self.out).into_values() {
                        if asked.sends < SENDS {
                            queue.waiting.push_back(asked);
                        } else {
                            queue.ready.push_back((asked.key, Err(Exchanged::Closed)));
                        }
                    }
                }
                Err(failed) => {
                    self.stream = None;
                    for asked in std::mem::take(&mut self.out).into_values() {
                        queue.ready.push_back((asked.key, Err(failed.copy())));
                    }
                }
            }
        }
    }
}

// Whether `reply` asks the question `query` asked: the same question count,
// the same name in any case, as a reply's name is read, and the same type
// and class. With several queries out on one connection, an id alone could
// match a reply to the wrong one.
fn asks(query: &[u8], reply: &[u8]) -> bool {
    let mut end = 12;
    while let Some(&label) = query.get(end) {
        if label == 0 {
            break;
        }
        end += 1 + usize::from(label);
    }
    // The root label.
    end += 1;
    end + 4 <= query.len()
        && query.get(4..6) == reply.get(4..6)
        && reply
            .get(12..end)
            .is_some_and(|name| name.eq_ignore_ascii_case(&query[12..end]))
        && query.get(end..end + 4) == reply.get(end..end + 4)
}

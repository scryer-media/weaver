// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! `sync/maindata` answered by response id, as qBittorrent's
//! `SyncController` answers it: the whole state when the client's id is not
//! the one the facade can send changes from, else what changed since the
//! state that id stands for.
//!
//! Each client keeps its own ids and state. A client is a login session,
//! else the API key it sends, as qBittorrent keeps a controller per web
//! session and a session per API key.
//!
//! A client's state is kept as of the last response, with the snapshot
//! revision it stands at, and changed in place: a poll reads only the rows
//! that moved or went since that revision. What changed since the id the
//! client holds is kept beside it, as qBittorrent keeps its sync buffer:
//! each torrent touched since, with the row the client holds for it and the
//! values last built, so a poll answers from those alone and never walks
//! every torrent.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};

#[cfg(test)]
pub(super) mod old;

// The clients whose state is kept; the one longest unheard from goes first,
// and its next request is answered whole.
const CLIENTS: usize = 16;
// Ids cycle from 1 to this (qBittorrent `maindataAction`).
const ID_CYCLE: u64 = 1_000_000;
// The fields of a torrent's row that move with the clock alone.
pub(super) const CLOCK_KEYS: usize = 7;

// Who a `sync/maindata` request is from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum SyncClient {
    // A login session, by its cookie.
    Session(String),
    // An API key sent with the request, by its hash.
    Key([u8; 32]),
    // A request with neither.
    Anonymous,
}

// A torrent as this poll reads it: its row is built only when `fingerprint`,
// which leaves the clock out, differs from the one the client was sent.
// `closed` marks the fields the row leaves out because they have no reading
// (a limit that lets nothing through): a client keeps a field it is not sent,
// so when that set moves, the client is sent everything again.
pub(super) struct Row<'a, V> {
    pub(super) hash: String,
    pub(super) fingerprint: u64,
    pub(super) closed: u8,
    // Where it sits in the list, whose order a response keeps.
    pub(super) place: (i64, u64),
    pub(super) view: &'a V,
}

// What `sync/maindata` reports this poll beside the torrents.
pub(super) struct Others {
    pub(super) categories: Vec<(String, Map<String, Value>)>,
    pub(super) tags: Vec<String>,
    pub(super) server_state: Map<String, Value>,
    // The server state's fields left out for having no reading, as a row's
    // `closed` marks its own.
    pub(super) server_closed: u8,
}

// How a torrent's row is built: every field that moves only with the view,
// and the fields that move with the clock alone.
pub(super) struct Fields<Steady, Clock> {
    pub(super) steady: Steady,
    pub(super) clock: Clock,
}

// A torrent's row as the client holds it, as hashes: its fields by their
// names, shared while the row is unchanged, and no JSON.
#[derive(Clone)]
struct KeptRow {
    fingerprint: u64,
    closed: u8,
    place: (i64, u64),
    steady: Arc<[(&'static str, u64)]>,
    clock: [u64; CLOCK_KEYS],
}

// A torrent touched since the id the client holds: the row it held then,
// none while it had none, and the values its row was last built with since.
struct Touched {
    before: Option<KeptRow>,
    steady: Option<Vec<(&'static str, Value)>>,
    clock: Option<[(&'static str, Value); CLOCK_KEYS]>,
}

impl KeptRow {
    fn same(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
            && self.closed == other.closed
            && self.steady == other.steady
            && self.clock == other.clock
    }
}

// Everything but the torrents, as hashes.
#[derive(Clone, Default)]
struct Rest {
    categories: HashMap<String, Keys>,
    tags: Vec<String>,
    server_state: Keys,
    server_closed: u8,
}

type Keys = HashMap<String, u64>;

// The state the last response left the client with.
struct State {
    // That response's id.
    id: u64,
    // The snapshot revision its torrents stand at.
    revision: u64,
    // The caller's scope, which shapes the rows.
    scope: u8,
    torrents: HashMap<Arc<str>, KeptRow>,
    rest: Rest,
    // What changed since the id the client holds.
    touched: HashMap<Arc<str>, Touched>,
    accepted_rest: Rest,
}

impl State {
    // The row the client held for `hash` at the id it holds, taken out.
    fn take_accepted(&mut self, hash: &str) -> Option<KeptRow> {
        let now = self.torrents.remove(hash);
        match self.touched.remove(hash) {
            Some(touched) => touched.before,
            None => now,
        }
    }

    // The rows the client held at the id it holds that are left, each as
    // gone.
    fn accepted_gone(&mut self) -> Vec<(Arc<str>, Touched)> {
        let touched = std::mem::take(&mut self.touched);
        let mut torrents = std::mem::take(&mut self.torrents);
        let gone = |before| Touched {
            before: Some(before),
            steady: None,
            clock: None,
        };
        let mut held: Vec<(Arc<str>, Touched)> = touched
            .into_iter()
            .filter_map(|(key, touched)| {
                torrents.remove(&key);
                touched.before.map(|before| (key, gone(before)))
            })
            .collect();
        held.extend(torrents.into_iter().map(|(key, row)| (key, gone(row))));
        held
    }
}

// One client's ids and state. A client's requests are answered one at a
// time, under its lock.
#[derive(Default)]
pub(super) struct ClientSync {
    // The id of the last response begun; 0 before the first.
    last_sent: u64,
    // The id the client has shown it holds.
    accepted: Option<u64>,
    state: Option<State>,
}

struct Entry {
    sync: Arc<Mutex<ClientSync>>,
    heard: u64,
}

#[derive(Default)]
pub(super) struct SyncTable {
    clients: HashMap<SyncClient, Entry>,
    heard: u64,
}

// Where a response starts: its id, and the revision of the state the client
// holds, if the response can be the changes from it.
pub(super) struct Start {
    id: u64,
    scope: u8,
    since: Option<u64>,
}

impl Start {
    #[cfg(test)]
    pub(super) fn id(&self) -> u64 {
        self.id
    }

    // The revision the rows that moved are read since; none for a full
    // update, which reads every row.
    pub(super) fn since(&self) -> Option<u64> {
        self.since
    }
}

impl SyncTable {
    #[cfg(test)]
    pub(super) fn holds(&self, client: &SyncClient) -> bool {
        self.clients.contains_key(client)
    }

    pub(super) fn forget(&mut self, client: &SyncClient) {
        self.clients.remove(client);
    }

    // The client's ids and state, made for a client not heard from before.
    pub(super) fn client(&mut self, client: &SyncClient) -> Arc<Mutex<ClientSync>> {
        if !self.clients.contains_key(client)
            && self.clients.len() >= CLIENTS
            && let Some(oldest) = self
                .clients
                .iter()
                .min_by_key(|(_, entry)| entry.heard)
                .map(|(key, _)| key.clone())
        {
            self.clients.remove(&oldest);
        }
        self.heard += 1;
        let entry = self.clients.entry(client.clone()).or_insert_with(|| Entry {
            sync: Arc::default(),
            heard: 0,
        });
        entry.heard = self.heard;
        Arc::clone(&entry.sync)
    }
}

impl ClientSync {
    // The bookkeeping before a response to `rid` from a caller of `scope`.
    pub(super) fn begin(&mut self, rid: u64, scope: u8) -> Start {
        // qBittorrent `maindataAction`: the client holds the last response
        // once it sends its id back, and changes can be sent while the id it
        // sends is the one it holds.
        if rid > 0
            && rid == self.last_sent
            && let Some(state) = &mut self.state
            && state.id == rid
            && self.accepted != Some(rid)
        {
            state.touched.clear();
            state.accepted_rest = state.rest.clone();
            self.accepted = Some(rid);
        }
        let since = self
            .state
            .as_ref()
            .filter(|state| rid > 0 && self.accepted == Some(rid) && state.scope == scope)
            .map(|state| state.revision);
        let id = self.last_sent % ID_CYCLE + 1;
        self.last_sent = id;
        Start { id, scope, since }
    }

    // The changes response that `start` begins, from the torrents that
    // moved or went since `start.since()`, read at `revision`. When the
    // changes cannot be sent (a row's or the server state's left-out fields
    // moved, or `start` asked for none), `others` comes back untouched, for
    // `whole` to answer with every torrent.
    pub(super) fn changes<V, Steady, Clock>(
        &mut self,
        start: &Start,
        revision: u64,
        moved: Vec<Row<'_, V>>,
        removed: Vec<String>,
        others: Others,
        fields: &Fields<Steady, Clock>,
    ) -> Result<Value, Box<Others>>
    where
        Steady: Fn(&V) -> Vec<(&'static str, Value)>,
        Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
    {
        if start.since.is_none() {
            return Err(Box::new(others));
        }
        let mut body = self.changed(start, revision, moved, removed, others, fields)?;
        body.insert("rid".to_owned(), Value::from(start.id));
        Ok(Value::Object(body))
    }

    // The full update that `start` begins, from every torrent, read at
    // `revision`.
    pub(super) fn whole<V, Steady, Clock>(
        &mut self,
        start: &Start,
        revision: u64,
        rows: Vec<Row<'_, V>>,
        others: Others,
        fields: &Fields<Steady, Clock>,
    ) -> Value
    where
        Steady: Fn(&V) -> Vec<(&'static str, Value)>,
        Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
    {
        let mut body = self.full(start, revision, rows, others, fields);
        body.insert("rid".to_owned(), Value::from(start.id));
        Value::Object(body)
    }

    // A full update: every key that holds anything (qBittorrent
    // `generateMaindataSyncData`).
    fn full<V, Steady, Clock>(
        &mut self,
        start: &Start,
        revision: u64,
        rows: Vec<Row<'_, V>>,
        others: Others,
        fields: &Fields<Steady, Clock>,
    ) -> Map<String, Value>
    where
        Steady: Fn(&V) -> Vec<(&'static str, Value)>,
        Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
    {
        // The id the client holds stays held: what differs from it is
        // touched, so that id sent again is answered with the changes.
        let mut held = self.state.take().filter(|_| self.accepted.is_some());
        let mut body = Map::new();
        body.insert("full_update".to_owned(), Value::Bool(true));
        let mut torrents = Map::new();
        let mut kept = HashMap::with_capacity(rows.len());
        let mut touched = HashMap::new();
        for row in rows {
            let built = (fields.steady)(row.view);
            let clock = (fields.clock)(row.view);
            let kept_row = KeptRow {
                fingerprint: row.fingerprint,
                closed: row.closed,
                place: row.place,
                steady: built
                    .iter()
                    .map(|(name, value)| (*name, digest(value)))
                    .collect(),
                clock: clock.each_ref().map(|(_, value)| digest(value)),
            };
            let key: Arc<str> = Arc::from(row.hash.as_str());
            if let Some(held) = &mut held {
                let before = held.take_accepted(&row.hash);
                if before.as_ref().is_none_or(|before| !before.same(&kept_row)) {
                    touched.insert(
                        Arc::clone(&key),
                        Touched {
                            before,
                            steady: Some(built.clone()),
                            clock: Some(clock.clone()),
                        },
                    );
                }
            }
            let fields: Map<String, Value> = built
                .into_iter()
                .chain(clock)
                .map(|(name, value)| (name.to_owned(), value))
                .collect();
            if !fields.is_empty() {
                torrents.insert(row.hash, Value::Object(fields));
            }
            kept.insert(key, kept_row);
        }
        let (rest, server_state) = rest_changes(&mut body, None, others);
        put(&mut body, "torrents", Value::Object(torrents));
        put(&mut body, "server_state", Value::Object(server_state));
        let accepted_rest = match held {
            Some(mut held) => {
                // What the client holds that is gone now.
                touched.extend(held.accepted_gone());
                held.accepted_rest
            }
            None => Rest::default(),
        };
        self.state = Some(State {
            id: start.id,
            revision,
            scope: start.scope,
            torrents: kept,
            rest,
            touched,
            accepted_rest,
        });
        body
    }

    // What changed since the id the client holds: a new torrent or category
    // whole, a changed one by its changed keys, a gone one by its key in
    // `_removed`, and the server state's changed keys (qBittorrent
    // `processMap`/`processHash`). `moved` and `removed` are what moved since
    // the state's revision; the torrents touched before then are kept.
    fn changed<V, Steady, Clock>(
        &mut self,
        start: &Start,
        revision: u64,
        moved: Vec<Row<'_, V>>,
        removed: Vec<String>,
        others: Others,
        fields: &Fields<Steady, Clock>,
    ) -> Result<Map<String, Value>, Box<Others>>
    where
        Steady: Fn(&V) -> Vec<(&'static str, Value)>,
        Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
    {
        let Some(state) = &mut self.state else {
            return Err(Box::new(others));
        };
        // A torrent or a server state whose left-out fields moved has no
        // changes to send: the response is the full update.
        let held = |hash: &str| match state.touched.get(hash) {
            Some(touched) => touched.before.as_ref(),
            None => state.torrents.get(hash),
        };
        // The torrents touched before, which a full update may have touched
        // with fields left out that the client still holds.
        let touched_closed = || {
            let moving: std::collections::HashSet<&str> =
                moved.iter().map(|row| row.hash.as_str()).collect();
            state.touched.iter().any(|(key, touched)| {
                if moving.contains(&**key) {
                    return false;
                }
                let now = state.torrents.get(key).map(|row| row.closed);
                touched
                    .before
                    .as_ref()
                    .is_some_and(|before| now.is_some_and(|now| now != before.closed))
            })
        };
        if state.accepted_rest.server_closed != others.server_closed
            || moved
                .iter()
                .any(|row| held(&row.hash).is_some_and(|held| held.closed != row.closed))
            || touched_closed()
        {
            return Err(Box::new(others));
        }
        // The gone first: a torrent removed and one added under its hash
        // since are both changes.
        for hash in removed {
            if let Some((key, kept)) = state.torrents.remove_entry(hash.as_str()) {
                state.touched.entry(key).or_insert(Touched {
                    before: Some(kept),
                    steady: None,
                    clock: None,
                });
            }
        }
        for row in moved {
            let (key, kept) = match state.torrents.get_key_value(row.hash.as_str()) {
                Some((key, kept)) => (Arc::clone(key), Some(kept)),
                None => (Arc::from(row.hash.as_str()), None),
            };
            let touched = state
                .touched
                .entry(Arc::clone(&key))
                .or_insert_with(|| Touched {
                    before: kept.cloned(),
                    steady: None,
                    clock: None,
                });
            let clock = (fields.clock)(row.view);
            let clock_hashes = clock.each_ref().map(|(_, value)| digest(value));
            touched.clock = Some(clock);
            // The view moved only with the clock: its clock fields alone.
            let steady = match kept {
                Some(kept) if kept.fingerprint == row.fingerprint => Arc::clone(&kept.steady),
                _ => {
                    let built = (fields.steady)(row.view);
                    let hashes = built
                        .iter()
                        .map(|(name, value)| (*name, digest(value)))
                        .collect();
                    touched.steady = Some(built);
                    hashes
                }
            };
            state.torrents.insert(
                key,
                KeptRow {
                    fingerprint: row.fingerprint,
                    closed: row.closed,
                    place: row.place,
                    steady,
                    clock: clock_hashes,
                },
            );
        }
        let mut body = Map::new();
        let mut torrents = Vec::new();
        let mut gone = Vec::new();
        for (key, touched) in &state.touched {
            let Some(now) = state.torrents.get(key) else {
                if touched.before.is_some() {
                    gone.push(&**key);
                }
                continue;
            };
            let before = touched.before.as_ref();
            let mut changed = Map::new();
            // Unbuilt since, the row's fields are the ones the client holds.
            if let Some(steady) = &touched.steady {
                for (index, (name, value)) in steady.iter().enumerate() {
                    let held = before.and_then(|before| hash_of(&before.steady, index, name));
                    if held != Some(now.steady[index].1) {
                        changed.insert((*name).to_owned(), value.clone());
                    }
                }
            }
            if let Some(clock) = &touched.clock {
                for (index, (name, value)) in clock.iter().enumerate() {
                    if before.map(|before| before.clock[index]) != Some(now.clock[index]) {
                        changed.insert((*name).to_owned(), value.clone());
                    }
                }
            }
            if !changed.is_empty() {
                torrents.push((now.place, key, changed));
            }
        }
        // In the list's order, as a read of every torrent gave them.
        torrents.sort_unstable_by_key(|(place, ..)| *place);
        let torrents = torrents
            .into_iter()
            .map(|(_, key, changed)| ((**key).to_owned(), Value::Object(changed)))
            .collect();
        gone.sort_unstable();
        let gone = Value::Array(gone.into_iter().map(Value::from).collect());
        let (rest, server_state) = rest_changes(&mut body, Some(&state.accepted_rest), others);
        put(&mut body, "torrents", Value::Object(torrents));
        put(&mut body, "torrents_removed", gone);
        put(&mut body, "server_state", Value::Object(server_state));
        state.id = start.id;
        state.revision = revision;
        state.rest = rest;
        Ok(body)
    }
}

// Puts into `body` what of the categories and tags changed from `from`,
// everything without it, and returns their state now with the server
// state's changed keys, which go after the torrents.
fn rest_changes(
    body: &mut Map<String, Value>,
    from: Option<&Rest>,
    now: Others,
) -> (Rest, Map<String, Value>) {
    let mut categories = Map::new();
    let mut kept_categories = HashMap::with_capacity(now.categories.len());
    for (name, category) in now.categories {
        let before = from.and_then(|from| from.categories.get(&name));
        let (changed, keys) = keys_changes(before, category);
        if !changed.is_empty() {
            categories.insert(name.clone(), Value::Object(changed));
        }
        kept_categories.insert(name, keys);
    }
    let (tags_added, tags_removed) = match from {
        Some(from) => (
            strings(now.tags.iter().filter(|tag| !from.tags.contains(tag))),
            strings(from.tags.iter().filter(|tag| !now.tags.contains(tag))),
        ),
        None => (strings(now.tags.iter()), Value::Array(Vec::new())),
    };
    let (server_state, kept_server_state) =
        keys_changes(from.map(|from| &from.server_state), now.server_state);
    put(body, "categories", Value::Object(categories));
    if let Some(from) = from {
        put(
            body,
            "categories_removed",
            gone(from.categories.keys().map(String::as_str), |name| {
                kept_categories.contains_key(name)
            }),
        );
    }
    put(body, "tags", tags_added);
    put(body, "tags_removed", tags_removed);
    let rest = Rest {
        categories: kept_categories,
        tags: now.tags,
        server_state: kept_server_state,
        server_closed: now.server_closed,
    };
    (rest, server_state)
}

// The hash `name` was kept with, looked for first at `index`, where it sits
// in a row built the same way.
fn hash_of(kept: &[(&'static str, u64)], index: usize, name: &str) -> Option<u64> {
    match kept.get(index) {
        Some((kept, hash)) if *kept == name => Some(*hash),
        _ => kept
            .iter()
            .find(|(kept, _)| *kept == name)
            .map(|(_, hash)| *hash),
    }
}

// The keys of `now` whose values differ from the hashes `before` kept, and
// the hashes `now` leaves.
fn keys_changes(before: Option<&Keys>, now: Map<String, Value>) -> (Map<String, Value>, Keys) {
    let mut keys = HashMap::with_capacity(now.len());
    let changed = now
        .into_iter()
        .filter_map(|(key, value)| {
            let hash = digest(&value);
            let same = before.and_then(|before| before.get(&key)) == Some(&hash);
            keys.insert(key.clone(), hash);
            (!same).then_some((key, value))
        })
        .collect();
    (changed, keys)
}

// The names of `before` that `now` no longer has.
fn gone<'a>(before: impl Iterator<Item = &'a str>, now: impl Fn(&str) -> bool) -> Value {
    let mut names: Vec<&str> = before.filter(|name| !now(name)).collect();
    names.sort_unstable();
    Value::Array(names.into_iter().map(Value::from).collect())
}

fn strings<'a>(items: impl Iterator<Item = &'a String>) -> Value {
    Value::Array(items.map(|item| Value::String(item.clone())).collect())
}

// Sets `key` unless `value` is empty.
fn put(body: &mut Map<String, Value>, key: &str, value: Value) {
    let empty = match &value {
        Value::Object(map) => map.is_empty(),
        Value::Array(list) => list.is_empty(),
        _ => false,
    };
    if !empty {
        body.insert(key.to_owned(), value);
    }
}

// A value's hash, which differs where the values do.
fn digest(value: &Value) -> u64 {
    let mut hasher = DefaultHasher::new();
    feed(value, &mut hasher);
    hasher.finish()
}

fn feed(value: &Value, hasher: &mut DefaultHasher) {
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(value) => (1u8, value).hash(hasher),
        Value::Number(number) => {
            2u8.hash(hasher);
            if let Some(value) = number.as_u64() {
                (0u8, value).hash(hasher);
            } else if let Some(value) = number.as_i64() {
                (1u8, value).hash(hasher);
            } else {
                (2u8, number.as_f64().map(f64::to_bits)).hash(hasher);
            }
        }
        Value::String(value) => (3u8, value).hash(hasher),
        Value::Array(items) => {
            (4u8, items.len()).hash(hasher);
            for item in items {
                feed(item, hasher);
            }
        }
        Value::Object(map) => {
            (5u8, map.len()).hash(hasher);
            for (key, item) in map {
                key.hash(hasher);
                feed(item, hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests;

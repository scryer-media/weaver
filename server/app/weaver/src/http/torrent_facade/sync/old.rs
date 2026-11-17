// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The answer `sync/maindata` gave before the snapshot: every torrent read
//! on every poll and compared with the state the client holds. Kept as the
//! reference the snapshot's answers are tested against.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde_json::{Map, Value};

use super::SyncClient;

// The clients whose state is kept; the one longest unheard from goes first,
// and its next request is answered whole.
const CLIENTS: usize = 16;
// Ids cycle from 1 to this (qBittorrent `maindataAction`).
const ID_CYCLE: u64 = 1_000_000;
use super::CLOCK_KEYS;

// A torrent as this poll reads it: its row is built only when `fingerprint`,
// which leaves the clock out, differs from the one the client was sent.
// `closed` marks the fields the row leaves out because they have no reading
// (a limit that lets nothing through): a client keeps a field it is not sent,
// so when that set moves, the client is sent everything again.
pub(crate) struct Row<'a, V> {
    pub(crate) hash: String,
    pub(crate) fingerprint: u64,
    pub(crate) closed: u8,
    pub(crate) view: &'a V,
}

// What `sync/maindata` reports this poll, keyed as the client keys it.
pub(crate) struct Poll<'a, V> {
    pub(crate) torrents: Vec<Row<'a, V>>,
    pub(crate) categories: Vec<(String, Map<String, Value>)>,
    pub(crate) tags: Vec<String>,
    pub(crate) server_state: Map<String, Value>,
    // The server state's fields left out for having no reading, as a row's
    // `closed` marks its own.
    pub(crate) server_closed: u8,
}

// How a torrent's row is built: every field that moves only with the view,
// and the fields that move with the clock alone.
pub(crate) struct Fields<Steady, Clock> {
    pub(crate) steady: Steady,
    pub(crate) clock: Clock,
}

// The state a response leaves the client with, as hashes: a row's fields by
// their names, shared while the row is unchanged, and no JSON.
#[derive(Default)]
pub(crate) struct Kept {
    torrents: HashMap<Arc<str>, KeptRow>,
    categories: HashMap<String, Keys>,
    tags: Vec<String>,
    server_state: Keys,
    server_closed: u8,
}

struct KeptRow {
    fingerprint: u64,
    closed: u8,
    steady: Arc<[(&'static str, u64)]>,
    clock: [u64; CLOCK_KEYS],
}

type Keys = HashMap<String, u64>;

#[derive(Default)]
struct Client {
    // The id of the last response begun; 0 before the first.
    last_sent: u64,
    // The id the client has shown it holds, and the state it stands for.
    accepted: Option<(u64, Arc<Kept>)>,
    // The last response's id and the state it left the client with.
    sent: Option<(u64, Arc<Kept>)>,
    heard: u64,
}

#[derive(Default)]
pub(crate) struct SyncTable {
    clients: HashMap<SyncClient, Client>,
    heard: u64,
}

// Where a response starts: its id, and the state the client holds, if the
// response can be the changes from it.
pub(crate) struct Start {
    id: u64,
    from: Option<Arc<Kept>>,
}

impl Start {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

impl SyncTable {
    // The bookkeeping before a response to `rid` from `client`: the response
    // is built from what this returns, outside the table's lock, and then
    // handed to `finish`.
    pub(crate) fn begin(&mut self, client: &SyncClient, rid: u64) -> Start {
        if !self.clients.contains_key(client)
            && self.clients.len() >= CLIENTS
            && let Some(oldest) = self
                .clients
                .iter()
                .min_by_key(|(_, client)| client.heard)
                .map(|(key, _)| key.clone())
        {
            self.clients.remove(&oldest);
        }
        self.heard += 1;
        let entry = self.clients.entry(client.clone()).or_default();
        entry.heard = self.heard;
        // qBittorrent `maindataAction`: the client holds the last response
        // once it sends its id back, and changes can be sent while the id it
        // sends is the one it holds.
        let mut from = None;
        if rid > 0 && entry.last_sent > 0 {
            if entry.last_sent == rid
                && let Some((sent, state)) = &entry.sent
                && *sent == rid
            {
                entry.accepted = Some((rid, Arc::clone(state)));
            }
            from = entry
                .accepted
                .as_ref()
                .filter(|(accepted, _)| *accepted == rid)
                .map(|(_, state)| Arc::clone(state));
        }
        let id = entry.last_sent % ID_CYCLE + 1;
        entry.last_sent = id;
        Start { id, from }
    }

    // Keeps the state the response `id` left the client with. Two requests
    // from one client in flight at once race, and the last writer wins: the
    // one begun last is kept whenever it finishes, and an earlier one that
    // finishes after it is dropped, so the state kept is always that of the
    // last id sent.
    pub(crate) fn finish(&mut self, client: &SyncClient, id: u64, kept: Kept) {
        if let Some(entry) = self.clients.get_mut(client)
            && entry.last_sent == id
        {
            entry.sent = Some((id, Arc::new(kept)));
        }
    }
}

// The response that `start` begins, given the state now, and the state it
// leaves the client with.
pub(crate) fn answer<V, Steady, Clock>(
    start: &Start,
    now: Poll<'_, V>,
    fields: &Fields<Steady, Clock>,
) -> (Value, Kept)
where
    Steady: Fn(&V) -> Vec<(&'static str, Value)>,
    Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
{
    let (mut body, kept) = respond(start.from.as_deref(), now, fields);
    body.insert("rid".to_owned(), Value::from(start.id));
    (Value::Object(body), kept)
}

// The response from the state `from` stands for to `now`, and the state it
// leaves the client with. Without `from`, a full update: every key that
// holds anything (qBittorrent `generateMaindataSyncData`). Else what changed:
// a new torrent or category whole, a changed one by its changed keys, a gone
// one by its key in `_removed`, and the server state's changed keys
// (qBittorrent `processMap`/`processHash`). A torrent or a server state whose
// left-out fields moved has no changes to send, so the response is the full
// update.
fn respond<V, Steady, Clock>(
    from: Option<&Kept>,
    now: Poll<'_, V>,
    fields: &Fields<Steady, Clock>,
) -> (Map<String, Value>, Kept)
where
    Steady: Fn(&V) -> Vec<(&'static str, Value)>,
    Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
{
    if from.is_some_and(|from| from.server_closed != now.server_closed) {
        return respond(None, now, fields);
    }
    let mut body = Map::new();
    if from.is_none() {
        body.insert("full_update".to_owned(), Value::Bool(true));
    }
    let mut torrents = Map::new();
    let mut kept_torrents = HashMap::with_capacity(now.torrents.len());
    for row in &now.torrents {
        let before = from.and_then(|from| from.torrents.get_key_value(row.hash.as_str()));
        if before.is_some_and(|(_, before)| before.closed != row.closed) {
            return respond(None, now, fields);
        }
        let clock = (fields.clock)(row.view);
        let clock_hashes = clock.each_ref().map(|(_, value)| digest(value));
        let mut changed = Map::new();
        let (key, steady) = match before {
            // The view moved only with the clock: its clock fields alone.
            Some((key, before)) if before.fingerprint == row.fingerprint => {
                (Arc::clone(key), Arc::clone(&before.steady))
            }
            _ => {
                let built = (fields.steady)(row.view);
                let hashes: Arc<[(&'static str, u64)]> = built
                    .iter()
                    .map(|(name, value)| (*name, digest(value)))
                    .collect();
                let old = before.map(|(_, before)| &before.steady[..]);
                for (index, ((name, value), (_, hash))) in
                    built.into_iter().zip(hashes.iter()).enumerate()
                {
                    if old.and_then(|old| hash_of(old, index, name)) != Some(*hash) {
                        changed.insert(name.to_owned(), value);
                    }
                }
                let key =
                    before.map_or_else(|| Arc::from(row.hash.as_str()), |(key, _)| Arc::clone(key));
                (key, hashes)
            }
        };
        let old_clock = before.map(|(_, before)| before.clock);
        for (index, (name, value)) in clock.into_iter().enumerate() {
            if old_clock.is_none_or(|old| old[index] != clock_hashes[index]) {
                changed.insert(name.to_owned(), value);
            }
        }
        if !changed.is_empty() {
            torrents.insert(row.hash.clone(), Value::Object(changed));
        }
        kept_torrents.insert(
            key,
            KeptRow {
                fingerprint: row.fingerprint,
                closed: row.closed,
                steady,
                clock: clock_hashes,
            },
        );
    }
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
    put(&mut body, "categories", Value::Object(categories));
    if let Some(from) = from {
        put(
            &mut body,
            "categories_removed",
            gone(from.categories.keys().map(String::as_str), |name| {
                kept_categories.contains_key(name)
            }),
        );
    }
    put(&mut body, "tags", tags_added);
    put(&mut body, "tags_removed", tags_removed);
    put(&mut body, "torrents", Value::Object(torrents));
    if let Some(from) = from {
        put(
            &mut body,
            "torrents_removed",
            gone(from.torrents.keys().map(|key| &**key), |hash| {
                kept_torrents.contains_key(hash)
            }),
        );
    }
    put(&mut body, "server_state", Value::Object(server_state));
    let kept = Kept {
        torrents: kept_torrents,
        categories: kept_categories,
        tags: now.tags,
        server_state: kept_server_state,
        server_closed: now.server_closed,
    };
    (body, kept)
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

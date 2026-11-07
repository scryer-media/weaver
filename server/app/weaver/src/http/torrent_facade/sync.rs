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

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde_json::{Map, Value};

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
    pub(super) view: &'a V,
}

// What `sync/maindata` reports this poll, keyed as the client keys it.
pub(super) struct Poll<'a, V> {
    pub(super) torrents: Vec<Row<'a, V>>,
    pub(super) categories: Vec<(String, Map<String, Value>)>,
    pub(super) tags: Vec<String>,
    pub(super) server_state: Map<String, Value>,
}

// How a torrent's row is built: every field that moves only with the view,
// and the fields that move with the clock alone.
pub(super) struct Fields<Steady, Clock> {
    pub(super) steady: Steady,
    pub(super) clock: Clock,
}

// The state a response leaves the client with, as hashes: a row's fields by
// their names, shared while the row is unchanged, and no JSON.
#[derive(Default)]
pub(super) struct Kept {
    torrents: HashMap<Arc<str>, KeptRow>,
    categories: HashMap<String, Keys>,
    tags: Vec<String>,
    server_state: Keys,
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
pub(super) struct SyncTable {
    clients: HashMap<SyncClient, Client>,
    heard: u64,
}

// Where a response starts: its id, and the state the client holds, if the
// response can be the changes from it.
pub(super) struct Start {
    id: u64,
    from: Option<Arc<Kept>>,
}

impl Start {
    pub(super) fn id(&self) -> u64 {
        self.id
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

    // The bookkeeping before a response to `rid` from `client`: the response
    // is built from what this returns, outside the table's lock, and then
    // handed to `finish`.
    pub(super) fn begin(&mut self, client: &SyncClient, rid: u64) -> Start {
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
    pub(super) fn finish(&mut self, client: &SyncClient, id: u64, kept: Kept) {
        if let Some(entry) = self.clients.get_mut(client)
            && entry.last_sent == id
        {
            entry.sent = Some((id, Arc::new(kept)));
        }
    }
}

// The response that `start` begins, given the state now, and the state it
// leaves the client with.
pub(super) fn answer<V, Steady, Clock>(
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
// (qBittorrent `processMap`/`processHash`). A torrent whose left-out fields
// moved has no changes to send, so the response is the full update.
fn respond<V, Steady, Clock>(
    from: Option<&Kept>,
    now: Poll<'_, V>,
    fields: &Fields<Steady, Clock>,
) -> (Map<String, Value>, Kept)
where
    Steady: Fn(&V) -> Vec<(&'static str, Value)>,
    Clock: Fn(&V) -> [(&'static str, Value); CLOCK_KEYS],
{
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use serde_json::json;

    use super::*;

    // A torrent as these tests read it: a name that moves with the view and
    // a tick that moves with the clock.
    struct Thing {
        hash: String,
        name: String,
        tick: u64,
    }

    fn thing(index: usize, name: &str) -> Thing {
        Thing {
            hash: format!("{index:040x}"),
            name: name.to_owned(),
            tick: 0,
        }
    }

    fn poll(things: &[Thing]) -> Poll<'_, Thing> {
        Poll {
            torrents: things
                .iter()
                .map(|thing| Row {
                    hash: thing.hash.clone(),
                    fingerprint: digest(&json!(thing.name)),
                    closed: 0,
                    view: thing,
                })
                .collect(),
            categories: Vec::new(),
            tags: Vec::new(),
            server_state: Map::new(),
        }
    }

    const CLOCK: [&str; CLOCK_KEYS] = [
        "eta",
        "popularity",
        "seen_complete",
        "time_active",
        "seeding_time",
        "last_activity",
        "reannounce",
    ];

    struct Rig {
        table: SyncTable,
        builds: Cell<usize>,
    }

    impl Rig {
        fn new() -> Self {
            Self {
                table: SyncTable::default(),
                builds: Cell::new(0),
            }
        }

        fn answer(&mut self, client: SyncClient, rid: u64, things: &[Thing]) -> Value {
            let builds = &self.builds;
            let fields = Fields {
                steady: |thing: &Thing| {
                    builds.set(builds.get() + 1);
                    vec![("name", json!(thing.name)), ("size", json!(1))]
                },
                clock: |thing: &Thing| CLOCK.map(|name| (name, json!(thing.tick))),
            };
            let start = self.table.begin(&client, rid);
            let (body, kept) = answer(&start, poll(things), &fields);
            self.table.finish(&client, start.id, kept);
            body
        }
    }

    // Ids run from 1 to a million and start again at 1, and the id after the
    // wrap still sends changes.
    #[test]
    fn ids_cycle_from_one_to_a_million() {
        let mut rig = Rig::new();
        let client = SyncClient::Anonymous;
        assert_eq!(rig.answer(client.clone(), 0, &[thing(0, "x")])["rid"], 1);
        rig.table.clients.get_mut(&client).unwrap().last_sent = ID_CYCLE;
        // The client never held a million: whole.
        let whole = rig.answer(client.clone(), 1, &[thing(0, "x")]);
        assert_eq!(whole["rid"], 1);
        assert_eq!(whole["full_update"], true);
        let changed = rig.answer(client, 1, &[thing(0, "y")]);
        assert_eq!(changed["rid"], 2);
        assert_eq!(
            changed["torrents"],
            json!({ format!("{:040x}", 0): { "name": "y" } })
        );
    }

    // Past the cap, the client longest unheard from is answered whole.
    #[test]
    fn the_client_longest_unheard_from_goes_first() {
        let mut rig = Rig::new();
        for key in 0..=CLIENTS {
            let mut hash = [0; 32];
            hash[0] = u8::try_from(key).unwrap();
            rig.answer(SyncClient::Key(hash), 0, &[thing(0, "x")]);
        }
        assert_eq!(rig.table.clients.len(), CLIENTS);
        assert!(!rig.table.clients.contains_key(&SyncClient::Key([0; 32])));
        let mut last = [0; 32];
        last[0] = u8::try_from(CLIENTS).unwrap();
        assert_eq!(
            rig.answer(SyncClient::Key(last), 1, &[thing(0, "x")])
                .get("full_update"),
            None
        );
    }

    // Two responses to one client built at once: the one begun last is
    // kept, whichever finishes first, and the client sending its id back
    // gets the changes from it.
    #[test]
    fn the_response_begun_last_is_kept() {
        let mut rig = Rig::new();
        let client = SyncClient::Anonymous;
        rig.answer(client.clone(), 0, &[thing(0, "x")]);
        let fields = Fields {
            steady: |thing: &Thing| vec![("name", json!(thing.name))],
            clock: |thing: &Thing| CLOCK.map(|name| (name, json!(thing.tick))),
        };
        let (early, late) = ([thing(0, "early")], [thing(0, "late")]);
        let first = rig.table.begin(&client, 1);
        let second = rig.table.begin(&client, 1);
        let (_, kept_second) = answer(&second, poll(&late), &fields);
        rig.table.finish(&client, second.id(), kept_second);
        let (_, kept_first) = answer(&first, poll(&early), &fields);
        rig.table.finish(&client, first.id(), kept_first);
        let next = rig.answer(client, second.id(), &[thing(0, "late")]);
        assert!(next.get("full_update").is_none(), "{next}");
        assert!(next.get("torrents").is_none(), "{next}");
    }

    // A row is built only for a torrent whose view moved: none when only the
    // clock did, and one, sending just its changed key, when one torrent's
    // view did. What is kept per torrent is a hash per field, the field names
    // shared with the last build.
    #[test]
    fn a_poll_builds_only_the_rows_whose_view_moved() {
        let mut rig = Rig::new();
        let client = SyncClient::Anonymous;
        let mut things: Vec<Thing> = (0..50).map(|index| thing(index, "x")).collect();
        let whole = rig.answer(client.clone(), 0, &things);
        assert_eq!(whole["torrents"].as_object().unwrap().len(), 50);
        assert_eq!(rig.builds.get(), 50);

        let kept = rig.table.clients[&client].sent.clone().unwrap().1;
        assert_eq!(kept.torrents.len(), 50);
        for row in kept.torrents.values() {
            assert_eq!(
                &*row.steady,
                &[("name", digest(&json!("x"))), ("size", digest(&json!(1)))]
            );
        }

        // Nothing moved: no row built, and nothing to send.
        let same = rig.answer(client.clone(), 1, &things);
        assert_eq!(rig.builds.get(), 50);
        assert_eq!(same, json!({ "rid": 2 }));
        let shared = &rig.table.clients[&client].sent.as_ref().unwrap().1.torrents;
        let hash = things[0].hash.as_str();
        assert!(Arc::ptr_eq(
            &shared[hash].steady,
            &kept.torrents[hash].steady
        ));

        // The clock alone moved: no row built, the clock fields sent.
        things[3].tick = 9;
        let ticked = rig.answer(client.clone(), 2, &things);
        assert_eq!(rig.builds.get(), 50);
        let row = CLOCK.map(|name| (name.to_owned(), json!(9)));
        assert_eq!(
            ticked["torrents"],
            json!({ things[3].hash.clone(): Map::from_iter(row) })
        );

        // One view moved: that row alone built, and only its changed key sent.
        things[7].name = "y".to_owned();
        let moved = rig.answer(client.clone(), 3, &things);
        assert_eq!(rig.builds.get(), 51);
        assert_eq!(
            moved["torrents"],
            json!({ things[7].hash.clone(): { "name": "y" } })
        );

        // A gone torrent by its hash.
        let gone = things.remove(0);
        let removed = rig.answer(client, 4, &things);
        assert_eq!(rig.builds.get(), 51);
        assert_eq!(removed["torrents_removed"], json!([gone.hash]));
        assert!(removed.get("torrents").is_none());
    }
}

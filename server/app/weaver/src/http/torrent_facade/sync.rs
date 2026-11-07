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
use std::sync::Arc;

use serde_json::{Map, Value};

// The clients whose state is kept; the one longest unheard from goes first,
// and its next request is answered whole.
const CLIENTS: usize = 16;
// Ids cycle from 1 to this (qBittorrent `maindataAction`).
const ID_CYCLE: u64 = 1_000_000;

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

// What `sync/maindata` reports, keyed as the client keys it.
#[derive(Default)]
pub(super) struct Maindata {
    pub(super) torrents: Map<String, Value>,
    pub(super) categories: Map<String, Value>,
    pub(super) tags: Vec<Value>,
    pub(super) server_state: Map<String, Value>,
}

#[derive(Default)]
struct Client {
    // The id of the last response; 0 before the first.
    last_sent: u64,
    // The id the client has shown it holds, and the state it stands for.
    accepted: Option<(u64, Arc<Maindata>)>,
    // The state the last response left the client with.
    sent: Option<Arc<Maindata>>,
    heard: u64,
}

#[derive(Default)]
pub(super) struct SyncTable {
    clients: HashMap<SyncClient, Client>,
    heard: u64,
}

impl SyncTable {
    pub(super) fn forget(&mut self, client: &SyncClient) {
        self.clients.remove(client);
    }

    // The response to `rid` from `client`, given the state now.
    pub(super) fn answer(&mut self, client: SyncClient, rid: u64, now: Maindata) -> Value {
        if !self.clients.contains_key(&client)
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
        let entry = self.clients.entry(client).or_default();
        entry.heard = self.heard;
        // qBittorrent `maindataAction`: the client holds the last response
        // once it sends its id back, and changes can be sent while the id it
        // sends is the one it holds.
        let mut from = None;
        if rid > 0 && entry.last_sent > 0 {
            if entry.last_sent == rid
                && let Some(sent) = &entry.sent
            {
                entry.accepted = Some((rid, Arc::clone(sent)));
            }
            from = entry
                .accepted
                .as_ref()
                .filter(|(accepted, _)| *accepted == rid)
                .map(|(_, state)| Arc::clone(state));
        }
        let id = entry.last_sent % ID_CYCLE + 1;
        let mut body = match &from {
            Some(from) => changes(from, &now),
            None => whole(&now),
        };
        body.insert("rid".to_owned(), Value::from(id));
        entry.sent = Some(Arc::new(now));
        entry.last_sent = id;
        Value::Object(body)
    }
}

// A full update: every key that holds anything
// (qBittorrent `generateMaindataSyncData`).
fn whole(now: &Maindata) -> Map<String, Value> {
    let mut body = Map::new();
    body.insert("full_update".to_owned(), Value::Bool(true));
    put(
        &mut body,
        "categories",
        Value::Object(now.categories.clone()),
    );
    put(&mut body, "tags", Value::Array(now.tags.clone()));
    put(&mut body, "torrents", Value::Object(now.torrents.clone()));
    put(
        &mut body,
        "server_state",
        Value::Object(now.server_state.clone()),
    );
    body
}

// What changed from `from` to `now`: a new torrent or category whole, a
// changed one by its changed keys, a gone one by its key in `_removed`, and
// the server state's changed keys (qBittorrent `processMap`/`processHash`).
fn changes(from: &Maindata, now: &Maindata) -> Map<String, Value> {
    let mut body = Map::new();
    let (categories, categories_removed) = hash_changes(&from.categories, &now.categories);
    put(&mut body, "categories", Value::Object(categories));
    put(&mut body, "categories_removed", categories_removed);
    let (tags, tags_removed) = list_changes(&from.tags, &now.tags);
    put(&mut body, "tags", tags);
    put(&mut body, "tags_removed", tags_removed);
    let (torrents, torrents_removed) = hash_changes(&from.torrents, &now.torrents);
    put(&mut body, "torrents", Value::Object(torrents));
    put(&mut body, "torrents_removed", torrents_removed);
    put(
        &mut body,
        "server_state",
        Value::Object(map_changes(&from.server_state, &now.server_state)),
    );
    body
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

// The keys of `now` whose values differ from `from`'s, a nested map by its
// own changed keys (qBittorrent `processMap`).
fn map_changes(from: &Map<String, Value>, now: &Map<String, Value>) -> Map<String, Value> {
    now.iter()
        .filter_map(|(key, value)| match (from.get(key), value) {
            (Some(Value::Object(before)), Value::Object(after)) => {
                let changed = map_changes(before, after);
                (!changed.is_empty()).then(|| (key.clone(), Value::Object(changed)))
            }
            (Some(before), after) if before == after => None,
            _ => Some((key.clone(), value.clone())),
        })
        .collect()
}

// Items keyed by name: new ones whole, changed ones by their changed keys,
// and the names of the gone ones (qBittorrent `processHash`).
fn hash_changes(
    from: &Map<String, Value>,
    now: &Map<String, Value>,
) -> (Map<String, Value>, Value) {
    let removed = from
        .keys()
        .filter(|key| !now.contains_key(*key))
        .map(|key| Value::String(key.clone()))
        .collect();
    (map_changes(from, now), Value::Array(removed))
}

// A list's new items and its gone ones (qBittorrent `processList`).
fn list_changes(from: &[Value], now: &[Value]) -> (Value, Value) {
    let added = now
        .iter()
        .filter(|item| !from.contains(item))
        .cloned()
        .collect();
    let removed = from
        .iter()
        .filter(|item| !now.contains(item))
        .cloned()
        .collect();
    (Value::Array(added), Value::Array(removed))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn state(name: &str) -> Maindata {
        let mut torrents = Map::new();
        torrents.insert("a".repeat(40), json!({ "name": name, "size": 1 }));
        Maindata {
            torrents,
            ..Maindata::default()
        }
    }

    // Ids run from 1 to a million and start again at 1, and the id after the
    // wrap still sends changes.
    #[test]
    fn ids_cycle_from_one_to_a_million() {
        let mut table = SyncTable::default();
        let client = SyncClient::Anonymous;
        assert_eq!(table.answer(client.clone(), 0, state("x"))["rid"], 1);
        table.clients.get_mut(&client).unwrap().last_sent = ID_CYCLE;
        // The client never held a million: whole.
        let whole = table.answer(client.clone(), 1, state("x"));
        assert_eq!(whole["rid"], 1);
        assert_eq!(whole["full_update"], true);
        let changed = table.answer(client, 1, state("y"));
        assert_eq!(changed["rid"], 2);
        assert_eq!(
            changed["torrents"],
            json!({ "a".repeat(40): { "name": "y" } })
        );
    }

    // Past the cap, the client longest unheard from is answered whole.
    #[test]
    fn the_client_longest_unheard_from_goes_first() {
        let mut table = SyncTable::default();
        for key in 0..=CLIENTS {
            let mut hash = [0; 32];
            hash[0] = u8::try_from(key).unwrap();
            table.answer(SyncClient::Key(hash), 0, state("x"));
        }
        assert_eq!(table.clients.len(), CLIENTS);
        assert!(!table.clients.contains_key(&SyncClient::Key([0; 32])));
        let mut last = [0; 32];
        last[0] = u8::try_from(CLIENTS).unwrap();
        assert_eq!(
            table
                .answer(SyncClient::Key(last), 1, state("x"))
                .get("full_update"),
            None
        );
    }
}

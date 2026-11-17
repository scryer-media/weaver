// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::cell::Cell;
use std::collections::BTreeMap;

use serde_json::json;

use super::*;

// A torrent as these tests read it: a name that moves with the view, a tick
// that moves with the clock, and its left-out fields.
#[derive(Clone, PartialEq)]
struct Thing {
    place: u64,
    hash: String,
    name: String,
    tick: u64,
    closed: u8,
}

fn thing(index: usize, name: &str) -> Thing {
    Thing {
        place: index as u64,
        hash: format!("{index:040x}"),
        name: name.to_owned(),
        tick: 0,
        closed: 0,
    }
}

fn row(thing: &Thing) -> Row<'_, Thing> {
    Row {
        hash: thing.hash.clone(),
        fingerprint: digest(&json!(thing.name)),
        closed: thing.closed,
        place: (0, thing.place),
        view: thing,
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

fn steady(thing: &Thing) -> Vec<(&'static str, Value)> {
    let mut fields = vec![("name", json!(thing.name)), ("size", json!(1))];
    if thing.closed == 0 {
        fields.push(("dl_limit", json!(0)));
    }
    fields
}

fn clock(thing: &Thing) -> [(&'static str, Value); CLOCK_KEYS] {
    CLOCK.map(|name| (name, json!(thing.tick)))
}

#[derive(Clone)]
struct World {
    things: Vec<Thing>,
    tags: Vec<String>,
    category: Option<String>,
    server_closed: u8,
}

impl World {
    fn of(things: Vec<Thing>) -> Self {
        Self {
            things,
            tags: Vec::new(),
            category: None,
            server_closed: 0,
        }
    }

    fn others(&self) -> Others {
        Others {
            categories: self
                .category
                .iter()
                .map(|name| {
                    let mut category = Map::new();
                    category.insert("savePath".to_owned(), json!(format!("/{name}")));
                    ("films".to_owned(), category)
                })
                .collect(),
            tags: self.tags.clone(),
            server_state: Map::from_iter([
                ("connection_status".to_owned(), json!("connected")),
                (
                    "dl_info_speed".to_owned(),
                    json!(self.things.iter().map(|thing| thing.tick).sum::<u64>()),
                ),
            ]),
            server_closed: self.server_closed,
        }
    }
}

// A snapshot of the things as each poll saw them, by revision, so a poll can
// read what moved since a revision as the service's snapshot gives it.
struct Rig {
    table: SyncTable,
    builds: Cell<usize>,
    revisions: Vec<BTreeMap<String, Thing>>,
}

impl Rig {
    fn new() -> Self {
        Self {
            table: SyncTable::default(),
            builds: Cell::new(0),
            revisions: Vec::new(),
        }
    }

    fn answer(&mut self, client: SyncClient, rid: u64, things: &[Thing]) -> Value {
        self.answer_world(client, rid, &World::of(things.to_vec()))
    }

    fn answer_world(&mut self, client: SyncClient, rid: u64, world: &World) -> Value {
        let now: BTreeMap<String, Thing> = world
            .things
            .iter()
            .map(|thing| (thing.hash.clone(), thing.clone()))
            .collect();
        if self.revisions.last() != Some(&now) {
            self.revisions.push(now.clone());
        }
        let revision = self.revisions.len() as u64;
        let builds = &self.builds;
        let fields = Fields {
            steady: |thing: &Thing| {
                builds.set(builds.get() + 1);
                steady(thing)
            },
            clock,
        };
        let sync = self.table.client(&client);
        let mut sync = sync.lock().unwrap();
        let start = sync.begin(rid, 0);
        let answer = match start.since() {
            None => Err(Box::new(world.others())),
            Some(since) => {
                let before = &self.revisions[since as usize - 1];
                let moved = world
                    .things
                    .iter()
                    .filter(|thing| before.get(&thing.hash) != Some(thing))
                    .map(row)
                    .collect();
                let removed = before
                    .keys()
                    .filter(|hash| !now.contains_key(*hash))
                    .cloned()
                    .collect();
                sync.changes(&start, revision, moved, removed, world.others(), &fields)
            }
        };
        answer.unwrap_or_else(|others| {
            let rows = world.things.iter().map(row).collect();
            sync.whole(&start, revision, rows, *others, &fields)
        })
    }
}

// The answer the facade gave before the snapshot, which read every torrent
// on every poll.
struct OldRig {
    table: old::SyncTable,
}

impl OldRig {
    fn answer(&mut self, client: SyncClient, rid: u64, world: &World) -> Value {
        let others = world.others();
        let fields = old::Fields { steady, clock };
        let now = old::Poll {
            torrents: world
                .things
                .iter()
                .map(|thing| old::Row {
                    hash: thing.hash.clone(),
                    fingerprint: digest(&json!(thing.name)),
                    closed: thing.closed,
                    view: thing,
                })
                .collect(),
            categories: others.categories,
            tags: others.tags,
            server_state: others.server_state,
            server_closed: others.server_closed,
        };
        let start = self.table.begin(&client, rid);
        let (body, kept) = old::answer(&start, now, &fields);
        self.table.finish(&client, start.id(), kept);
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
    rig.table.client(&client).lock().unwrap().last_sent = ID_CYCLE;
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

// A client's requests are answered one after another under its lock: of two
// sent at once, the one answered last is the state kept, and the client
// sending its id back gets the changes from it.
#[test]
fn the_response_answered_last_is_kept() {
    let mut rig = Rig::new();
    let client = SyncClient::Anonymous;
    rig.answer(client.clone(), 0, &[thing(0, "x")]);
    let early = rig.answer(client.clone(), 1, &[thing(0, "early")]);
    let late = rig.answer(client.clone(), 1, &[thing(0, "late")]);
    assert_eq!(
        early["torrents"],
        json!({ format!("{:040x}", 0): { "name": "early" } })
    );
    assert_eq!(
        late["torrents"],
        json!({ format!("{:040x}", 0): { "name": "late" } })
    );
    let next = rig.answer(client, late["rid"].as_u64().unwrap(), &[thing(0, "late")]);
    assert!(next.get("full_update").is_none(), "{next}");
    assert!(next.get("torrents").is_none(), "{next}");
}

// A row is built only for a torrent whose view moved, and a poll reads only
// the torrents that moved: none when only the clock did, and one, sending
// just its changed key, when one torrent's view did. What is kept per
// torrent is a hash per field, the field names shared with the last build.
#[test]
fn a_poll_builds_only_the_rows_whose_view_moved() {
    let mut rig = Rig::new();
    let client = SyncClient::Anonymous;
    let mut things: Vec<Thing> = (0..50).map(|index| thing(index, "x")).collect();
    let whole = rig.answer(client.clone(), 0, &things);
    assert_eq!(whole["torrents"].as_object().unwrap().len(), 50);
    assert_eq!(rig.builds.get(), 50);

    let shared = |rig: &mut Rig, hash: &str| {
        let sync = rig.table.client(&client);
        let sync = sync.lock().unwrap();
        let state = sync.state.as_ref().unwrap();
        assert_eq!(state.torrents.len(), 50);
        Arc::clone(&state.torrents[hash].steady)
    };
    let first = shared(&mut rig, &things[0].hash);
    assert_eq!(
        &*first,
        &[
            ("name", digest(&json!("x"))),
            ("size", digest(&json!(1))),
            ("dl_limit", digest(&json!(0)))
        ]
    );

    // Nothing moved: no row built, and nothing to send.
    let same = rig.answer(client.clone(), 1, &things);
    assert_eq!(rig.builds.get(), 50);
    assert_eq!(same, json!({ "rid": 2 }));
    assert!(Arc::ptr_eq(&shared(&mut rig, &things[0].hash), &first));

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

// Scripted changes to 2,400 torrents, answered to clients that send back the
// id they were given, resend an older one, send one never sent, or start
// over: each answer is the one the facade gave before the snapshot, which
// read every torrent on every poll.
#[test]
fn the_snapshot_answers_as_reading_every_torrent_did() {
    const TORRENTS: usize = 2_400;
    let mut new = Rig::new();
    let mut old = OldRig {
        table: old::SyncTable::default(),
    };
    let mut world = World::of((0..TORRENTS).map(|index| thing(index, "x")).collect());
    let clients = [
        SyncClient::Anonymous,
        SyncClient::Key([1; 32]),
        SyncClient::Key([2; 32]),
        SyncClient::Session("s".to_owned()),
    ];
    // Each client's last id, and the one before it.
    let mut ids = [(0u64, 0u64); 4];
    let mut next_index = TORRENTS;
    let mut fulls = 0;
    let mut changes = 0;
    for step in 0..60usize {
        match step % 10 {
            // Steady fields move in some.
            0 | 5 => {
                for thing in world.things.iter_mut().skip(step).step_by(53) {
                    thing.name = format!("n{step}");
                }
            }
            // The clock alone moves in many.
            1 | 6 => {
                for thing in world.things.iter_mut().step_by(3) {
                    thing.tick += 1;
                }
            }
            // Some go and some come, one under a gone torrent's hash.
            2 => {
                let gone: Vec<Thing> = world.things.drain(step..step + 30).collect();
                for _ in 0..20 {
                    world.things.push(thing(next_index, "new"));
                    next_index += 1;
                }
                world.things.push(Thing {
                    place: next_index as u64,
                    name: "again".to_owned(),
                    ..gone[0].clone()
                });
                next_index += 1;
            }
            // A row's limit closes, then opens again.
            3 => world.things[11].closed = 1,
            7 => world.things[11].closed = 0,
            // Tags and categories come and go, and the server's limit
            // closes and opens.
            4 => {
                world.tags = vec!["a".to_owned(), format!("t{step}")];
                world.category = Some(format!("c{step}"));
            }
            8 => {
                world.tags.clear();
                world.category = None;
                world.server_closed ^= 1;
            }
            // A change back to what a client held, between two of its polls.
            9 => {
                world.things[5].name = "flip".to_owned();
                for (index, client) in clients.iter().enumerate() {
                    let rid = ids[index].0;
                    let body = old.answer(client.clone(), rid, &world);
                    assert_eq!(new.answer_world(client.clone(), rid, &world), body);
                    ids[index] = (body["rid"].as_u64().unwrap(), rid);
                }
                world.things[5].name = format!("n{}", step - 4);
            }
            _ => {}
        }
        for (index, client) in clients.iter().enumerate() {
            let (last, before) = ids[index];
            let rid = match (index + step) % 7 {
                // An older id again, as after a lost response.
                3 => before,
                // An id never sent.
                5 => 999_999,
                // A fresh start.
                6 if step % 3 == 0 => 0,
                _ => last,
            };
            let body = old.answer(client.clone(), rid, &world);
            assert_eq!(
                new.answer_world(client.clone(), rid, &world),
                body,
                "step {step}, client {index}, rid {rid}"
            );
            if body.get("full_update").is_some() {
                fulls += 1;
            } else if body.get("torrents").is_some() {
                changes += 1;
            }
            ids[index] = (body["rid"].as_u64().unwrap(), last);
        }
    }
    assert!(fulls > 10, "{fulls}");
    assert!(changes > 50, "{changes}");
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::servers::persistence::ServersPersistenceQueries;
use crate::settings::service::SettingsServiceQueries;
fn server(id: u32) -> crate::servers::ServerConfig {
    serde_json::from_value(serde_json::json!({
        "id": id, "host": format!("news-{id}.example.test"), "port": 119, "tls": false,
        "connections": 1, "active": id.is_multiple_of(2)
    }))
    .unwrap()
}

// Fifty servers are taken, active or not; the fifty-first is refused in
// words, and nothing of it is stored.
#[test]
fn a_fifty_first_server_is_refused() {
    let db = crate::Database::open_in_memory().unwrap();
    for id in 1..=50 {
        db.insert_server(&server(id)).unwrap();
    }
    let error = db.insert_server(&server(51)).unwrap_err();
    assert_eq!(
        error.refusal().map(|refusal| refusal.code),
        Some("SERVER_LIMIT")
    );
    assert_eq!(error.to_string(), "Weaver takes at most 50 servers");
    let stored = db.load_config().unwrap().servers;
    assert_eq!(stored.len(), 50);
    assert!(stored.iter().all(|server| server.id != 51));
}

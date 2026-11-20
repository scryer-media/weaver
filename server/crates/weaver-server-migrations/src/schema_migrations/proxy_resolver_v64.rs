// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Migration 64, upgrade step: every saved proxy is given the resolver that
// keeps how it resolves names today.
//
// A proxy now says who resolves a name dialled through it, Weaver or the
// proxy, and its DNS server list belongs to that resolver. Before 64 a
// proxy's DNS servers were always asked through the proxy, and a proxy with
// none handed the name to the proxy, except WireGuard, which has no way to
// take a name and was resolved by Weaver's own DNS. So a WireGuard proxy
// with no DNS servers is resolved by Weaver and every other proxy by the
// proxy. A profile that already names its resolver is left alone.
//
// The fields and values are spelled out here as they stood at schema 64, so
// later changes to the profile type do not change what this step does.

use serde_json::Value;

use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlConn};

pub const HOOK_ID: &str = "name_proxy_resolvers_v64";
// The schema that first names a proxy's resolver. A backup taken below it
// carries profiles without one.
pub const SCHEMA_VERSION: i64 = 64;

pub const RESOLVER: &str = "resolver";

// Run the step on a connection that is already inside the transaction it
// belongs to.
pub async fn name_proxy_resolvers(conn: &mut SqlConn<'_>) -> Result<(), StateError> {
    let rows = conn
        .fetch_all("SELECT id, config FROM proxy_profiles ORDER BY id", &[])
        .await?;
    for row in rows {
        let id = row.i64("id")?;
        let Some(named) = named(&row.text("config")?) else {
            continue;
        };
        conn.execute(
            "UPDATE proxy_profiles SET config = {} WHERE id = {}",
            &[SqlArg::Text(named), SqlArg::I64(id)],
        )
        .await?;
    }
    Ok(())
}

// The profile with its resolver named, or `None` when it already names one
// or cannot be read.
fn named(raw: &str) -> Option<String> {
    let mut document: Value = match serde_json::from_str(raw) {
        Ok(document) => document,
        Err(_) => {
            tracing::warn!("a saved proxy could not be read and was not given a resolver");
            return None;
        }
    };
    let object = document.as_object_mut()?;
    if object.contains_key(RESOLVER) {
        return None;
    }
    let wireguard = object.get("kind").and_then(Value::as_str) == Some("WireGuard");
    let no_servers = object
        .get("dns_servers")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    let resolver = if wireguard && no_servers {
        "Weaver"
    } else {
        "Proxy"
    };
    object.insert(RESOLVER.into(), Value::from(resolver));
    serde_json::to_string(&document).ok()
}

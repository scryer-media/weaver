// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Two-step confirmation for catalog mutations with lasting effects.
//
// The first `run_mutation` call runs nothing. It returns a summary of what
// would run and a single-use `confirm_token`. The agent shows the summary to
// the operator and calls again with the token, unchanged arguments and the
// operator's agreement. The token is bound to the API key, the operation's
// text and the variables, so it authorizes exactly the call that was
// summarized and nothing else.
//
// What this guards: an agent acting on text it read (a release name, a feed
// title, a log line) cannot change anything in one call, and a token
// approved for one call cannot be spent on another. What it cannot guard: an
// agent that ignores its operator can still call twice. The MCP client's own
// tool approval, which `run_mutation`'s destructive annotation asks for, is
// the human check; this makes that check name the real effect.
//
// Tokens are 256 random bits from the system generator. Only their BLAKE3
// digest is kept, in memory, so a token never sits in the table, a log or a
// lookup timing. They expire after `CONFIRM_TTL` and are lost on restart,
// which only means asking again. The table is capped per key and in total;
// a full table drops its oldest entries, so one key cannot crowd out the
// others.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use serde_json::{Map, Value};

use super::catalog::{Operation, OperationKind};

// How long an issued token stays good.
pub(crate) const CONFIRM_TTL: Duration = Duration::from_secs(5 * 60);

// Tokens one API key may hold at once.
const PER_KEY_MAX: usize = 16;

// Tokens held at once across every key.
const TOTAL_MAX: usize = 1024;

// Characters of a token: 32 random bytes as hex.
pub(crate) const TOKEN_CHARS: usize = 64;

// BLAKE3 key-derivation contexts, one per use, so a binding digest can never
// be mistaken for a token digest.
const BINDING_CONTEXT: &str = "weaver mcp confirmation binding v1";
const TOKEN_CONTEXT: &str = "weaver mcp confirmation token v1";

// Mutation root fields with no lasting effect. A catalog mutation runs
// without confirmation only when every root field it selects is listed here;
// anything new needs confirmation until it is added on purpose. None today.
pub(crate) const NO_LASTING_EFFECT: &[&str] = &[];

// Variable names whose values a summary shows as `<redacted>`, matched as a
// case-blind suffix. The token still binds the real values.
const SECRET_NAMES: &[&str] = &[
    "apikey",
    "api_key",
    "password",
    "passwords",
    "passwordcandidates",
    "passphrase",
    "secret",
    "token",
    "clientsecret",
    "client_secret",
    "privatekey",
    "privatekeypem",
    "presharedkey",
    "autobackupkey",
];

// Whether running `operation` needs a confirmed second call.
pub(crate) fn needs_confirmation(operation: &Operation) -> bool {
    operation.kind == OperationKind::Mutation
        && !operation
            .root_fields
            .iter()
            .all(|field| NO_LASTING_EFFECT.contains(&field.as_str()))
}

// The digest a token binds: the operation's text and the variables in a
// canonical form, so key order and spacing do not matter but any value does.
pub(crate) fn binding(operation: &Operation, variables: Option<&Value>) -> [u8; 32] {
    let mut canonical_text = String::new();
    canonical_text.push_str(&operation.text.len().to_string());
    canonical_text.push(':');
    canonical_text.push_str(operation.text);
    canonical(
        variables.unwrap_or(&Value::Object(Map::new())),
        &mut canonical_text,
    );
    blake3::derive_key(BINDING_CONTEXT, canonical_text.as_bytes())
}

fn token_id(token: &str) -> [u8; 32] {
    blake3::derive_key(TOKEN_CONTEXT, token.as_bytes())
}

// `value` as JSON with every object's keys sorted, so the digest does not
// depend on the order the client sent them in.
fn canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(fields) => {
            let mut keys = fields.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                canonical(&fields[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

// `variables` for the operator to read, with secret-named values hidden.
pub(crate) fn redacted(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, field)| {
                    let shown = if is_secret_name(key) && !field.is_null() {
                        Value::String("<redacted>".into())
                    } else {
                        redacted(field)
                    };
                    (key.clone(), shown)
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redacted).collect()),
        scalar => scalar.clone(),
    }
}

fn is_secret_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    SECRET_NAMES.iter().any(|secret| lower.ends_with(secret))
}

// The API key a token belongs to, as the digest admission identified it by.
pub(crate) type KeyId = [u8; 32];

struct Pending {
    key: KeyId,
    binding: [u8; 32],
    issued: Instant,
}

// What presenting a token came to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Redemption {
    // The token matched this key and call; it is now spent.
    Accepted,
    // No live token for this key: never issued, expired, already spent or
    // issued to another key.
    Unknown,
    // The token was this key's but for a different call. It is spent, so the
    // changed call needs its own confirmation.
    Mismatch,
}

// The pending tokens, keyed by their digests.
#[derive(Default)]
pub(crate) struct Confirmations {
    pending: Mutex<HashMap<[u8; 32], Pending>>,
}

impl Confirmations {
    // A new token for this key and call.
    pub(crate) fn issue(&self, key: &KeyId, binding: [u8; 32]) -> String {
        self.issue_at(key, binding, Instant::now())
    }

    pub(crate) fn redeem(&self, token: &str, key: &KeyId, binding: [u8; 32]) -> Redemption {
        self.redeem_at(token, key, binding, Instant::now())
    }

    pub(super) fn issue_at(&self, key: &KeyId, binding: [u8; 32], now: Instant) -> String {
        let mut raw = [0u8; 32];
        SystemRandom::new()
            .fill(&mut raw)
            .expect("the system random generator is available");
        let token = raw
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        pending.retain(|_, entry| now.saturating_duration_since(entry.issued) < CONFIRM_TTL);
        let held_by_key = pending.values().filter(|entry| entry.key == *key).count();
        if held_by_key >= PER_KEY_MAX {
            remove_oldest(&mut pending, |entry| entry.key == *key);
        }
        if pending.len() >= TOTAL_MAX {
            remove_oldest(&mut pending, |_| true);
        }
        pending.insert(
            token_id(&token),
            Pending {
                key: *key,
                binding,
                issued: now,
            },
        );
        token
    }

    pub(super) fn redeem_at(
        &self,
        token: &str,
        key: &KeyId,
        binding: [u8; 32],
        now: Instant,
    ) -> Redemption {
        let id = token_id(token);
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        // Another key's token is left alone: presenting it cannot spend it.
        let Some(entry) = pending.get(&id).filter(|entry| entry.key == *key) else {
            return Redemption::Unknown;
        };
        let live = now.saturating_duration_since(entry.issued) < CONFIRM_TTL;
        let matches = entry.binding == binding;
        pending.remove(&id);
        match (live, matches) {
            (false, _) => Redemption::Unknown,
            (true, false) => Redemption::Mismatch,
            (true, true) => Redemption::Accepted,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }
}

fn remove_oldest(pending: &mut HashMap<[u8; 32], Pending>, which: impl Fn(&Pending) -> bool) {
    if let Some(oldest) = pending
        .iter()
        .filter(|(_, entry)| which(entry))
        .min_by_key(|(_, entry)| entry.issued)
        .map(|(id, _)| *id)
    {
        pending.remove(&oldest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::mcp::catalog::{Catalog, SkillOperationFile};
    use serde_json::json;

    const KEY_A: KeyId = [1; 32];
    const KEY_B: KeyId = [2; 32];
    const BINDING: [u8; 32] = [7; 32];
    const OTHER: [u8; 32] = [9; 32];

    #[test]
    fn a_token_is_spent_by_the_call_it_was_issued_for() {
        let table = Confirmations::default();
        let token = table.issue(&KEY_A, BINDING);
        assert_eq!(token.len(), TOKEN_CHARS);
        assert_eq!(table.redeem(&token, &KEY_A, BINDING), Redemption::Accepted);
        assert_eq!(
            table.redeem(&token, &KEY_A, BINDING),
            Redemption::Unknown,
            "a token works once"
        );
    }

    #[test]
    fn a_changed_call_spends_the_token_without_running() {
        let table = Confirmations::default();
        let token = table.issue(&KEY_A, BINDING);
        assert_eq!(table.redeem(&token, &KEY_A, OTHER), Redemption::Mismatch);
        assert_eq!(table.redeem(&token, &KEY_A, BINDING), Redemption::Unknown);
    }

    #[test]
    fn another_key_cannot_use_or_spend_a_token() {
        let table = Confirmations::default();
        let token = table.issue(&KEY_A, BINDING);
        assert_eq!(table.redeem(&token, &KEY_B, BINDING), Redemption::Unknown);
        assert_eq!(table.redeem(&token, &KEY_A, BINDING), Redemption::Accepted);
    }

    #[test]
    fn a_token_expires() {
        let table = Confirmations::default();
        let start = Instant::now();
        let token = table.issue_at(&KEY_A, BINDING, start);
        assert_eq!(
            table.redeem_at(&token, &KEY_A, BINDING, start + CONFIRM_TTL),
            Redemption::Unknown
        );
        let token = table.issue_at(&KEY_A, BINDING, start);
        assert_eq!(
            table.redeem_at(
                &token,
                &KEY_A,
                BINDING,
                start + CONFIRM_TTL - Duration::from_secs(1)
            ),
            Redemption::Accepted
        );
    }

    #[test]
    fn one_key_cannot_crowd_out_another() {
        let table = Confirmations::default();
        let start = Instant::now();
        let kept = table.issue_at(&KEY_B, BINDING, start);
        let first = table.issue_at(&KEY_A, BINDING, start);
        for step in 1..=PER_KEY_MAX as u64 {
            table.issue_at(&KEY_A, BINDING, start + Duration::from_millis(step));
        }
        assert_eq!(table.len(), PER_KEY_MAX + 1);
        assert_eq!(
            table.redeem_at(&first, &KEY_A, BINDING, start + Duration::from_secs(1)),
            Redemption::Unknown,
            "the key's oldest token made room"
        );
        assert_eq!(
            table.redeem_at(&kept, &KEY_B, BINDING, start + Duration::from_secs(1)),
            Redemption::Accepted
        );
    }

    #[test]
    fn the_table_is_capped_in_total() {
        let table = Confirmations::default();
        let start = Instant::now();
        let first = table.issue_at(&[0; 32], BINDING, start);
        for index in 1..=TOTAL_MAX as u64 {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&index.to_le_bytes());
            table.issue_at(&key, BINDING, start + Duration::from_millis(index));
        }
        assert_eq!(table.len(), TOTAL_MAX);
        assert_eq!(
            table.redeem_at(&first, &[0; 32], BINDING, start + Duration::from_secs(2)),
            Redemption::Unknown,
            "the oldest token overall made room"
        );
    }

    #[test]
    fn the_binding_ignores_key_order_but_not_values() {
        let catalog = Catalog::build(
            &[SkillOperationFile {
                skill: "skill-a",
                file: "Fixture.graphql",
                text: "mutation Fixture($id: Int!) { pauseJob(id: $id) }\n",
            }],
            &[],
        );
        let operation = catalog.get("Fixture").expect("fixture");
        assert!(needs_confirmation(operation));
        let a = json!({"input": {"name": "Fixture", "enabled": true}});
        let b = json!({"input": {"enabled": true, "name": "Fixture"}});
        let c = json!({"input": {"enabled": false, "name": "Fixture"}});
        assert_eq!(binding(operation, Some(&a)), binding(operation, Some(&b)));
        assert_ne!(binding(operation, Some(&a)), binding(operation, Some(&c)));
        assert_eq!(
            binding(operation, None),
            binding(operation, Some(&json!({})))
        );
    }

    #[test]
    fn summaries_hide_secret_values() {
        let shown = redacted(&json!({"input": {
            "server": {"host": "news.fixture.invalid", "password": "fixture-password"},
            "proxies": [{"privateKey": "fixture-key", "presharedKey": "fixture-psk", "name": "x"}],
            "autoBackupKey": "fixture-backup-key",
            "passwordCandidates": ["one", "two"],
            "apiKey": null,
        }}));
        assert_eq!(shown["input"]["server"]["password"], "<redacted>");
        assert_eq!(shown["input"]["server"]["host"], "news.fixture.invalid");
        assert_eq!(shown["input"]["proxies"][0]["privateKey"], "<redacted>");
        assert_eq!(shown["input"]["proxies"][0]["presharedKey"], "<redacted>");
        assert_eq!(shown["input"]["proxies"][0]["name"], "x");
        assert_eq!(shown["input"]["autoBackupKey"], "<redacted>");
        assert_eq!(shown["input"]["passwordCandidates"], "<redacted>");
        assert_eq!(shown["input"]["apiKey"], Value::Null);
    }

    #[test]
    fn every_shipped_mutation_needs_confirmation() {
        // Nothing is listed as free of lasting effects, so every catalog
        // mutation asks first.
        assert!(NO_LASTING_EFFECT.is_empty());
        for operation in crate::http::mcp::catalog::CATALOG.operations() {
            if operation.kind == OperationKind::Mutation {
                assert!(needs_confirmation(operation), "{}", operation.name);
            }
        }
    }
}

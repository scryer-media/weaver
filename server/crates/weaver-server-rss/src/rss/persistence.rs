// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_db::persistence::{StateError, encryption::EncryptionKey};
// What a seen item's stored id starts with: `b3:`, then its digest in hex.
pub const SEEN_KEY_PREFIX: &str = "b3:";
pub const SEEN_KEY_LEN: usize = SEEN_KEY_PREFIX.len() + 64;
// The BLAKE3 key derivation contexts a seen key is made under: one for an
// item the feed gives an id or a link, another for an item named by its
// title alone, so a title never makes another item's id key. Fixed: a
// change to either, or to what is hashed, makes every item seen before new.
const SEEN_ID_CONTEXT: &str = "weaver rss seen item id v1";
const SEEN_TITLE_CONTEXT: &str = "weaver rss seen item title v1";

// What names a feed's item. Nothing time-like: a feed's dates for an item
// change.
#[derive(Clone, Copy, Debug)]
pub enum SeenId<'a> {
    // The id the feed gives it, else its link; or a stored key already.
    Id(&'a str),
    // Neither: its title. Such an item has no link, so it is never added;
    // its key keeps its seen row alone.
    Title(&'a str),
}

// Whether `id` is a seen key as this makes it: `b3:` and 64 hex digits.
pub fn is_seen_key(id: &str) -> bool {
    id.len() == SEEN_KEY_LEN
        && id.starts_with(SEEN_KEY_PREFIX)
        && id[SEEN_KEY_PREFIX.len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

// A seen item's key, as it is stored: one BLAKE3 over what names the item,
// under a fixed context, and nothing else. A feed's item id is often the
// item's link, and a private tracker's link carries the passkey, so the
// stored key is a digest: the same for the same item, which is all a seen
// lookup needs. No key, clock or start goes into it, so it reads the same
// after any restart or key loss. An id that is a key already is its own.
pub fn seen_item_key(id: SeenId<'_>) -> String {
    let digest = match id {
        SeenId::Id(id) if is_seen_key(id) => return id.to_owned(),
        SeenId::Id(id) => blake3::derive_key(SEEN_ID_CONTEXT, id.as_bytes()),
        SeenId::Title(title) => blake3::derive_key(SEEN_TITLE_CONTEXT, title.as_bytes()),
    };
    format!("{SEEN_KEY_PREFIX}{}", hex::encode(digest))
}

// The stored ids an item named by `item_id` may be found under: its key,
// and the id itself, as a row stored before the key holds it until the
// sealing pass, which runs only under a key a store keeps.
pub fn seen_item_keys(item_id: &str) -> [String; 2] {
    [seen_item_key(SeenId::Id(item_id)), item_id.to_owned()]
}

// The key a row stored before keys is put back under. An item with neither
// id nor link was stored under a SHA-256 of its title and published date,
// which is told by making it again: of the title and the stored date, or
// of the title alone, as the stored date may be the feed's updated date
// where the old id took the published date only. Such a row goes back
// under its title key; any other id is the feed's own.
pub fn legacy_seen_key(item_id: &str, title: &str, published_at: Option<i64>) -> String {
    // The id an item with neither id nor link was once stored under.
    fn legacy_untitled_id(title: &str, published_at: Option<i64>) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(title.as_bytes());
        if let Some(published_at) = published_at {
            hasher.update(published_at.to_le_bytes());
        }
        hex::encode(hasher.finalize())
    }
    if (published_at.is_some() && item_id == legacy_untitled_id(title, published_at))
        || item_id == legacy_untitled_id(title, None)
    {
        return seen_item_key(SeenId::Title(title));
    }
    seen_item_key(SeenId::Id(item_id))
}

// What the idempotency scope of an RSS feed's submissions starts with; the
// feed's id follows. Its key is the item's seen key (`seen_item_key`).
pub const RSS_IDEMPOTENCY_SCOPE: &str = "rss:feed:";

// A page of the RSS idempotency rows still keyed by the item's id, from a
// job on: walked by the table's job index, as a prefix of the scope is not
// a range under every collation a PostgreSQL store may have.
pub const RSS_IDEMPOTENCY_PAGE_SQL: &str =
    "SELECT caller_scope, idempotency_key, request_hash, job_id, created_at
       FROM submission_idempotency
      WHERE job_id >= {}
        AND substr(caller_scope, 1, 9) = 'rss:feed:'
        AND NOT (idempotency_key LIKE 'b3:%' AND length(idempotency_key) = 67)
      ORDER BY job_id LIMIT {}";

// One seen item forgotten, by either id it may be stored under: an
// equality on the table's key `(feed_id, item_id)`.
pub const SEEN_DELETE_SQL: &str =
    "DELETE FROM rss_seen_items WHERE feed_id = {} AND item_id IN ({}, {})";

// A seen item's stored link, opened. One that does not open, under no key
// or another key, reads as no link.
pub fn open_seen_link(key: Option<&EncryptionKey>, stored: Option<String>) -> Option<String> {
    use weaver_server_db::persistence::encryption::{decrypt_value, is_encrypted};
    let stored = stored?;
    if !is_encrypted(&stored) {
        return Some(stored);
    }
    decrypt_value(key?, &stored).ok()
}

// A link as it is stored: encrypted with the instance key, whatever it
// starts with. Under a key held in memory only it is stored as it is, as it
// was before links were encrypted: one encrypted under that key would be
// lost at the next start.
pub fn sealed_link(key: Option<&EncryptionKey>, link: &str) -> Result<String, StateError> {
    if link.is_empty() {
        return Ok(String::new());
    }
    match key {
        None => Err(StateError::Database(
            "encryption key is required to store secrets".to_string(),
        )),
        Some(key) if key.is_ephemeral() => Ok(link.to_owned()),
        Some(key) => weaver_server_db::persistence::encryption::encrypt_value(key, link)
            .map_err(StateError::Database),
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::settings::model::{
    ArchivePasswords, Client, Leaf, PeerTransport, SeedingPolicy, TorrentEncryption,
    TrackerCertificate, TrackerRule, TransportRule,
};
use weaver_server_db::persistence::StateError;
use weaver_server_db::persistence::connection::Database;
use weaver_server_db::persistence::encryption::{
    EncryptionKey, decrypt_value, encrypt_value, is_encrypted,
};
use weaver_server_db::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime, SqlTx};

use weaver_server_net::proxies::{Consumer, TorrentScope};

const TRACKER_RULE_COLUMNS: &str = "id, name, seeding, never_pause, unpack, client, torrent_route, dht, pex, lsd, encryption, upload_cap, forced, ssl_certificate, ssl_private_key, bound, archive_passwords, peer_tcp, peer_utp, prefer_downloading, prefer_seeding";

fn prefer_of(row: &SqlRow, column: &str) -> Result<Option<PeerTransport>, StateError> {
    row.opt_text(column)?
        .map(|transport| {
            PeerTransport::parse(&transport).ok_or_else(|| {
                StateError::Database(format!("tracker rule {column} {transport} is not known"))
            })
        })
        .transpose()
}

fn tracker_rule_from_row(
    row: &SqlRow,
    key: Option<&EncryptionKey>,
) -> Result<TrackerRule, StateError> {
    let seeding = row
        .opt_text("seeding")?
        .map(|json| {
            serde_json::from_str(&json).map_err(|error| {
                StateError::Database(format!("tracker rule seeding policy is not valid: {error}"))
            })
        })
        .transpose()?;
    let id = row.i64("id")?;
    Ok(TrackerRule {
        id: u32::try_from(id)
            .map_err(|_| StateError::Database(format!("tracker rule id {id} is not valid")))?,
        name: row.text("name")?,
        // Filled from `tracker_rule_hosts` by the caller.
        host_patterns: Vec::new(),
        seeding: SeedingPolicy::kept(seeding),
        never_pause: row.bool("never_pause")?,
        unpack: row.opt_bool("unpack")?,
        client: row
            .opt_text("client")?
            .map(|client| {
                Client::parse(&client).ok_or_else(|| {
                    StateError::Database(format!("tracker rule client {client} is not known"))
                })
            })
            .transpose()?,
        torrent_route: row.bool("torrent_route")?,
        dht: row.opt_bool("dht")?,
        pex: row.opt_bool("pex")?,
        lsd: row.opt_bool("lsd")?,
        encryption: row
            .opt_text("encryption")?
            .map(|encryption| {
                TorrentEncryption::parse(&encryption).ok_or_else(|| {
                    StateError::Database(format!(
                        "tracker rule encryption {encryption} is not known"
                    ))
                })
            })
            .transpose()?,
        transports: TransportRule {
            tcp: row.opt_bool("peer_tcp")?,
            utp: row.opt_bool("peer_utp")?,
            prefer_downloading: prefer_of(row, "prefer_downloading")?,
            prefer_seeding: prefer_of(row, "prefer_seeding")?,
        },
        upload_cap: {
            let text = row.opt_text("upload_cap")?;
            Leaf::from_text(text.as_deref()).ok_or_else(|| {
                StateError::Database(format!(
                    "tracker rule upload cap {} is not valid",
                    text.unwrap_or_default()
                ))
            })?
        },
        forced: row.bool("forced")?,
        ssl: match (
            row.opt_text("ssl_certificate")?,
            row.opt_text("ssl_private_key")?,
        ) {
            (Some(certificate_pem), Some(sealed)) => Some(TrackerCertificate {
                certificate_pem,
                private_key_pem: open_private_key(key, &sealed)?,
            }),
            _ => None,
        },
        bound: row
            .opt_text("bound")?
            .map(|json| {
                serde_json::from_str(&json).map_err(|error| {
                    StateError::Database(format!("tracker rule binding is not valid: {error}"))
                })
            })
            .transpose()?,
        archive_passwords: match row.opt_text("archive_passwords")? {
            Some(sealed) => open_archive_passwords(key, &sealed)?,
            None => ArchivePasswords::default(),
        },
    })
}

// A rule's archive passwords as they are kept: one JSON list sealed with the
// database's key, or nothing for none.
fn seal_archive_passwords(
    key: Option<&EncryptionKey>,
    passwords: &ArchivePasswords,
) -> Result<Option<String>, StateError> {
    if passwords.is_empty() {
        return Ok(None);
    }
    let key = key.ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
    let json = serde_json::to_string(passwords.as_slice())
        .map_err(|error| StateError::Database(error.to_string()))?;
    encrypt_value(key, &json).map(Some).map_err(|_| {
        StateError::Conflict("could not encrypt a tracker rule's archive passwords".into())
    })
}

fn open_archive_passwords(
    key: Option<&EncryptionKey>,
    sealed: &str,
) -> Result<ArchivePasswords, StateError> {
    let key = key.ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
    if !is_encrypted(sealed) {
        return Err(StateError::Conflict(
            "a tracker rule's archive passwords are not encrypted".into(),
        ));
    }
    let json = decrypt_value(key, sealed).map_err(|_| {
        StateError::Conflict("could not decrypt a tracker rule's archive passwords".into())
    })?;
    let values: Vec<String> = serde_json::from_str(&json).map_err(|_| {
        StateError::Database("a tracker rule's archive passwords are not valid".into())
    })?;
    ArchivePasswords::new(values).map_err(|refusal| {
        StateError::Database(format!("a tracker rule's archive passwords {refusal}"))
    })
}

// A rule's private key as it is kept: sealed with the database's key.
fn seal_private_key(key: Option<&EncryptionKey>, pem: &str) -> Result<String, StateError> {
    let key = key.ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
    encrypt_value(key, pem)
        .map_err(|_| StateError::Conflict("could not encrypt the SSL private key".into()))
}

fn open_private_key(key: Option<&EncryptionKey>, sealed: &str) -> Result<String, StateError> {
    let key = key.ok_or_else(|| StateError::Conflict("encryption key required".into()))?;
    if !is_encrypted(sealed) {
        return Err(StateError::Conflict(
            "a tracker rule's SSL private key is not encrypted".into(),
        ));
    }
    decrypt_value(key, sealed)
        .map_err(|_| StateError::Conflict("could not decrypt the SSL private key".into()))
}

// The rule's columns after its id, in `TRACKER_RULE_COLUMNS` order.
fn tracker_rule_args(
    rule: &TrackerRule,
    key: Option<&EncryptionKey>,
) -> Result<Vec<SqlArg>, StateError> {
    let seeding = SeedingPolicy::kept(rule.seeding)
        .map(|policy| serde_json::to_string(&policy))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    Ok(vec![
        SqlArg::Text(rule.name.clone()),
        SqlArg::OptText(seeding),
        SqlArg::Bool(rule.never_pause),
        SqlArg::OptBool(rule.unpack),
        SqlArg::OptText(rule.client.map(|client| client.as_str().to_owned())),
        SqlArg::Bool(rule.torrent_route),
        SqlArg::OptBool(rule.dht),
        SqlArg::OptBool(rule.pex),
        SqlArg::OptBool(rule.lsd),
        SqlArg::OptText(
            rule.encryption
                .map(|encryption| encryption.as_str().to_owned()),
        ),
        SqlArg::OptText(rule.upload_cap.to_text()),
        SqlArg::Bool(rule.forced),
        SqlArg::OptText(rule.ssl.as_ref().map(|ssl| ssl.certificate_pem.clone())),
        SqlArg::OptText(
            rule.ssl
                .as_ref()
                .map(|ssl| seal_private_key(key, &ssl.private_key_pem))
                .transpose()?,
        ),
        SqlArg::OptText(
            rule.bound
                .as_ref()
                .map(serde_json::to_string)
                .transpose()
                .map_err(|error| StateError::Database(error.to_string()))?,
        ),
        SqlArg::OptText(seal_archive_passwords(key, &rule.archive_passwords)?),
        SqlArg::OptBool(rule.transports.tcp),
        SqlArg::OptBool(rule.transports.utp),
        SqlArg::OptText(
            rule.transports
                .prefer_downloading
                .map(|transport| transport.as_str().to_owned()),
        ),
        SqlArg::OptText(
            rule.transports
                .prefer_seeding
                .map(|transport| transport.as_str().to_owned()),
        ),
    ])
}

// Replace the hosts the rule `id` covers with `patterns`, in their order.
async fn write_hosts(tx: &mut SqlTx<'_>, id: u32, patterns: &[String]) -> Result<(), StateError> {
    tx.execute(
        "DELETE FROM tracker_rule_hosts WHERE rule_id = {}",
        &[SqlArg::I64(i64::from(id))],
    )
    .await?;
    if patterns.is_empty() {
        return Ok(());
    }
    let mut sql =
        String::from("INSERT INTO tracker_rule_hosts (pattern, rule_id, position) VALUES ");
    let mut args = Vec::with_capacity(patterns.len() * 3);
    for (position, pattern) in patterns.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        sql.push_str("({}, {}, {})");
        args.extend([
            SqlArg::Text(pattern.clone()),
            SqlArg::I64(i64::from(id)),
            SqlArg::I64(position as i64),
        ]);
    }
    tx.execute(&sql, &args).await?;
    Ok(())
}

// Why a tracker rule was not saved, by kind. Each kind words itself as the
// sentence it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerRuleRefusal {
    // A field or host the rule's tracker file locked, worded by the lock.
    Locked(String),
    // Another rule has the name, whatever the case.
    NameTaken(String),
    // Another rule covers one of the rule's hosts.
    Covered { pattern: String, by: String },
    // No rule has the id.
    NotFound(u32),
}

impl std::fmt::Display for TrackerRuleRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Locked(sentence) => f.write_str(sentence),
            Self::NameTaken(name) => write!(f, "a rule named {name} exists"),
            Self::Covered { pattern, by } => write!(f, "{pattern} is covered by {by}"),
            Self::NotFound(id) => write!(f, "no tracker rule {id}"),
        }
    }
}

impl std::error::Error for TrackerRuleRefusal {}

// Why `rule` may not be saved beside `rules` as the rule `id` names (none
// for a new rule): another rule has its name, whatever the case, or covers
// one of its hosts.
fn collision(
    rules: &[TrackerRule],
    id: Option<u32>,
    rule: &TrackerRule,
) -> Option<TrackerRuleRefusal> {
    let others = || rules.iter().filter(move |other| Some(other.id) != id);
    let key = TrackerRule::name_key(&rule.name);
    if let Some(other) = others().find(|other| TrackerRule::name_key(&other.name) == key) {
        return Some(TrackerRuleRefusal::NameTaken(other.name.clone()));
    }
    others().find_map(|other| {
        rule.host_patterns
            .iter()
            .find(|pattern| other.host_patterns.contains(pattern))
            .map(|pattern| TrackerRuleRefusal::Covered {
                pattern: pattern.clone(),
                by: other.name.clone(),
            })
    })
}

pub trait SettingsRepositoryQueries {
    fn list_tracker_rules(&self) -> Result<Vec<TrackerRule>, StateError>;
    fn insert_tracker_rule(
        &self,
        rule: &TrackerRule,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError>;
    fn update_tracker_rule(&self, rule: &TrackerRule, now_ms: i64) -> Result<bool, StateError>;
    fn delete_tracker_rule(&self, id: u32) -> Result<bool, StateError>;
    fn save_tracker_rule(
        &self,
        id: Option<u32>,
        rule: TrackerRule,
        keeps_ssl: bool,
        keeps_passwords: bool,
        now_ms: i64,
    ) -> Result<Result<Vec<TrackerRule>, TrackerRuleRefusal>, StateError>;
    fn apply_tracker_file(
        &self,
        file: &super::tracker_file::TrackerFile,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError>;
    fn unbind_tracker_rule(&self, id: u32, now_ms: i64) -> Result<bool, StateError>;
}

impl<T: std::borrow::Borrow<Database> + ?Sized> SettingsRepositoryQueries for T {
    fn list_tracker_rules(&self) -> Result<Vec<TrackerRule>, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        let key = db.encryption_key().cloned();
        db.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("SELECT {TRACKER_RULE_COLUMNS} FROM tracker_rules ORDER BY id"),
                &[],
            )
            .await?;
            let mut rules = rows
                .iter()
                .map(|row| tracker_rule_from_row(row, key.as_ref()))
                .collect::<Result<Vec<_>, _>>()?;
            let hosts = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT rule_id, pattern FROM tracker_rule_hosts ORDER BY rule_id, position",
                &[],
            )
            .await?;
            let at: std::collections::HashMap<i64, usize> = rules
                .iter()
                .enumerate()
                .map(|(place, rule)| (i64::from(rule.id), place))
                .collect();
            for host in hosts {
                if let Some(&place) = at.get(&host.i64("rule_id")?) {
                    rules[place].host_patterns.push(host.text("pattern")?);
                }
            }
            Ok(rules)
        })
    }

    // Add `rule` under the next free id, which it returns with.
    fn insert_tracker_rule(
        &self,
        rule: &TrackerRule,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        let args = tracker_rule_args(rule, db.encryption_key())?;
        let rule = rule.clone();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_tracker_rule", |tx| {
                let args = args.clone();
                let mut rule = rule.clone();
                Box::pin(async move {
                    let max = tx
                        .fetch_optional("SELECT MAX(id) AS id FROM tracker_rules", &[])
                        .await?
                        .map(|row| row.opt_i64("id"))
                        .transpose()?
                        .flatten()
                        .unwrap_or(0);
                    rule.id = u32::try_from(max + 1).map_err(|_| {
                        StateError::Database("no tracker rule id is left".to_string())
                    })?;
                    let mut values = vec![SqlArg::I64(i64::from(rule.id))];
                    values.extend(args);
                    values.extend([SqlArg::I64(now_ms), SqlArg::I64(now_ms)]);
                    tx.execute(
                        &format!(
                            "INSERT INTO tracker_rules ({TRACKER_RULE_COLUMNS}, created_at_ms, updated_at_ms)
                             VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
                        ),
                        &values,
                    )
                    .await?;
                    write_hosts(tx, rule.id, &rule.host_patterns).await?;
                    Ok(rule)
                })
            })
            .await
        })
    }

    // Returns whether the rule exists.
    fn update_tracker_rule(&self, rule: &TrackerRule, now_ms: i64) -> Result<bool, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        let mut args = tracker_rule_args(rule, db.encryption_key())?;
        args.extend([SqlArg::I64(now_ms), SqlArg::I64(i64::from(rule.id))]);
        let (id, patterns) = (rule.id, rule.host_patterns.clone());
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_tracker_rule", |tx| {
                let args = args.clone();
                let patterns = patterns.clone();
                Box::pin(async move {
            let changed = tx.execute(
                "UPDATE tracker_rules
                    SET name = {}, seeding = {}, never_pause = {}, unpack = {}, client = {}, torrent_route = {},
                        dht = {}, pex = {}, lsd = {}, encryption = {}, upload_cap = {}, forced = {},
                        ssl_certificate = {}, ssl_private_key = {}, bound = {}, archive_passwords = {},
                        peer_tcp = {}, peer_utp = {}, prefer_downloading = {}, prefer_seeding = {},
                        updated_at_ms = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            if changed > 0 {
                write_hosts(tx, id, &patterns).await?;
            }
            Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Returns whether the rule existed; its hosts go with it. A torrent it
    // matched keeps no rule until its trackers are matched again. Its torrent route goes with it,
    // so a rule given its id later starts on the default route.
    fn delete_tracker_rule(&self, id: u32) -> Result<bool, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_tracker_rule", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "DELETE FROM tracker_rules WHERE id = {}",
                            &[SqlArg::I64(i64::from(id))],
                        )
                        .await?;
                    tx.execute(
                        "DELETE FROM proxy_routes WHERE consumer = {}",
                        &[SqlArg::Text(
                            Consumer::Torrent(TorrentScope::TrackerRule(id)).key(),
                        )],
                    )
                    .await?;
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // Add `rule`, or replace the rule `id` names, and return every rule. A
    // rule `keeps_ssl` keeps its identity, and one `keeps_passwords` its
    // archive passwords. A rule a tracker file binds stays
    // bound, and a change to a field the file locked is refused, as is a
    // second rule of one name or for one pattern; the refusal is the inner
    // error.
    fn save_tracker_rule(
        &self,
        id: Option<u32>,
        mut rule: TrackerRule,
        keeps_ssl: bool,
        keeps_passwords: bool,
        now_ms: i64,
    ) -> Result<Result<Vec<TrackerRule>, TrackerRuleRefusal>, StateError> {
        let rules = self.list_tracker_rules()?;
        let existing = id.and_then(|id| rules.iter().find(|other| other.id == id));
        if keeps_ssl {
            rule.ssl = existing.and_then(|other| other.ssl.clone());
        }
        if keeps_passwords {
            rule.archive_passwords = existing
                .map(|other| other.archive_passwords.clone())
                .unwrap_or_default();
        }
        rule.bound = existing.and_then(|other| other.bound.clone());
        if let Some(existing) = existing
            && let Err(refusal) = super::tracker_file::check_locks(existing, &rule)
        {
            return Ok(Err(TrackerRuleRefusal::Locked(refusal)));
        }
        if let Some(refusal) = collision(&rules, id, &rule) {
            return Ok(Err(refusal));
        }
        match id {
            Some(_) if !self.update_tracker_rule(&rule, now_ms)? => {
                return Ok(Err(TrackerRuleRefusal::NotFound(rule.id)));
            }
            Some(_) => {}
            None => {
                self.insert_tracker_rule(&rule, now_ms)?;
            }
        }
        self.list_tracker_rules().map(Ok)
    }

    // Fill the rule that holds one of the file's hosts, or a new one, from
    // `file`, and return the rule. A file two rules' hosts cover, or one
    // whose new rule's name another rule has, is refused as a conflict.
    fn apply_tracker_file(
        &self,
        file: &super::tracker_file::TrackerFile,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError> {
        let rules = self.list_tracker_rules()?;
        let existing = file.rule_in(&rules).map_err(StateError::Conflict)?;
        let rule = file.apply(existing, now_ms);
        if let Some(refusal) = collision(&rules, existing.map(|rule| rule.id), &rule) {
            return Err(StateError::Conflict(refusal.to_string()));
        }
        match existing {
            Some(_) => {
                self.update_tracker_rule(&rule, now_ms)?;
                Ok(rule)
            }
            None => self.insert_tracker_rule(&rule, now_ms),
        }
    }

    // Release the rule `id` names from the tracker file that filled it,
    // keeping every value it holds. Returns whether the rule exists.
    fn unbind_tracker_rule(&self, id: u32, now_ms: i64) -> Result<bool, StateError> {
        let db = self.borrow();
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE tracker_rules SET bound = NULL, updated_at_ms = {} WHERE id = {}",
                &[SqlArg::I64(now_ms), SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

#[cfg(test)]
mod tracker_rule_refusal_tests {
    use super::TrackerRuleRefusal;

    // Each kind words itself as the sentence it replaced.
    #[test]
    fn each_kind_words_itself_as_the_sentence_it_replaced() {
        let cases = [
            (
                TrackerRuleRefusal::Locked("the tracker file locks the name".into()),
                "the tracker file locks the name",
            ),
            (
                TrackerRuleRefusal::NameTaken("Linux".into()),
                "a rule named Linux exists",
            ),
            (
                TrackerRuleRefusal::Covered {
                    pattern: "*.example.org".into(),
                    by: "Linux".into(),
                },
                "*.example.org is covered by Linux",
            ),
            (TrackerRuleRefusal::NotFound(7), "no tracker rule 7"),
        ];
        for (refusal, sentence) in cases {
            assert_eq!(refusal.to_string(), sentence);
        }
    }
}

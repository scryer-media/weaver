// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::persistence::Database;
use crate::persistence::encryption::{EncryptionKey, decrypt_value, encrypt_value, is_encrypted};
use crate::persistence::sql_runtime::{SqlArg, SqlRow, SqlRuntime};
use crate::settings::model::{
    ArchivePasswords, Client, Leaf, SeedingPolicy, TorrentEncryption, TrackerCertificate,
    TrackerRule,
};

#[cfg(test)]
use crate::categories::CategoryConfig;
#[cfg(test)]
use crate::servers::ServerConfig;
#[cfg(test)]
use crate::settings::Config;

const TRACKER_RULE_COLUMNS: &str = "id, host_pattern, seeding, never_pause, unpack, client, torrent_route, dht, pex, lsd, encryption, upload_cap, forced, ssl_certificate, ssl_private_key, bound, archive_passwords";

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
        host_pattern: row.text("host_pattern")?,
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
        SqlArg::Text(rule.host_pattern.clone()),
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
    ])
}

impl Database {
    pub fn list_tracker_rules(&self) -> Result<Vec<TrackerRule>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                &format!("SELECT {TRACKER_RULE_COLUMNS} FROM tracker_rules ORDER BY id"),
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| tracker_rule_from_row(row, key.as_ref()))
                .collect()
        })
    }

    // Add `rule` under the next free id, which it returns with.
    pub fn insert_tracker_rule(
        &self,
        rule: &TrackerRule,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError> {
        let datastore = self.datastore();
        let args = tracker_rule_args(rule, self.encryption_key())?;
        let rule = rule.clone();
        self.run_sql_blocking(async move {
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
                             VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
                        ),
                        &values,
                    )
                    .await?;
                    Ok(rule)
                })
            })
            .await
        })
    }

    // Returns whether the rule exists.
    pub fn update_tracker_rule(&self, rule: &TrackerRule, now_ms: i64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let mut args = tracker_rule_args(rule, self.encryption_key())?;
        args.extend([SqlArg::I64(now_ms), SqlArg::I64(i64::from(rule.id))]);
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE tracker_rules
                    SET host_pattern = {}, seeding = {}, never_pause = {}, unpack = {}, client = {}, torrent_route = {},
                        dht = {}, pex = {}, lsd = {}, encryption = {}, upload_cap = {}, forced = {},
                        ssl_certificate = {}, ssl_private_key = {}, bound = {}, archive_passwords = {},
                        updated_at_ms = {}
                  WHERE id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // Returns whether the rule existed. A torrent it matched keeps no rule
    // until its trackers are matched again.
    pub fn delete_tracker_rule(&self, id: u32) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM tracker_rules WHERE id = {}",
                &[SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // Add `rule`, or replace the rule `id` names, and return every rule. A
    // rule `keeps_ssl` keeps its identity, and one `keeps_passwords` its
    // archive passwords. A rule a tracker file binds stays
    // bound, and a change to a field the file locked is refused, as is a
    // second rule for one pattern; the refusal is the inner error.
    pub fn save_tracker_rule(
        &self,
        id: Option<u32>,
        mut rule: TrackerRule,
        keeps_ssl: bool,
        keeps_passwords: bool,
        now_ms: i64,
    ) -> Result<Result<Vec<TrackerRule>, String>, StateError> {
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
            return Ok(Err(refusal));
        }
        if rules
            .iter()
            .any(|other| other.host_pattern == rule.host_pattern && Some(other.id) != id)
        {
            return Ok(Err(format!("a rule for {} exists", rule.host_pattern)));
        }
        match id {
            Some(_) if !self.update_tracker_rule(&rule, now_ms)? => {
                return Ok(Err(format!("no tracker rule {}", rule.id)));
            }
            Some(_) => {}
            None => {
                self.insert_tracker_rule(&rule, now_ms)?;
            }
        }
        self.list_tracker_rules().map(Ok)
    }

    // Fill the rule whose pattern is the file's, or a new one, from `file`,
    // and return the rule.
    pub fn apply_tracker_file(
        &self,
        file: &super::tracker_file::TrackerFile,
        now_ms: i64,
    ) -> Result<TrackerRule, StateError> {
        let rules = self.list_tracker_rules()?;
        let existing = rules
            .iter()
            .find(|rule| rule.host_pattern == file.host_pattern);
        let rule = file.apply(existing, now_ms);
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
    pub fn unbind_tracker_rule(&self, id: u32, now_ms: i64) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE tracker_rules SET bound = NULL, updated_at_ms = {} WHERE id = {}",
                &[SqlArg::I64(now_ms), SqlArg::I64(i64::from(id))],
            )
            .await?;
            Ok(changed > 0)
        })
    }
}

#[cfg(test)]
mod tests;

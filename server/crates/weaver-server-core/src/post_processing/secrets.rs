//! Named secrets: a value the operator types once, stored encrypted, and
//! linked by reference from any number of instance inputs.
//!
//! A secret's value never leaves this module except to a run that resolves a
//! linked input. Listings carry names and where each secret is used, never
//! values.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::persistence::encryption::encrypt_value;
use crate::persistence::sql_runtime::{
    SqlArg, SqlRuntime, SqlTx, is_foreign_key_violation, is_unique_violation,
};
use crate::persistence::{Database, StateError};

const MAX_SECRET_NAME_BYTES: usize = 128;
const MAX_SECRET_VALUE_BYTES: usize = 64 * 1024;

/// A link from an input to a secret, as an input shows it.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SecretRef {
    pub id: String,
    pub name: String,
}

/// An instance that links a secret.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct SecretUsage {
    pub instance_id: String,
    pub instance_name: String,
}

/// A secret as it is listed: everything but its value.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Secret {
    pub id: String,
    pub name: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// The instances linking it, by instance name.
    pub used_by: Vec<SecretUsage>,
}

#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("a secret with that name already exists")]
    NameTaken,
    #[error("the secret is used by {}", in_use_names(.0))]
    InUse(Vec<SecretUsage>),
    #[error("secret does not exist")]
    NotFound,
    #[error(transparent)]
    Storage(#[from] StateError),
}

fn in_use_names(usages: &[SecretUsage]) -> String {
    if usages.is_empty() {
        return "a script job".to_string();
    }
    usages
        .iter()
        .map(|usage| usage.instance_name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// The form a name is compared in: names that differ only by case are one
/// name.
pub(crate) fn secret_name_key(name: &str) -> String {
    name.to_lowercase()
}

fn valid_name(name: &str) -> Result<String, SecretError> {
    let name = name.trim();
    if name.is_empty() || name.len() > MAX_SECRET_NAME_BYTES || name.chars().any(char::is_control) {
        return Err(SecretError::Invalid(
            "secret name must be 1 to 128 bytes without control characters",
        ));
    }
    Ok(name.to_string())
}

fn valid_value(value: &str) -> Result<(), SecretError> {
    if value.len() > MAX_SECRET_VALUE_BYTES || value.contains('\0') {
        return Err(SecretError::Invalid("secret value is invalid"));
    }
    Ok(())
}

/// How a change inside a transaction came out.
enum Outcome {
    Done,
    NotFound,
    NameTaken,
    InUse(Vec<SecretUsage>),
}

impl Database {
    fn seal_secret(&self, value: &str) -> Result<String, SecretError> {
        valid_value(value)?;
        let key = self.encryption_key().ok_or(SecretError::Invalid(
            "an encryption key is required to store a secret",
        ))?;
        encrypt_value(key, value).map_err(|error| SecretError::Storage(StateError::Database(error)))
    }

    /// Every secret, by name, with the instances that link it. Values are
    /// never read.
    pub fn secrets(&self) -> Result<Vec<Secret>, StateError> {
        let mut usages = self.secret_usages()?;
        let datastore = self.datastore();
        let rows = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT id, name, created_at_ms, updated_at_ms FROM secrets
                  ORDER BY name_key, id",
                &[],
            )
            .await
        })?;
        rows.into_iter()
            .map(|row| {
                let id = row.text("id")?;
                Ok(Secret {
                    used_by: usages.remove(&id).unwrap_or_default(),
                    name: row.text("name")?,
                    created_at_ms: row.i64("created_at_ms")?,
                    updated_at_ms: row.i64("updated_at_ms")?,
                    id,
                })
            })
            .collect()
    }

    pub fn secret(&self, id: &str) -> Result<Option<Secret>, StateError> {
        Ok(self.secrets()?.into_iter().find(|secret| secret.id == id))
    }

    /// For each linked secret, the instances that link it, by instance name.
    pub fn secret_usages(&self) -> Result<BTreeMap<String, Vec<SecretUsage>>, StateError> {
        let datastore = self.datastore();
        let rows = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT DISTINCT i.secret_id, s.id AS instance_id, s.name AS instance_name
                   FROM script_instance_inputs i
                   JOIN script_instances s ON s.id = i.instance_id
                  WHERE i.secret_id IS NOT NULL",
                &[],
            )
            .await
        })?;
        let mut usages = BTreeMap::<String, Vec<SecretUsage>>::new();
        for row in rows {
            usages
                .entry(row.text("secret_id")?)
                .or_default()
                .push(SecretUsage {
                    instance_id: row.text("instance_id")?,
                    instance_name: row.text("instance_name")?,
                });
        }
        for list in usages.values_mut() {
            list.sort_by(|a, b| {
                (a.instance_name.to_lowercase(), &a.instance_id)
                    .cmp(&(b.instance_name.to_lowercase(), &b.instance_id))
            });
        }
        Ok(usages)
    }

    pub fn create_secret(&self, name: &str, value: &str) -> Result<Secret, SecretError> {
        let name = valid_name(name)?;
        let sealed = self.seal_secret(value)?;
        let mut entropy = [0_u8; 12];
        getrandom::fill(&mut entropy)
            .map_err(|error| SecretError::Storage(StateError::Database(error.to_string())))?;
        let id = hex::encode(entropy);
        let datastore = self.datastore();
        let now = chrono::Utc::now().timestamp_millis();
        let created = id.clone();
        let outcome = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_secret", |tx| {
                let (id, name, sealed) = (id.clone(), name.clone(), sealed.clone());
                Box::pin(async move {
                    if name_taken_tx(tx, &name, None).await? {
                        return Ok(Outcome::NameTaken);
                    }
                    tx.execute(
                        "INSERT INTO secrets
                            (id, name, name_key, value, created_at_ms, updated_at_ms)
                         VALUES ({}, {}, {}, {}, {}, {})",
                        &[
                            SqlArg::Text(id),
                            SqlArg::Text(name.clone()),
                            SqlArg::Text(secret_name_key(&name)),
                            SqlArg::Text(sealed),
                            SqlArg::I64(now),
                            SqlArg::I64(now),
                        ],
                    )
                    .await?;
                    Ok(Outcome::Done)
                })
            })
            .await
        });
        self.finish(name_race_outcome(outcome)?, &created)
    }

    /// Rename a secret, give it a new value, or both. A run started after
    /// this resolves the new value.
    pub fn update_secret(
        &self,
        id: &str,
        name: Option<&str>,
        value: Option<&str>,
    ) -> Result<Secret, SecretError> {
        let name = name.map(valid_name).transpose()?;
        let sealed = value.map(|value| self.seal_secret(value)).transpose()?;
        let datastore = self.datastore();
        let now = chrono::Utc::now().timestamp_millis();
        let target = id.to_string();
        let outcome = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "update_secret", |tx| {
                let (id, name, sealed) = (target.clone(), name.clone(), sealed.clone());
                Box::pin(async move {
                    if tx
                        .fetch_optional(
                            "SELECT id FROM secrets WHERE id = {}",
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?
                        .is_none()
                    {
                        return Ok(Outcome::NotFound);
                    }
                    if let Some(name) = &name {
                        if name_taken_tx(tx, name, Some(&id)).await? {
                            return Ok(Outcome::NameTaken);
                        }
                        tx.execute(
                            "UPDATE secrets SET name = {}, name_key = {}, updated_at_ms = {}
                              WHERE id = {}",
                            &[
                                SqlArg::Text(name.clone()),
                                SqlArg::Text(secret_name_key(name)),
                                SqlArg::I64(now),
                                SqlArg::Text(id.clone()),
                            ],
                        )
                        .await?;
                    }
                    if let Some(sealed) = sealed {
                        tx.execute(
                            "UPDATE secrets SET value = {}, updated_at_ms = {} WHERE id = {}",
                            &[SqlArg::Text(sealed), SqlArg::I64(now), SqlArg::Text(id)],
                        )
                        .await?;
                    }
                    Ok(Outcome::Done)
                })
            })
            .await
        });
        self.finish(name_race_outcome(outcome)?, id)
    }

    /// Remove a secret. Refused while any instance links it.
    pub fn delete_secret(&self, id: &str) -> Result<(), SecretError> {
        let datastore = self.datastore();
        let target = id.to_string();
        let outcome = self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_secret", |tx| {
                let id = target.clone();
                Box::pin(async move {
                    let mut usages = tx
                        .fetch_all(
                            "SELECT DISTINCT s.id AS instance_id, s.name AS instance_name
                               FROM script_instance_inputs i
                               JOIN script_instances s ON s.id = i.instance_id
                              WHERE i.secret_id = {}",
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?
                        .into_iter()
                        .map(|row| {
                            Ok(SecretUsage {
                                instance_id: row.text("instance_id")?,
                                instance_name: row.text("instance_name")?,
                            })
                        })
                        .collect::<Result<Vec<_>, StateError>>()?;
                    if !usages.is_empty() {
                        usages.sort();
                        return Ok(Outcome::InUse(usages));
                    }
                    let deleted = tx
                        .execute("DELETE FROM secrets WHERE id = {}", &[SqlArg::Text(id)])
                        .await?;
                    Ok(if deleted == 0 {
                        Outcome::NotFound
                    } else {
                        Outcome::Done
                    })
                })
            })
            .await
        });
        let outcome = match outcome {
            Ok(outcome) => outcome,
            // An instance linked the secret between the check and the delete;
            // the foreign key refused it. Name whoever links it now.
            Err(error) if is_foreign_key_violation(&error) => Outcome::InUse(
                self.secret_usages()?
                    .remove(id)
                    .map(|mut usages| {
                        usages.sort();
                        usages
                    })
                    .unwrap_or_default(),
            ),
            Err(error) => return Err(error.into()),
        };
        match outcome {
            Outcome::Done => Ok(()),
            Outcome::NotFound => Err(SecretError::NotFound),
            Outcome::NameTaken => Err(SecretError::NameTaken),
            Outcome::InUse(usages) => Err(SecretError::InUse(usages)),
        }
    }

    fn finish(&self, outcome: Outcome, id: &str) -> Result<Secret, SecretError> {
        match outcome {
            Outcome::Done => self.secret(id)?.ok_or(SecretError::NotFound),
            Outcome::NotFound => Err(SecretError::NotFound),
            Outcome::NameTaken => Err(SecretError::NameTaken),
            Outcome::InUse(usages) => Err(SecretError::InUse(usages)),
        }
    }
}

/// A name write that lost a race to another with the same name hits the
/// unique index on `name_key`: that is the name being taken, not a failure.
fn name_race_outcome(result: Result<Outcome, StateError>) -> Result<Outcome, SecretError> {
    match result {
        Ok(outcome) => Ok(outcome),
        Err(error) if is_unique_violation(&error) => Ok(Outcome::NameTaken),
        Err(error) => Err(error.into()),
    }
}

/// Whether every id names a stored secret, read inside `tx`.
pub(crate) async fn secrets_exist_tx(
    tx: &mut SqlTx<'_>,
    ids: &[String],
) -> Result<bool, StateError> {
    for id in ids {
        if tx
            .fetch_optional(
                "SELECT id FROM secrets WHERE id = {}",
                &[SqlArg::Text(id.clone())],
            )
            .await?
            .is_none()
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn name_taken_tx(
    tx: &mut SqlTx<'_>,
    name: &str,
    except: Option<&str>,
) -> Result<bool, StateError> {
    Ok(tx
        .fetch_optional(
            "SELECT id FROM secrets WHERE name_key = {}",
            &[SqlArg::Text(secret_name_key(name))],
        )
        .await?
        .map(|row| row.text("id"))
        .transpose()?
        .is_some_and(|found| Some(found.as_str()) != except))
}

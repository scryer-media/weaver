use super::service::{decode_jwt_secret, encode_jwt_secret, generate_jwt_secret};
use crate::StateError;
use crate::persistence::Database;
use crate::persistence::encryption::{decrypt_value, encrypt_secret_for_write, is_encrypted};
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};

const JWT_SIGNING_SECRET_SETTING_KEY: &str = "auth.jwt_signing_secret";

/// Stored login credentials (single user).
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    pub username: String,
    pub password_hash: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Database {
    /// Load the persistent JWT signing secret, creating one on first use.
    pub fn get_or_create_jwt_signing_secret(&self) -> Result<[u8; 32], StateError> {
        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "get_or_create_jwt_signing_secret", |tx| {
                let encryption_key = encryption_key.clone();
                Box::pin(async move {
                    get_or_create_jwt_signing_secret_tx(tx, encryption_key.as_ref()).await
                })
            })
            .await
        })
    }

    /// Replace the persistent JWT signing secret and return the new value.
    pub fn rotate_jwt_signing_secret(&self) -> Result<[u8; 32], StateError> {
        let datastore = self.datastore();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "rotate_jwt_signing_secret", |tx| {
                let encryption_key = encryption_key.clone();
                Box::pin(
                    async move { rotate_jwt_signing_secret_tx(tx, encryption_key.as_ref()).await },
                )
            })
            .await
        })
    }

    /// Get the stored login credentials, if any.
    pub fn get_auth_credentials(&self) -> Result<Option<AuthCredentials>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT username, password_hash, created_at, updated_at FROM auth_credentials WHERE id = 1",
                &[],
            )
            .await?
            .map(|row| {
                Ok(AuthCredentials {
                    username: row.text("username")?,
                    password_hash: row.text("password_hash")?,
                    created_at: row.i64("created_at")?,
                    updated_at: row.i64("updated_at")?,
                })
            })
            .transpose()
        })
    }

    /// Set (insert or replace) login credentials.
    pub fn set_auth_credentials(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let username = username.to_string();
        let password_hash = password_hash.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO auth_credentials (id, username, password_hash, created_at, updated_at)
                 VALUES (1, {}, {}, {}, {})
                 ON CONFLICT(id) DO UPDATE SET username = excluded.username, password_hash = excluded.password_hash, updated_at = excluded.updated_at",
                &[
                    SqlArg::Text(username),
                    SqlArg::Text(password_hash),
                    SqlArg::I64(now),
                    SqlArg::I64(now),
                ],
            )
            .await?;
            Ok(())
        })
    }

    /// Clear login credentials (disable login).
    pub fn clear_auth_credentials(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM auth_credentials WHERE id = 1",
                &[],
            )
            .await?;
            Ok(())
        })
    }
}

async fn get_or_create_jwt_signing_secret_tx(
    tx: &mut SqlTx<'_>,
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<[u8; 32], StateError> {
    let select_sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    if let Some(stored) = select_jwt_signing_secret_tx(tx, select_sql).await? {
        let secret = decode_stored_jwt_signing_secret(&stored, encryption_key)?;
        if !is_encrypted(&stored) && encryption_key.is_some() {
            persist_jwt_signing_secret_tx(tx, &secret, encryption_key).await?;
        }
        return Ok(secret);
    }

    let generated = generate_jwt_secret();
    let stored = encode_stored_jwt_signing_secret(&generated, encryption_key)?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {})
         ON CONFLICT(key) DO NOTHING",
        &[
            SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string()),
            SqlArg::Text(stored),
        ],
    )
    .await?;

    select_jwt_signing_secret_tx(tx, select_sql)
        .await?
        .ok_or_else(|| StateError::Database("JWT signing secret was not persisted".to_string()))
        .and_then(|stored| decode_stored_jwt_signing_secret(&stored, encryption_key))
}

async fn select_jwt_signing_secret_tx(
    tx: &mut SqlTx<'_>,
    sql: &str,
) -> Result<Option<String>, StateError> {
    tx.fetch_optional(
        sql,
        &[SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string())],
    )
    .await?
    .map(|row| row.text("value"))
    .transpose()
}

async fn rotate_jwt_signing_secret_tx(
    tx: &mut SqlTx<'_>,
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<[u8; 32], StateError> {
    let generated = generate_jwt_secret();
    upsert_jwt_signing_secret_tx(tx, &generated, encryption_key).await?;
    Ok(generated)
}

async fn upsert_jwt_signing_secret_tx(
    tx: &mut SqlTx<'_>,
    secret: &[u8; 32],
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<(), StateError> {
    let stored = encode_stored_jwt_signing_secret(secret, encryption_key)?;
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {})
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        &[
            SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string()),
            SqlArg::Text(stored),
        ],
    )
    .await?;
    Ok(())
}

async fn persist_jwt_signing_secret_tx(
    tx: &mut SqlTx<'_>,
    secret: &[u8; 32],
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<(), StateError> {
    let stored = encode_stored_jwt_signing_secret(secret, encryption_key)?;
    tx.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text(stored),
            SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string()),
        ],
    )
    .await?;
    Ok(())
}

fn encode_stored_jwt_signing_secret(
    secret: &[u8; 32],
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<String, StateError> {
    let encoded = encode_jwt_secret(secret);
    encrypt_secret_for_write(encryption_key, &Some(encoded))
        .map_err(|error| {
            StateError::Database(format!("failed to encrypt JWT signing secret: {error}"))
        })?
        .ok_or_else(|| StateError::Database("JWT signing secret cannot be empty".to_string()))
}

fn decode_stored_jwt_signing_secret(
    stored: &str,
    encryption_key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<[u8; 32], StateError> {
    let encoded = if is_encrypted(stored) {
        let Some(key) = encryption_key else {
            return Err(StateError::Database(
                "JWT signing secret is encrypted but no encryption key is available".to_string(),
            ));
        };
        decrypt_value(key, stored).map_err(|error| {
            StateError::Database(format!("failed to decrypt JWT signing secret: {error}"))
        })?
    } else {
        stored.to_string()
    };
    decode_jwt_secret(&encoded)
        .map_err(|error| StateError::Database(format!("invalid JWT signing secret: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_signing_secret_is_persisted() {
        let db = Database::open_in_memory().unwrap();
        let first = db.get_or_create_jwt_signing_secret().unwrap();
        let second = db.get_or_create_jwt_signing_secret().unwrap();
        assert_eq!(first, second);

        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
        assert_eq!(
            decode_stored_jwt_signing_secret(&stored, db.encryption_key()).unwrap(),
            first
        );
    }

    #[test]
    fn malformed_jwt_signing_secret_fails_closed() {
        let db = Database::open_in_memory().unwrap();
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, "not-hex")
            .unwrap();
        let err = db.get_or_create_jwt_signing_secret().unwrap_err();
        assert!(err.to_string().contains("invalid JWT signing secret"));
    }

    #[test]
    fn jwt_signing_secret_rotation_replaces_persisted_value() {
        let db = Database::open_in_memory().unwrap();
        let first = db.get_or_create_jwt_signing_secret().unwrap();
        let rotated = db.rotate_jwt_signing_secret().unwrap();
        assert_ne!(first, rotated);
        assert_eq!(db.get_or_create_jwt_signing_secret().unwrap(), rotated);
    }

    #[test]
    fn plaintext_jwt_signing_secret_is_reencrypted() {
        let db = Database::open_in_memory().unwrap();
        let secret = [11u8; 32];
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, &encode_jwt_secret(&secret))
            .unwrap();

        assert_eq!(db.get_or_create_jwt_signing_secret().unwrap(), secret);
        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
    }
}

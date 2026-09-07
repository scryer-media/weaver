use super::service::{decode_jwt_secret, encode_jwt_secret, generate_jwt_secret};
use crate::StateError;
use crate::persistence::Database;
use crate::persistence::encryption::{decrypt_value, encrypt_secret_for_write, is_encrypted};
use crate::persistence::sql_runtime::{SqlArg, SqlExec, SqlRuntime, SqlTx};

const JWT_SIGNING_SECRET_SETTING_KEY: &str = "auth.jwt_signing_secret";
pub const SETUP_COMPLETED_SETTING_KEY: &str = "auth.setup_completed_v1";
pub const SETUP_PENDING_SETTING_KEY: &str = "auth.setup_pending_v1";

#[derive(Debug, Clone)]
pub struct InitialAuthenticatedSetup {
    pub username: String,
    pub password_hash: String,
    pub jwt_secret: [u8; 32],
    pub browser_session: BrowserSession,
    pub completed_at: i64,
    pub bind_address: Option<String>,
    pub trusted_networks: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialSetupOutcome {
    Created,
    AlreadyCompleted,
}

/// Stored login credentials (single user).
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    pub username: String,
    pub password_hash: String,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Server-side record for one browser credential. Raw credentials and CSRF
/// values never enter this type: callers pass their one-way verifiers only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserSession {
    pub token_hash: String,
    pub csrf_verifier: String,
    pub origin: String,
    pub client_ip: Option<String>,
    pub remembered: bool,
    pub created_at: i64,
    pub expires_at: i64,
    pub revoked_at: Option<i64>,
}

impl Database {
    /// Persist the explicit pending state before exposing first-run setup.
    /// Callers must have independently established that this datastore is new.
    pub fn mark_initial_setup_pending(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "mark_initial_setup_pending", |tx| {
                Box::pin(async move {
                    for (key, value) in [
                        (SETUP_PENDING_SETTING_KEY, "pending"),
                        (crate::security::SETTING_SECURITY_POLICY_REVISION, crate::security::AUTHENTICATED_POLICY_REVISION),
                    ] {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[SqlArg::Text(key.to_string()), SqlArg::Text(value.to_string())],
                        ).await?;
                    }
                    Ok(())
                })
            }).await
        })
    }

    /// Atomically establishes a first-run authenticated installation. The
    /// completion marker is the transaction claim: a concurrent setup loses
    /// without changing credentials or browser sessions, and any later error
    /// rolls that claim back with the rest of the transaction.
    pub fn complete_initial_authenticated_setup(
        &self,
        setup: &InitialAuthenticatedSetup,
    ) -> Result<InitialSetupOutcome, StateError> {
        let datastore = self.datastore();
        let setup = setup.clone();
        let encryption_key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "complete_initial_authenticated_setup", |tx| {
                let setup = setup.clone();
                let encryption_key = encryption_key.clone();
                Box::pin(async move {
                    if SqlRuntime::fetch_optional(
                        SqlExec::Tx(tx),
                        "SELECT key FROM settings WHERE key = {}",
                        &[SqlArg::Text(SETUP_PENDING_SETTING_KEY.to_string())],
                    )
                    .await?
                    .is_none()
                    {
                        return Ok(InitialSetupOutcome::AlreadyCompleted);
                    }
                    if SqlRuntime::fetch_optional(SqlExec::Tx(tx), "SELECT id FROM auth_credentials WHERE id = 1", &[]).await?.is_some() {
                        return Ok(InitialSetupOutcome::AlreadyCompleted);
                    }
                    let claimed = tx.execute(
                        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO NOTHING",
                        &[SqlArg::Text(SETUP_COMPLETED_SETTING_KEY.to_string()), SqlArg::Text(setup.completed_at.to_string())],
                    ).await?;
                    if claimed == 0 {
                        return Ok(InitialSetupOutcome::AlreadyCompleted);
                    }
                    tx.execute(
                        "INSERT INTO auth_credentials (id, username, password_hash, created_at, updated_at) VALUES (1, {}, {}, {}, {})",
                        &[SqlArg::Text(setup.username), SqlArg::Text(setup.password_hash), SqlArg::I64(setup.completed_at), SqlArg::I64(setup.completed_at)],
                    ).await?;
                    let secret = encode_stored_jwt_signing_secret(&setup.jwt_secret, encryption_key.as_ref())?;
                    tx.execute(
                        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        &[SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string()), SqlArg::Text(secret)],
                    ).await?;
                    if let Some(bind_address) = setup.bind_address {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[SqlArg::Text(crate::security::SETTING_HTTP_BIND_ADDRESS.to_string()), SqlArg::Text(bind_address)],
                        ).await?;
                    }
                    if let Some(trusted_networks) = setup.trusted_networks {
                        tx.execute(
                            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                            &[SqlArg::Text(crate::security::SETTING_TRUSTED_NETWORKS.to_string()), SqlArg::Text(trusted_networks)],
                        ).await?;
                    }
                    tx.execute(
                        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        &[SqlArg::Text(crate::security::SETTING_ACCESS_MODE.to_string()), SqlArg::Text("login_required".to_string())],
                    ).await?;
                    tx.execute(
                        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                        &[SqlArg::Text(crate::security::SETTING_SECURITY_POLICY_REVISION.to_string()), SqlArg::Text(crate::security::AUTHENTICATED_POLICY_REVISION.to_string())],
                    ).await?;
                    let session = setup.browser_session;
                    tx.execute(
                        "INSERT INTO browser_sessions (token_hash, csrf_verifier, origin, client_ip, remembered, created_at, expires_at, revoked_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                        &[SqlArg::Text(session.token_hash.clone()), SqlArg::Text(session.csrf_verifier), SqlArg::Text(session.origin), SqlArg::OptText(session.client_ip), SqlArg::Bool(session.remembered), SqlArg::I64(session.created_at), SqlArg::I64(session.expires_at), SqlArg::OptI64(session.revoked_at)],
                    ).await?;
                    tx.execute(
                        "INSERT INTO browser_session_verifications (token_hash, verified_at) VALUES ({}, {})",
                        &[SqlArg::Text(session.token_hash), SqlArg::I64(session.created_at)],
                    ).await?;
                    tx.execute(
                        "DELETE FROM settings WHERE key = {}",
                        &[SqlArg::Text(SETUP_PENDING_SETTING_KEY.to_string())],
                    ).await?;
                    Ok(InitialSetupOutcome::Created)
                })
            }).await
        })
    }
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
            // Unconditional single-statement upsert (no read, no guard), so run it
            // as one autocommit statement rather than wrapping it in a redundant
            // BEGIN/COMMIT round-trip. Mirrors `set_auth_credentials`; the
            // `rotate_jwt_signing_secret_tx` helper stays for the in-transaction
            // reuse inside `get_or_create_jwt_signing_secret_tx`.
            let generated = generate_jwt_secret();
            let stored = encode_stored_jwt_signing_secret(&generated, encryption_key.as_ref())?;
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO settings (key, value) VALUES ({}, {})
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                &[
                    SqlArg::Text(JWT_SIGNING_SECRET_SETTING_KEY.to_string()),
                    SqlArg::Text(stored),
                ],
            )
            .await?;
            Ok(generated)
        })
    }

    /// Get the stored login credentials, if any.
    pub fn get_auth_credentials(&self) -> Result<Option<AuthCredentials>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
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

    /// Atomically changes the only administrator password and invalidates all
    /// durable browser sessions that could have been established with it.
    pub fn change_auth_credentials_and_revoke_sessions(
        &self,
        username: &str,
        password_hash: &str,
        now: i64,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let username = username.to_string();
        let password_hash = password_hash.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "change_auth_credentials", |tx| {
                let username = username.clone();
                let password_hash = password_hash.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE auth_credentials SET username = {}, password_hash = {}, updated_at = {} WHERE id = 1",
                        &[SqlArg::Text(username), SqlArg::Text(password_hash), SqlArg::I64(now)],
                    ).await?;
                    tx.execute(
                        "UPDATE browser_sessions SET revoked_at = {} WHERE revoked_at IS NULL",
                        &[SqlArg::I64(now)],
                    ).await?;
                    Ok(())
                })
            }).await
        })
    }

    /// Clear login credentials (disable login).
    pub fn clear_auth_credentials(&self) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            SqlRuntime::run_in_transaction(&datastore, "clear_auth_credentials", |tx| {
                Box::pin(async move {
                    tx.execute("DELETE FROM auth_credentials WHERE id = 1", &[])
                        .await?;
                    tx.execute(
                        "UPDATE browser_sessions SET revoked_at = {} WHERE revoked_at IS NULL",
                        &[SqlArg::I64(now)],
                    )
                    .await?;
                    tx.execute(
                        "DELETE FROM settings WHERE key = {}",
                        &[SqlArg::Text(SETUP_COMPLETED_SETTING_KEY.to_string())],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn create_browser_session(&self, session: &BrowserSession) -> Result<(), StateError> {
        let datastore = self.datastore();
        let session = session.clone();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_browser_session", |tx| {
                let session = session.clone();
                Box::pin(async move {
                    let token_hash = session.token_hash.clone();
                    let verified_at = session.created_at;
                    tx.execute(
                        "INSERT INTO browser_sessions (token_hash, csrf_verifier, origin, client_ip, remembered, created_at, expires_at, revoked_at)
                         VALUES ({}, {}, {}, {}, {}, {}, {}, {})",
                        &[
                            SqlArg::Text(session.token_hash),
                            SqlArg::Text(session.csrf_verifier),
                            SqlArg::Text(session.origin),
                            SqlArg::OptText(session.client_ip),
                            SqlArg::Bool(session.remembered),
                            SqlArg::I64(session.created_at),
                            SqlArg::I64(session.expires_at),
                            SqlArg::OptI64(session.revoked_at),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "INSERT INTO browser_session_verifications (token_hash, verified_at) VALUES ({}, {})",
                        &[SqlArg::Text(token_hash), SqlArg::I64(verified_at)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn get_active_browser_session(
        &self,
        token_hash: &str,
        now: i64,
    ) -> Result<Option<BrowserSession>, StateError> {
        let datastore = self.datastore();
        let token_hash = token_hash.to_string();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT token_hash, csrf_verifier, origin, client_ip, remembered, created_at, expires_at, revoked_at
                 FROM browser_sessions WHERE token_hash = {} AND revoked_at IS NULL AND expires_at > {}",
                &[SqlArg::Text(token_hash), SqlArg::I64(now)],
            )
            .await?
            .map(|row| {
                Ok(BrowserSession {
                    token_hash: row.text("token_hash")?,
                    csrf_verifier: row.text("csrf_verifier")?,
                    origin: row.text("origin")?,
                    client_ip: row.opt_text("client_ip")?,
                    remembered: row.bool("remembered")?,
                    created_at: row.i64("created_at")?,
                    expires_at: row.i64("expires_at")?,
                    revoked_at: row.opt_i64("revoked_at")?,
                })
            })
            .transpose()
        })
    }

    pub fn revoke_browser_session(&self, token_hash: &str, now: i64) -> Result<(), StateError> {
        let datastore = self.datastore();
        let token_hash = token_hash.to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE browser_sessions SET revoked_at = {} WHERE token_hash = {} AND revoked_at IS NULL",
                &[SqlArg::I64(now), SqlArg::Text(token_hash)],
            )
            .await
            .map(|_| ())
        })
    }

    pub fn revoke_all_browser_sessions(&self, now: i64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE browser_sessions SET revoked_at = {} WHERE revoked_at IS NULL",
                &[SqlArg::I64(now)],
            )
            .await
            .map(|_| ())
        })
    }

    /// Record a successful password check for one active browser session.
    /// The session token hash is already a durable verifier, so this table
    /// stores no additional credential material.
    pub fn verify_browser_session_password(
        &self,
        token_hash: &str,
        verified_at: i64,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let token_hash = token_hash.to_string();
        self.run_sql_blocking(async move {
            let updated = SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO browser_session_verifications (token_hash, verified_at)
                 SELECT token_hash, {} FROM browser_sessions
                 WHERE token_hash = {} AND revoked_at IS NULL AND expires_at > {}
                 ON CONFLICT(token_hash) DO UPDATE SET verified_at = excluded.verified_at",
                &[
                    SqlArg::I64(verified_at),
                    SqlArg::Text(token_hash),
                    SqlArg::I64(verified_at),
                ],
            )
            .await?;
            if updated == 0 {
                return Err(StateError::Database(
                    "browser session is no longer active".to_string(),
                ));
            }
            Ok(())
        })
    }

    pub fn browser_session_password_verified_at(
        &self,
        token_hash: &str,
        now: i64,
    ) -> Result<Option<i64>, StateError> {
        let datastore = self.datastore();
        let token_hash = token_hash.to_string();
        self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT verification.verified_at
                 FROM browser_session_verifications verification
                 JOIN browser_sessions session ON session.token_hash = verification.token_hash
                 WHERE verification.token_hash = {} AND session.revoked_at IS NULL AND session.expires_at > {}",
                &[SqlArg::Text(token_hash), SqlArg::I64(now)],
            )
            .await?
            .map(|row| row.i64("verified_at"))
            .transpose()
        })
    }

    pub(crate) fn has_encrypted_persisted_jwt_signing_secret(&self) -> Result<bool, StateError> {
        Ok(self
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)?
            .is_some_and(|stored| is_encrypted(&stored)))
    }

    pub(crate) fn validate_persisted_jwt_signing_secret(
        &self,
        encryption_key: &crate::persistence::encryption::EncryptionKey,
    ) -> Result<(), StateError> {
        let Some(stored) = self.get_setting(JWT_SIGNING_SECRET_SETTING_KEY)? else {
            return Ok(());
        };
        decode_stored_jwt_signing_secret(&stored, Some(encryption_key)).map(|_| ())
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
        let secret = match decode_stored_jwt_signing_secret(&stored, encryption_key) {
            Ok(secret) => secret,
            Err(error) if is_encrypted(&stored) && encryption_key.is_some() => {
                tracing::warn!(
                    %error,
                    "rotating JWT signing secret after decrypt failure; existing login sessions will be invalidated"
                );
                rotate_jwt_signing_secret_tx(tx, encryption_key).await?
            }
            Err(error) => return Err(error),
        };
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

    fn browser_session(token_hash: &str, created_at: i64) -> BrowserSession {
        BrowserSession {
            token_hash: token_hash.to_string(),
            csrf_verifier: format!("csrf-{token_hash}"),
            origin: "http://localhost".to_string(),
            client_ip: Some("127.0.0.1".to_string()),
            remembered: true,
            created_at,
            expires_at: created_at + 3_600,
            revoked_at: None,
        }
    }

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
    fn encrypted_jwt_signing_secret_with_wrong_key_is_rotated() {
        let db = Database::open_in_memory().unwrap();
        let stale_secret = [7u8; 32];
        let wrong_key = crate::persistence::encryption::EncryptionKey::generate();
        let stale_stored = crate::persistence::encryption::encrypt_value(
            &wrong_key,
            &encode_jwt_secret(&stale_secret),
        )
        .unwrap();
        db.set_setting(JWT_SIGNING_SECRET_SETTING_KEY, &stale_stored)
            .unwrap();

        let rotated = db.get_or_create_jwt_signing_secret().unwrap();
        assert_ne!(rotated, stale_secret);

        let stored = db
            .get_setting(JWT_SIGNING_SECRET_SETTING_KEY)
            .unwrap()
            .unwrap();
        assert!(crate::persistence::encryption::is_encrypted(&stored));
        assert_eq!(
            decode_stored_jwt_signing_secret(&stored, db.encryption_key()).unwrap(),
            rotated
        );
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

    #[test]
    fn password_verification_is_session_bound_and_revocation_is_live() {
        let db = Database::open_in_memory().unwrap();
        db.create_browser_session(&browser_session("first", 1_000))
            .unwrap();
        db.create_browser_session(&browser_session("second", 1_000))
            .unwrap();

        db.verify_browser_session_password("first", 1_100).unwrap();
        assert_eq!(
            db.browser_session_password_verified_at("first", 1_101)
                .unwrap(),
            Some(1_100)
        );
        assert_eq!(
            db.browser_session_password_verified_at("second", 1_101)
                .unwrap(),
            Some(1_000)
        );

        db.revoke_all_browser_sessions(1_102).unwrap();
        assert_eq!(
            db.browser_session_password_verified_at("first", 1_103)
                .unwrap(),
            None
        );
        assert_eq!(
            db.browser_session_password_verified_at("second", 1_103)
                .unwrap(),
            None
        );
    }
}

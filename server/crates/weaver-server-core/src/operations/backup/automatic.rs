use super::manifest::io_err;
use super::{BackupInfo, BackupService, BackupServiceError, BackupTrigger};
use crate::persistence::encryption::{decrypt_value, encrypt_value};
use chrono::{DateTime, Duration, Local, LocalResult, TimeZone, Utc};
use serde::{Deserialize, Serialize};

pub(super) const AUTO_SETTINGS_KEY: &str = "backup.automatic";

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct StoredAutoSettings {
    pub enabled: bool,
    pub daily_time_local: String,
    pub encrypted_key: Option<String>,
}

pub(super) fn decode_auto_settings(value: &str) -> Result<StoredAutoSettings, String> {
    let value: serde_json::Value = match serde_json::from_str(value) {
        Ok(value) => value,
        Err(error) => {
            tracing::error!(%error, "corrupt automatic backup settings disabled in memory; original settings retained for recovery");
            return Ok(StoredAutoSettings::default());
        }
    };
    let Some(object) = value.as_object() else {
        tracing::error!(
            "non-object automatic backup settings disabled in memory; original settings retained for recovery"
        );
        return Ok(StoredAutoSettings::default());
    };
    let encrypted_key = match object.get("encrypted_key") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(key)) => Some(key.clone()),
        Some(_) => {
            tracing::error!(
                "automatic backup settings with invalid password type disabled in memory; original settings retained for recovery"
            );
            return Ok(StoredAutoSettings::default());
        }
    };
    match serde_json::from_value(value) {
        Ok(settings) => Ok(settings),
        Err(error) => {
            tracing::warn!(%error, "invalid automatic backup schedule disabled; stored encryption key preserved");
            Ok(StoredAutoSettings {
                encrypted_key,
                ..StoredAutoSettings::default()
            })
        }
    }
}

impl crate::Database {
    pub(crate) fn automatic_backup_ciphertext(&self) -> Result<Option<String>, crate::StateError> {
        self.get_setting(AUTO_SETTINGS_KEY)?
            .map(|value| {
                decode_auto_settings(&value)
                    .map(|settings| settings.encrypted_key)
                    .map_err(|error| {
                        crate::StateError::Database(format!(
                            "invalid automatic backup settings: {error}"
                        ))
                    })
            })
            .transpose()
            .map(Option::flatten)
    }
}

impl Default for StoredAutoSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            daily_time_local: "03:00".into(),
            encrypted_key: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AutoBackupSettings {
    pub enabled: bool,
    pub daily_time_local: String,
    pub auto_backup_key_present: bool,
    pub next_run_at: Option<String>,
}

pub struct AutoBackupSettingsInput {
    pub enabled: bool,
    pub daily_time_local: String,
    pub set_auto_backup_key: Option<String>,
    pub clear_auto_backup_key: bool,
}

pub fn compute_next_auto_backup_run_at(
    daily_time_local: &str,
    now: DateTime<Local>,
) -> Result<DateTime<Local>, BackupServiceError> {
    compute_next_run(daily_time_local, now, |candidate| {
        Local.from_local_datetime(&candidate)
    })
}

fn compute_next_run<Tz: TimeZone>(
    daily_time_local: &str,
    now: DateTime<Tz>,
    resolve: impl Fn(chrono::NaiveDateTime) -> LocalResult<DateTime<Tz>>,
) -> Result<DateTime<Tz>, BackupServiceError> {
    let time = crate::bandwidth::schedule::parse_time(daily_time_local)
        .filter(|_| daily_time_local.len() == 5)
        .ok_or_else(|| BackupServiceError::Validation("daily time must use HH:MM format".into()))?;
    for day in 0..=1 {
        let candidate = (now.date_naive() + Duration::days(day)).and_time(time);
        for minute in 0..=180 {
            let resolved = match resolve(candidate + Duration::minutes(minute)) {
                LocalResult::Single(value) => Some(value),
                LocalResult::Ambiguous(first, second) => Some(first.min(second)),
                LocalResult::None => None,
            };
            if let Some(value) = resolved {
                if value >= now {
                    return Ok(value);
                }
                break;
            }
        }
    }
    Err(BackupServiceError::Validation(
        "could not resolve the next local backup time".into(),
    ))
}

impl BackupService {
    pub(super) async fn stored_auto_settings(
        &self,
    ) -> Result<StoredAutoSettings, BackupServiceError> {
        let db = self.inner.db.clone();
        let value = tokio::task::spawn_blocking(move || db.get_setting(AUTO_SETTINGS_KEY))
            .await
            .map_err(io_err)?
            .map_err(io_err)?;
        value
            .map(|value| decode_auto_settings(&value).map_err(BackupServiceError::Validation))
            .unwrap_or_else(|| Ok(StoredAutoSettings::default()))
    }

    pub async fn auto_backup_settings(&self) -> Result<AutoBackupSettings, BackupServiceError> {
        let settings = self.stored_auto_settings().await?;
        Ok(AutoBackupSettings {
            enabled: settings.enabled,
            daily_time_local: settings.daily_time_local,
            auto_backup_key_present: settings.encrypted_key.is_some(),
            next_run_at: self
                .inner
                .next_run
                .read()
                .unwrap_or_else(|error| error.into_inner())
                .clone(),
        })
    }

    pub async fn update_auto_backup_settings(
        &self,
        input: AutoBackupSettingsInput,
    ) -> Result<AutoBackupSettings, BackupServiceError> {
        let _guard = self.inner.settings_lock.lock().await;
        let next = compute_next_auto_backup_run_at(
            &input.daily_time_local,
            crate::e2e_clock::local_now(),
        )?;
        let mut settings = self.stored_auto_settings().await?;
        if input.clear_auto_backup_key && input.set_auto_backup_key.is_some() {
            return Err(BackupServiceError::Validation(
                "choose either setting or clearing the automatic backup key".into(),
            ));
        }
        if input.clear_auto_backup_key && input.enabled {
            return Err(BackupServiceError::Validation(
                "automatic backup key cannot be cleared while automatic backups are enabled".into(),
            ));
        }
        if let Some(key) = input.set_auto_backup_key {
            if key
                .chars()
                .filter(|character| !character.is_whitespace())
                .count()
                < 8
            {
                return Err(BackupServiceError::Validation(
                    "automatic backup key needs at least 8 non-whitespace characters".into(),
                ));
            }
            let master_key = self.inner.db.encryption_key().ok_or_else(|| {
                BackupServiceError::Validation("an encryption master key is required".into())
            })?;
            settings.encrypted_key =
                Some(encrypt_value(master_key, &key).map_err(BackupServiceError::Validation)?);
        }
        if input.clear_auto_backup_key {
            settings.encrypted_key = None;
        }
        if input.enabled && settings.encrypted_key.is_none() {
            return Err(BackupServiceError::Validation(
                "an automatic backup key is required before enabling automatic backups".into(),
            ));
        }
        settings.enabled = input.enabled;
        settings.daily_time_local = input.daily_time_local;
        let json = serde_json::to_string(&settings).map_err(io_err)?;
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || db.set_setting(AUTO_SETTINGS_KEY, &json))
            .await
            .map_err(io_err)?
            .map_err(io_err)?;
        *self
            .inner
            .next_run
            .write()
            .unwrap_or_else(|error| error.into_inner()) = settings
            .enabled
            .then(|| next.with_timezone(&Utc).to_rfc3339());
        self.inner
            .settings_changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        self.auto_backup_settings().await
    }

    pub async fn run_auto_backup(&self) -> Result<Option<BackupInfo>, BackupServiceError> {
        let settings = self.stored_auto_settings().await?;
        if !settings.enabled {
            return Ok(None);
        }
        let encrypted = settings
            .encrypted_key
            .ok_or(BackupServiceError::PasswordRequired)?;
        let key = self.inner.db.encryption_key().ok_or_else(|| {
            BackupServiceError::Validation("an encryption master key is required".into())
        })?;
        let password = decrypt_value(key, &encrypted).map_err(BackupServiceError::Validation)?;
        let (_, finished) = self
            .begin_backup(Some(password), BackupTrigger::Auto)
            .await?;
        finished.await.map_err(io_err)?.map(Some)
    }

    pub fn start_background_auto_backup_scheduler(&self) -> tokio::task::JoinHandle<()> {
        self.start_auto_backup_scheduler_with_clock(crate::e2e_clock::local_now)
    }

    pub(super) fn start_auto_backup_scheduler_with_clock(
        &self,
        clock: impl Fn() -> DateTime<Local> + Send + Sync + 'static,
    ) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        let mut changes = self.inner.settings_changed.subscribe();
        tokio::spawn(async move {
            if let Err(error) = service.cleanup_stale_backup_files().await {
                tracing::warn!(%error, "could not clean abandoned backup files");
            }
            let mut last_run = None;
            let mut runs = tokio::task::JoinSet::new();
            loop {
                changes.borrow_and_update();
                let settings = match service.stored_auto_settings().await {
                    Ok(settings) => settings,
                    Err(error) => {
                        tracing::warn!(%error, "could not read automatic backup settings");
                        if changes.changed().await.is_err() {
                            return;
                        }
                        continue;
                    }
                };
                if !settings.enabled {
                    *service
                        .inner
                        .next_run
                        .write()
                        .unwrap_or_else(|error| error.into_inner()) = None;
                    tokio::select! {
                        changed = changes.changed() => { if changed.is_err() { return; } }
                        _ = runs.join_next(), if !runs.is_empty() => {}
                    }
                    continue;
                }
                let now = clock();
                let after = last_run.map_or(now, |last: DateTime<Local>| {
                    now.max(last + Duration::seconds(1))
                });
                let next = match compute_next_auto_backup_run_at(&settings.daily_time_local, after)
                {
                    Ok(next) => next,
                    Err(error) => {
                        tracing::warn!(%error, "could not compute automatic backup time");
                        if changes.changed().await.is_err() {
                            return;
                        }
                        continue;
                    }
                };
                *service
                    .inner
                    .next_run
                    .write()
                    .unwrap_or_else(|error| error.into_inner()) =
                    Some(next.with_timezone(&Utc).to_rfc3339());
                let delay = (next - now)
                    .to_std()
                    .unwrap_or_default()
                    .min(crate::e2e_clock::schedule_poll_interval());
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {
                        if clock() < next { continue; }
                        last_run = Some(next);
                        if runs.is_empty() {
                            let service = service.clone();
                            runs.spawn(async move { service.run_auto_backup().await });
                        } else {
                            tracing::warn!(scheduled_at = %next, "skipping automatic backup because a previous run is still active");
                        }
                    }
                    changed = changes.changed() => { if changed.is_err() { return; } }
                    result = runs.join_next(), if !runs.is_empty() => {
                        match result { Some(Ok(Err(error))) => tracing::warn!(%error, "automatic backup failed"), Some(Err(error)) => tracing::warn!(%error, "automatic backup task failed"), _ => {} }
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, NaiveDate, Timelike};

    #[test]
    fn malformed_schedule_fields_preserve_encryption_key_validation() {
        let db = crate::Database::open_in_memory().unwrap();
        let key = crate::persistence::encryption::EncryptionKey::generate();
        let ciphertext = encrypt_value(&key, "automatic password").unwrap();
        let json = serde_json::json!({
            "enabled": "invalid",
            "daily_time_local": 300,
            "encrypted_key": ciphertext,
        })
        .to_string();
        db.set_setting(AUTO_SETTINGS_KEY, &json).unwrap();
        let recovered = decode_auto_settings(&json).unwrap();
        assert!(!recovered.enabled);
        assert_eq!(recovered.daily_time_local, "03:00");
        assert_eq!(db.automatic_backup_ciphertext().unwrap(), Some(ciphertext));
        assert!(db.has_encrypted_credentials().unwrap());
        db.validate_encrypted_credentials(&key).unwrap();
        assert!(
            db.validate_encrypted_credentials(
                &crate::persistence::encryption::EncryptionKey::generate()
            )
            .is_err()
        );
        assert_eq!(db.get_setting(AUTO_SETTINGS_KEY).unwrap(), Some(json));
        for corrupt in ["{broken", "[]", r#"{"encrypted_key":42}"#] {
            db.set_setting(AUTO_SETTINGS_KEY, corrupt).unwrap();
            assert!(!decode_auto_settings(corrupt).unwrap().enabled);
            assert!(!db.has_encrypted_credentials().unwrap());
            db.validate_encrypted_credentials(&key).unwrap();
            assert_eq!(
                db.get_setting(AUTO_SETTINGS_KEY).unwrap().as_deref(),
                Some(corrupt)
            );
        }
    }

    #[test]
    fn daily_time_uses_today_at_exact_time_otherwise_next_day() {
        let utc = FixedOffset::east_opt(0).unwrap();
        let now = utc.with_ymd_and_hms(2026, 1, 2, 3, 0, 0).unwrap();
        let resolve = |candidate| utc.from_local_datetime(&candidate);
        assert_eq!(compute_next_run("03:00", now, resolve).unwrap(), now);
        assert_eq!(
            compute_next_run("03:00", now + Duration::seconds(1), resolve).unwrap(),
            now + Duration::days(1)
        );
        assert_eq!(
            compute_next_run("04:00", now, resolve).unwrap(),
            now + Duration::hours(1)
        );
        for invalid in ["3:00", "24:00", "12:60", "bogus"] {
            assert!(compute_next_run(invalid, now, resolve).is_err());
        }
    }

    #[test]
    fn daily_time_walks_dst_gap_and_uses_earlier_ambiguous_instant() {
        let standard = FixedOffset::west_opt(5 * 3600).unwrap();
        let daylight = FixedOffset::west_opt(4 * 3600).unwrap();
        let spring = NaiveDate::from_ymd_opt(2026, 3, 8).unwrap();
        let now = standard
            .from_local_datetime(&spring.and_hms_opt(0, 0, 0).unwrap())
            .unwrap();
        let next = compute_next_run("02:30", now, |candidate| {
            if candidate.date() == spring && candidate.hour() == 2 {
                LocalResult::None
            } else if candidate.hour() >= 3 {
                daylight.from_local_datetime(&candidate)
            } else {
                standard.from_local_datetime(&candidate)
            }
        })
        .unwrap();
        assert_eq!(next.naive_local(), spring.and_hms_opt(3, 0, 0).unwrap());
        let fall = NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
        let now = daylight
            .from_local_datetime(&fall.and_hms_opt(0, 0, 0).unwrap())
            .unwrap();
        let next = compute_next_run("01:30", now, |candidate| {
            if candidate.date() == fall && candidate.hour() == 1 {
                LocalResult::Ambiguous(
                    standard.from_local_datetime(&candidate).unwrap(),
                    daylight.from_local_datetime(&candidate).unwrap(),
                )
            } else {
                standard.from_local_datetime(&candidate)
            }
        })
        .unwrap();
        assert_eq!(next.offset(), &daylight);
        assert!(compute_next_run("01:30", now, |_| LocalResult::None).is_err());
    }
}

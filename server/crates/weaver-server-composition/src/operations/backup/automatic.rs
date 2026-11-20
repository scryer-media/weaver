// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::settings::queries::SettingsQueries;
use serde::{Deserialize, Serialize};

pub const AUTO_SETTINGS_KEY: &str = "backup.automatic";

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredAutoSettings {
    pub enabled: bool,
    pub daily_time_local: String,
    pub encrypted_key: Option<String>,
}

pub fn decode_auto_settings(value: &str) -> Result<StoredAutoSettings, String> {
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
    pub fn automatic_backup_ciphertext(&self) -> Result<Option<String>, crate::StateError> {
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

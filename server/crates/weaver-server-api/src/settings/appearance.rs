// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::{Arc, Mutex};

use async_graphql::{Enum, InputObject, MaybeUndefined, SimpleObject};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use weaver_server_core::settings::persistence::SettingsPersistenceQueries;
use weaver_server_core::settings::queries::SettingsQueries;
use weaver_server_core::{Database, StateError};

const KEY: &str = "web.appearance.v1";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Enum, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebPalette {
    #[default]
    Weaver,
    Phosphor,
    Amber,
    Terminal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Enum, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebMode {
    Dark,
    Light,
    #[default]
    System,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enum, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebAccent {
    Teal,
    Blue,
    Green,
    Amber,
    Violet,
    Rose,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, SimpleObject, Serialize, Deserialize)]
pub struct WebAppearance {
    pub palette: WebPalette,
    pub mode: WebMode,
    pub accent: Option<WebAccent>,
}

#[derive(InputObject)]
pub struct WebAppearanceInput {
    pub palette: Option<WebPalette>,
    pub mode: Option<WebMode>,
    /// Omit to retain the accent. Null restores the palette's own highlight.
    pub accent: MaybeUndefined<WebAccent>,
}

#[derive(Clone, Default)]
pub struct AppearanceStore {
    saved: watch::Sender<WebAppearance>,
    // Only writers take this lock, inside a blocking database task.
    writer: Arc<Mutex<()>>,
}

impl AppearanceStore {
    pub fn load(db: &Database) -> Result<Self, StateError> {
        let appearance = db
            .get_setting(KEY)?
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default();
        Ok(Self {
            saved: watch::Sender::new(appearance),
            ..Self::default()
        })
    }

    pub fn current(&self) -> WebAppearance {
        *self.saved.borrow()
    }

    pub(crate) fn save(
        &self,
        db: &Database,
        input: WebAppearanceInput,
    ) -> Result<WebAppearance, StateError> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| StateError::Database("appearance writer unavailable".into()))?;
        let saved = self.current();
        let next = WebAppearance {
            palette: input.palette.unwrap_or(saved.palette),
            mode: input.mode.unwrap_or(saved.mode),
            accent: match input.accent {
                MaybeUndefined::Undefined => saved.accent,
                MaybeUndefined::Null => None,
                MaybeUndefined::Value(accent) => Some(accent),
            },
        };
        if next != saved {
            let value = serde_json::to_string(&next)
                .map_err(|error| StateError::Database(error.to_string()))?;
            db.set_setting(KEY, &value)?;
            self.saved.send_replace(next);
        }
        Ok(next)
    }
}

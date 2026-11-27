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

#[derive(
    Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Enum, Serialize, Deserialize,
)]
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

/// A job stage whose progress colour can be picked. Queued is always grey.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Enum, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebProgressStage {
    Downloading,
    Paused,
    /// Verifying and repairing.
    Checking,
    Copying,
    Extracting,
    Completed,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enum, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebProgressColor {
    Grey,
    Teal,
    Cyan,
    Blue,
    Indigo,
    Violet,
    Magenta,
    Rose,
    Red,
    Amber,
    Lime,
    Green,
}

/// One palette's chosen colour for one stage. Stages without a pick use the palette's default.
#[derive(Clone, Copy, Debug, Eq, PartialEq, SimpleObject, Serialize, Deserialize)]
pub struct WebProgressColorPick {
    pub palette: WebPalette,
    pub stage: WebProgressStage,
    pub color: WebProgressColor,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, SimpleObject, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebAppearance {
    pub palette: WebPalette,
    pub mode: WebMode,
    pub accent: Option<WebAccent>,
    /// Picks for every palette, ordered by palette then stage.
    #[serde(default)]
    pub progress_colors: Vec<WebProgressColorPick>,
}

#[derive(InputObject)]
pub struct WebProgressColorChange {
    pub palette: WebPalette,
    pub stage: WebProgressStage,
    /// Null restores the palette's default for this stage.
    pub color: Option<WebProgressColor>,
}

#[derive(InputObject)]
pub struct WebAppearanceInput {
    pub palette: Option<WebPalette>,
    pub mode: Option<WebMode>,
    /// Omit to retain the accent. Null restores the palette's own highlight.
    pub accent: MaybeUndefined<WebAccent>,
    /// Changes applied over the saved picks; other picks are kept. At most
    /// one change per palette and stage (4 x 7).
    #[graphql(validator(max_items = 28))]
    pub progress_colors: Option<Vec<WebProgressColorChange>>,
}

#[derive(Clone, Default)]
pub struct AppearanceStore {
    saved: watch::Sender<WebAppearance>,
    // Only writers take this lock, inside a blocking database task.
    writer: Arc<Mutex<()>>,
}

fn apply_progress_changes(
    picks: &mut Vec<WebProgressColorPick>,
    changes: Vec<WebProgressColorChange>,
) {
    for change in changes {
        picks.retain(|pick| (pick.palette, pick.stage) != (change.palette, change.stage));
        if let Some(color) = change.color {
            picks.push(WebProgressColorPick {
                palette: change.palette,
                stage: change.stage,
                color,
            });
        }
    }
    picks.sort_by_key(|pick| (pick.palette, pick.stage));
}

impl AppearanceStore {
    pub fn load(db: &Database) -> Result<Self, StateError> {
        let appearance: WebAppearance = db
            .get_setting(KEY)?
            .and_then(|value| serde_json::from_str(&value).ok())
            .unwrap_or_default();
        Ok(Self {
            saved: watch::Sender::new(appearance),
            ..Self::default()
        })
    }

    pub fn current(&self) -> WebAppearance {
        self.saved.borrow().clone()
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
        let mut progress_colors = saved.progress_colors.clone();
        if let Some(changes) = input.progress_colors {
            apply_progress_changes(&mut progress_colors, changes);
        }
        let next = WebAppearance {
            palette: input.palette.unwrap_or(saved.palette),
            mode: input.mode.unwrap_or(saved.mode),
            accent: match input.accent {
                MaybeUndefined::Undefined => saved.accent,
                MaybeUndefined::Null => None,
                MaybeUndefined::Value(accent) => Some(accent),
            },
            progress_colors,
        };
        if next != saved {
            let value = serde_json::to_string(&next)
                .map_err(|error| StateError::Database(error.to_string()))?;
            db.set_setting(KEY, &value)?;
            self.saved.send_replace(next.clone());
        }
        Ok(next)
    }
}

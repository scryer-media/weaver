// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::settings::queries::SettingsQueries;
use weaver_server_db::persistence::connection::Database;
use weaver_server_jobs::jobs::control::{Hold, HoldOwner, HoldSet};

// The operator's holds, as last kept.
pub async fn load_holds_from_db(db: &Database) -> Result<HoldSet, String> {
    let mut holds = HoldSet::default();
    for (owner, setting) in HoldOwner::kept() {
        let db = db.clone();
        let value = tokio::task::spawn_blocking(move || db.get_setting(setting))
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        if value.as_deref().and_then(|raw| raw.parse::<bool>().ok()) == Some(true) {
            holds.hold(Hold::manual(owner));
        }
    }
    Ok(holds)
}

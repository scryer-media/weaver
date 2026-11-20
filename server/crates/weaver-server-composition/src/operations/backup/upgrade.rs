// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::settings::persistence::SettingsPersistenceQueries;

pub const LAST_VERSION: &str = "last_started_version";

pub fn record_started_version_for(
    db: &crate::Database,
    version: &str,
) -> Result<(), crate::StateError> {
    db.set_setting(LAST_VERSION, version)
}

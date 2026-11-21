// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job's metadata pairs as the metadata column stores them.

use weaver_server_db::persistence::StateError;

pub fn metadata_json(metadata: &[(String, String)]) -> Result<Option<String>, StateError> {
    if metadata.is_empty() {
        Ok(None)
    } else {
        serde_json::to_string(metadata)
            .map(Some)
            .map_err(|error| StateError::Database(error.to_string()))
    }
}

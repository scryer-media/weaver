// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::StateError;

// Server feed policy, persisted with the checkpoint refusals it caused.
// File indices refer to this active job's immutable NZB file list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FeedVerdict {
    version: u8,
    pub(crate) reason: String,
    pub(crate) abandoned_files: BTreeSet<u32>,
}

impl FeedVerdict {
    pub(crate) fn new(reason: String) -> Self {
        Self {
            version: 1,
            reason,
            abandoned_files: BTreeSet::new(),
        }
    }

    pub(crate) fn encode(&self) -> Result<String, StateError> {
        serde_json::to_string(self).map_err(|error| {
            StateError::Database(format!("feed verdict could not be encoded: {error}"))
        })
    }

    pub(crate) fn decode(value: &str) -> Result<Self, StateError> {
        let verdict: Self = serde_json::from_str(value).map_err(|error| {
            StateError::Database(format!("feed verdict could not be decoded: {error}"))
        })?;
        if verdict.version != 1 {
            return Err(StateError::Database(format!(
                "unsupported feed verdict version {}",
                verdict.version
            )));
        }
        Ok(verdict)
    }
}

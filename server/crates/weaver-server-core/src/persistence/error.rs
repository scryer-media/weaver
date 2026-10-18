// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An operator's input that was refused. `code` is stable and part of the API;
// `message` is the plain sentence; `params` holds the values the sentence
// names, for a client that words the refusal itself.
#[derive(Debug, Clone, PartialEq)]
pub struct Refusal {
    pub code: &'static str,
    pub message: String,
    pub params: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(String),

    #[error("state conflict: {0}")]
    Conflict(String),

    // The input was refused; nothing is wrong with storage.
    #[error("{}", .0.message)]
    Refused(Refusal),
}

impl StateError {
    pub fn refusal(&self) -> Option<&Refusal> {
        match self {
            Self::Refused(refusal) => Some(refusal),
            _ => None,
        }
    }
}

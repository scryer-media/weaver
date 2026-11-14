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

    // The database's disk, or the quota on it, had no room for the write:
    // nothing is wrong with the statement or the rows.
    #[error("database error: {0}")]
    OutOfSpace(String),

    #[error("state conflict: {0}")]
    Conflict(String),

    // The input was refused; nothing is wrong with storage.
    #[error("{}", .0.message)]
    Refused(Refusal),
}

// Why stored state did not load. `Read`: the database did not return it, or
// what it returned did not decrypt, and a later try may read it. `Rows`: it
// returned it, and a value in it is not one this build takes.
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("{0}")]
    Read(StateError),
    #[error("{0}")]
    Rows(StateError),
}

impl From<LoadError> for StateError {
    fn from(error: LoadError) -> Self {
        match error {
            LoadError::Read(error) | LoadError::Rows(error) => error,
        }
    }
}

impl StateError {
    pub fn refusal(&self) -> Option<&Refusal> {
        match self {
            Self::Refused(refusal) => Some(refusal),
            _ => None,
        }
    }
}

// Why an ordered write did not commit, as its waiter is told: the message,
// and whether the database's disk had no room for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WriteFailed {
    pub(crate) message: String,
    pub(crate) out_of_space: bool,
}

impl WriteFailed {
    // A write that failed for a reason other than room.
    pub(crate) fn other(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            out_of_space: false,
        }
    }
}

impl From<&StateError> for WriteFailed {
    fn from(error: &StateError) -> Self {
        Self {
            message: error.to_string(),
            out_of_space: match error {
                StateError::OutOfSpace(_) => true,
                StateError::Io(error) => crate::operations::is_out_of_space(error),
                _ => false,
            },
        }
    }
}

impl std::fmt::Display for WriteFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

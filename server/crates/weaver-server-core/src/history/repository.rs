use crate::StateError;
#[cfg(test)]
use crate::history::{HistoryFilter, IntegrationEventRow, JobHistoryRow};
#[cfg(test)]
use crate::persistence::Database;

pub(crate) fn db_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

#[cfg(test)]
mod repository_tests;

#[cfg(test)]
mod integration_tests;

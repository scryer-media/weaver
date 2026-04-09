use crate::StateError;

#[cfg(test)]
pub(crate) use crate::persistence::Database;

pub(crate) fn db_err(e: impl std::fmt::Display) -> StateError {
    StateError::Database(e.to_string())
}

#[cfg(test)]
mod tests;

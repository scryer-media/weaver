#![allow(dead_code)]

use std::env;
use std::path::{Path, PathBuf};

use crate::persistence::StateError;

pub(crate) const DATABASE_URL_ENV: &str = "WEAVER_DATABASE_URL";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseTarget {
    SqlitePath(PathBuf),
    SqliteUrl(String),
    PostgresUrl(String),
}

impl DatabaseTarget {
    pub(crate) fn from_config_path(config_path: &Path) -> Self {
        let (db_path, _) = super::setup::resolve_database_paths(config_path);
        Self::SqlitePath(db_path)
    }

    pub(crate) fn resolve(config_path: &Path) -> Result<Self, StateError> {
        match env::var(DATABASE_URL_ENV) {
            Ok(raw) => Self::from_database_url(raw, config_path),
            Err(env::VarError::NotPresent) => Ok(Self::from_config_path(config_path)),
            Err(error) => Err(StateError::Database(format!(
                "failed to read {DATABASE_URL_ENV}: {error}"
            ))),
        }
    }

    pub(crate) fn from_database_url(
        raw: impl Into<String>,
        config_path: &Path,
    ) -> Result<Self, StateError> {
        let raw = raw.into();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::from_config_path(config_path));
        }

        if trimmed.starts_with("postgres://") || trimmed.starts_with("postgresql://") {
            return Ok(Self::PostgresUrl(trimmed.to_string()));
        }
        if trimmed.starts_with("sqlite:") {
            return Ok(Self::SqliteUrl(trimmed.to_string()));
        }

        Err(StateError::Database(format!(
            "unsupported {DATABASE_URL_ENV} scheme; expected postgres://, postgresql://, or sqlite:"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_sqlite_path_from_config_path() {
        let target = DatabaseTarget::from_config_path(Path::new("/var/lib/weaver/weaver.toml"));
        assert_eq!(
            target,
            DatabaseTarget::SqlitePath(PathBuf::from("/var/lib/weaver/weaver.db"))
        );
    }

    #[test]
    fn empty_database_url_keeps_default_sqlite_path() {
        let target =
            DatabaseTarget::from_database_url("   ", Path::new("/var/lib/weaver")).unwrap();
        assert_eq!(
            target,
            DatabaseTarget::SqlitePath(PathBuf::from("/var/lib/weaver/weaver.db"))
        );
    }

    #[test]
    fn recognizes_postgres_urls() {
        let target = DatabaseTarget::from_database_url(
            "postgresql://weaver:secret@example.com/weaver",
            Path::new("/var/lib/weaver"),
        )
        .unwrap();
        assert_eq!(
            target,
            DatabaseTarget::PostgresUrl("postgresql://weaver:secret@example.com/weaver".into())
        );
    }

    #[test]
    fn recognizes_sqlite_urls() {
        let target = DatabaseTarget::from_database_url(
            "sqlite:///var/lib/weaver/weaver.db",
            Path::new("/var/lib/weaver"),
        )
        .unwrap();
        assert_eq!(
            target,
            DatabaseTarget::SqliteUrl("sqlite:///var/lib/weaver/weaver.db".into())
        );
    }

    #[test]
    fn rejects_unsupported_database_url_scheme() {
        let error =
            DatabaseTarget::from_database_url("mysql://example", Path::new("/var/lib/weaver"))
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported WEAVER_DATABASE_URL")
        );
    }
}

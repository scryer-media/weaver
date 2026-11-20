// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

#![allow(dead_code)]

use std::env;
use std::path::{Path, PathBuf};

use crate::persistence::StateError;

pub const DATABASE_URL_ENV: &str = "WEAVER_DATABASE_URL";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatabaseTarget {
    SqlitePath(PathBuf),
    SqliteUrl(String),
    PostgresUrl(String),
}

impl DatabaseTarget {
    pub fn sqlite_path(&self) -> Result<Option<PathBuf>, StateError> {
        match self {
            Self::SqlitePath(path) => Ok(Some(path.clone())),
            Self::SqliteUrl(url) => {
                let options: sqlx::sqlite::SqliteConnectOptions = url.parse().map_err(|error| {
                    StateError::Database(format!("invalid SQLite URL: {error}"))
                })?;
                Ok(Some(options.get_filename().to_path_buf()))
            }
            Self::PostgresUrl(_) => Ok(None),
        }
    }

    pub fn from_config_path(config_path: &Path) -> Self {
        let (db_path, _) = resolve_database_paths(config_path);
        Self::SqlitePath(db_path)
    }

    pub fn resolve(config_path: &Path) -> Result<Self, StateError> {
        match env::var(DATABASE_URL_ENV) {
            Ok(raw) => Self::from_database_url(raw, config_path),
            Err(env::VarError::NotPresent) => Ok(Self::from_config_path(config_path)),
            Err(error) => Err(StateError::Database(format!(
                "failed to read {DATABASE_URL_ENV}: {error}"
            ))),
        }
    }

    pub fn from_database_url(
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

pub fn is_explicit_config_file(config_path: &Path) -> bool {
    config_path
        .extension()
        .is_some_and(|extension| extension == "toml")
        || config_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".toml.migrated"))
        || config_path.is_file()
}

pub fn resolve_database_paths(config_path: &Path) -> (PathBuf, Option<PathBuf>) {
    if is_explicit_config_file(config_path) {
        let dir = config_path.parent().unwrap_or(Path::new("."));
        (dir.join("weaver.db"), Some(config_path.to_path_buf()))
    } else {
        let dir = config_path;
        let toml_candidate = dir.join("weaver.toml");
        let toml_path = toml_candidate.exists().then_some(toml_candidate);
        (dir.join("weaver.db"), toml_path)
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

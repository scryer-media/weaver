pub mod connection;
pub(crate) mod database_target;
pub mod encryption;
pub mod error;
pub(crate) mod maintenance;
pub mod migrations;
pub(crate) mod postgres_migrations;
pub mod setup;
pub(crate) mod sql_runtime;
pub(crate) mod sql_services;
pub(crate) mod sqlite_writer;

pub use connection::Database;
pub(crate) use connection::DatabaseWriterExecutor;
pub use error::StateError;
pub use setup::{
    bootstrap_encryption, finish_open_db_and_config, open_database, open_db_and_config,
    resolve_database_paths,
};

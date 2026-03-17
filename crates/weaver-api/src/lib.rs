pub mod auth;
mod backup;
pub mod jwt;
mod mutation;
mod query;
pub mod rss;
mod runtime;
mod submit;
mod subscription;
mod timeline;
mod types;

use async_graphql::Schema;
use weaver_core::config::SharedConfig;
use weaver_state::Database;

pub use backup::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
};
pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use rss::RssService;
pub use runtime::load_global_pause_from_db;
pub use submit::{SubmitNzbError, SubmittedJob, init_job_counter, submit_nzb_bytes};
pub use subscription::SubscriptionRoot;
pub use types::*;

/// The full GraphQL schema.
pub type WeaverSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// Build the schema with the given scheduler handle, shared config, and database.
pub fn build_schema(
    handle: weaver_scheduler::SchedulerHandle,
    config: SharedConfig,
    db: Database,
    rss: rss::RssService,
) -> WeaverSchema {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(handle)
        .data(config)
        .data(db)
        .data(rss)
        .finish()
}

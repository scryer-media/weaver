pub mod auth;
mod backup;
mod facade;
pub mod jwt;
mod metrics_history;
mod mutation;
mod query;
pub mod rss;
mod runtime;
mod submit;
mod subscription;
mod timeline;
mod types;

use std::time::Duration;

use async_graphql::Schema;
use weaver_core::config::SharedConfig;
use weaver_state::Database;

pub use backup::{
    BackupArtifact, BackupInspectResult, BackupManifest, BackupService, BackupServiceError,
    BackupStatus, CategoryRemapInput, CategoryRemapRequirement, RestoreOptions, RestoreReport,
};
pub use facade::*;
pub use mutation::MutationRoot;
pub use query::QueryRoot;
pub use rss::RssService;
pub use runtime::load_global_pause_from_db;
pub use submit::{
    SubmitNzbError, SubmittedJob, fetch_nzb_from_url, init_job_counter, submit_nzb_bytes,
};
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
    schedules: weaver_scheduler::schedule::SharedSchedules,
    log_buffer: weaver_core::log_buffer::LogRingBuffer,
) -> WeaverSchema {
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent("weaver/0.1")
        .gzip(true)
        .brotli(true)
        .deflate(true)
        .zstd(true)
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()
        .expect("http client build should succeed");

    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(handle)
        .data(config)
        .data(db)
        .data(rss)
        .data(schedules)
        .data(http_client)
        .data(log_buffer)
        .finish()
}

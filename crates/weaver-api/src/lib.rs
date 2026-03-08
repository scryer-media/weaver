mod mutation;
mod query;
mod subscription;
mod types;

use async_graphql::Schema;
use weaver_core::config::SharedConfig;

pub use mutation::{MutationRoot, init_job_counter};
pub use query::QueryRoot;
pub use subscription::SubscriptionRoot;
pub use types::*;

/// The full GraphQL schema.
pub type WeaverSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

/// Build the schema with the given scheduler handle and shared config.
pub fn build_schema(
    handle: weaver_scheduler::SchedulerHandle,
    config: SharedConfig,
) -> WeaverSchema {
    Schema::build(QueryRoot, MutationRoot, SubscriptionRoot)
        .data(handle)
        .data(config)
        .finish()
}

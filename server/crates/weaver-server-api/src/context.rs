use std::time::Duration;

use async_graphql::Schema;
use weaver_server_core::Database;
use weaver_server_core::settings::SharedConfig;

use crate::auth::LoginAuthCache;
use crate::rss::RssService;
use crate::schema::{MutationRoot, QueryRoot, SubscriptionRoot};

pub type WeaverSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub fn build_schema(
    handle: weaver_server_core::SchedulerHandle,
    config: SharedConfig,
    db: Database,
    auth_cache: LoginAuthCache,
    rss: RssService,
    schedules: weaver_server_core::bandwidth::schedule::SharedSchedules,
    log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
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

    Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    )
    .data(handle)
    .data(config)
    .data(db)
    .data(auth_cache)
    .data(rss)
    .data(schedules)
    .data(http_client)
    .data(log_buffer)
    .finish()
}

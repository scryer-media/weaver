use std::time::Duration;

use async_graphql::Schema;
use weaver_server_core::Database;
use weaver_server_core::auth::ApiKeyCache;
use weaver_server_core::settings::SharedConfig;

use crate::auth::LoginAuthCache;
use crate::jobs::replay::QueueEventReplay;
use crate::rss::RssService;
use crate::schema::{MutationRoot, QueryRoot, SubscriptionRoot};

pub type WeaverSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;

pub struct SchemaContext {
    pub handle: weaver_server_core::SchedulerHandle,
    pub config: SharedConfig,
    pub db: Database,
    pub auth_cache: LoginAuthCache,
    pub api_key_cache: ApiKeyCache,
    pub rss: RssService,
    pub schedules: weaver_server_core::bandwidth::schedule::SharedSchedules,
    pub log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
}

pub fn build_schema(context: SchemaContext) -> WeaverSchema {
    let replay = QueueEventReplay::default();
    replay.spawn_producer(context.handle.clone(), context.config.clone());

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
    .data(context.handle)
    .data(context.config)
    .data(context.db)
    .data(context.auth_cache)
    .data(context.api_key_cache)
    .data(context.rss)
    .data(context.schedules)
    .data(http_client)
    .data(context.log_buffer)
    .data(replay)
    .finish()
}

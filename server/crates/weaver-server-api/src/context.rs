use std::sync::Arc;
use std::time::Duration;

use async_graphql::{Schema, SchemaBuilder};
use weaver_nntp::pool::NntpPool;
use weaver_server_core::Database;
use weaver_server_core::auth::ApiKeyCache;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::watch_folder::WatchFolderService;

use crate::auth::LoginAuthCache;
use crate::jobs::replay::QueueEventReplay;
use crate::jobs::staging::StagedUploadManager;
use crate::rss::RssService;
use crate::schema::{MutationRoot, QueryRoot, SubscriptionRoot};

pub type WeaverSchema = Schema<QueryRoot, MutationRoot, SubscriptionRoot>;
pub const GRAPHQL_MAX_COMPLEXITY: usize = 512;
pub const GRAPHQL_MAX_DEPTH: usize = 16;

pub fn apply_graphql_query_guards<Query, Mutation, Subscription>(
    builder: SchemaBuilder<Query, Mutation, Subscription>,
) -> SchemaBuilder<Query, Mutation, Subscription> {
    builder
        .disable_introspection()
        .limit_complexity(GRAPHQL_MAX_COMPLEXITY)
        .limit_depth(GRAPHQL_MAX_DEPTH)
}

pub struct SchemaContext {
    pub handle: weaver_server_core::SchedulerHandle,
    pub config: SharedConfig,
    pub db: Database,
    pub auth_cache: LoginAuthCache,
    pub api_key_cache: ApiKeyCache,
    pub rss: RssService,
    pub watch_folder: WatchFolderService,
    pub schedules: weaver_server_core::bandwidth::schedule::SharedSchedules,
    pub log_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
    /// Live NNTP pool for per-server health metrics. `None` in contexts without a pool (tests).
    pub nntp_pool: Option<Arc<NntpPool>>,
}

pub fn build_schema(context: SchemaContext) -> WeaverSchema {
    let replay = QueueEventReplay::default();
    replay.spawn_producer(context.handle.clone(), context.config.clone());
    let history_delete_manager = crate::history::delete_ops::HistoryDeleteManager::new(
        context.db.clone(),
        context.handle.clone(),
        replay.clone(),
    );
    history_delete_manager.spawn_worker();

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
    let staged_upload_manager = StagedUploadManager::new();
    staged_upload_manager.spawn_cleanup_worker();

    let schema = apply_graphql_query_guards(Schema::build(
        QueryRoot::default(),
        MutationRoot::default(),
        SubscriptionRoot::default(),
    ))
    .data(context.handle)
    .data(context.config)
    .data(context.db)
    .data(context.auth_cache)
    .data(context.api_key_cache)
    .data(context.rss)
    .data(context.watch_folder)
    .data(context.schedules)
    .data(http_client)
    .data(context.log_buffer)
    .data(context.nntp_pool)
    .data(replay)
    .data(history_delete_manager)
    .data(staged_upload_manager);
    schema.finish()
}

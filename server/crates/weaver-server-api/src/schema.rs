use std::path::Path;
use std::time::Duration;

use async_graphql::{
    Context, MergedObject, MergedSubscription, Object, Result, SimpleObject, Subscription,
};
use tokio_stream::{Stream, StreamExt};

use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::runtime::log_buffer::LogRingBuffer;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{Database, JobHistoryRow, JobInfo, JobStatus, SchedulerHandle};

use crate::auth::types::{ApiKey, ApiKeyScope};
use crate::auth::{AdminGuard, ReadGuard, graphql_error};
use crate::categories::types::Category;
use crate::history::timeline::build_job_timeline;
use crate::history::types::{EventKind, HistoryItem, JobEvent, JobTimeline};
use crate::history::types::{history_item_from_row, matches_history_filter};
use crate::jobs::types::{
    GlobalQueueState, Job, JobOutputFile, JobOutputResult, JobStatusGql, PersistedQueueEvent,
    QueueEvent, QueueFilterInput, QueueItem, QueueSnapshot, QueueSummary,
};
use crate::jobs::types::{
    decode_event_cursor, decode_offset_cursor, encode_event_cursor, global_queue_state,
    matches_queue_event_filter, matches_queue_filter, metrics_from_snapshot,
    queue_event_from_record, queue_item_from_job, queue_summary,
};
use crate::rss::types::{RssFeed, RssSeenItem};
use crate::servers::types::Server;
use crate::settings::types::GeneralSettings;
use crate::system::metrics_history::build_metrics_history;
use crate::system::types::{
    DirectoryBrowseEntry, DirectoryBrowseResult, MetricsHistoryResult, ServiceLogsPayload,
    SystemStatus,
};
use crate::system::types::{DownloadBlock, Metrics, PipelineEventGql};

#[derive(Debug, Clone, SimpleObject)]
pub struct JobsSnapshot {
    pub jobs: Vec<Job>,
    pub metrics: Metrics,
    pub is_paused: bool,
    pub download_block: DownloadBlock,
}

#[derive(Default, MergedObject)]
pub struct QueryRoot(
    auth_query::AuthQuery,
    categories_query::CategoriesQuery,
    history_query::HistoryQuery,
    jobs_query::JobsQuery,
    rss_query::RssQuery,
    servers_query::ServersQuery,
    settings_query::SettingsQuery,
    system_query::SystemQuery,
);

#[derive(Default, MergedObject)]
pub struct MutationRoot(
    auth_mutation::AuthMutation,
    categories_mutation::CategoriesMutation,
    jobs_mutation::JobsMutation,
    rss_mutation::RssMutation,
    servers_mutation::ServersMutation,
    settings_mutation::SettingsMutation,
);

#[derive(Default, MergedSubscription)]
pub struct SubscriptionRoot(
    history_subscription::HistorySubscription,
    jobs_subscription::JobsSubscription,
    system_subscription::SystemSubscription,
);

#[path = "auth/query.rs"]
pub mod auth_query;

#[path = "categories/query.rs"]
pub mod categories_query;

#[path = "history/query.rs"]
pub mod history_query;

#[path = "jobs/query.rs"]
pub mod jobs_query;

#[path = "rss/query.rs"]
pub mod rss_query;

#[path = "servers/query.rs"]
pub mod servers_query;

#[path = "settings/query.rs"]
pub mod settings_query;

#[path = "system/query.rs"]
pub mod system_query;

#[path = "auth/mutation.rs"]
pub mod auth_mutation;

#[path = "categories/mutation.rs"]
pub mod categories_mutation;

#[path = "jobs/mutation.rs"]
pub mod jobs_mutation;

#[path = "rss/mutation.rs"]
pub mod rss_mutation;

#[path = "servers/mutation.rs"]
pub mod servers_mutation;

#[path = "settings/mutation.rs"]
pub mod settings_mutation;

#[path = "history/subscription.rs"]
pub mod history_subscription;

#[path = "jobs/subscription.rs"]
pub mod jobs_subscription;

#[path = "system/subscription.rs"]
pub mod system_subscription;

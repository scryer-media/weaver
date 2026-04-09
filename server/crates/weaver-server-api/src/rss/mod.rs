pub mod types;

pub use weaver_server_core::rss::{RssFeedSyncReport, RssService, RssServiceError, RssSyncReport};

pub use crate::schema::rss_mutation as mutation;
pub use crate::schema::rss_query as query;

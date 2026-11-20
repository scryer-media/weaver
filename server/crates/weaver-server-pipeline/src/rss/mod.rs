// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod model;
mod persistence;
pub(crate) use persistence::RssPersistenceInternalQueries;
pub use persistence::RssPersistenceQueries;
mod poller;
pub(crate) use queries::RssInternalQueries;
pub use queries::RssQueries;
pub(crate) use queries::ScheduleCache;
use weaver_server_rss::rss::queries;
use weaver_server_rss::rss::record;
pub mod repository;
pub mod service;

pub use record::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenDecision, RssSeenItemRow};
pub use service::{RssFeedSyncReport, RssService, RssServiceError, RssSyncReport};

impl queries::ScheduleStore for crate::Database {
    fn schedule_cache(&self) -> &ScheduleCache {
        &self.rss_schedule_cache
    }
}

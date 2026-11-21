// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub use persistence::RssPersistenceInternalQueries;
pub use persistence::RssPersistenceQueries;
pub use queries::RssInternalQueries;
pub use queries::RssQueries;
pub(crate) use queries::ScheduleCache;
pub use weaver_server_net_runtime::rss::{model, persistence};
use weaver_server_rss::rss::queries;
use weaver_server_rss::rss::record;
pub mod repository;

pub use record::{RssFeedRow, RssRuleAction, RssRuleRow, RssSeenDecision, RssSeenItemRow};

impl queries::ScheduleStore for crate::Database {
    fn schedule_cache(&self) -> &ScheduleCache {
        &self.rss_schedule_cache
    }
}

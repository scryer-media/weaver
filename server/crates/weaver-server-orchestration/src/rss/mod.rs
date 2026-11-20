// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// RSS above the pipeline: the pipeline's RSS, plus the service
// and its poller, which submit to the scheduler.
pub use weaver_server_composition::rss::*;

mod poller;
pub mod service;

pub use service::{RssFeedSyncReport, RssService, RssServiceError, RssSyncReport};

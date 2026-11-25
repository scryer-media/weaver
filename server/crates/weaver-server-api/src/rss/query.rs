// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::observability::{spawn_blocking_with, state_error};
use weaver_server_core::rss::RssQueries;

#[derive(Default)]
pub(crate) struct RssQuery;

#[Object]
impl RssQuery {
    /// List configured RSS feeds.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feeds(&self, ctx: &Context<'_>) -> Result<Vec<RssFeed>> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        spawn_blocking_with(
            move || {
                let feeds = db.list_rss_feeds()?;
                let mut out = Vec::with_capacity(feeds.len());
                for feed in feeds {
                    let rules = db
                        .list_rss_rules(feed.id)?
                        .iter()
                        .map(crate::rss::types::RssRule::from_row)
                        .collect();
                    out.push(RssFeed::from_row(&feed, rules));
                }
                Ok::<_, weaver_server_core::StateError>(out)
            },
            state_error,
        )
        .await
    }
    /// Get a single RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feed(&self, ctx: &Context<'_>, id: u32) -> Result<Option<RssFeed>> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        spawn_blocking_with(
            move || {
                let Some(feed) = db.get_rss_feed(id)? else {
                    return Ok(None);
                };
                let rules = db
                    .list_rss_rules(feed.id)?
                    .iter()
                    .map(crate::rss::types::RssRule::from_row)
                    .collect();
                Ok::<_, weaver_server_core::StateError>(Some(RssFeed::from_row(&feed, rules)))
            },
            state_error,
        )
        .await
    }
    /// List recently seen RSS items, optionally scoped to one feed.
    #[graphql(guard = "AdminGuard")]
    async fn rss_seen_items(
        &self,
        ctx: &Context<'_>,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItem>> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        spawn_blocking_with(
            move || {
                let items = db.list_rss_seen_items(feed_id, limit)?;
                Ok::<_, weaver_server_core::StateError>(
                    items
                        .iter()
                        .map(crate::rss::types::RssSeenItem::from_row)
                        .collect(),
                )
            },
            state_error,
        )
        .await
    }
}

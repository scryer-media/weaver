use super::*;

#[derive(Default)]
pub(crate) struct RssQuery;

#[Object]
impl RssQuery {
    /// List configured RSS feeds.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feeds(&self, ctx: &Context<'_>) -> Result<Vec<RssFeed>> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        tokio::task::spawn_blocking(move || {
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
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
    /// Get a single RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feed(&self, ctx: &Context<'_>, id: u32) -> Result<Option<RssFeed>> {
        let db = ctx.data::<weaver_server_core::Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let Some(feed) = db.get_rss_feed(id)? else {
                return Ok(None);
            };
            let rules = db
                .list_rss_rules(feed.id)?
                .iter()
                .map(crate::rss::types::RssRule::from_row)
                .collect();
            Ok::<_, weaver_server_core::StateError>(Some(RssFeed::from_row(&feed, rules)))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
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
        tokio::task::spawn_blocking(move || {
            let items = db.list_rss_seen_items(feed_id, limit)?;
            Ok::<_, weaver_server_core::StateError>(
                items
                    .iter()
                    .map(crate::rss::types::RssSeenItem::from_row)
                    .collect(),
            )
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }
}

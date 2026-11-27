// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use weaver_server_core::torrents::service::listing::{CategoryPick, Pick, Shown, page};

use super::types::{ScheduleSeedingGql, TorrentGql, TorrentPageGql, TorrentRouteGql, page_args};
use super::{read_torrent, refuse_detail_only, service};
use crate::auth::ReadGuard;
use crate::jobs::types::encode_offset_cursor;

/// The torrents a `torrents` page holds when `first` is not given.
pub const DEFAULT_TORRENT_PAGE: usize = 100;
/// The most torrents a `torrents` page holds: the most a queue page holds.
pub const MAX_TORRENT_PAGE: usize = crate::jobs::paging::MAX_QUEUE_PAGE_SIZE;

#[derive(Default)]
pub struct TorrentsQuery;

#[Object]
impl TorrentsQuery {
    /// One torrent by its job id; null when there is none. Its files, peer
    /// connections and unpacked files are read here only.
    #[graphql(guard = "ReadGuard")]
    async fn torrent(&self, ctx: &Context<'_>, id: u64) -> Result<Option<TorrentGql>> {
        read_torrent(ctx, &ctx.look_ahead(), id).await
    }

    /// The torrents the schedule's pause leaves seeding, by the tracker rule
    /// that keeps them; empty while no such pause is in force.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_schedule_seeding(&self, ctx: &Context<'_>) -> Result<Vec<ScheduleSeedingGql>> {
        Ok(service(ctx)?
            .schedule_seeding()
            .await
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Every torrent route, by the scopes that run on it, with whether
    /// peers can reach its session.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_routes(&self, ctx: &Context<'_>) -> Result<Vec<TorrentRouteGql>> {
        Ok(service(ctx)?
            .routes()
            .await
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Every torrent, or a category's, in the order they were added: the
    /// `first` after the offset cursor `after`, 100 when not given and at
    /// most 500, with the `after` of the next page, null on the last. One
    /// torrent past the page is read to know whether there is a next;
    /// nothing is counted. A torrent's files, peer connections and unpacked
    /// files are read on `torrent(id)`, and asking for them here is refused.
    #[graphql(guard = "ReadGuard")]
    async fn torrents(
        &self,
        ctx: &Context<'_>,
        category: Option<String>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<TorrentPageGql> {
        let field = ctx.look_ahead().field("items");
        refuse_detail_only(&field, "torrents")?;
        let (offset, limit) = page_args(
            first,
            after.as_deref(),
            DEFAULT_TORRENT_PAGE,
            MAX_TORRENT_PAGE,
        )?;
        let mut items = read_page(ctx, field, category, offset, limit + 1).await?;
        // A page with no items has no next page, so `first: 0` never hands
        // back a cursor that does not advance.
        let next_cursor =
            (limit > 0 && items.len() > limit).then(|| encode_offset_cursor(offset + limit));
        items.truncate(limit);
        Ok(TorrentPageGql { items, next_cursor })
    }
}

// The `limit` torrents after `offset`, reading only what `field`, the
// selection of one torrent, shows.
async fn read_page(
    ctx: &Context<'_>,
    field: async_graphql::Lookahead<'_>,
    category: Option<String>,
    offset: usize,
    limit: usize,
) -> Result<Vec<TorrentGql>> {
    let shown = Shown {
        files: false,
        trackers: field.field("trackers").exists(),
        tags: field.field("tags").exists(),
    };
    let pick = Pick {
        category: category.as_deref().map(CategoryPick::Named),
        ..Pick::default()
    };
    // The save path is read from the files: without them, from their
    // totals, which say whether it is one file named as the torrent is.
    if field.field("savePath").exists() {
        return Ok(service(ctx)?
            .picked_rows(pick, page(offset, limit), shown)
            .await
            .map_err(super::list_error)?
            .into_iter()
            .map(|(view, totals)| TorrentGql::of(view, totals.one_file))
            .collect());
    }
    Ok(service(ctx)?
        .picked(pick, page(offset, limit), shown)
        .await
        .map_err(super::list_error)?
        .into_iter()
        .map(Into::into)
        .collect())
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::listing::{CategoryPick, Pick, Shown, page};

use super::mutation::action_error;
use super::service;
use super::types::{
    ScheduleSeedingGql, Torrent, TorrentPage, TorrentPeerGql, TorrentRouteGql, TorrentTrackerGql,
};
use crate::auth::{ReadGuard, graphql_error};
use crate::jobs::types::{decode_offset_cursor, encode_offset_cursor};

/// The torrents a `torrents` page holds when `first` is not given.
pub const DEFAULT_TORRENT_PAGE: usize = 100;
/// The most torrents a `torrents` page holds: the most a queue page holds.
pub const MAX_TORRENT_PAGE: usize = crate::jobs::paging::MAX_QUEUE_PAGE_SIZE;

#[derive(Default)]
pub struct TorrentsQuery;

#[Object]
impl TorrentsQuery {
    #[graphql(guard = "ReadGuard")]
    async fn torrent(&self, ctx: &Context<'_>, id: u64) -> Result<Option<Torrent>> {
        let field = ctx.look_ahead();
        // The save path is read from the files: a torrent of one file
        // named as it is sits in its folder.
        let shown = Shown {
            files: field.field("files").exists() || field.field("savePath").exists(),
            trackers: field.field("trackers").exists(),
            tags: field.field("tags").exists(),
        };
        Ok(service(ctx)?
            .torrent_shown(JobId(id), shown)
            .await
            .map(Into::into))
    }

    /// The torrent's live peer connections, by address.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_peers(&self, ctx: &Context<'_>, id: u64) -> Result<Vec<TorrentPeerGql>> {
        Ok(service(ctx)?
            .peers(JobId(id))
            .await
            .map_err(action_error)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// The torrent's trackers, each with how announcing to it went.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_trackers(&self, ctx: &Context<'_>, id: u64) -> Result<Vec<TorrentTrackerGql>> {
        Ok(service(ctx)?
            .trackers(JobId(id))
            .await
            .map_err(action_error)?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// The torrents the schedule's pause leaves seeding, by the tracker rule
    /// that keeps them; empty while no such pause is in force.
    #[graphql(guard = "ReadGuard")]
    async fn schedule_seeding(&self, ctx: &Context<'_>) -> Result<Vec<ScheduleSeedingGql>> {
        let Some(torrents) =
            ctx.data_opt::<weaver_server_core::torrents::service::TorrentService>()
        else {
            return Ok(Vec::new());
        };
        Ok(torrents
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
    /// `first` after the offset cursor `after`, as `queueItems` pages.
    /// `first` is 100 when not given, and at most 500.
    #[graphql(guard = "ReadGuard")]
    async fn torrents(
        &self,
        ctx: &Context<'_>,
        category: Option<String>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<Torrent>> {
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = page_limit(first);
        read_page(ctx, ctx.look_ahead(), category, offset, limit).await
    }

    /// `torrents` as a page: the same torrents, order, limits and cursor,
    /// with the `after` of the next page, null on the last. One torrent past
    /// the page is read to know whether there is a next; nothing is counted.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_page(
        &self,
        ctx: &Context<'_>,
        category: Option<String>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<TorrentPage> {
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = page_limit(first);
        let mut items = read_page(
            ctx,
            ctx.look_ahead().field("items"),
            category,
            offset,
            limit + 1,
        )
        .await?;
        // A page with no items has no next page, so `first: 0` never hands
        // back a cursor that does not advance.
        let next_cursor =
            (limit > 0 && items.len() > limit).then(|| encode_offset_cursor(offset + limit));
        items.truncate(limit);
        Ok(TorrentPage { items, next_cursor })
    }
}

// A page's size: `first`, 100 when not given, at most 500.
fn page_limit(first: Option<u32>) -> usize {
    first
        .map_or(DEFAULT_TORRENT_PAGE, |first| first as usize)
        .min(MAX_TORRENT_PAGE)
}

// The `limit` torrents after `offset`, reading only what `field`, the
// selection of one torrent, shows.
async fn read_page(
    ctx: &Context<'_>,
    field: async_graphql::Lookahead<'_>,
    category: Option<String>,
    offset: usize,
    limit: usize,
) -> Result<Vec<Torrent>> {
    let shown = Shown {
        files: field.field("files").exists(),
        trackers: field.field("trackers").exists(),
        tags: field.field("tags").exists(),
    };
    let pick = Pick {
        category: category.as_deref().map(CategoryPick::Named),
        ..Pick::default()
    };
    // The save path is read from the files: without them, from their
    // totals, which say whether it is one file named as the torrent is.
    if !shown.files && field.field("savePath").exists() {
        return Ok(service(ctx)?
            .picked_rows(pick, page(offset, limit), shown)
            .await
            .map_err(super::list_error)?
            .into_iter()
            .map(|(view, totals)| Torrent::of(view, totals.one_file))
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

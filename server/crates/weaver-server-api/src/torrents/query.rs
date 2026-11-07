// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use weaver_server_core::jobs::ids::JobId;

use super::mutation::action_error;
use super::service;
use super::types::{
    ScheduleSeedingGql, Torrent, TorrentPeerGql, TorrentRouteGql, TorrentTrackerGql,
};
use crate::auth::{ReadGuard, graphql_error};
use crate::jobs::types::decode_offset_cursor;

#[derive(Default)]
pub struct TorrentsQuery;

#[Object]
impl TorrentsQuery {
    #[graphql(guard = "ReadGuard")]
    async fn torrent(&self, ctx: &Context<'_>, id: u64) -> Result<Option<Torrent>> {
        Ok(service(ctx)?.torrent(JobId(id)).await.map(Into::into))
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
        let limit = first.unwrap_or(u32::MAX) as usize;
        Ok(service(ctx)?
            .torrents(category.as_deref())
            .await
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(Into::into)
            .collect())
    }
}

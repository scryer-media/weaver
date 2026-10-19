// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Object, Result};
use weaver_server_core::jobs::ids::JobId;

use super::service;
use super::types::Torrent;
use crate::auth::ReadGuard;

#[derive(Default)]
pub struct TorrentsQuery;

#[Object]
impl TorrentsQuery {
    #[graphql(guard = "ReadGuard")]
    async fn torrent(&self, ctx: &Context<'_>, id: u64) -> Result<Option<Torrent>> {
        Ok(service(ctx)?.torrent(JobId(id)).await.map(Into::into))
    }

    #[graphql(guard = "ReadGuard")]
    async fn torrents(&self, ctx: &Context<'_>, category: Option<String>) -> Result<Vec<Torrent>> {
        Ok(service(ctx)?
            .torrents(category.as_deref())
            .await
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

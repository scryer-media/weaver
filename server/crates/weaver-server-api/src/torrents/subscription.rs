// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use async_graphql::{Context, Result, Subscription};
use tokio_stream::Stream;
use weaver_server_core::jobs::ids::JobId;

use super::service;
use super::types::Torrent;
use crate::auth::ReadGuard;

/// How often the torrents are read again between pushes, for their rates.
const BEAT: Duration = Duration::from_secs(3);

#[derive(Default)]
pub struct TorrentsSubscription;

#[Subscription]
impl TorrentsSubscription {
    /// The torrents as they move: the one `id` names, or every torrent. The
    /// first push is the torrents as they are; each later one comes when one
    /// of them changes, a tracker's answer as soon as it lands.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_updates(
        &self,
        ctx: &Context<'_>,
        id: Option<u64>,
    ) -> Result<impl Stream<Item = Vec<Torrent>>> {
        let mut updates = service(ctx)?.updates(id.map(JobId), BEAT);
        Ok(async_stream::stream! {
            loop {
                yield updates.next().await.into_iter().map(Into::into).collect();
            }
        })
    }
}

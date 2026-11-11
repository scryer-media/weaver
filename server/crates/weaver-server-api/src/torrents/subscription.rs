// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use async_graphql::{Context, Result, Subscription};
use tokio_stream::Stream;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::updates::Follow;

use super::query::MAX_TORRENT_PAGE;
use super::service;
use super::types::TorrentChangesGql;
use crate::auth::ReadGuard;

/// How often the torrents are read again between pushes, for their rates.
const BEAT: Duration = Duration::from_secs(3);

#[derive(Default)]
pub struct TorrentsSubscription;

#[Subscription]
impl TorrentsSubscription {
    /// The torrents as they move: the one `id` names, the ones `ids` names
    /// (at most 500), or every torrent. The first push is the torrents as
    /// they are, with `fullUpdate`; each later one comes when one of them
    /// changes, a tracker's answer as soon as it lands, and carries only the
    /// torrents that changed or came and the ids of the ones gone.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_updates(
        &self,
        ctx: &Context<'_>,
        id: Option<u64>,
        ids: Option<Vec<u64>>,
    ) -> Result<impl Stream<Item = TorrentChangesGql>> {
        let follow = match (id, ids) {
            (Some(_), Some(_)) => return Err("give id or ids, not both".into()),
            (Some(id), None) => Follow::One(JobId(id)),
            (None, Some(ids)) if ids.len() > MAX_TORRENT_PAGE => {
                return Err(format!("at most {MAX_TORRENT_PAGE} ids").into());
            }
            (None, Some(ids)) => Follow::Some(ids.into_iter().map(JobId).collect()),
            (None, None) => Follow::Every,
        };
        let mut updates = service(ctx)?.follow(follow, BEAT);
        Ok(async_stream::stream! {
            loop {
                yield updates.next().await.into();
            }
        })
    }
}

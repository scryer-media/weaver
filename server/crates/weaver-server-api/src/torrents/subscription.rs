// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Result, Subscription};
use tokio_stream::Stream;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::updates::{Follow, FollowError, MAX_FOLLOWED};

use super::service;
use super::types::TorrentChangesGql;
use crate::auth::ReadGuard;

#[derive(Default)]
pub struct TorrentsSubscription;

#[Subscription]
impl TorrentsSubscription {
    /// The torrents as they move: the one `id` names, whole, or the ones
    /// `ids` names (at most 500), as a list shows them. Give one of the two.
    /// The first push is the torrents as they are, with `fullUpdate`; each
    /// later one comes when one of them changes and carries only the
    /// torrents that changed or came and the ids of the ones gone.
    ///
    /// `ids` reads each torrent's row as the service last published it: no
    /// files and no trackers, and a change in the fields that move with the
    /// clock alone is told with the next other change. Those torrents are
    /// followed only once the stored torrents are all read at start: asked
    /// before, it is an error with code `TORRENTS_LOADING`. `id` reads its
    /// torrent whole, files and trackers with it, each time it moves.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_updates(
        &self,
        ctx: &Context<'_>,
        id: Option<u64>,
        ids: Option<Vec<u64>>,
    ) -> Result<impl Stream<Item = TorrentChangesGql>> {
        let follow = match (id, ids) {
            (Some(_), Some(_)) => {
                return Err(crate::auth::invalid_input("give id or ids, not both"));
            }
            (Some(id), None) => Follow::One(JobId(id)),
            (None, Some(ids)) if ids.len() > MAX_FOLLOWED => {
                return Err(crate::auth::invalid_input(format!(
                    "at most {MAX_FOLLOWED} ids"
                )));
            }
            (None, Some(ids)) => Follow::Some(ids.into_iter().map(JobId).collect()),
            (None, None) => {
                return Err(crate::auth::invalid_input(
                    "give id, or ids (at most 500): every torrent is not followed",
                ));
            }
        };
        let service = service(ctx)?;
        let mut updates = service.follow(follow).map_err(|error| match error {
            FollowError::NotReady(why) => super::not_ready(why),
            FollowError::TooMany(_) => error.to_string().into(),
        })?;
        Ok(async_stream::stream! {
            loop {
                yield updates.next().await.into();
            }
        })
    }
}

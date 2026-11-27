// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use async_graphql::{Context, Result, Subscription};
use tokio_stream::Stream;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::listing::Shown;
use weaver_server_core::torrents::service::updates::{Follow, FollowError, MAX_FOLLOWED};

use super::types::{
    DEFAULT_FILE_PAGE, MAX_FILE_PAGE, TorrentChangesGql, TorrentFilePageGql, TorrentGql, page_args,
};
use super::{refuse_detail_only, service};
use crate::auth::{ReadGuard, invalid_input, not_found};

#[derive(Default)]
pub struct TorrentsSubscription;

#[Subscription]
impl TorrentsSubscription {
    /// One torrent as it moves: pushed as it is now, then again each time
    /// what the subscription selects changes, and a last null once the
    /// torrent is gone, after which the stream ends. Only the fields
    /// selected are read; its files are read whole when selected.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_updates(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<impl Stream<Item = Option<TorrentGql>>> {
        let field = ctx.look_ahead();
        let shown = Shown {
            files: field.field("files").exists(),
            trackers: field.field("trackers").exists(),
            tags: field.field("tags").exists(),
        };
        let mut updates = service(ctx)?
            .follow(Follow::One(JobId(id), shown))
            .map_err(follow_error)?;
        Ok(async_stream::stream! {
            loop {
                let changes = updates.next().await;
                let gone = changes.torrents.is_empty();
                yield TorrentChangesGql::from(changes).torrents.into_iter().next();
                if gone {
                    break;
                }
            }
        })
    }

    /// The torrents `ids` names (at most 500) as they move, as a list shows
    /// them. The first push is the torrents as they are, with `fullUpdate`;
    /// each later one comes when one of them changes and carries only the
    /// torrents that changed or came and the ids of the ones gone.
    ///
    /// Each torrent's row is read as the service last published it: no
    /// files, and a change in the fields that move with the clock alone is
    /// told with the next other change. A torrent's files, peer connections
    /// and unpacked files are read on `torrent(id)`, and asking for them
    /// here is refused. The torrents are followed only once the stored
    /// torrents are all read at start: asked before, it is an error with
    /// code `TORRENTS_LOADING`.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_row_updates(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
    ) -> Result<impl Stream<Item = TorrentChangesGql>> {
        refuse_detail_only(&ctx.look_ahead().field("torrents"), "torrentRowUpdates")?;
        if ids.len() > MAX_FOLLOWED {
            return Err(invalid_input(format!("at most {MAX_FOLLOWED} ids")));
        }
        let mut updates = service(ctx)?
            .follow(Follow::Some(ids.into_iter().map(JobId).collect()))
            .map_err(follow_error)?;
        Ok(async_stream::stream! {
            loop {
                yield updates.next().await.into();
            }
        })
    }

    /// One page of a torrent's files as they move: the `first` after the
    /// offset cursor `after`, 200 when not given and at most 1000. Sent at
    /// once, then each time the page changes, at most twice a second. A
    /// torrent that does not exist is an error with code `NOT_FOUND`; once
    /// the torrent is gone, the stream ends.
    #[graphql(guard = "ReadGuard")]
    async fn torrent_file_updates(
        &self,
        ctx: &Context<'_>,
        id: u64,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<impl Stream<Item = TorrentFilePageGql>> {
        let (offset, limit) = page_args(first, after.as_deref(), DEFAULT_FILE_PAGE, MAX_FILE_PAGE)?;
        let service = service(ctx)?;
        let mut updates = service.file_updates(JobId(id), offset, limit);
        let Some(page) = updates.next().await else {
            return Err(not_found(format!("torrent {id} not found")));
        };
        Ok(async_stream::stream! {
            yield TorrentFilePageGql::of(page, offset, limit);
            while let Some(page) = updates.next().await {
                yield TorrentFilePageGql::of(page, offset, limit);
            }
        })
    }
}

fn follow_error(error: FollowError) -> async_graphql::Error {
    match error {
        FollowError::NotReady(why) => super::not_ready(why),
        FollowError::TooMany(_) => invalid_input(error.to_string()),
    }
}

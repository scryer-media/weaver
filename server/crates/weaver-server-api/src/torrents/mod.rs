// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod mutation;
mod query;
mod subscription;
pub mod types;

pub use mutation::TorrentsMutation;
pub use query::TorrentsQuery;
pub use subscription::TorrentsSubscription;

use async_graphql::{Context, Lookahead, Result};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::listing::Shown;
use weaver_server_core::torrents::service::{ListError, NotReady, TorrentService, TorrentSettings};

use crate::auth::{graphql_error, invalid_input, not_found};
use types::TorrentGql;

// The torrent client. It starts with the server; the settings turn it on
// and off while it runs.
fn service<'a>(ctx: &'a Context<'_>) -> Result<&'a TorrentService> {
    ctx.data_opt::<TorrentService>()
        .ok_or_else(|| graphql_error("TORRENTS_DISABLED", "the torrent client is not running"))
}

// What one torrent's selection `field` shows beside its own fields. Its
// files are paged by their own read, so the view is read without them.
fn shown_of(field: &Lookahead<'_>) -> Shown {
    Shown {
        files: false,
        trackers: field.field("trackers").exists(),
        tags: field.field("tags").exists(),
    }
}

// The torrent `id`, reading only what `field`, its selection, shows.
async fn read_torrent(
    ctx: &Context<'_>,
    field: &Lookahead<'_>,
    id: u64,
) -> Result<Option<TorrentGql>> {
    Ok(service(ctx)?
        .torrent_read(JobId(id), shown_of(field))
        .await
        .map(|(view, one_file)| TorrentGql::of(view, one_file)))
}

// The torrent `id` a mutation changed, as `field` selects it.
async fn changed_torrent(ctx: &Context<'_>, id: u64) -> Result<TorrentGql> {
    read_torrent(ctx, &ctx.look_ahead(), id)
        .await?
        .ok_or_else(|| not_found(format!("torrent {id} left before it was read")))
}

// A torrent's fields read on `torrent(id)` alone, refused under `under`, a
// list: `field` is the selection of one of its torrents. Asked before any
// read.
fn refuse_detail_only(field: &Lookahead<'_>, under: &str) -> Result<()> {
    let found = [
        ("files", field.field("files").exists()),
        ("peerConnections", field.field("peerConnections").exists()),
        (
            "unpacking.files",
            field.field("unpacking").field("files").exists(),
        ),
    ];
    match found.iter().find(|(_, exists)| *exists) {
        Some((name, _)) => Err(invalid_input(format!(
            "{name} is read on torrent(id), not under {under}"
        ))),
        None => Ok(()),
    }
}

// After a tracker rule changes: the torrent client matches every torrent's
// trackers against the rules as they are now.
// An answer that needs every torrent, asked for while the stored torrents
// are still being read at start: said under its own code, so a client asks
// again rather than showing some of them as all.
fn not_ready(why: NotReady) -> async_graphql::Error {
    graphql_error("TORRENTS_LOADING", why.to_string())
}

fn list_error(error: ListError) -> async_graphql::Error {
    match error {
        ListError::NotReady(why) => not_ready(why),
        ListError::State(error) => crate::observability::state_error(error),
    }
}

pub(crate) async fn reload_tracker_rules(ctx: &Context<'_>) -> Result<()> {
    if let Some(torrents) = ctx.data_opt::<TorrentService>() {
        torrents
            .reload_tracker_rules()
            .await
            .map_err(|error| crate::auth::graphql_error("INTERNAL", error))?;
    }
    Ok(())
}

// After a settings or category change: the torrent client takes the
// settings as they are now.
pub(crate) async fn reconcile(ctx: &Context<'_>) -> Result<()> {
    if let Some(torrents) = ctx.data_opt::<TorrentService>() {
        let config = ctx.data::<weaver_server_core::settings::SharedConfig>()?;
        torrents
            .set_settings(TorrentSettings::of(&*config.read().await))
            .await;
    }
    Ok(())
}

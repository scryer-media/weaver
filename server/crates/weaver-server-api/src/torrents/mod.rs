// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod mutation;
mod query;
mod subscription;
pub mod types;

pub use mutation::TorrentsMutation;
pub use query::TorrentsQuery;
pub use subscription::TorrentsSubscription;

use async_graphql::{Context, Result};
use weaver_server_core::torrents::service::{TorrentService, TorrentSettings};

use crate::auth::graphql_error;

// The torrent client. It starts with the server; the settings turn it on
// and off while it runs.
fn service<'a>(ctx: &'a Context<'_>) -> Result<&'a TorrentService> {
    ctx.data_opt::<TorrentService>()
        .ok_or_else(|| graphql_error("TORRENTS_DISABLED", "the torrent client is not running"))
}

// After a tracker rule changes: the torrent client matches every torrent's
// trackers against the rules as they are now.
pub(crate) async fn reload_tracker_rules(ctx: &Context<'_>) -> Result<()> {
    if let Some(torrents) = ctx.data_opt::<TorrentService>() {
        torrents
            .reload_tracker_rules()
            .await
            .map_err(async_graphql::Error::new)?;
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

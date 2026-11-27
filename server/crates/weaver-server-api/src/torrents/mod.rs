// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod mutation;
mod query;
mod subscription;
pub mod types;

pub use mutation::TorrentsMutation;
pub use query::TorrentsQuery;
pub use subscription::TorrentsSubscription;

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use async_graphql::{Context, Lookahead, Result};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::listing::Shown;
use weaver_server_core::torrents::service::snapshot::TorrentRow;
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

/// The published torrent rows a list of queue items shares: read once for
/// the whole list, and only when one of its items' `torrent` is asked for.
#[derive(Clone, Default)]
pub struct QueueTorrentRows(Option<Arc<SharedRows>>);

struct SharedRows {
    ids: Vec<JobId>,
    rows: OnceLock<HashMap<JobId, Arc<TorrentRow>>>,
}

impl std::fmt::Debug for QueueTorrentRows {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QueueTorrentRows")
    }
}

// A share of rows is how an item is read, not part of the item.
impl PartialEq for QueueTorrentRows {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl QueueTorrentRows {
    /// One share of the published rows for every torrent of `items`.
    pub(crate) fn share(items: &mut [crate::jobs::types::QueueItem]) {
        let ids: Vec<JobId> = items
            .iter()
            .filter(|item| item.protocol == crate::jobs::types::ProtocolGql::Torrent)
            .map(|item| JobId(item.id))
            .collect();
        if ids.is_empty() {
            return;
        }
        let shared = Self(Some(Arc::new(SharedRows {
            ids,
            rows: OnceLock::new(),
        })));
        for item in items {
            item.torrent_rows = shared.clone();
        }
    }
}

// The queue item `id`'s torrent, from the rows `rows` shares, or from its
// own row when none are shared. Never waits on the pipeline: the rows are
// the published ones `torrentRowUpdates` reads.
pub(crate) fn queue_torrent(
    ctx: &Context<'_>,
    rows: &QueueTorrentRows,
    id: u64,
) -> Result<Option<TorrentGql>> {
    refuse_detail_only(&ctx.look_ahead(), "QueueItem.torrent")?;
    let Some(service) = ctx.data_opt::<TorrentService>() else {
        return Ok(None);
    };
    let snapshot = service.snapshot();
    let job_id = JobId(id);
    let row = match &rows.0 {
        Some(shared) => shared
            .rows
            .get_or_init(|| {
                snapshot
                    .rows_of(&shared.ids)
                    .into_iter()
                    .map(|row| (row.view.job_id, row))
                    .collect()
            })
            .get(&job_id)
            .cloned(),
        None => snapshot.rows_of([&job_id]).into_iter().next(),
    };
    Ok(row.map(|row| TorrentGql::of(row.view.clone(), row.totals.one_file)))
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

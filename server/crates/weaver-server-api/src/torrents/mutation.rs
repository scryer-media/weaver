// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{IpAddr, SocketAddr};

use async_graphql::{Context, Object, Result};
use base64::Engine;
use weaver_server_core::StateError;
use weaver_server_core::ingest::Kind;
use weaver_server_core::jobs::class::AddRefusal;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::{ActionError, AddRequest, AddSource};

use super::types::{
    SubmitTorrentInputGql, TorrentFilePriorityInputGql, TorrentGql, TorrentSourceInput,
    TorrentTrackerInput,
};
use super::{changed_torrent, service};
use crate::auth::{CallerScope, ControlGuard, graphql_error, invalid_input};
use crate::jobs::staging::{classify_upload, upload_refusal};
use crate::settings::types::SeedingPolicyInput;

pub(super) fn action_error(error: ActionError) -> async_graphql::Error {
    let code = match &error {
        ActionError::NotFound(_) => "NOT_FOUND",
        ActionError::UnmetGoal(_) => "SEEDING_GOAL_UNMET",
        ActionError::Invalid(_) => "INVALID_INPUT",
        ActionError::State(message) => {
            tracing::error!(error = %message, "torrent action failed on its state");
            return crate::observability::state_error(StateError::Database(message.clone()));
        }
        ActionError::Forbidden(_) => "FORBIDDEN",
    };
    graphql_error(code, error.to_string())
}

/// A torrent source's metainfo or magnet link, an upload read within the
/// bounds `submit` reads one in.
pub(crate) async fn torrent_source(
    ctx: &Context<'_>,
    source: TorrentSourceInput,
) -> Result<AddSource> {
    Ok(match source {
        TorrentSourceInput::MetainfoBase64(encoded) => AddSource::Metainfo(
            base64::engine::general_purpose::STANDARD
                .decode(encoded.trim())
                .map_err(|_| invalid_input("metainfoBase64 is not base64"))?,
        ),
        TorrentSourceInput::MetainfoUpload(upload) => {
            let upload = upload
                .value(ctx)
                .map_err(|error| invalid_input(format!("invalid upload: {error}")))?;
            match classify_upload(&upload, true).await.map_err(upload_refusal)? {
                (Kind::Metainfo, decoded) => {
                    AddSource::Metainfo(decoded.unwrap_or_else(|| upload.content.to_vec()))
                }
                _ => return Err(invalid_input("metainfoUpload is not a torrent file")),
            }
        }
        TorrentSourceInput::Magnet(magnet) => AddSource::Magnet(magnet),
    })
}

#[derive(Default)]
pub struct TorrentsMutation;

impl TorrentsMutation {
    /// Add a torrent, with the client's refusal in its own words.
    pub(crate) async fn add(ctx: &Context<'_>, request: AddRequest) -> Result<JobId> {
        service(ctx)?.add(request).await.map_err(|error| {
            crate::jobs::mutation::add_refusal_error(
                weaver_server_core::jobs::Protocol::Torrent,
                AddRefusal::from(error),
            )
        })
    }
}

#[Object]
impl TorrentsMutation {
    /// Add a torrent from its metainfo or a magnet link, and return it.
    /// An uploaded metainfo file is bounded as `submit` bounds one.
    #[graphql(guard = "ControlGuard")]
    async fn submit_torrent(
        &self,
        ctx: &Context<'_>,
        input: SubmitTorrentInputGql,
    ) -> Result<TorrentGql> {
        service(ctx)?;
        let source = torrent_source(ctx, input.source).await?;
        let policy_override = input
            .seeding
            .map(SeedingPolicyInput::into_policy)
            .transpose()?;
        let job_id = Self::add(
            ctx,
            AddRequest {
                source,
                category: input.category,
                policy_override,
                paused: input.paused,
                forced: input.forced,
                unpack: input.unpack,
                metadata: Vec::new(),
                upload_limit: input.upload_limit_bps,
                download_limit: input.download_limit_bps,
                placement: Default::default(),
                tags: input.tags,
            },
        )
        .await?;
        changed_torrent(ctx, job_id.0).await
    }

    /// Remove the torrent, and its files when `deleteFiles`; return its id.
    /// Never refused for a seeding goal still to meet.
    #[graphql(guard = "ControlGuard")]
    async fn remove_torrent(
        &self,
        ctx: &Context<'_>,
        id: u64,
        #[graphql(default)] delete_files: bool,
    ) -> Result<u64> {
        service(ctx)?
            .remove(JobId(id), delete_files)
            .await
            .map_err(action_error)?;
        Ok(id)
    }

    /// Run the torrent outside the queue's limits, or under them again.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_forced(
        &self,
        ctx: &Context<'_>,
        id: u64,
        forced: bool,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .set_forced(JobId(id), forced)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Move the torrent's directory under `path`; it goes on from there.
    /// A key that only controls may name only a path under the default
    /// save folder or a category's folder.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_location(&self, ctx: &Context<'_>, id: u64, path: String) -> Result<TorrentGql> {
        let caller = *ctx.data::<CallerScope>()?;
        service(ctx)?
            .set_location_as(caller, JobId(id), path)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Set each named file's level; the others keep theirs.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_file_priorities(
        &self,
        ctx: &Context<'_>,
        id: u64,
        priorities: Vec<TorrentFilePriorityInputGql>,
    ) -> Result<TorrentGql> {
        let priorities: Vec<_> = priorities
            .into_iter()
            .map(|file| (file.index, file.priority.into()))
            .collect();
        service(ctx)?
            .set_file_priorities(JobId(id), &priorities)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Check every piece of the torrent again, trusting nothing saved.
    #[graphql(guard = "ControlGuard")]
    async fn recheck_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<TorrentGql> {
        service(ctx)?
            .recheck(JobId(id))
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Fetch the torrent's pieces in order, or not.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_sequential(
        &self,
        ctx: &Context<'_>,
        id: u64,
        sequential: bool,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .set_sequential(JobId(id), sequential)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Fetch each file's first and last pieces first, or not.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_first_last_piece_first(
        &self,
        ctx: &Context<'_>,
        id: u64,
        first_last_piece_first: bool,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .set_first_last_piece_first(JobId(id), first_last_piece_first)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Ban a peer's address from the torrent: an IP, or an IP and port
    /// whose port is ignored.
    #[graphql(guard = "ControlGuard")]
    async fn ban_torrent_peer(&self, ctx: &Context<'_>, id: u64, address: String) -> Result<TorrentGql> {
        let address = address.trim();
        let ip = address
            .parse::<SocketAddr>()
            .map(|addr| addr.ip())
            .or_else(|_| address.parse::<IpAddr>())
            .map_err(|_| graphql_error("INVALID_INPUT", "address is not an IP address"))?;
        service(ctx)?
            .ban_peers(JobId(id), &[ip])
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Set both of the torrent's own limits in bytes per second, in force on
    /// its peers at once. As `setBandwidth`: null or left out is no limit, 0
    /// lets nothing through, and a value past the largest stored one is
    /// refused.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_rate_limits(
        &self,
        ctx: &Context<'_>,
        id: u64,
        upload_bps: Option<u64>,
        download_bps: Option<u64>,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .set_rate_limits(JobId(id), Some(upload_bps), Some(download_bps))
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Add `urls` as one tier after the torrent's last; the ones it lists
    /// already are left out.
    #[graphql(guard = "ControlGuard")]
    async fn add_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        urls: Vec<String>,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .add_trackers(JobId(id), vec![urls])
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// List `trackers` in place of the torrent's own.
    #[graphql(guard = "ControlGuard")]
    async fn replace_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        trackers: Vec<TorrentTrackerInput>,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .replace_trackers(
                JobId(id),
                trackers
                    .into_iter()
                    .map(|tracker| (tracker.tier, tracker.url))
                    .collect(),
            )
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Take `urls` out of the torrent's tracker list.
    #[graphql(guard = "ControlGuard")]
    async fn remove_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        urls: Vec<String>,
    ) -> Result<TorrentGql> {
        service(ctx)?
            .remove_trackers(JobId(id), &urls)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Announce now, to every tracker or to `tracker` alone.
    #[graphql(guard = "ControlGuard")]
    async fn reannounce_torrent(
        &self,
        ctx: &Context<'_>,
        id: u64,
        tracker: Option<String>,
    ) -> Result<TorrentGql> {
        let service = service(ctx)?;
        match tracker {
            Some(url) => service.reannounce_tracker(JobId(id), &url).await,
            None => service.reannounce(JobId(id)).await,
        }
        .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// Look the torrent up on the DHT now.
    #[graphql(guard = "ControlGuard")]
    async fn dht_announce_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<TorrentGql> {
        service(ctx)?
            .dht_announce(JobId(id))
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }

    /// The torrent's own seeding policy; null returns it to its category's
    /// and the global one.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_seeding(
        &self,
        ctx: &Context<'_>,
        id: u64,
        seeding: Option<SeedingPolicyInput>,
    ) -> Result<TorrentGql> {
        let policy = seeding.map(SeedingPolicyInput::into_policy).transpose()?;
        service(ctx)?
            .set_policy_override(JobId(id), policy)
            .await
            .map_err(action_error)?;
        changed_torrent(ctx, id).await
    }
}

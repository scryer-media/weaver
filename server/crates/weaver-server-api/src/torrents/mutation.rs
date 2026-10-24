// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{IpAddr, SocketAddr};

use async_graphql::{Context, MaybeUndefined, Object, Result};
use base64::Engine;
use weaver_server_core::SchedulerHandle;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::{ActionError, AddError, AddRequest, AddSource};

use super::service;
use super::types::{SubmitTorrentInput, Torrent, TorrentFilePriorityInput, TorrentTrackerInput};
use crate::auth::{ControlGuard, graphql_error};
use crate::settings::types::SeedingPolicyInput;

fn add_error(error: AddError) -> async_graphql::Error {
    let code = match &error {
        AddError::Invalid(_) => "INVALID_INPUT",
        AddError::Refused(_) => "REFUSED",
        AddError::Duplicate(_) => "DUPLICATE",
        AddError::NoNetwork(_) => "NO_NETWORK",
        AddError::State(_) => "INTERNAL",
        AddError::Off => "TORRENTS_DISABLED",
    };
    graphql_error(code, error.to_string())
}

pub(super) fn action_error(error: ActionError) -> async_graphql::Error {
    let code = match &error {
        ActionError::NotFound(_) => "NOT_FOUND",
        ActionError::UnmetGoal(_) => "SEEDING_GOAL_UNMET",
        ActionError::Invalid(_) => "INVALID_INPUT",
        ActionError::State(_) => "INTERNAL",
    };
    graphql_error(code, error.to_string())
}

#[derive(Default)]
pub struct TorrentsMutation;

impl TorrentsMutation {
    /// Add a torrent, with the client's refusal in its own words.
    pub(crate) async fn add(ctx: &Context<'_>, request: AddRequest) -> Result<JobId> {
        service(ctx)?.add(request).await.map_err(add_error)
    }
}

#[Object]
impl TorrentsMutation {
    #[graphql(guard = "ControlGuard")]
    async fn submit_torrent(
        &self,
        ctx: &Context<'_>,
        input: SubmitTorrentInput,
    ) -> Result<Torrent> {
        let torrents = service(ctx)?;
        let source = match (input.metainfo_base64, input.magnet) {
            (Some(encoded), None) => AddSource::Metainfo(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded.trim())
                    .map_err(|_| graphql_error("INVALID_INPUT", "metainfoBase64 is not base64"))?,
            ),
            (None, Some(magnet)) => AddSource::Magnet(magnet),
            _ => {
                return Err(graphql_error(
                    "INVALID_INPUT",
                    "give one of metainfoBase64 or magnet",
                ));
            }
        };
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
            },
        )
        .await?;
        torrents
            .torrent(job_id)
            .await
            .map(Into::into)
            .ok_or_else(|| graphql_error("NOT_FOUND", "the torrent left before it was read"))
    }

    #[graphql(guard = "ControlGuard")]
    async fn remove_torrent(
        &self,
        ctx: &Context<'_>,
        id: u64,
        #[graphql(default)] delete_files: bool,
        #[graphql(default)] confirm_unmet_goal: bool,
    ) -> Result<bool> {
        service(ctx)?
            .remove(JobId(id), delete_files, confirm_unmet_goal)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn pause_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        service(ctx)?.pause(JobId(id)).await.map_err(action_error)?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn resume_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        service(ctx)?
            .resume(JobId(id))
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Pause every torrent, seeding too, as one hold. Each torrent's own
    /// pause or resume stands under it.
    #[graphql(guard = "ControlGuard")]
    async fn pause_torrents(&self, ctx: &Context<'_>) -> Result<bool> {
        ctx.data::<SchedulerHandle>()?
            .pause_torrents()
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        Ok(true)
    }

    /// Lift the operator's pause of every torrent.
    #[graphql(guard = "ControlGuard")]
    async fn resume_torrents(&self, ctx: &Context<'_>) -> Result<bool> {
        ctx.data::<SchedulerHandle>()?
            .resume_torrents()
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_forced(&self, ctx: &Context<'_>, id: u64, forced: bool) -> Result<bool> {
        service(ctx)?
            .set_forced(JobId(id), forced)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Move the torrent's directory under `path`; it goes on from there.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_location(&self, ctx: &Context<'_>, id: u64, path: String) -> Result<bool> {
        service(ctx)?
            .set_location(JobId(id), path)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Set each named file's level; the others keep theirs.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_file_priorities(
        &self,
        ctx: &Context<'_>,
        id: u64,
        priorities: Vec<TorrentFilePriorityInput>,
    ) -> Result<bool> {
        let priorities: Vec<_> = priorities
            .into_iter()
            .map(|file| (file.index, file.priority.into()))
            .collect();
        service(ctx)?
            .set_file_priorities(JobId(id), &priorities)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Check every piece of the torrent again, trusting nothing saved.
    #[graphql(guard = "ControlGuard")]
    async fn recheck_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        service(ctx)?
            .recheck(JobId(id))
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_sequential(
        &self,
        ctx: &Context<'_>,
        id: u64,
        sequential: bool,
    ) -> Result<bool> {
        service(ctx)?
            .set_sequential(JobId(id), sequential)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_first_last_piece_first(
        &self,
        ctx: &Context<'_>,
        id: u64,
        first_last_piece_first: bool,
    ) -> Result<bool> {
        service(ctx)?
            .set_first_last_piece_first(JobId(id), first_last_piece_first)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_category(
        &self,
        ctx: &Context<'_>,
        id: u64,
        category: Option<String>,
    ) -> Result<bool> {
        service(ctx)?
            .set_category(JobId(id), category)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Ban a peer's address from the torrent: an IP, or an IP and port
    /// whose port is ignored.
    #[graphql(guard = "ControlGuard")]
    async fn ban_torrent_peer(&self, ctx: &Context<'_>, id: u64, address: String) -> Result<bool> {
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
        Ok(true)
    }

    /// The torrent's own upload and download limits in bytes per second,
    /// in force on its peers at once; null is unlimited and 0 closes that
    /// direction. A limit left out is kept.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_rate_limits(
        &self,
        ctx: &Context<'_>,
        id: u64,
        upload: MaybeUndefined<u64>,
        download: MaybeUndefined<u64>,
    ) -> Result<bool> {
        service(ctx)?
            .set_rate_limits(JobId(id), upload.into(), download.into())
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Add `urls` as one tier after the torrent's last; the ones it lists
    /// already are left out.
    #[graphql(guard = "ControlGuard")]
    async fn add_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        urls: Vec<String>,
    ) -> Result<bool> {
        service(ctx)?
            .add_trackers(JobId(id), vec![urls])
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// List `trackers` in place of the torrent's own.
    #[graphql(guard = "ControlGuard")]
    async fn replace_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        trackers: Vec<TorrentTrackerInput>,
    ) -> Result<bool> {
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
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn remove_torrent_trackers(
        &self,
        ctx: &Context<'_>,
        id: u64,
        urls: Vec<String>,
    ) -> Result<bool> {
        service(ctx)?
            .remove_trackers(JobId(id), &urls)
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// Announce now, to every tracker or to `tracker` alone.
    #[graphql(guard = "ControlGuard")]
    async fn reannounce_torrent(
        &self,
        ctx: &Context<'_>,
        id: u64,
        tracker: Option<String>,
    ) -> Result<bool> {
        let service = service(ctx)?;
        match tracker {
            Some(url) => service.reannounce_tracker(JobId(id), &url).await,
            None => service.reannounce(JobId(id)).await,
        }
        .map_err(action_error)?;
        Ok(true)
    }

    /// Look the torrent up on the DHT now.
    #[graphql(guard = "ControlGuard")]
    async fn dht_announce_torrent(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        service(ctx)?
            .dht_announce(JobId(id))
            .await
            .map_err(action_error)?;
        Ok(true)
    }

    /// The torrent's own seeding policy; null returns it to its category's
    /// and the global one.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrent_seeding(
        &self,
        ctx: &Context<'_>,
        id: u64,
        seeding: Option<SeedingPolicyInput>,
    ) -> Result<bool> {
        let policy = seeding.map(SeedingPolicyInput::into_policy).transpose()?;
        service(ctx)?
            .set_policy_override(JobId(id), policy)
            .await
            .map_err(action_error)?;
        Ok(true)
    }
}

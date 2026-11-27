// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{IpAddr, SocketAddr};

use async_graphql::{Context, Object, Result};
use base64::Engine;
use weaver_server_core::StateError;
use weaver_server_core::ingest::Kind;
use weaver_server_core::jobs::class::AddRefusal;
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::torrents::service::{
    ActionError, AddError, AddRequest, AddSource, TorrentSetting,
};

use super::types::{
    SubmitTorrentInputGql, SubmitTorrentResultGql, SubmitTorrentStatusGql,
    TorrentFilePriorityInputGql, TorrentGql, TorrentSourceInput, TorrentTrackerInput,
};
use super::{changed_torrent, read_torrent, service};
use crate::auth::{CallerScope, ControlGuard, graphql_error, invalid_input, not_found};
use crate::jobs::batch::MAX_QUEUE_BATCH_IDS;
use crate::jobs::staging::{classify_upload, upload_refusal};
use crate::jobs::types::{
    CLIENT_REQUEST_ID_ATTRIBUTE_KEY, QueueBatchRefusal, QueueBatchResult, submit_metadata,
};
use crate::settings::types::SeedingPolicyInput;

pub(super) fn action_error(error: ActionError) -> async_graphql::Error {
    let (code, message) = action_refusal(&error);
    graphql_error(code, message)
}

// The code and message a refused torrent action answers with, one torrent
// or a batch of them: the one mapping of action refusals to codes. A
// failure on its state is logged, and answered as any state failure is.
fn action_refusal(error: &ActionError) -> (&'static str, String) {
    let code = match error {
        ActionError::NotFound(_) => "NOT_FOUND",
        // No torrent mutation removes under a seeding goal's gate: one that
        // got here is refused as the queue refuses it.
        ActionError::UnmetGoal(_) => "CONFLICT",
        ActionError::Invalid(_) => "INVALID_INPUT",
        ActionError::State(message) => {
            tracing::error!(error = %message, "torrent action failed on its state");
            let error = StateError::Database(message.clone());
            return (crate::observability::state_code(&error), error.to_string());
        }
        ActionError::Forbidden(_) => "FORBIDDEN",
    };
    (code, error.to_string())
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
            match classify_upload(&upload, true)
                .await
                .map_err(upload_refusal)?
            {
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

#[Object]
impl TorrentsMutation {
    /// Add a torrent from its metainfo or a magnet link, and say how the
    /// add came out. A torrent already added is answered, not refused: as
    /// a replay when the add carries the client request id it was added
    /// with, as a duplicate otherwise. An uploaded metainfo file is bounded
    /// as `submit` bounds one.
    #[graphql(guard = "ControlGuard")]
    async fn submit_torrent(
        &self,
        ctx: &Context<'_>,
        input: SubmitTorrentInputGql,
    ) -> Result<SubmitTorrentResultGql> {
        service(ctx)?;
        let source = torrent_source(ctx, input.source).await?;
        let policy_override = input
            .seeding
            .map(SeedingPolicyInput::into_policy)
            .transpose()?;
        let metadata = submit_metadata(input.attributes, input.client_request_id.clone())
            .map_err(|message| graphql_error("INVALID_INPUT", message))?;
        let request = AddRequest {
            source,
            category: input.category,
            policy_override,
            paused: input.paused,
            forced: input.forced,
            unpack: input.unpack,
            metadata,
            upload_limit: input.upload_limit_bps,
            download_limit: input.download_limit_bps,
            placement: Default::default(),
            tags: input.tags,
        };
        let client_request_id = input.client_request_id;
        let (job_id, status) = match service(ctx)?.add(request).await {
            Ok(job_id) => (job_id, SubmitTorrentStatusGql::Accepted),
            Err(AddError::Duplicate(existing)) => {
                let replay = same_request(ctx, existing, client_request_id.as_deref()).await?;
                let status = if replay {
                    SubmitTorrentStatusGql::IdempotentReplay
                } else {
                    SubmitTorrentStatusGql::Duplicate
                };
                (existing, status)
            }
            Err(error) => return Err(add_error(error)),
        };
        let torrent = read_torrent(ctx, &ctx.look_ahead().field("torrent"), job_id.0)
            .await?
            .ok_or_else(|| not_found(format!("torrent {} left before it was read", job_id.0)))?;
        Ok(SubmitTorrentResultGql {
            accepted: status != SubmitTorrentStatusGql::Duplicate,
            status,
            torrent,
            client_request_id,
        })
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
    async fn set_torrent_location(
        &self,
        ctx: &Context<'_>,
        id: u64,
        path: String,
    ) -> Result<TorrentGql> {
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
    async fn ban_torrent_peer(
        &self,
        ctx: &Context<'_>,
        id: u64,
        address: String,
    ) -> Result<TorrentGql> {
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

    /// Check every piece of each torrent named again, as one unit. One
    /// refused ends the rest: they are listed refused too.
    #[graphql(guard = "ControlGuard")]
    async fn recheck_torrents(&self, ctx: &Context<'_>, ids: Vec<u64>) -> Result<QueueBatchResult> {
        let ids = batch_ids(&ids)?;
        let outcome = service(ctx)?.recheck_many(&ids).await;
        Ok(torrent_batch_result(&ids, outcome))
    }

    /// Run each torrent named outside the queue's limits, or under them
    /// again, as one unit.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrents_forced(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        forced: bool,
    ) -> Result<QueueBatchResult> {
        set_torrents(ctx, &ids, TorrentSetting::Forced(forced)).await
    }

    /// Set both of each named torrent's own limits in bytes per second, as
    /// `setTorrentRateLimits` sets one torrent's, as one unit.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrents_rate_limits(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        upload_bps: Option<u64>,
        download_bps: Option<u64>,
    ) -> Result<QueueBatchResult> {
        let setting = TorrentSetting::RateLimits {
            upload: Some(upload_bps),
            download: Some(download_bps),
        };
        set_torrents(ctx, &ids, setting).await
    }

    /// Each named torrent's own seeding policy, as `setTorrentSeeding` sets
    /// one torrent's, as one unit; null returns them to their category's and
    /// the global one.
    #[graphql(guard = "ControlGuard")]
    async fn set_torrents_seeding(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        seeding: Option<SeedingPolicyInput>,
    ) -> Result<QueueBatchResult> {
        let policy = seeding.map(SeedingPolicyInput::into_policy).transpose()?;
        set_torrents(ctx, &ids, TorrentSetting::Policy(policy)).await
    }

    /// Remove each torrent named, and its files when `deleteFiles`, as one
    /// unit. Never refused for a seeding goal still to meet.
    #[graphql(guard = "ControlGuard")]
    async fn remove_torrents(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        #[graphql(default)] delete_files: bool,
    ) -> Result<QueueBatchResult> {
        let ids = batch_ids(&ids)?;
        let removals: Vec<(JobId, bool)> = ids.iter().map(|&id| (id, delete_files)).collect();
        let outcome = service(ctx)?.remove_many(&removals, true).await;
        Ok(torrent_batch_result(&ids, outcome))
    }
}

// Whether an add refused as the torrent `existing` already is carries the
// client request id that torrent was added with. One read of its row's
// metadata, and only when the add carries an id.
async fn same_request(
    ctx: &Context<'_>,
    existing: JobId,
    client_request_id: Option<&str>,
) -> Result<bool> {
    let Some(asked) = client_request_id.map(str::trim).filter(|id| !id.is_empty()) else {
        return Ok(false);
    };
    let metadata = service(ctx)?
        .added_metadata(existing)
        .await
        .map_err(add_error)?;
    Ok(metadata
        .iter()
        .any(|(key, value)| key == CLIENT_REQUEST_ID_ATTRIBUTE_KEY && value == asked))
}

// A refused add's GraphQL error, under the one mapping of refusals to codes.
fn add_error(error: AddError) -> async_graphql::Error {
    crate::jobs::mutation::add_refusal_error(
        weaver_server_core::jobs::Protocol::Torrent,
        AddRefusal::from(error),
    )
}

// The distinct ids a torrent batch names, in the order given, within the
// queue batches' limit.
fn batch_ids(ids: &[u64]) -> Result<Vec<JobId>> {
    let mut seen = std::collections::BTreeSet::new();
    let ids: Vec<JobId> = ids
        .iter()
        .copied()
        .filter(|id| seen.insert(*id))
        .map(JobId)
        .collect();
    if ids.len() > MAX_QUEUE_BATCH_IDS {
        return Err(graphql_error(
            "BATCH_TOO_LARGE",
            format!("a batch names at most {MAX_QUEUE_BATCH_IDS} ids"),
        ));
    }
    Ok(ids)
}

// `setting` made to each torrent `ids` names, as one unit. The setting is
// checked first, as the single mutation checks it: refused, nothing changes.
async fn set_torrents(
    ctx: &Context<'_>,
    ids: &[u64],
    setting: TorrentSetting,
) -> Result<QueueBatchResult> {
    let ids = batch_ids(ids)?;
    let outcome = service(ctx)?
        .set_many(ids.iter().map(|&id| (id, setting)).collect())
        .await;
    if let Some(Err(error)) = outcome
        .values()
        .find(|result| matches!(result, Err(ActionError::Invalid(_))))
    {
        return Err(action_error(error.clone()));
    }
    Ok(torrent_batch_result(&ids, outcome))
}

// The answer a torrent batch returns, in the queue batches' shape: each id
// it named, and why each one that did not take did not, under the codes
// one torrent's action answers with. An id the batch did not reach, after
// one refused, is refused as not reached.
fn torrent_batch_result(
    ids: &[JobId],
    outcome: std::collections::BTreeMap<JobId, std::result::Result<(), ActionError>>,
) -> QueueBatchResult {
    let matched = u32::try_from(ids.len()).unwrap_or(u32::MAX);
    let refused: Vec<QueueBatchRefusal> = ids
        .iter()
        .filter_map(|id| {
            let (code, message) = match outcome.get(id) {
                Some(Ok(())) => return None,
                Some(Err(error)) => action_refusal(error),
                None => (
                    "CONFLICT",
                    "not reached: a torrent before it in the batch was refused".to_owned(),
                ),
            };
            Some(QueueBatchRefusal {
                id: id.0,
                code: code.to_owned(),
                message,
            })
        })
        .collect();
    QueueBatchResult {
        matched,
        applied: matched.saturating_sub(u32::try_from(refused.len()).unwrap_or(u32::MAX)),
        refused,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A batch refuses each torrent under the code and message the action
    // on that one torrent answers with; only an id it did not reach is
    // refused as not reached.
    #[test]
    fn a_batch_refuses_each_torrent_as_one_torrent_is_refused() {
        let errors = [
            ActionError::NotFound(JobId(1)),
            ActionError::UnmetGoal("ratio 0.5 of 1".into()),
            ActionError::Invalid("no such priority".into()),
            ActionError::State("disk I/O error".into()),
            ActionError::Forbidden("outside the save folders".into()),
        ];
        let ids: Vec<JobId> = (1..=6).map(JobId).collect();
        let outcome = ids
            .iter()
            .zip(errors.iter().cloned().map(Err))
            .map(|(&id, result)| (id, result))
            .collect();
        let result = torrent_batch_result(&ids, outcome);
        assert_eq!((result.matched, result.applied), (6, 0));
        let single: Vec<(String, String)> = errors
            .into_iter()
            .map(|error| {
                let error = action_error(error);
                let code = error
                    .extensions
                    .as_ref()
                    .and_then(|ext| ext.get("code"))
                    .map(|code| code.to_string().trim_matches('"').to_owned())
                    .unwrap();
                (code, error.message)
            })
            .collect();
        let batch: Vec<(String, String)> = result
            .refused
            .iter()
            .map(|refused| (refused.code.clone(), refused.message.clone()))
            .collect();
        assert_eq!(batch[..5], single[..]);
        assert_eq!(
            single
                .iter()
                .map(|(code, _)| code.as_str())
                .collect::<Vec<_>>(),
            [
                "NOT_FOUND",
                "CONFLICT",
                "INVALID_INPUT",
                "INTERNAL",
                "FORBIDDEN"
            ]
        );
        assert_eq!(batch[5].0, "CONFLICT");
        assert!(batch[5].1.starts_with("not reached"));
    }
}

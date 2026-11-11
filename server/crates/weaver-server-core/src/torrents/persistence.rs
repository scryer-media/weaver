// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};
use crate::settings::SeedingPolicy;
use crate::settings::model::tracker_host;
use crate::torrents::model::{file_priorities_str, named_by_str};
use crate::torrents::queries::{TORRENT_COLUMNS, count, counters_from_row, torrent_by_infohash_tx};
use crate::torrents::store::Kept;
use crate::torrents::{
    FilePriority, TorrentCounters, TorrentInsert, TorrentIntent, TorrentRecord, TrackerEntry,
};
use weaver_torrent::ResumePeers;

// A tracker row holds where the tracker sits in the announce list and who
// named it; how announcing to it goes is read from the session.
const TRACKER_INSERT: &str =
    "INSERT INTO torrent_trackers (job_id, tier, url, named_by) VALUES ({}, {}, {}, {})";
// Every host a torrent has announced to stays known after the torrent is
// gone, so a tracker rule can be picked from them. The URL is not kept: a
// private tracker's carries the operator's passkey.
const TRACKER_HOST_INSERT: &str =
    "INSERT INTO tracker_hosts (host) VALUES ({}) ON CONFLICT (host) DO NOTHING";

// The statement that writes a torrent's ledger and the peers and bans it
// begins with, each only when given; none when neither is.
fn resume_update(
    job_id: JobId,
    counters: Option<&TorrentCounters>,
    peers: Option<&ResumePeers>,
) -> Result<Option<(String, Vec<SqlArg>)>, StateError> {
    let mut sets = Vec::new();
    let mut args = Vec::new();
    if let Some(counters) = counters {
        sets.push(
            "uploaded_bytes = {}, downloaded_bytes = {}, active_duration_s = {}, \
             last_seen_complete_ms = {}, finished_duration_s = {}, last_activity_ms = {}, \
             consecutive_seeding_s = {}",
        );
        args.extend([
            SqlArg::I64(stored(counters.uploaded_bytes)?),
            SqlArg::I64(stored(counters.downloaded_bytes)?),
            SqlArg::I64(stored(counters.active_duration_s)?),
            SqlArg::OptI64(counters.last_seen_complete_ms),
            SqlArg::I64(stored(counters.finished_duration_s)?),
            SqlArg::OptI64(counters.last_activity_ms),
            SqlArg::I64(stored(counters.consecutive_seeding_s)?),
        ]);
    }
    if let Some(peers) = peers {
        sets.push("peers = {}, banned_peers = {}");
        args.extend([
            SqlArg::OptText(stored_list(&peers.peers)?),
            SqlArg::OptText(stored_list(&peers.banned)?),
        ]);
    }
    if sets.is_empty() {
        return Ok(None);
    }
    args.push(SqlArg::I64(stored(job_id.0)?));
    let sql = format!(
        "UPDATE torrent_counters SET {} WHERE job_id = {{}}",
        sets.join(", ")
    );
    Ok(Some((sql, args)))
}

// The one place a torrent's tracker rows are written: each row, and the
// host of each, recorded as `tracker_host` spells it.
async fn insert_trackers(
    tx: &mut SqlTx<'_>,
    job_id: i64,
    trackers: &[TrackerEntry],
) -> Result<(), StateError> {
    for tracker in trackers {
        let args = [
            SqlArg::I64(job_id),
            SqlArg::I64(i64::from(tracker.tier)),
            SqlArg::Text(tracker.url.clone()),
            SqlArg::Text(named_by_str(tracker.named_by).to_owned()),
        ];
        tx.execute(TRACKER_INSERT, &args).await?;
    }
    let hosts: std::collections::BTreeSet<String> = trackers
        .iter()
        .filter_map(|tracker| tracker_host(&tracker.url))
        .collect();
    for host in hosts {
        tx.execute(TRACKER_HOST_INSERT, &[SqlArg::Text(host)])
            .await?;
    }
    Ok(())
}

fn stored(value: u64) -> Result<i64, StateError> {
    i64::try_from(value)
        .map_err(|_| StateError::Database(format!("{value} exceeds the database range")))
}

// What was on disk before a torrent's files, as JSON; NULL when nothing.
fn adopted_text(adopted: &super::model::Adopted) -> Result<Option<String>, StateError> {
    (!adopted.is_empty())
        .then(|| serde_json::to_string(adopted))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))
}

// A list as a JSON array of its strings; NULL when empty.
fn stored_list<T: serde::Serialize>(items: &[T]) -> Result<Option<String>, StateError> {
    (!items.is_empty())
        .then(|| serde_json::to_string(items))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))
}

fn listed<T: serde::de::DeserializeOwned>(
    column: &str,
    json: Option<String>,
) -> Result<Vec<T>, StateError> {
    json.map(|json| {
        serde_json::from_str(&json).map_err(|error| {
            StateError::Database(format!("stored torrent {column} is not valid: {error}"))
        })
    })
    .transpose()
    .map(Option::unwrap_or_default)
}

fn torrent_args(torrent: &TorrentRecord) -> Result<Vec<SqlArg>, StateError> {
    let policy_override = SeedingPolicy::kept(torrent.policy_override)
        .map(|policy| serde_json::to_string(&policy))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    let metadata = (!torrent.metadata.is_empty())
        .then(|| serde_json::to_string(&torrent.metadata))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    Ok(vec![
        SqlArg::I64(stored(torrent.job_id.0)?),
        SqlArg::OptBytes(torrent.infohash_v1.map(|hash| hash.to_vec())),
        SqlArg::OptBytes(torrent.infohash_v2.map(|hash| hash.to_vec())),
        SqlArg::Text(torrent.name.clone()),
        SqlArg::Text(torrent.source.as_str().to_string()),
        SqlArg::OptBytes(torrent.metainfo.as_deref().map(<[u8]>::to_vec)),
        SqlArg::OptI64(torrent.category_id.map(i64::from)),
        SqlArg::I64(torrent.added_at_ms),
        SqlArg::Text(torrent.intent.as_str().to_string()),
        SqlArg::Bool(torrent.forced),
        SqlArg::OptI64(torrent.completed_at_ms),
        SqlArg::OptText(torrent.storage_dir.clone()),
        SqlArg::OptText(policy_override),
        SqlArg::OptBool(torrent.unpack),
        SqlArg::OptBool(torrent.unpack_in_force),
        SqlArg::OptText(metadata),
        SqlArg::OptI64(torrent.upload_limit.map(stored).transpose()?),
        SqlArg::OptI64(torrent.download_limit.map(stored).transpose()?),
        SqlArg::Bool(torrent.sequential),
        SqlArg::Bool(torrent.first_last_piece_first),
        SqlArg::OptText(torrent.save_path.clone()),
        SqlArg::OptText(
            torrent
                .content_layout
                .map(|layout| layout.as_str().to_owned()),
        ),
        SqlArg::OptText(torrent.files_dir.clone()),
        SqlArg::Bool(torrent.seed_mode),
        SqlArg::OptText(torrent.move_to.clone()),
        SqlArg::I64(torrent.queue_position),
        SqlArg::OptI64(torrent.pool_member.map(i64::from)),
    ])
}

impl Database {
    // Add a torrent with its trackers and an empty seeding ledger,
    // unless a torrent with the same infohash is already a job.
    pub fn insert_torrent(
        &self,
        torrent: &TorrentRecord,
        trackers: &[TrackerEntry],
    ) -> Result<TorrentInsert, StateError> {
        self.insert_placed_torrent(torrent, trackers, &super::model::Adopted::default())
    }

    // [`Self::insert_torrent`], with what was already on disk where its
    // files go, in the same transaction.
    pub(crate) fn insert_placed_torrent(
        &self,
        torrent: &TorrentRecord,
        trackers: &[TrackerEntry],
        adopted: &super::model::Adopted,
    ) -> Result<TorrentInsert, StateError> {
        let adopted = adopted_text(adopted)?;
        let infohashes = torrent.infohashes();
        if infohashes.is_empty() {
            return Err(StateError::Database(
                "a torrent needs a v1 or a v2 infohash".to_string(),
            ));
        }
        let job_id = torrent.job_id;
        let torrent_args = torrent_args(torrent)?;
        let id = stored(job_id.0)?;
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_torrent", |tx| {
                let infohashes = infohashes.clone();
                let torrent_args = torrent_args.clone();
                let trackers = trackers.clone();
                let adopted = adopted.clone();
                Box::pin(async move {
                    if let Some(existing) = torrent_by_infohash_tx(tx, &infohashes).await? {
                        return Ok(TorrentInsert::Duplicate(existing.job_id));
                    }
                    // One placeholder per value: the column list and the
                    // values are both TORRENT_COLUMNS' order.
                    let values = vec!["{}"; torrent_args.len()].join(", ");
                    tx.execute(
                        &format!("INSERT INTO torrents ({TORRENT_COLUMNS}) VALUES ({values})"),
                        &torrent_args,
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    if let Some(adopted) = adopted.clone() {
                        tx.execute(
                            "UPDATE torrents SET adopted = {} WHERE job_id = {}",
                            &[SqlArg::Text(adopted), SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    tx.execute(
                        "INSERT INTO torrent_counters (job_id) VALUES ({})",
                        &[SqlArg::I64(job_id.0 as i64)],
                    )
                    .await?;
                    Ok(TorrentInsert::Inserted)
                })
            })
            .await
        })
    }

    // What the operator asked of the torrent, and no other column: its
    // completion is written apart, so a pause racing a completion keeps
    // both. Returns whether the torrent exists.
    pub fn update_torrent_intent(
        &self,
        job_id: JobId,
        intent: TorrentIntent,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::Text(intent.as_str().to_string()),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("state", args)
    }

    // When the torrent completed. Returns whether the torrent exists.
    pub fn set_torrent_completed(&self, job_id: JobId, at_ms: i64) -> Result<bool, StateError> {
        let args = vec![SqlArg::I64(at_ms), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("completed_at_ms", args)
    }

    // The torrent's ledger. Returns whether the torrent exists.
    pub fn update_torrent_counters(
        &self,
        job_id: JobId,
        counters: &TorrentCounters,
    ) -> Result<bool, StateError> {
        self.save_torrent_resume(job_id, Some(counters), None)
    }

    // The peers and bans a torrent begins its next start with. Returns
    // whether the torrent exists.
    pub fn save_torrent_peers(
        &self,
        job_id: JobId,
        peers: &ResumePeers,
    ) -> Result<bool, StateError> {
        self.save_torrent_resume(job_id, None, Some(peers))
    }

    // The row a torrent's next start reads as it ran: its ledger, and the
    // peers and bans it begins with, each written only when given, in one
    // statement. The row is apart from the one that holds the metainfo, so
    // writing it as the torrent runs never writes the metainfo again.
    // Returns whether the torrent exists.
    pub fn save_torrent_resume(
        &self,
        job_id: JobId,
        counters: Option<&TorrentCounters>,
        peers: Option<&ResumePeers>,
    ) -> Result<bool, StateError> {
        let Some((sql, args)) = resume_update(job_id, counters, peers)? else {
            return Ok(false);
        };
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            Ok(changed > 0)
        })
    }

    // The pieces the torrent has in its files, and its ledger and the peers
    // and bans it begins with as they stood with them, each written only
    // when given, in one transaction: a crash recovers the pieces and the
    // counters of one moment.
    pub(crate) fn save_torrent_checkpoint(
        &self,
        job_id: JobId,
        pieces: Option<&Kept>,
        counters: Option<&TorrentCounters>,
        peers: Option<&ResumePeers>,
    ) -> Result<(), StateError> {
        let resume = resume_update(job_id, counters, peers)?;
        let pieces = pieces
            .map(|kept| -> Result<_, StateError> {
                Ok(vec![
                    SqlArg::OptBytes(Some(kept.bits.clone())),
                    SqlArg::Bool(kept.checking),
                    SqlArg::I64(stored(job_id.0)?),
                ])
            })
            .transpose()?;
        if pieces.is_none() && resume.is_none() {
            return Ok(());
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_torrent_checkpoint", |tx| {
                let pieces = pieces.clone();
                let resume = resume.clone();
                Box::pin(async move {
                    if let Some(args) = &pieces {
                        tx.execute(
                            "UPDATE torrent_counters SET pieces = {}, pieces_checking = {} \
                             WHERE job_id = {}",
                            args,
                        )
                        .await?;
                    }
                    if let Some((sql, args)) = &resume {
                        tx.execute(sql, args).await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // The pieces [`Self::save_torrent_checkpoint`] last kept; None when it
    // kept none.
    pub(crate) fn torrent_pieces(&self, job_id: JobId) -> Result<Option<Kept>, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT pieces, pieces_checking FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(row) = row else { return Ok(None) };
            let Some(bits) = row.opt_bytes("pieces")? else {
                return Ok(None);
            };
            Ok(Some(Kept {
                bits,
                checking: row.bool("pieces_checking")?,
            }))
        })
    }

    // Unpacking is over: the files it placed beside the torrent's own are
    // kept on its row, with those an earlier unpacking that a crash cut
    // short kept there, and its job's checkpoint goes, in one transaction.
    // Run on the torrent's own lane, so no write of the job's checkpoint
    // queued before it lands after.
    pub(crate) fn finish_torrent_unpack(
        &self,
        job_id: JobId,
        outputs: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let outputs = self.outputs_with(job_id, outputs)?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "finish_torrent_unpack", |tx| {
                let outputs = outputs.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE torrents SET outputs = {} WHERE job_id = {}",
                        &[SqlArg::OptText(outputs), SqlArg::I64(id)],
                    )
                    .await?;
                    for table in crate::jobs::graph_checkpoint::TABLES {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // The files unpacking is about to place beside the torrent's own, kept
    // on its row with those it kept before: a crash before
    // [`Self::finish_torrent_unpack`] leaves none its delete does not know.
    pub(crate) fn keep_torrent_outputs(
        &self,
        job_id: JobId,
        outputs: &[String],
    ) -> Result<(), StateError> {
        let id = stored(job_id.0)?;
        let outputs = self.outputs_with(job_id, outputs)?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET outputs = {} WHERE job_id = {}",
                &[SqlArg::OptText(outputs), SqlArg::I64(id)],
            )
            .await?;
            Ok(())
        })
    }

    // The row's outputs with `outputs` added, as the row keeps them.
    fn outputs_with(
        &self,
        job_id: JobId,
        outputs: &[String],
    ) -> Result<Option<String>, StateError> {
        let mut kept = self.torrent_outputs(job_id)?;
        for output in outputs {
            if !kept.contains(output) {
                kept.push(output.clone());
            }
        }
        (!kept.is_empty())
            .then(|| serde_json::to_string(&kept))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))
    }

    // The files [`Self::finish_torrent_unpack`] kept, relative to the
    // directory the torrent's files are in.
    pub(crate) fn torrent_outputs(&self, job_id: JobId) -> Result<Vec<String>, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT outputs FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(text) = row
                .map(|row| row.opt_text("outputs"))
                .transpose()?
                .flatten()
            else {
                return Ok(Vec::new());
            };
            serde_json::from_str(&text).map_err(|error| StateError::Database(error.to_string()))
        })
    }

    // What was on disk where the torrent's files go before Weaver wrote
    // them; nothing kept is nothing adopted.
    pub(crate) fn set_torrent_adopted(
        &self,
        job_id: JobId,
        adopted: &super::model::Adopted,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(adopted_text(adopted)?),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("adopted", args)
    }

    // What [`Self::set_torrent_adopted`] kept.
    pub(crate) fn torrent_adopted(
        &self,
        job_id: JobId,
    ) -> Result<super::model::Adopted, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT adopted FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            let Some(text) = row
                .map(|row| row.opt_text("adopted"))
                .transpose()?
                .flatten()
            else {
                return Ok(super::model::Adopted::default());
            };
            serde_json::from_str(&text).map_err(|error| StateError::Database(error.to_string()))
        })
    }

    // Removes the torrent with its files, trackers, ledger and checkpoint.
    pub fn delete_torrent(&self, job_id: JobId) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "delete_torrent", |tx| {
                Box::pin(async move {
                    let changed = tx
                        .execute("DELETE FROM torrents WHERE job_id = {}", &[SqlArg::I64(id)])
                        .await?;
                    for table in crate::jobs::graph_checkpoint::TABLES {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    Ok(changed > 0)
                })
            })
            .await
        })
    }

    // The torrent's category. Returns whether the torrent exists.
    pub fn set_torrent_category(
        &self,
        job_id: JobId,
        category_id: Option<u32>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptI64(category_id.map(i64::from)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("category_id", args)
    }

    // Whether the torrent runs past the download queue. Returns whether the
    // torrent exists.
    pub fn set_torrent_forced(&self, job_id: JobId, forced: bool) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(forced), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("forced", args)
    }

    // The torrent's own seeding policy; one whose every leaf inherits is
    // kept as none. Returns whether the torrent exists.
    pub fn set_torrent_policy_override(
        &self,
        job_id: JobId,
        policy_override: Option<SeedingPolicy>,
    ) -> Result<bool, StateError> {
        let policy_override = SeedingPolicy::kept(policy_override)
            .map(|policy| serde_json::to_string(&policy))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))?;
        let args = vec![
            SqlArg::OptText(policy_override),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("policy_override", args)
    }

    // The torrent's trackers, as edited (libtorrent `write_resume_data`
    // keeps them as its `trackers` and `tiers`). Returns whether the torrent
    // exists.
    pub fn set_torrent_trackers(
        &self,
        job_id: JobId,
        trackers: &[TrackerEntry],
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_trackers", |tx| {
                let trackers = trackers.clone();
                Box::pin(async move {
                    let known = tx
                        .fetch_optional(
                            "SELECT job_id FROM torrents WHERE job_id = {}",
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    if known.is_none() {
                        return Ok(false);
                    }
                    tx.execute(
                        "DELETE FROM torrent_trackers WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    Ok(true)
                })
            })
            .await
        })
    }

    // Whether the torrent's archives are extracted, fixed when its job is
    // built. Returns whether the torrent exists.
    pub fn set_torrent_unpack_in_force(
        &self,
        job_id: JobId,
        unpack: bool,
    ) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(unpack), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET unpack_in_force = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // A magnet's metainfo once its peers sent it: the hashes and name it
    // carries, the file, the directory its files go to, and the files and
    // trackers it lists. Returns whether the torrent exists.
    #[allow(clippy::too_many_arguments)]
    pub fn set_torrent_metainfo(
        &self,
        job_id: JobId,
        infohash_v1: Option<[u8; 20]>,
        infohash_v2: Option<[u8; 32]>,
        name: &str,
        metainfo: &[u8],
        storage_dir: &str,
        trackers: &[TrackerEntry],
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let head = vec![
            SqlArg::OptBytes(infohash_v1.map(|hash| hash.to_vec())),
            SqlArg::OptBytes(infohash_v2.map(|hash| hash.to_vec())),
            SqlArg::Text(name.to_owned()),
            SqlArg::Bytes(metainfo.to_vec()),
            SqlArg::Text(storage_dir.to_owned()),
            SqlArg::I64(id),
        ];
        let trackers = trackers.to_vec();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_metainfo", |tx| {
                let head = head.clone();
                let trackers = trackers.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE torrents SET infohash_v1 = {}, infohash_v2 = {}, name = {}, metainfo = {},
                                     storage_dir = {}
                              WHERE job_id = {}",
                            &head,
                        )
                        .await?;
                    if changed == 0 {
                        return Ok(false);
                    }
                    tx.execute(
                        "DELETE FROM torrent_trackers WHERE job_id = {}",
                        &[SqlArg::I64(id)],
                    )
                    .await?;
                    insert_trackers(tx, id, &trackers).await?;
                    Ok(true)
                })
            })
            .await
        })
    }

    // The directory a torrent's files now live under. Returns whether the
    // torrent exists.
    pub fn set_torrent_storage_dir(&self, job_id: JobId, dir: &str) -> Result<bool, StateError> {
        let args = vec![SqlArg::Text(dir.to_owned()), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET storage_dir = {} WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The torrent's own upload and download limits in bytes per second:
    // `None` keeps a limit as it is, `Some(None)` is unlimited and zero
    // closed. Returns whether the torrent exists.
    pub fn set_torrent_rate_limits(
        &self,
        job_id: JobId,
        upload: Option<Option<u64>>,
        download: Option<Option<u64>>,
    ) -> Result<bool, StateError> {
        let mut args = Vec::new();
        for limit in [upload, download] {
            args.push(SqlArg::I64(i64::from(limit.is_some())));
            args.push(SqlArg::OptI64(limit.flatten().map(stored).transpose()?));
        }
        args.push(SqlArg::I64(stored(job_id.0)?));
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET \
                 upload_limit = CASE WHEN {} = 1 THEN {} ELSE upload_limit END, \
                 download_limit = CASE WHEN {} = 1 THEN {} ELSE download_limit END \
                 WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The operator's level for every file of a torrent, in the metainfo's
    // order, written whole. Returns whether the torrent exists.
    pub fn set_torrent_file_priorities(
        &self,
        job_id: JobId,
        levels: &[FilePriority],
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(file_priorities_str(levels)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("file_priorities", args)
    }

    // Whether the torrent fetches its pieces in order. Returns whether the
    // torrent exists.
    pub fn set_torrent_sequential(&self, job_id: JobId, on: bool) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(on), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("sequential", args)
    }

    // Whether each of the torrent's files has its first and last pieces
    // fetched first. Returns whether the torrent exists.
    pub fn set_torrent_first_last_piece_first(
        &self,
        job_id: JobId,
        on: bool,
    ) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(on), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("first_last_piece_first", args)
    }

    // Where the torrent's files sit when not in its storage directory.
    pub fn set_torrent_files_dir(
        &self,
        job_id: JobId,
        dir: Option<&str>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(dir.map(str::to_owned)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("files_dir", args)
    }

    // The directory the torrent's files go under, as asked or as it moved.
    pub fn set_torrent_save_path(
        &self,
        job_id: JobId,
        dir: Option<&str>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptText(dir.map(str::to_owned)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("save_path", args)
    }

    // The torrent's session left seed mode: from here its pieces are what
    // its job has, and none needs the check it passed in seed mode kept.
    pub fn clear_torrent_seed_mode(&self, job_id: JobId) -> Result<bool, StateError> {
        let args = vec![SqlArg::Bool(false), SqlArg::I64(stored(job_id.0)?)];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.write_exec(),
                "UPDATE torrents SET seed_mode = {}, seed_verified = NULL WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // The pieces that passed their check in seed mode, a bitfield as the
    // wire's: the first piece the high bit of the first byte (libtorrent's
    // `verified_pieces` in resume data).
    pub fn set_torrent_seed_verified(
        &self,
        job_id: JobId,
        pieces: &[u8],
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptBytes(Some(pieces.to_vec())),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("seed_verified", args)
    }

    // The pieces [`Self::set_torrent_seed_verified`] last kept; None when it
    // kept none.
    pub fn torrent_seed_verified(&self, job_id: JobId) -> Result<Option<Vec<u8>>, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT seed_verified FROM torrents WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await?;
            Ok(row
                .map(|row| row.opt_bytes("seed_verified"))
                .transpose()?
                .flatten())
        })
    }

    // The pool member the torrent is pinned to, or none.
    pub fn set_torrent_pool_member(
        &self,
        job_id: JobId,
        member: Option<u32>,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::OptI64(member.map(i64::from)),
            SqlArg::I64(stored(job_id.0)?),
        ];
        self.set_torrent_column("pool_member", args)
    }

    // The torrent moved where it was to move once complete, or will not.
    pub fn clear_torrent_move_to(&self, job_id: JobId) -> Result<bool, StateError> {
        let args = vec![SqlArg::OptText(None), SqlArg::I64(stored(job_id.0)?)];
        self.set_torrent_column("move_to", args)
    }

    // Where each torrent stands in the download queue, in one transaction.
    pub fn set_torrent_queue_positions(
        &self,
        positions: &[(JobId, i64)],
    ) -> Result<(), StateError> {
        let rows = positions
            .iter()
            .map(|&(job_id, position)| {
                Ok(vec![SqlArg::I64(position), SqlArg::I64(stored(job_id.0)?)])
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_queue_positions", |tx| {
                let rows = rows.clone();
                Box::pin(async move {
                    for args in &rows {
                        tx.execute(
                            "UPDATE torrents SET queue_position = {} WHERE job_id = {}",
                            args,
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    // Write one column of a torrent and no other, so the writers of
    // different columns never undo each other.
    fn set_torrent_column(&self, column: &str, args: Vec<SqlArg>) -> Result<bool, StateError> {
        let sql = format!("UPDATE torrents SET {column} = {{}} WHERE job_id = {{}}");
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(datastore.write_exec(), &sql, &args).await?;
            Ok(changed > 0)
        })
    }

    // The peers and bans a torrent last saved; none when it saved none or
    // is not a torrent.
    pub fn torrent_peers(&self, job_id: JobId) -> Result<ResumePeers, StateError> {
        self.torrent_read();
        let id = stored(job_id.0)?;
        let datastore = self.datastore();
        let row = self.run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT peers, banned_peers FROM torrent_counters WHERE job_id = {}",
                &[SqlArg::I64(id)],
            )
            .await
        })?;
        let Some(row) = row else {
            return Ok(ResumePeers::default());
        };
        Ok(ResumePeers {
            peers: listed("peers", row.opt_text("peers")?)?,
            banned: listed("banned_peers", row.opt_text("banned_peers")?)?,
        })
    }

    // Every torrent's ledger, saved peers and kept pieces, by torrent, in
    // one read: what [`Self::get_torrent_counters`],
    // [`Self::torrent_peers`] and [`Self::torrent_pieces`] give a
    // torrent's restore, for all of them. Each part reads on its own, so
    // one that does not read is that part of that torrent's alone.
    pub(crate) fn torrent_counters_by_job(
        &self,
    ) -> Result<std::collections::BTreeMap<JobId, CountersRow>, StateError> {
        self.torrent_read();
        let datastore = self.datastore();
        self.run_sql_blocking_read(async move {
            let rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT job_id, uploaded_bytes, downloaded_bytes, active_duration_s,
                        last_seen_complete_ms, finished_duration_s, last_activity_ms,
                        consecutive_seeding_s, pieces, pieces_checking, peers, banned_peers
                   FROM torrent_counters",
                &[],
            )
            .await?;
            rows.iter()
                .map(|row| {
                    let job_id = JobId(count(row, "job_id")?);
                    let kept = row.opt_bytes("pieces").and_then(|bits| {
                        bits.map(|bits| {
                            Ok(Kept {
                                bits,
                                checking: row.bool("pieces_checking")?,
                            })
                        })
                        .transpose()
                    });
                    let peers = row
                        .opt_text("peers")
                        .and_then(|peers| listed("peers", peers))
                        .and_then(|peers| {
                            let banned = row
                                .opt_text("banned_peers")
                                .and_then(|banned| listed("banned_peers", banned))?;
                            Ok(ResumePeers { peers, banned })
                        });
                    Ok((
                        job_id,
                        CountersRow {
                            counters: counters_from_row(row),
                            peers,
                            kept,
                        },
                    ))
                })
                .collect()
        })
    }
}

// One torrent's counters row as a boot reads it.
pub(crate) struct CountersRow {
    pub(crate) counters: Result<TorrentCounters, StateError>,
    pub(crate) peers: Result<ResumePeers, StateError>,
    pub(crate) kept: Result<Option<Kept>, StateError>,
}

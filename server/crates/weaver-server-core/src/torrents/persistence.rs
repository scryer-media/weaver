// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::StateError;
use crate::jobs::ids::JobId;
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::settings::SeedingPolicy;
use crate::torrents::queries::{TORRENT_COLUMNS, torrent_by_infohash_tx};
use crate::torrents::{
    TorrentCounters, TorrentFile, TorrentInsert, TorrentIntent, TorrentRecord, TrackerEntry,
};

// A tracker row holds where the tracker sits in the announce list; how
// announcing to it goes is read from the session. The status column is NOT
// NULL in the schema, and nothing reads it.
const TRACKER_INSERT: &str = "INSERT INTO torrent_trackers (job_id, tier, url, status)
     VALUES ({}, {}, {}, 'not_contacted')";

fn stored(value: u64) -> Result<i64, StateError> {
    i64::try_from(value)
        .map_err(|_| StateError::Database(format!("{value} exceeds the database range")))
}

fn torrent_args(torrent: &TorrentRecord) -> Result<Vec<SqlArg>, StateError> {
    let policy_override = torrent
        .policy_override
        .map(|policy| serde_json::to_string(&policy))
        .transpose()
        .map_err(|error| StateError::Database(error.to_string()))?;
    Ok(vec![
        SqlArg::I64(stored(torrent.job_id.0)?),
        SqlArg::OptBytes(torrent.infohash_v1.map(|hash| hash.to_vec())),
        SqlArg::OptBytes(torrent.infohash_v2.map(|hash| hash.to_vec())),
        SqlArg::Text(torrent.name.clone()),
        SqlArg::Text(torrent.source.as_str().to_string()),
        SqlArg::OptBytes(torrent.metainfo.clone()),
        SqlArg::Bool(torrent.private),
        SqlArg::OptI64(torrent.category_id.map(i64::from)),
        SqlArg::I64(torrent.added_at_ms),
        SqlArg::Text(torrent.intent.as_str().to_string()),
        SqlArg::Bool(torrent.forced),
        SqlArg::OptI64(torrent.completed_at_ms),
        SqlArg::OptText(torrent.storage_dir.clone()),
        SqlArg::OptText(policy_override),
    ])
}

impl Database {
    // Add a torrent with its files and trackers and an empty seeding ledger,
    // unless a torrent with the same infohash is already a job.
    pub fn insert_torrent(
        &self,
        torrent: &TorrentRecord,
        files: &[TorrentFile],
        trackers: &[TrackerEntry],
    ) -> Result<TorrentInsert, StateError> {
        let infohash = torrent.infohash().ok_or_else(|| {
            StateError::Database("a torrent needs a v1 or a v2 infohash".to_string())
        })?;
        let job_id = torrent.job_id;
        let added_at_ms = torrent.added_at_ms;
        let torrent_args = torrent_args(torrent)?;
        let file_args = files
            .iter()
            .map(|file| {
                Ok(vec![
                    SqlArg::I64(stored(job_id.0)?),
                    SqlArg::I64(i64::from(file.index)),
                    SqlArg::Text(file.path.clone()),
                    SqlArg::I64(stored(file.length)?),
                    SqlArg::Bool(file.selected),
                ])
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let tracker_args = trackers
            .iter()
            .map(|tracker| {
                Ok(vec![
                    SqlArg::I64(stored(job_id.0)?),
                    SqlArg::I64(i64::from(tracker.tier)),
                    SqlArg::Text(tracker.url.clone()),
                ])
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "insert_torrent", |tx| {
                let torrent_args = torrent_args.clone();
                let file_args = file_args.clone();
                let tracker_args = tracker_args.clone();
                Box::pin(async move {
                    if let Some(existing) = torrent_by_infohash_tx(tx, infohash).await? {
                        return Ok(TorrentInsert::Duplicate(existing.job_id));
                    }
                    tx.execute(
                        &format!(
                            "INSERT INTO torrents ({TORRENT_COLUMNS})
                             VALUES ({{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}}, {{}})"
                        ),
                        &torrent_args,
                    )
                    .await?;
                    for args in &file_args {
                        tx.execute(
                            "INSERT INTO torrent_files (job_id, file_index, path, length, selected)
                             VALUES ({}, {}, {}, {}, {})",
                            args,
                        )
                        .await?;
                    }
                    for args in &tracker_args {
                        tx.execute(
                            TRACKER_INSERT,
                            args,
                        )
                        .await?;
                    }
                    tx.execute(
                        "INSERT INTO torrent_counters (job_id, updated_at_ms) VALUES ({}, {})",
                        &[SqlArg::I64(job_id.0 as i64), SqlArg::I64(added_at_ms)],
                    )
                    .await?;
                    Ok(TorrentInsert::Inserted)
                })
            })
            .await
        })
    }

    // What the operator asked of the torrent, and when it completed.
    // Returns whether the torrent exists.
    pub fn update_torrent_intent(
        &self,
        job_id: JobId,
        intent: TorrentIntent,
        completed_at_ms: Option<i64>,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE torrents SET state = {}, completed_at_ms = {} WHERE job_id = {}",
                &[
                    SqlArg::Text(intent.as_str().to_string()),
                    SqlArg::OptI64(completed_at_ms),
                    SqlArg::I64(stored(job_id.0)?),
                ],
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // Returns whether the torrent exists.
    pub fn update_torrent_counters(
        &self,
        job_id: JobId,
        counters: &TorrentCounters,
    ) -> Result<bool, StateError> {
        let args = vec![
            SqlArg::I64(stored(counters.uploaded_bytes)?),
            SqlArg::I64(stored(counters.downloaded_bytes)?),
            SqlArg::I64(stored(counters.finished_duration_s)?),
            SqlArg::OptI64(counters.last_activity_ms),
            SqlArg::I64(stored(counters.last_announce_uploaded_bytes)?),
            SqlArg::I64(counters.updated_at_ms),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE torrent_counters
                    SET uploaded_bytes = {}, downloaded_bytes = {}, finished_duration_s = {},
                        last_activity_ms = {}, last_announce_uploaded_bytes = {},
                        updated_at_ms = {}
                  WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
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

    // The torrent's category, forced flag and seeding override. Returns
    // whether the torrent exists.
    pub fn update_torrent_options(
        &self,
        job_id: JobId,
        category_id: Option<u32>,
        forced: bool,
        policy_override: Option<SeedingPolicy>,
    ) -> Result<bool, StateError> {
        let policy_override = policy_override
            .map(|policy| serde_json::to_string(&policy))
            .transpose()
            .map_err(|error| StateError::Database(error.to_string()))?;
        let args = vec![
            SqlArg::OptI64(category_id.map(i64::from)),
            SqlArg::Bool(forced),
            SqlArg::OptText(policy_override),
            SqlArg::I64(stored(job_id.0)?),
        ];
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE torrents SET category_id = {}, forced = {}, policy_override = {}
                  WHERE job_id = {}",
                &args,
            )
            .await?;
            Ok(changed > 0)
        })
    }

    // A magnet's metainfo once its peers sent it: the name, the file, the
    // directory its files go to, and the files and trackers it lists.
    // Returns whether the torrent exists.
    #[allow(clippy::too_many_arguments)]
    pub fn set_torrent_metainfo(
        &self,
        job_id: JobId,
        name: &str,
        metainfo: &[u8],
        private: bool,
        storage_dir: &str,
        files: &[TorrentFile],
        trackers: &[TrackerEntry],
    ) -> Result<bool, StateError> {
        let id = stored(job_id.0)?;
        let head = vec![
            SqlArg::Text(name.to_owned()),
            SqlArg::Bytes(metainfo.to_vec()),
            SqlArg::Bool(private),
            SqlArg::Text(storage_dir.to_owned()),
            SqlArg::I64(id),
        ];
        let file_args = files
            .iter()
            .map(|file| {
                Ok(vec![
                    SqlArg::I64(id),
                    SqlArg::I64(i64::from(file.index)),
                    SqlArg::Text(file.path.clone()),
                    SqlArg::I64(stored(file.length)?),
                    SqlArg::Bool(file.selected),
                ])
            })
            .collect::<Result<Vec<_>, StateError>>()?;
        let tracker_args = trackers
            .iter()
            .map(|tracker| {
                vec![
                    SqlArg::I64(id),
                    SqlArg::I64(i64::from(tracker.tier)),
                    SqlArg::Text(tracker.url.clone()),
                ]
            })
            .collect::<Vec<_>>();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "set_torrent_metainfo", |tx| {
                let head = head.clone();
                let file_args = file_args.clone();
                let tracker_args = tracker_args.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE torrents SET name = {}, metainfo = {}, private = {}, storage_dir = {}
                              WHERE job_id = {}",
                            &head,
                        )
                        .await?;
                    if changed == 0 {
                        return Ok(false);
                    }
                    for table in ["torrent_files", "torrent_trackers"] {
                        tx.execute(
                            &format!("DELETE FROM {table} WHERE job_id = {{}}"),
                            &[SqlArg::I64(id)],
                        )
                        .await?;
                    }
                    for args in &file_args {
                        tx.execute(
                            "INSERT INTO torrent_files (job_id, file_index, path, length, selected)
                             VALUES ({}, {}, {}, {}, {})",
                            args,
                        )
                        .await?;
                    }
                    for args in &tracker_args {
                        tx.execute(
                            TRACKER_INSERT,
                            args,
                        )
                        .await?;
                    }
                    Ok(true)
                })
            })
            .await
        })
    }
}

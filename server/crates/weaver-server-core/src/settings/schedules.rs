use crate::StateError;
use crate::bandwidth::{ScheduleAction, ScheduleEntry, Weekday};
use crate::persistence::Database;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime, SqlTx};

const TRACKS_VERSION_KEY: &str = "schedule_tracks_version";
const TRACKS_VERSION: &str = "1";

impl Database {
    pub fn list_schedules(&self) -> Result<Vec<ScheduleEntry>, StateError> {
        if self.get_setting(TRACKS_VERSION_KEY)?.as_deref() == Some(TRACKS_VERSION) {
            return decode(
                &self
                    .get_setting("schedules")?
                    .unwrap_or_else(|| "[]".into()),
            );
        }
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "migrate_schedule_tracks", |tx| {
                Box::pin(async move {
                    let version = lock_version(tx).await?;
                    let json = tx
                        .fetch_optional(
                            "SELECT value FROM settings WHERE key = {}",
                            &[SqlArg::Text("schedules".into())],
                        )
                        .await?
                        .map(|row| row.text("value"))
                        .transpose()?
                        .unwrap_or_else(|| "[]".into());
                    let entries = decode(&json)?;
                    if version == TRACKS_VERSION {
                        return Ok(entries);
                    }
                    let entries = migrate_legacy_tracks(entries);
                    write_schedules(tx, &entries).await?;
                    Ok(entries)
                })
            })
            .await
        })
    }

    pub fn save_schedules(&self, entries: &[ScheduleEntry]) -> Result<(), StateError> {
        let datastore = self.datastore();
        let entries = entries.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_schedule_tracks", |tx| {
                let entries = entries.clone();
                Box::pin(async move {
                    lock_version(tx).await?;
                    for entry in &entries {
                        if let ScheduleAction::FetchRss {
                            feed_id: Some(feed_id),
                        } = entry.action
                            && tx
                                .fetch_optional(
                                    "SELECT id FROM rss_feeds WHERE id = {}",
                                    &[SqlArg::I64(i64::from(feed_id))],
                                )
                                .await?
                                .is_none()
                        {
                            return Err(StateError::Database(format!(
                                "RSS feed {feed_id} not found"
                            )));
                        }
                        if let ScheduleAction::SetServerActive { server_id, .. } = entry.action
                            && tx
                                .fetch_optional(
                                    "SELECT id FROM servers WHERE id = {}",
                                    &[SqlArg::I64(i64::from(server_id))],
                                )
                                .await?
                                .is_none()
                        {
                            return Err(StateError::Database(format!(
                                "server {server_id} not found"
                            )));
                        }
                    }
                    write_schedules(tx, &entries).await
                })
            })
            .await
        })
    }

    pub(crate) async fn remove_feed_schedules(
        tx: &mut SqlTx<'_>,
        feed_id: u32,
    ) -> Result<(), StateError> {
        let version = lock_version(tx).await?;
        let json = tx
            .fetch_optional(
                "SELECT value FROM settings WHERE key = {}",
                &[SqlArg::Text("schedules".into())],
            )
            .await?
            .map(|row| row.text("value"))
            .transpose()?
            .unwrap_or_else(|| "[]".into());
        let mut entries = decode(&json)?;
        if version != TRACKS_VERSION {
            entries = migrate_legacy_tracks(entries);
        }
        entries.retain(|entry| {
            !matches!(entry.action,
            ScheduleAction::FetchRss { feed_id: Some(id) } if id == feed_id)
        });
        write_schedules(tx, &entries).await
    }

    pub(crate) async fn remove_server_schedules(
        tx: &mut SqlTx<'_>,
        server_id: u32,
    ) -> Result<(), StateError> {
        let version = lock_version(tx).await?;
        let json = tx
            .fetch_optional(
                "SELECT value FROM settings WHERE key = {}",
                &[SqlArg::Text("schedules".into())],
            )
            .await?
            .map(|row| row.text("value"))
            .transpose()?
            .unwrap_or_else(|| "[]".into());
        let mut entries = decode(&json)?;
        if version != TRACKS_VERSION {
            entries = migrate_legacy_tracks(entries);
        }
        entries.retain(|entry| !matches!(entry.action, ScheduleAction::SetServerActive { server_id: id, .. } if id == server_id));
        write_schedules(tx, &entries).await
    }
}

fn decode(json: &str) -> Result<Vec<ScheduleEntry>, StateError> {
    serde_json::from_str(json).map_err(|error| StateError::Database(error.to_string()))
}

/// Serialize migration and schedule saves on both SQL backends. The version and
/// rules commit together, including the first save on an empty installation.
async fn lock_version(tx: &mut SqlTx<'_>) -> Result<String, StateError> {
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO NOTHING",
        &[
            SqlArg::Text(TRACKS_VERSION_KEY.into()),
            SqlArg::Text("0".into()),
        ],
    )
    .await?;
    let sql = match tx {
        SqlTx::Postgres(_) => "SELECT value FROM settings WHERE key = {} FOR UPDATE",
        SqlTx::Sqlite(_) => "SELECT value FROM settings WHERE key = {}",
    };
    let version = tx
        .fetch_optional(sql, &[SqlArg::Text(TRACKS_VERSION_KEY.into())])
        .await?
        .ok_or_else(|| StateError::Database("missing schedule tracks version".into()))?
        .text("value")?;
    if version != "0" && version != TRACKS_VERSION {
        return Err(StateError::Database(format!(
            "unsupported schedule tracks version: {version}"
        )));
    }
    Ok(version)
}

async fn write_schedules(tx: &mut SqlTx<'_>, entries: &[ScheduleEntry]) -> Result<(), StateError> {
    if entries
        .iter()
        .any(|entry| entry.enabled && matches!(entry.action, ScheduleAction::PauseAll))
    {
        tx.execute(
            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO NOTHING",
            &[
                SqlArg::Text("schedule_pause_all_used".into()),
                SqlArg::Text("true".into()),
            ],
        )
        .await?;
    }
    let json =
        serde_json::to_string(entries).map_err(|error| StateError::Database(error.to_string()))?;
    for (key, value) in [
        ("schedules", json),
        (TRACKS_VERSION_KEY, TRACKS_VERSION.into()),
    ] {
        tx.execute(
            "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            &[SqlArg::Text(key.into()), SqlArg::Text(value)],
        ).await?;
    }
    Ok(())
}

/// Preserve legacy speed resets. Watch-folder and profile actions never reset
/// the live limiter, so only download and speed actions participate. Expand a
/// week to find exactly which days of a pause/resume need an explicit reset.
fn migrate_legacy_tracks(entries: Vec<ScheduleEntry>) -> Vec<ScheduleEntry> {
    const DAYS: [Weekday; 7] = [
        Weekday::Mon,
        Weekday::Tue,
        Weekday::Wed,
        Weekday::Thu,
        Weekday::Fri,
        Weekday::Sat,
        Weekday::Sun,
    ];
    let mut week = Vec::new();
    for (day_index, day) in DAYS.iter().enumerate() {
        for (index, entry) in entries.iter().enumerate() {
            // An existing disabled limit can be enabled later. Preserve its
            // legacy end at the next enabled download rule as well.
            if (!entry.enabled && !matches!(entry.action, ScheduleAction::SpeedLimit { .. }))
                || (!entry.days.is_empty() && !entry.days.contains(day))
            {
                continue;
            }
            if !matches!(
                entry.action,
                ScheduleAction::Pause
                    | ScheduleAction::Resume
                    | ScheduleAction::SpeedLimit { .. }
                    | ScheduleAction::ConfiguredSpeedLimit
            ) {
                continue;
            }
            if let Some(time) = crate::bandwidth::schedule::parse_time(&entry.time) {
                week.push((day_index, time, index));
            }
        }
    }
    week.retain(|&(day, time, index)| {
        !entries.iter().skip(index + 1).any(|entry| {
            entry.enabled
                && (entry.days.is_empty() || entry.days.contains(&DAYS[day]))
                && crate::bandwidth::schedule::parse_time(&entry.time) == Some(time)
                && matches!(
                    entry.action,
                    ScheduleAction::PauseWatchFolderScanning
                        | ScheduleAction::ResumeWatchFolderScanning
                )
        })
    });
    week.sort_unstable();
    let mut resets = vec![Vec::new(); entries.len()];
    for (position, &(day, time, index)) in week.iter().enumerate() {
        let previous = week[(position + week.len() - 1) % week.len()].2;
        let (next_day, next_time, next) = week[(position + 1) % week.len()];
        // A saved compatibility reset (or an operator's equivalent rule)
        // already covers this occurrence. Do not insert it twice.
        if next_day == day
            && next_time == time
            && matches!(entries[next].action, ScheduleAction::ConfiguredSpeedLimit)
        {
            continue;
        }
        if matches!(
            entries[index].action,
            ScheduleAction::Pause | ScheduleAction::Resume
        ) && matches!(entries[previous].action, ScheduleAction::SpeedLimit { .. })
        {
            resets[index].push(DAYS[day]);
        }
    }
    let mut ids: std::collections::HashSet<String> =
        entries.iter().map(|entry| entry.id.clone()).collect();
    let mut migrated = Vec::with_capacity(entries.len());
    for (entry, days) in entries.into_iter().zip(resets) {
        if days.is_empty() {
            migrated.push(entry);
            continue;
        }
        let mut id = format!("{}-speed-reset", entry.id);
        while !ids.insert(id.clone()) {
            id.push_str("-reset");
        }
        let reset = ScheduleEntry {
            id,
            enabled: true,
            label: "Preserve legacy speed reset".into(),
            days: if days.len() == 7 { Vec::new() } else { days },
            time: entry.time.clone(),
            times: Vec::new(),
            every_hour_at_minute: None,
            action: ScheduleAction::ConfiguredSpeedLimit,
        };
        migrated.push(entry);
        // Preserve same-minute ordering: a later explicit speed rule still wins.
        migrated.push(reset);
    }
    migrated
}

#[cfg(test)]
mod tests;

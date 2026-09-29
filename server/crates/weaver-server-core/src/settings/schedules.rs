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
                    let entries = migrate_legacy_tracks_with_links(tx, entries).await?;
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
                let mut entries = entries.clone();
                Box::pin(async move {
                    lock_version(tx).await?;
                    sync_legacy_reset_enablement(tx, &mut entries).await?;
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
            entries = migrate_legacy_tracks_with_links(tx, entries).await?;
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
            entries = migrate_legacy_tracks_with_links(tx, entries).await?;
        }
        entries.retain(|entry| !matches!(entry.action, ScheduleAction::SetServerActive { server_id: id, .. } if id == server_id));
        write_schedules(tx, &entries).await
    }
}

fn decode(json: &str) -> Result<Vec<ScheduleEntry>, StateError> {
    serde_json::from_str(json).map_err(|error| StateError::Database(error.to_string()))
}

async fn migrate_legacy_tracks_with_links(
    tx: &mut SqlTx<'_>,
    entries: Vec<ScheduleEntry>,
) -> Result<Vec<ScheduleEntry>, StateError> {
    let original_ids: std::collections::HashSet<_> =
        entries.iter().map(|entry| entry.id.clone()).collect();
    let migrated = migrate_legacy_tracks(entries);
    let links: std::collections::BTreeMap<_, _> = migrated
        .windows(2)
        .filter(|pair| !original_ids.contains(&pair[1].id))
        .map(|pair| (pair[1].id.clone(), pair[0].id.clone()))
        .collect();
    tx.execute(
        "INSERT INTO settings (key, value) VALUES ({}, {}) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        &[
            SqlArg::Text("schedule_legacy_speed_reset_links".into()),
            SqlArg::Text(serde_json::to_string(&links).map_err(|error| StateError::Database(error.to_string()))?),
        ],
    ).await?;
    Ok(migrated)
}

async fn sync_legacy_reset_enablement(
    tx: &mut SqlTx<'_>,
    entries: &mut [ScheduleEntry],
) -> Result<(), StateError> {
    let Some(row) = tx
        .fetch_optional(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text("schedule_legacy_speed_reset_links".into())],
        )
        .await?
    else {
        return Ok(());
    };
    let links: std::collections::BTreeMap<String, String> =
        serde_json::from_str(&row.text("value")?)
            .map_err(|error| StateError::Database(error.to_string()))?;
    let previous = tx
        .fetch_optional(
            "SELECT value FROM settings WHERE key = {}",
            &[SqlArg::Text("schedules".into())],
        )
        .await?
        .map(|row| row.text("value"))
        .transpose()?
        .unwrap_or_else(|| "[]".into());
    let previous = decode(&previous)?;
    let mut retained_links = std::collections::BTreeMap::new();
    for (reset_id, original_id) in links {
        // Once an operator edits or removes the generated companion, it becomes
        // independent and later toggles of the original must not revive it.
        let old_reset = previous.iter().find(|entry| entry.id == reset_id);
        let new_reset = entries.iter().find(|entry| entry.id == reset_id);
        if old_reset.is_none() || old_reset != new_reset {
            continue;
        }
        let Some(old) = previous.iter().find(|entry| entry.id == original_id) else {
            continue;
        };
        let Some(new) = entries.iter().find(|entry| entry.id == original_id) else {
            continue;
        };
        let enabled = new.enabled;
        if enabled != old.enabled
            && let Some(reset) = entries.iter_mut().find(|entry| {
                entry.id == reset_id
                    && entry.action == ScheduleAction::ConfiguredSpeedLimit
                    && entry.enabled == old.enabled
            })
        {
            reset.enabled = enabled;
        }
        retained_links.insert(reset_id, original_id);
    }
    tx.execute(
        "UPDATE settings SET value = {} WHERE key = {}",
        &[
            SqlArg::Text(
                serde_json::to_string(&retained_links)
                    .map_err(|error| StateError::Database(error.to_string()))?,
            ),
            SqlArg::Text("schedule_legacy_speed_reset_links".into()),
        ],
    )
    .await?;
    Ok(())
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
            // Disabled legacy rules can be enabled later; migrate their reset
            // companions too, preserving the original enabled state.
            if !entry.days.is_empty() && !entry.days.contains(day) {
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
        let previous = (1..=week.len())
            .map(|offset| week[(position + week.len() - offset) % week.len()].2)
            .find(|&candidate| {
                entries[candidate].enabled
                    || matches!(entries[candidate].action, ScheduleAction::SpeedLimit { .. })
            })
            .unwrap_or(index);
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
            enabled: entry.enabled,
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

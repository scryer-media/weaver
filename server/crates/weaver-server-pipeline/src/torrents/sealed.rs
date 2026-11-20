// A torrent's secrets at rest: its tracker URLs, which carry a private
// tracker's passkey, and its metainfo, which carries them again. Each is
// sealed with the instance key (AES-256-GCM) and opened only where it is
// used: a tracker URL as text (`enc:v1:`), a metainfo as bytes (`enc:b1:`,
// no base64, so it is stored at its own size plus a nonce and a tag). A
// value written before sealing reads as it was until the one pass that
// seals it.

use crate::StateError;
use crate::persistence::encryption::{
    ENCRYPTED_BYTES_PREFIX, EncryptionKey, decrypt_value, encrypt_value, is_encrypted,
};
use crate::rss::RssPersistenceInternalQueries;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;

pub(crate) use weaver_server_torrents::torrents::sealed::{
    is_sealed, open_metainfo, seal_metainfo, seal_url, url_key,
};

// The rows a sealing pass reads at a time: a torrent brings its metainfo,
// or its tracker rows, so a page stays small whatever the table holds.
pub(crate) const SEAL_PAGE: i64 = 64;

// The setting that says the one sealing pass has run: `begun` from before
// its first write, `done` once nothing is left. With it set, a start reads
// none of the sealed tables to look for work.
pub(crate) const SEALED_MARK: &str = "secrets_sealed_at_rest";
const SEAL_BEGUN: &str = "begun";
const SEAL_DONE: &str = "done";

// The keys an RSS item's link and id are kept under in a job's metadata. A
// torrent keeps neither; a usenet job keeps both sealed.
pub(crate) const RSS_LINK_KEYS: [&str; 2] = crate::history::attributes::RSS_LINK_ATTRIBUTE_KEYS;

// What selects, in SQL, a metadata that may name an RSS item's link or id.
const RSS_LINK_LIKE: &str = "%\"rss.item_%";

// A usenet RSS job's metadata as it is stored: the item's link and id
// sealed with the instance key. A write always seals, whatever a value
// starts with. Under a key held in memory only they stay plain, as a seen
// item's link does: a key gone at the next start would lose them.
pub(crate) fn seal_rss_link_metadata(
    key: Option<&EncryptionKey>,
    metadata: &mut [(String, String)],
) -> Result<(), StateError> {
    let Some(key) = key.filter(|key| !key.is_ephemeral()) else {
        return Ok(());
    };
    for (name, value) in metadata.iter_mut() {
        if RSS_LINK_KEYS.contains(&name.as_str()) {
            *value = encrypt_value(key, value).map_err(StateError::Database)?;
        }
    }
    Ok(())
}

// A job's stored RSS link or id, opened: a plain one as it is, a sealed one
// decrypted, and one that does not decrypt, under a lost key, none.
fn open_rss_link(key: Option<&EncryptionKey>, stored: &str) -> Option<String> {
    if !is_encrypted(stored) {
        return Some(stored.to_string());
    }
    decrypt_value(key?, stored).ok()
}

// A metadata list with its RSS link and id sealed, or none when it holds
// none still to seal. A value already sealed stays as it is: one another
// key sealed reads as absent.
fn with_sealed_rss_link(key: &EncryptionKey, metadata: &str) -> Option<String> {
    let mut pairs: Vec<(String, String)> = serde_json::from_str(metadata).ok()?;
    let mut changed = false;
    for (name, value) in &mut pairs {
        if RSS_LINK_KEYS.contains(&name.as_str()) && !is_encrypted(value) {
            *value = encrypt_value(key, value).ok()?;
            changed = true;
        }
    }
    changed.then(|| serde_json::to_string(&pairs).ok())?
}

// A metadata list without an RSS item's link or id, or none when it holds
// neither.
fn without_rss_link(metadata: &str) -> Option<String> {
    let mut pairs: Vec<(String, String)> = serde_json::from_str(metadata).ok()?;
    let before = pairs.len();
    pairs.retain(|(key, _)| !RSS_LINK_KEYS.contains(&key.as_str()));
    (pairs.len() != before).then(|| serde_json::to_string(&pairs).ok())?
}

// Take a page of history rows' RSS link and id out of the attributes the
// history is filtered by: they were listed there plain before they were
// kept apart. One statement a page, by the attributes' key.
async fn drop_rss_link_attributes_tx(
    tx: &mut crate::persistence::sql_runtime::SqlTx<'_>,
    page: &[(i64, String)],
) -> Result<(), StateError> {
    use crate::persistence::sql_runtime::SqlArg;
    let sql = format!(
        "DELETE FROM job_history_attributes WHERE job_id IN ({}) AND key IN ({{}}, {{}})",
        vec!["{}"; page.len()].join(", ")
    );
    let mut args: Vec<SqlArg> = page
        .iter()
        .map(|(job_id, _)| SqlArg::I64(*job_id))
        .collect();
    args.extend(
        RSS_LINK_KEYS
            .iter()
            .map(|key| SqlArg::Text((*key).to_string())),
    );
    tx.execute(&sql, &args).await?;
    Ok(())
}

impl crate::Database {
    // The key new secrets are sealed with: the instance key when a store
    // keeps it, none while it is held in memory only.
    pub(crate) fn sealing_key(&self) -> Option<&EncryptionKey> {
        self.encryption_key().filter(|key| !key.is_ephemeral())
    }

    // What a caller is shown of an RSS job's link and id: the opened values
    // to an admin, nothing to anyone else, and nothing for a value that does
    // not open. `link` is a job's stored pairs under those keys (from
    // `rss_link_pairs`); every surface that shows a job's metadata adds
    // these and only these.
    pub fn rss_link_attributes(
        &self,
        link: &[(String, String)],
        scope: crate::auth::CallerScope,
    ) -> Vec<(String, String)> {
        if !scope.is_admin() {
            return Vec::new();
        }
        link.iter()
            .filter(|(name, _)| RSS_LINK_KEYS.contains(&name.as_str()))
            .filter_map(|(name, stored)| {
                open_rss_link(self.encryption_key(), stored).map(|value| (name.clone(), value))
            })
            .collect()
    }

    // Seal, once, every secret stored before sealing: each torrent's
    // metainfo and tracker URLs, each RSS feed's URL and each seen item's
    // link and id, take an RSS item's link and id out of the torrents' kept
    // metadata and the history's attribute rows, seal them in each usenet
    // job's, queued or in history, and key each RSS submission's
    // idempotency row by the item's seen key. Each step selects in SQL only
    // the rows it has yet to do, a page at a time, each page in one write transaction, so a pass a
    // crash cut short goes on from where it stood. The pass is marked begun
    // before its first write and done after its last: a start that finds it
    // done reads the mark and nothing else. Runs at start, once the
    // instance key is set, before any torrent is read. Under a key held in
    // memory only nothing is sealed and nothing is marked.
    pub fn seal_stored_secrets(&self) -> Result<(), StateError> {
        use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
        use crate::torrents::model::{named_by_str, parse_named_by};

        if self.get_setting(SEALED_MARK)?.as_deref() == Some(SEAL_DONE) {
            return Ok(());
        }
        let Some(key) = self.sealing_key().cloned() else {
            if self.encryption_key().is_some() {
                tracing::warn!(
                    "the encryption key is held in memory only: stored torrent and RSS secrets \
                     are not sealed, and no torrent is added, until a key store keeps one"
                );
            }
            return Ok(());
        };
        self.set_setting(SEALED_MARK, SEAL_BEGUN)?;
        let datastore = self.datastore();
        let sealing = key.clone();
        self.run_sql_blocking(async move {
            let key = sealing;
            // Each metainfo not yet sealed as bytes: one stored plain, and
            // one sealed as text, which comes back at its own size.
            let mut after = i64::MIN;
            let (mut sealed, mut unread) = (0usize, 0usize);
            loop {
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT job_id, metainfo FROM torrents \
                      WHERE job_id > {} AND metainfo IS NOT NULL \
                        AND substr(metainfo, 1, 7) <> {} ORDER BY job_id LIMIT {}",
                    &[
                        SqlArg::I64(after),
                        SqlArg::Bytes(ENCRYPTED_BYTES_PREFIX.to_vec()),
                        SqlArg::I64(SEAL_PAGE),
                    ],
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = last.i64("job_id")?;
                let mut page = Vec::new();
                for row in &rows {
                    let Some(stored) = row.opt_bytes("metainfo")? else {
                        continue;
                    };
                    // One another key sealed stays as it is: its torrent
                    // is held, with why, until that key is back.
                    match open_metainfo(Some(&key), stored) {
                        Ok(plain) => page.push((
                            row.i64("job_id")?,
                            SqlArg::Bytes(seal_metainfo(Some(&key), &plain)?),
                        )),
                        Err(_) => unread += 1,
                    }
                }
                if page.is_empty() {
                    continue;
                }
                sealed += page.len();
                SqlRuntime::run_in_transaction(&datastore, "seal_torrent_metainfo", |tx| {
                    let page = page.clone();
                    Box::pin(async move {
                        crate::torrents::persistence::update_column_tx(tx, "metainfo", &page)
                            .await
                    })
                })
                .await?;
            }
            if sealed > 0 || unread > 0 {
                tracing::info!(
                    count = sealed,
                    undecryptable = unread,
                    "sealed the torrents' stored metainfo"
                );
            }

            // Each tracker row with no key yet: its URL sealed if it was
            // not, and its key made. A row is taken out and put back whole,
            // and one the same URL already has in its tier is not put back.
            let mut after = i64::MIN;
            let (mut sealed, mut unread) = (0usize, 0usize);
            loop {
                let jobs = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT DISTINCT job_id FROM torrent_trackers \
                      WHERE job_id > {} AND url_key IS NULL ORDER BY job_id LIMIT {}",
                    &[SqlArg::I64(after), SqlArg::I64(SEAL_PAGE)],
                )
                .await?
                .iter()
                .map(|row| row.i64("job_id"))
                .collect::<Result<Vec<_>, _>>()?;
                let Some(&last) = jobs.last() else { break };
                after = last;
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT job_id, tier, url, named_by FROM torrent_trackers \
                          WHERE job_id IN ({}) AND url_key IS NULL",
                        vec!["{}"; jobs.len()].join(", ")
                    ),
                    &jobs.iter().copied().map(SqlArg::I64).collect::<Vec<_>>(),
                )
                .await?;
                // Each row as it is stored, and as it goes back.
                let mut keyed = Vec::new();
                for row in &rows {
                    let stored = row.text("url")?;
                    // Whether a stored URL is sealed is what decrypting it
                    // says, not what it starts with.
                    let plain = if is_encrypted(&stored) {
                        match decrypt_value(&key, &stored) {
                            Ok(plain) => plain,
                            Err(_) => {
                                unread += 1;
                                continue;
                            }
                        }
                    } else {
                        stored.clone()
                    };
                    let named_by = row.text("named_by")?;
                    let named_by = parse_named_by(&named_by).ok_or_else(|| {
                        StateError::Database(format!(
                            "stored torrent tracker naming is not valid: {named_by}"
                        ))
                    })?;
                    keyed.push((
                        row.i64("job_id")?,
                        row.i64("tier")?,
                        stored,
                        seal_url(Some(&key), &plain)?,
                        url_key(Some(&key), &plain)?,
                        named_by_str(named_by).to_owned(),
                    ));
                }
                if keyed.is_empty() {
                    continue;
                }
                sealed += keyed.len();
                SqlRuntime::run_in_transaction(&datastore, "seal_torrent_trackers", |tx| {
                    let keyed = keyed.clone();
                    Box::pin(async move {
                        let rows = crate::persistence::sql_runtime::max_rows_for_tx(tx, 5);
                        for chunk in keyed.chunks(rows.max(1)) {
                            // Three binds a row to take it out, five to put
                            // it back sealed and keyed.
                            let gone = vec!["(job_id = {} AND tier = {} AND url = {})"; chunk.len()]
                                .join(" OR ");
                            let mut args = Vec::with_capacity(chunk.len() * 3);
                            for (job_id, tier, stored, ..) in chunk {
                                args.extend([
                                    SqlArg::I64(*job_id),
                                    SqlArg::I64(*tier),
                                    SqlArg::Text(stored.clone()),
                                ]);
                            }
                            tx.execute(&format!("DELETE FROM torrent_trackers WHERE {gone}"), &args)
                                .await?;
                            let mut args = Vec::with_capacity(chunk.len() * 5);
                            for (job_id, tier, _, sealed, url_key, named_by) in chunk {
                                args.extend([
                                    SqlArg::I64(*job_id),
                                    SqlArg::I64(*tier),
                                    SqlArg::Text(sealed.clone()),
                                    SqlArg::Bytes(url_key.clone()),
                                    SqlArg::Text(named_by.clone()),
                                ]);
                            }
                            tx.execute(
                                &format!(
                                    "INSERT INTO torrent_trackers (job_id, tier, url, url_key, named_by) \
                                     VALUES {} ON CONFLICT (job_id, tier, url_key) DO NOTHING",
                                    vec!["({}, {}, {}, {}, {})"; chunk.len()].join(", ")
                                ),
                                &args,
                            )
                            .await?;
                        }
                        Ok(())
                    })
                })
                .await?;
            }
            if sealed > 0 || unread > 0 {
                tracing::info!(
                    count = sealed,
                    undecryptable = unread,
                    "sealed the torrents' stored tracker URLs"
                );
            }

            // Each torrent whose kept metadata names an RSS item's link or
            // id: without them.
            let mut after = i64::MIN;
            loop {
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT job_id, metadata FROM torrents \
                      WHERE job_id > {} AND metadata LIKE {} ORDER BY job_id LIMIT {}",
                    &[
                        SqlArg::I64(after),
                        SqlArg::Text(RSS_LINK_LIKE.to_string()),
                        SqlArg::I64(SEAL_PAGE),
                    ],
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = last.i64("job_id")?;
                let mut page = Vec::new();
                for row in &rows {
                    if let Some(kept) = row.opt_text("metadata")?.as_deref().and_then(without_rss_link)
                    {
                        page.push((row.i64("job_id")?, SqlArg::Text(kept)));
                    }
                }
                if page.is_empty() {
                    continue;
                }
                SqlRuntime::run_in_transaction(&datastore, "strip_torrent_rss_links", |tx| {
                    let page = page.clone();
                    Box::pin(async move {
                        crate::torrents::persistence::update_column_tx(tx, "metadata", &page).await
                    })
                })
                .await?;
            }

            // The same of every torrent's history row, read from the
            // history itself by its key: a torrent removed since has no
            // row among the torrents, and its history is kept. A usenet
            // job's row is never selected. The walk is the history's key
            // in order, once, in this one pass.
            let mut after = i64::MIN;
            loop {
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT job_id, metadata FROM job_history \
                      WHERE job_id > {} AND protocol = {} AND metadata LIKE {} \
                      ORDER BY job_id LIMIT {}",
                    &[
                        SqlArg::I64(after),
                        SqlArg::Text(crate::jobs::Protocol::Torrent.as_str().to_string()),
                        SqlArg::Text(RSS_LINK_LIKE.to_string()),
                        SqlArg::I64(SEAL_PAGE),
                    ],
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = last.i64("job_id")?;
                let mut had = Vec::new();
                for row in &rows {
                    if let Some(kept) = row.opt_text("metadata")?.as_deref().and_then(without_rss_link)
                    {
                        had.push((row.i64("job_id")?, kept));
                    }
                }
                if had.is_empty() {
                    continue;
                }
                SqlRuntime::run_in_transaction(&datastore, "strip_history_rss_links", |tx| {
                    let had = had.clone();
                    Box::pin(async move {
                        let sql = format!(
                            "UPDATE job_history SET metadata = CASE job_id {}END \
                              WHERE job_id IN ({})",
                            "WHEN {} THEN {} ".repeat(had.len()),
                            vec!["{}"; had.len()].join(", ")
                        );
                        let mut args = Vec::with_capacity(had.len() * 3);
                        for (job_id, kept) in &had {
                            args.extend([SqlArg::I64(*job_id), SqlArg::Text(kept.clone())]);
                        }
                        args.extend(had.iter().map(|(job_id, _)| SqlArg::I64(*job_id)));
                        tx.execute(&sql, &args).await?;
                        drop_rss_link_attributes_tx(tx, &had).await
                    })
                })
                .await?;
            }

            // Each usenet job's metadata that names an RSS item's link or id
            // still plain: the queue's, then the history's, with them sealed.
            // A torrent's history row is never selected: it keeps neither.
            for (table, usenet_only) in [("active_jobs", ""), ("job_history", " AND protocol <> {}")]
            {
                let select = format!(
                    "SELECT job_id, metadata FROM {table} \
                      WHERE job_id > {{}}{usenet_only} AND metadata LIKE {{}} \
                      ORDER BY job_id LIMIT {{}}"
                );
                let mut after = i64::MIN;
                loop {
                    let mut args = vec![SqlArg::I64(after)];
                    if !usenet_only.is_empty() {
                        args.push(SqlArg::Text(
                            crate::jobs::Protocol::Torrent.as_str().to_string(),
                        ));
                    }
                    args.extend([
                        SqlArg::Text(RSS_LINK_LIKE.to_string()),
                        SqlArg::I64(SEAL_PAGE),
                    ]);
                    let rows = SqlRuntime::fetch_all(datastore.read_exec(), &select, &args).await?;
                    let Some(last) = rows.last() else { break };
                    after = last.i64("job_id")?;
                    let mut had = Vec::new();
                    for row in &rows {
                        if let Some(sealed) = row
                            .opt_text("metadata")?
                            .as_deref()
                            .and_then(|metadata| with_sealed_rss_link(&key, metadata))
                        {
                            had.push((row.i64("job_id")?, sealed));
                        }
                    }
                    if had.is_empty() {
                        continue;
                    }
                    SqlRuntime::run_in_transaction(&datastore, "seal_job_rss_links", |tx| {
                        let had = had.clone();
                        Box::pin(async move {
                            let sql = format!(
                                "UPDATE {table} SET metadata = CASE job_id {}END \
                                  WHERE job_id IN ({})",
                                "WHEN {} THEN {} ".repeat(had.len()),
                                vec!["{}"; had.len()].join(", ")
                            );
                            let mut args = Vec::with_capacity(had.len() * 3);
                            for (job_id, sealed) in &had {
                                args.extend([SqlArg::I64(*job_id), SqlArg::Text(sealed.clone())]);
                            }
                            args.extend(had.iter().map(|(job_id, _)| SqlArg::I64(*job_id)));
                            tx.execute(&sql, &args).await?;
                            if table == "job_history" {
                                drop_rss_link_attributes_tx(tx, &had).await?;
                            }
                            Ok(())
                        })
                    })
                    .await?;
                }
            }
            Ok(())
        })?;
        self.seal_rss_links(&key)?;
        self.key_rss_idempotency()?;
        self.set_setting(SEALED_MARK, SEAL_DONE)
    }
}

// A torrent's secrets at rest: its tracker URLs, which carry a private
// tracker's passkey, and its metainfo, which carries them again. Each is
// sealed with the instance key by the one scheme every stored secret uses
// (`enc:v1:`, AES-256-GCM), and opened only where it is used. A value
// written before sealing reads as it was until the start that seals it.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;

use crate::StateError;
use crate::persistence::encryption::{EncryptionKey, decrypt_value, encrypt_value, is_encrypted};

// What every sealed value starts with. A bencoded metainfo starts with `d`,
// so a stored metainfo is told from a sealed one by it.
const SEALED: &[u8] = b"enc:v1:";

fn needs_key() -> StateError {
    StateError::Database("encryption key is required to store secrets".to_string())
}

// A tracker URL as it is stored: sealed.
pub(crate) fn seal_url(key: Option<&EncryptionKey>, url: &str) -> Result<String, StateError> {
    if is_encrypted(url) {
        return Ok(url.to_string());
    }
    encrypt_value(key.ok_or_else(needs_key)?, url).map_err(StateError::Database)
}

// A stored tracker URL, opened.
pub(crate) fn open_url(key: Option<&EncryptionKey>, stored: &str) -> Result<String, String> {
    if !is_encrypted(stored) {
        return Ok(stored.to_string());
    }
    decrypt_value(
        key.ok_or("no instance key to decrypt a tracker URL")?,
        stored,
    )
}

// Whether a stored metainfo is sealed.
pub(crate) fn is_sealed(stored: &[u8]) -> bool {
    stored.starts_with(SEALED)
}

// A metainfo as it is stored: its bytes in base64, sealed.
pub(crate) fn seal_metainfo(
    key: Option<&EncryptionKey>,
    metainfo: &[u8],
) -> Result<Vec<u8>, StateError> {
    if is_sealed(metainfo) {
        return Ok(metainfo.to_vec());
    }
    encrypt_value(key.ok_or_else(needs_key)?, &STANDARD.encode(metainfo))
        .map(String::into_bytes)
        .map_err(StateError::Database)
}

// A stored metainfo, opened.
pub(crate) fn open_metainfo(
    key: Option<&EncryptionKey>,
    stored: Vec<u8>,
) -> Result<Vec<u8>, String> {
    if !is_sealed(&stored) {
        return Ok(stored);
    }
    let text = std::str::from_utf8(&stored)
        .map_err(|error| format!("a sealed metainfo is not text: {error}"))?;
    let encoded = decrypt_value(key.ok_or("no instance key to decrypt a metainfo")?, text)?;
    STANDARD
        .decode(encoded)
        .map_err(|error| format!("a sealed metainfo does not decode: {error}"))
}

// The torrents a sealing pass reads at a time: each brings its metainfo, or
// its tracker rows, so a page stays small whatever the table holds.
const SEAL_PAGE: i64 = 64;

impl crate::Database {
    // Seal every torrent secret stored before sealing: each metainfo, then
    // each tracker URL, a page of torrents at a time, each page in one write
    // transaction. A value already sealed is passed by, so a pass a crash
    // cut short goes on from where it stood at the next start, and one with
    // nothing left to seal writes nothing. Runs at start, once the instance
    // key is set, before any torrent is read.
    pub fn seal_torrent_secrets(&self) -> Result<(), StateError> {
        use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
        use crate::torrents::model::{named_by_str, parse_named_by};

        let Some(key) = self.encryption_key().cloned() else {
            return Ok(());
        };
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let mut after = i64::MIN;
            let mut sealed = 0usize;
            loop {
                let rows = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT job_id, metainfo FROM torrents \
                      WHERE job_id > {} AND metainfo IS NOT NULL ORDER BY job_id LIMIT {}",
                    &[SqlArg::I64(after), SqlArg::I64(SEAL_PAGE)],
                )
                .await?;
                let Some(last) = rows.last() else { break };
                after = last.i64("job_id")?;
                let mut page = Vec::new();
                for row in &rows {
                    let Some(stored) = row.opt_bytes("metainfo")? else {
                        continue;
                    };
                    if !is_sealed(&stored) {
                        page.push((
                            row.i64("job_id")?,
                            SqlArg::Bytes(seal_metainfo(Some(&key), &stored)?),
                        ));
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
            if sealed > 0 {
                tracing::info!(count = sealed, "sealed the torrents' stored metainfo");
            }

            let mut after = i64::MIN;
            let mut sealed = 0usize;
            loop {
                let jobs = SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    "SELECT DISTINCT job_id FROM torrent_trackers \
                      WHERE job_id > {} ORDER BY job_id LIMIT {}",
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
                          WHERE job_id IN ({})",
                        vec!["{}"; jobs.len()].join(", ")
                    ),
                    &jobs.iter().copied().map(SqlArg::I64).collect::<Vec<_>>(),
                )
                .await?;
                // The plain rows, each to go as it is and come back sealed.
                let mut plain = Vec::new();
                for row in &rows {
                    let url = row.text("url")?;
                    if is_encrypted(&url) {
                        continue;
                    }
                    let named_by = row.text("named_by")?;
                    let named_by = parse_named_by(&named_by).ok_or_else(|| {
                        StateError::Database(format!(
                            "stored torrent tracker naming is not valid: {named_by}"
                        ))
                    })?;
                    plain.push((
                        row.i64("job_id")?,
                        row.i64("tier")?,
                        url.clone(),
                        seal_url(Some(&key), &url)?,
                        named_by_str(named_by).to_owned(),
                    ));
                }
                if plain.is_empty() {
                    continue;
                }
                sealed += plain.len();
                SqlRuntime::run_in_transaction(&datastore, "seal_torrent_trackers", |tx| {
                    let plain = plain.clone();
                    Box::pin(async move {
                        let rows = crate::persistence::sql_runtime::max_rows_for_tx(tx, 4);
                        for chunk in plain.chunks(rows.max(1)) {
                            // Three binds a row to take it out, four to put
                            // it back sealed.
                            let gone = vec!["(job_id = {} AND tier = {} AND url = {})"; chunk.len()]
                                .join(" OR ");
                            let mut args = Vec::with_capacity(chunk.len() * 3);
                            for (job_id, tier, url, _, _) in chunk {
                                args.extend([
                                    SqlArg::I64(*job_id),
                                    SqlArg::I64(*tier),
                                    SqlArg::Text(url.clone()),
                                ]);
                            }
                            tx.execute(&format!("DELETE FROM torrent_trackers WHERE {gone}"), &args)
                                .await?;
                            let mut args = Vec::with_capacity(chunk.len() * 4);
                            for (job_id, tier, _, sealed, named_by) in chunk {
                                args.extend([
                                    SqlArg::I64(*job_id),
                                    SqlArg::I64(*tier),
                                    SqlArg::Text(sealed.clone()),
                                    SqlArg::Text(named_by.clone()),
                                ]);
                            }
                            tx.execute(
                                &format!(
                                    "INSERT INTO torrent_trackers (job_id, tier, url, named_by) VALUES {}",
                                    vec!["({}, {}, {}, {})"; chunk.len()].join(", ")
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
            if sealed > 0 {
                tracing::info!(count = sealed, "sealed the torrents' stored tracker URLs");
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sealed_url_and_metainfo_open_to_what_was_sealed_and_hold_no_passkey() {
        let key = EncryptionKey::generate();
        let url = "https://tracker.example/announce?passkey=0123abcd";
        let sealed = seal_url(Some(&key), url).unwrap();
        assert!(!sealed.contains("0123abcd"));
        assert_eq!(seal_url(Some(&key), &sealed).unwrap(), sealed);
        assert_eq!(open_url(Some(&key), &sealed).unwrap(), url);
        assert_eq!(open_url(Some(&key), url).unwrap(), url);

        let metainfo = format!("d8:announce{}:{url}4:infodee", url.len()).into_bytes();
        let stored = seal_metainfo(Some(&key), &metainfo).unwrap();
        assert!(is_sealed(&stored));
        assert!(!stored.windows(8).any(|at| at == b"0123abcd"));
        assert_eq!(open_metainfo(Some(&key), stored.clone()).unwrap(), metainfo);
        assert_eq!(
            open_metainfo(Some(&key), metainfo.clone()).unwrap(),
            metainfo
        );

        let other = EncryptionKey::generate();
        assert!(open_url(Some(&other), &sealed).is_err());
        assert!(open_metainfo(Some(&other), stored).is_err());
        assert!(seal_url(None, url).is_err());
    }
}

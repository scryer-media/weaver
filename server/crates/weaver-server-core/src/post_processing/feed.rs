use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use super::events::{EventContext, has_subscriber, run_event};
use super::executor::{execution_refusal, strict_security_enabled};
use super::model::{
    ScriptEventLabel, ScriptKind, ScriptList, ScriptListEntry, ScriptName, ScriptStatus,
};
use super::runner::CompatibilityFacts;
use crate::settings::SharedConfig;
use crate::{Database, RssFeedRow};

pub async fn transform_feed(
    db: &Database,
    config: &SharedConfig,
    feed: &RssFeedRow,
    body: Vec<u8>,
    ceiling: u64,
) -> Result<Vec<u8>, String> {
    let scripts = if feed.scripts.is_empty() {
        None
    } else {
        Some(
            ScriptList::new(
                feed.scripts
                    .iter()
                    .map(|name| {
                        Ok(ScriptListEntry {
                            script: ScriptName::new(name.clone())
                                .map_err(|error| error.to_string())?,
                            enabled: true,
                            timeout_seconds: None,
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )
            .map_err(|error| error.to_string())?,
        )
    };
    let mut context = EventContext {
        job_id: None,
        event: ScriptEventLabel::Feed(u64::from(feed.id)),
        category: None,
        cwd: PathBuf::new(),
        env: BTreeMap::new(),
        facts: CompatibilityFacts::default(),
        scripts,
    };
    let admission_db = db.clone();
    let admission_context = context.clone();
    let has_script = tokio::task::spawn_blocking(move || {
        let settings = admission_db
            .post_processing_settings()
            .map_err(|error| error.to_string())?;
        if execution_refusal(&settings, strict_security_enabled()).is_none()
            && let Some(scripts) = &admission_context.scripts
        {
            let root = admission_db
                .post_processing_script_directory()
                .map_err(|error| error.to_string())?;
            validate_explicit_feed_scripts(&root, scripts)?;
        }
        has_subscriber(&admission_db, &admission_context).map_err(|error| error.to_string())
    })
    .await
    .map_err(|error| error.to_string())??;
    if !has_script {
        return Ok(body);
    }
    {
        let config = config.read().await;
        context.facts.data_dir = Some(PathBuf::from(&config.data_dir));
        context.facts.intermediate_dir = Some(PathBuf::from(config.intermediate_dir()));
        context.facts.complete_dir = Some(PathBuf::from(config.complete_dir()));
    }
    let data_dir = context
        .facts
        .data_dir
        .clone()
        .expect("configured data directory");
    let scratch = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&data_dir)?;
        let scratch = tempfile::Builder::new()
            .prefix("script-feed-")
            .tempdir_in(data_dir)?;
        std::fs::write(scratch.path().join("feed.xml"), body)?;
        Ok::<_, std::io::Error>(scratch)
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    let input = scratch.path().join("feed.xml");
    context.cwd = scratch.path().to_path_buf();
    context
        .env
        .insert("NZBFP_FEEDID".into(), feed.id.to_string());
    context.env.insert(
        "NZBFP_FILENAME".into(),
        input.to_string_lossy().into_owned(),
    );
    let run_id = scratch
        .path()
        .file_name()
        .expect("scratch name")
        .to_string_lossy()
        .into_owned();
    let results = run_event(db, &mut context, &run_id, None, None)
        .await
        .map_err(|error| error.to_string())?;
    if results
        .iter()
        .any(|result| result.status != ScriptStatus::Succeeded)
    {
        return Err("feed script failed (exit 93 is required)".into());
    }
    tokio::task::spawn_blocking(move || {
        let _scratch = scratch;
        let mut body = Vec::new();
        std::fs::File::open(input)?
            .take(ceiling + 1)
            .read_to_end(&mut body)?;
        if body.len() as u64 > ceiling {
            return Err(std::io::Error::other(
                "rewritten RSS feed exceeds size limit",
            ));
        }
        Ok::<_, std::io::Error>(body)
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())
}

pub fn validate_feed_script_selection(
    db: &Database,
    names: &[String],
) -> Result<(), crate::StateError> {
    if names.is_empty() {
        return Ok(());
    }
    let entries = names
        .iter()
        .map(|name| ScriptName::new(name.clone()).map(ScriptListEntry::new))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| crate::StateError::Database(error.to_string()))?;
    let scripts =
        ScriptList::new(entries).map_err(|error| crate::StateError::Database(error.to_string()))?;
    let root = db.post_processing_script_directory()?;
    validate_explicit_feed_scripts(&root, &scripts).map_err(crate::StateError::Database)
}

fn validate_explicit_feed_scripts(root: &Path, scripts: &ScriptList) -> Result<(), String> {
    for entry in scripts.enabled_entries() {
        let script = super::listing::resolve_script(root, &entry.script)
            .map_err(|error| error.to_string())?;
        if !script.manifest.kinds().contains(&ScriptKind::Feed) {
            return Err(format!(
                "script '{}' does not support feed events",
                entry.script
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_explicit_feed_script_is_not_silently_skipped() {
        let directory = tempfile::tempdir().unwrap();
        let scripts = ScriptList::new(vec![ScriptListEntry::new(
            ScriptName::new("missing.py").unwrap(),
        )])
        .unwrap();
        let error = validate_explicit_feed_scripts(directory.path(), &scripts).unwrap_err();
        assert!(error.contains("missing.py"));
    }
}

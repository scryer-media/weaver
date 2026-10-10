use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use super::events::{EventContext, has_subscriber, run_event};
use super::instances::{InstanceTrigger, ScriptInstance};
use super::model::{ScriptEventLabel, ScriptStatus};
use super::runner::CompatibilityFacts;
use crate::settings::SharedConfig;
use crate::{Database, RssFeedRow};

/// Hand a fetched feed to the instances attached to it and return what they
/// leave of it.
pub async fn transform_feed(
    db: &Database,
    config: &SharedConfig,
    feed: &RssFeedRow,
    body: Vec<u8>,
    ceiling: u64,
) -> Result<Vec<u8>, String> {
    if feed.scripts.is_empty() {
        return Ok(body);
    }
    let admission_db = db.clone();
    let attached = feed.scripts.clone();
    let feed_id = feed.id;
    let admitted = tokio::task::spawn_blocking(move || {
        let instances = attached_instances(&admission_db, &attached)?;
        let context = EventContext {
            job_id: None,
            event: ScriptEventLabel::Feed(u64::from(feed_id)),
            category: None,
            cwd: PathBuf::new(),
            env: BTreeMap::new(),
            facts: CompatibilityFacts::default(),
            instances: Some(instances),
            scratch: None,
        };
        Ok::<_, crate::StateError>(has_subscriber(&admission_db, &context)?.then_some(context))
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    let Some(mut context) = admitted else {
        return Ok(body);
    };
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
        Ok::<_, std::io::Error>(Arc::new(scratch))
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())?;
    let input = scratch.path().join("feed.xml");
    context.cwd = scratch.path().to_path_buf();
    context.scratch = Some(scratch.clone());
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
    if let Some(result) = results.iter().find(|result| {
        matches!(
            result.status,
            ScriptStatus::Failed
                | ScriptStatus::TimedOut
                | ScriptStatus::Cancelled
                | ScriptStatus::Interrupted
        )
    }) {
        return Err(format!(
            "feed script {} did not complete: {}",
            result.label(),
            result.status.as_str()
        ));
    }
    drop(context);
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

/// Refuse a feed's attachments unless each is a feed instance, named once.
pub fn validate_feed_script_selection(
    db: &Database,
    ids: &[String],
) -> Result<(), crate::StateError> {
    if ids.is_empty() {
        return Ok(());
    }
    let instances = db.script_instances()?;
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        let instance = instances
            .iter()
            .find(|instance| &instance.id == id)
            .ok_or_else(|| {
                crate::StateError::Database(format!("script job '{id}' does not exist"))
            })?;
        if instance.trigger != InstanceTrigger::Feed {
            return Err(crate::StateError::Database(format!(
                "'{}' does not run on feeds",
                instance.name
            )));
        }
        if !seen.insert(id) {
            return Err(crate::StateError::Database(format!(
                "'{}' is attached more than once",
                instance.name
            )));
        }
    }
    Ok(())
}

/// The feed instances behind `ids`, in that order. One that has since been
/// deleted or given another trigger is left out.
fn attached_instances(
    db: &Database,
    ids: &[String],
) -> Result<Vec<ScriptInstance>, crate::StateError> {
    let mut instances = db.script_instances()?;
    Ok(ids
        .iter()
        .filter_map(|id| {
            let position = instances.iter().position(|instance| {
                &instance.id == id && instance.trigger == InstanceTrigger::Feed
            })?;
            Some(instances.swap_remove(position))
        })
        .collect())
}

//! The first-run setup wizard's one piece of server state.
//!
//! The wizard walks a new install through its first provider and a look at its
//! folders and categories, straight after the access choice. Whether it is
//! still owed is kept here rather than in the browser, so a second browser
//! does not start it over and a finished install never sees it again.
//!
//! An install that has never recorded a stage owes the wizard only while it
//! has no provider: an install upgraded from before the wizard existed already
//! has one, and is left alone. Beginning records `started`, which keeps the
//! wizard open across a reload once its own first step has added a provider;
//! finishing — or skipping — records `complete`, which ends it for good.

use async_graphql::SimpleObject;
use weaver_server_core::Database;
use weaver_server_core::settings::SharedConfig;

use crate::observability::spawn_blocking_db;

const SETTING_FIRST_RUN_SETUP: &str = "first_run_setup";
const STAGE_STARTED: &str = "started";
const STAGE_COMPLETE: &str = "complete";

#[derive(Debug, Clone, Copy, SimpleObject)]
pub struct FirstRunSetup {
    /// Whether the browser should walk the operator through first-run setup.
    pub pending: bool,
}

fn pending(stage: Option<&str>, has_servers: bool) -> bool {
    match stage {
        Some(STAGE_COMPLETE) => false,
        Some(STAGE_STARTED) => true,
        _ => !has_servers,
    }
}

async fn has_servers(config: &SharedConfig) -> bool {
    !config.read().await.servers.is_empty()
}

async fn stored_stage(db: &Database) -> async_graphql::Result<Option<String>> {
    let db = db.clone();
    spawn_blocking_db("settings.first_run_setup.read", move || {
        db.get_setting(SETTING_FIRST_RUN_SETUP)
    })
    .await
}

async fn store_stage(db: &Database, stage: &'static str) -> async_graphql::Result<()> {
    let db = db.clone();
    spawn_blocking_db("settings.first_run_setup.write", move || {
        db.set_setting(SETTING_FIRST_RUN_SETUP, stage)
    })
    .await
}

pub(crate) async fn status(
    db: &Database,
    config: &SharedConfig,
) -> async_graphql::Result<FirstRunSetup> {
    let stage = stored_stage(db).await?;
    Ok(FirstRunSetup {
        pending: pending(stage.as_deref(), has_servers(config).await),
    })
}

/// Hold the wizard open until it is finished. Only a wizard that is actually
/// owed is recorded, so a stray call cannot reopen a finished install.
pub(crate) async fn begin(
    db: &Database,
    config: &SharedConfig,
) -> async_graphql::Result<FirstRunSetup> {
    let stage = stored_stage(db).await?;
    let owed = pending(stage.as_deref(), has_servers(config).await);
    if owed && stage.is_none() {
        store_stage(db, STAGE_STARTED).await?;
    }
    Ok(FirstRunSetup { pending: owed })
}

pub(crate) async fn finish(db: &Database) -> async_graphql::Result<FirstRunSetup> {
    store_stage(db, STAGE_COMPLETE).await?;
    Ok(FirstRunSetup { pending: false })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unrecorded_install_owes_the_wizard_only_without_a_provider() {
        assert!(pending(None, false));
        assert!(!pending(None, true));
    }

    #[test]
    fn a_started_wizard_survives_its_own_provider() {
        assert!(pending(Some(STAGE_STARTED), true));
    }

    #[test]
    fn a_finished_wizard_never_returns() {
        assert!(!pending(Some(STAGE_COMPLETE), false));
        assert!(!pending(Some(STAGE_COMPLETE), true));
    }
}

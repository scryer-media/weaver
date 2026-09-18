//! Orchestration for one in-application upgrade run.
//!
//! The mechanics all live in the shared `application-updater` crate. What is
//! here is the part that belongs to Weaver: admission (is this deployment
//! allowed to upgrade itself, and is the asked-for release the one the release
//! checker actually found), the single run of state the UI watches, its
//! persistence across a restart, and the journal recovery that decides on the
//! next boot whether the upgrade took.
//!
//! State lives in a [`tokio::sync::watch`] channel plus one settings row, the
//! same shape [`crate::update_check`] uses, so the GraphQL query and the
//! subscription read one source and a restart does not lose the run that caused
//! it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use application_updater::installation::{
    EligibilityReason, InstallationAssessment, InstallationKind, ManagementOwner,
};
use application_updater::journal::ApplicationUpgradeJournal;
use application_updater::pipeline::{
    DownloadProgress, FetchedUpgradeManifest, PortablePromotionFailure, PortableUpgradePaths,
    ProgressFuture, rename_path,
};
use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock, watch};
use tracing::{debug, warn};

use super::error::{
    ApplicationUpgradeError, ApplicationUpgradeResult, map_updater_error, to_updater_error,
};
use super::manifest::{UpgradeArtifact, UpgradeManifest};
use super::product::{JOURNAL_SCHEMA, WEAVER_PRODUCT};
use crate::persistence::Database;
use crate::runtime::restart::RestartController;
use crate::update_check::{UpdateCheckService, release_tag_for_version};

/// Stable progress phase names the upgrade UI reads.
pub use application_updater::phases;

/// The running version, as the manifest's `version` field spells it.
const WEAVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Settings key holding the JSON-encoded latest [`ApplicationUpgradeRun`].
const APPLICATION_UPGRADE_RUN_SETTING_KEY: &str = "application_upgrade_run";

/// Where an upgrade keeps its working state, under the profile directory.
const UPGRADE_DIR_NAME: &str = "application-upgrade";

/// Terminal state of an upgrade run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationUpgradeRunStatus {
    Running,
    Completed,
    Failed,
}

impl ApplicationUpgradeRunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    const fn is_terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// One upgrade attempt, as the UI sees it and as it survives a restart.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct ApplicationUpgradeRun {
    pub run_id: String,
    pub status: ApplicationUpgradeRunStatus,
    pub phase: String,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    /// The version this run installs.
    pub target_version: String,
    /// The release tag this run installs.
    pub target_tag: String,
    /// The version the run started from, for the completion summary.
    pub from_version: String,
    pub error: Option<String>,
    pub started_at_epoch_ms: i64,
    pub completed_at_epoch_ms: Option<i64>,
}

impl ApplicationUpgradeRun {
    fn checking(run_id: String, request: &UpgradeJobRequest) -> Self {
        Self {
            run_id,
            status: ApplicationUpgradeRunStatus::Running,
            phase: phases::CHECKING.to_string(),
            downloaded_bytes: 0,
            total_bytes: 0,
            target_version: request.expected_version.clone(),
            target_tag: request.expected_tag.clone(),
            from_version: WEAVER_VERSION.to_string(),
            error: None,
            started_at_epoch_ms: epoch_ms_now(),
            completed_at_epoch_ms: None,
        }
    }

    /// The run a journal describes, for a boot that finds one with no persisted
    /// run to match it — an upgrade applied by a build that crashed before it
    /// could write the row.
    fn from_journal(journal: &ApplicationUpgradeJournal) -> Self {
        Self {
            run_id: journal.run_id.clone(),
            status: ApplicationUpgradeRunStatus::Running,
            phase: journal.phase.clone(),
            downloaded_bytes: 0,
            total_bytes: 0,
            target_version: journal.expected_version.clone(),
            target_tag: journal.expected_tag.clone(),
            from_version: String::new(),
            error: None,
            started_at_epoch_ms: journal
                .written_at
                .map_or_else(epoch_ms_now, |at| at.timestamp_millis()),
            completed_at_epoch_ms: None,
        }
    }
}

/// Everything the upgrade surface answers in one read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationUpgradeSnapshot {
    pub current_version: String,
    pub update_version: Option<String>,
    pub update_tag: Option<String>,
    pub update_available: bool,
    pub installation_kind: InstallationKind,
    pub management_owner: ManagementOwner,
    pub eligible: bool,
    pub eligibility_reason: EligibilityReason,
    /// The run in flight, if one is.
    pub active_run: Option<ApplicationUpgradeRun>,
    /// The newest run, in flight or finished.
    pub latest_run: Option<ApplicationUpgradeRun>,
}

/// What the API asks for when the operator presses Install.
#[derive(Clone, Debug)]
pub struct ApplicationUpgradeStartRequest {
    pub expected_tag: String,
    pub expected_version: String,
}

/// The admitted request, with the installation facts the pipeline needs.
#[derive(Clone, Debug)]
struct UpgradeJobRequest {
    expected_tag: String,
    expected_version: String,
    installation_kind: InstallationKind,
    /// Tests and nonstandard hosts provide the startup evidence path directly.
    executable_path: Option<PathBuf>,
    /// Whether the desktop wrapper owns and supervises this process. Read by the
    /// Windows helper handoff, which is the only promotion that has to hand the
    /// shutdown and relaunch to the wrapper.
    #[cfg_attr(not(windows), allow(dead_code))]
    tray_supervised: bool,
}

/// The host-owned free-space admission check, injectable so tests can drive the
/// insufficient-space path without filling a filesystem.
type UpgradeSpaceCheck = fn(&Path, u64) -> ApplicationUpgradeResult<()>;
/// The rename primitive, injectable so tests can drive promotion and rollback
/// failures.
type UpgradeRename = fn(&Path, &Path) -> std::io::Result<()>;

struct UpgradePipelineDependencies<'a> {
    client: &'a reqwest::Client,
    artifact_url_override: Option<&'a str>,
    ensure_available_space: UpgradeSpaceCheck,
    #[cfg_attr(windows, allow(dead_code))]
    rename: UpgradeRename,
}

#[derive(Clone)]
pub struct ApplicationUpgradeService {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    /// The release checker is the only source of "what is available": an upgrade
    /// may only install the release it has already found and published.
    update_check: UpdateCheckService,
    /// Root of the upgrade's own working state, under the profile directory.
    root: PathBuf,
    /// Startup classification. Captured once, at startup, from the real process.
    assessment: InstallationAssessment,
    /// The running program file as startup evidence saw it.
    executable_path: Option<PathBuf>,
    state: watch::Sender<Option<ApplicationUpgradeRun>>,
    /// Single-flight: held for the whole run, so a second start is refused
    /// rather than queued.
    admission: Arc<Mutex<()>>,
    restart: RwLock<Option<RestartController>>,
    /// The staged-bundle signature check. A seam so the promotion tests never
    /// need a signing identity; the shipped build always runs `codesign`.
    bundle_signature_check: RwLock<Option<application_updater::macos_bundle::BundleSignatureCheck>>,
    /// Monotonic suffix so two runs in the same millisecond cannot share an id.
    run_sequence: AtomicU64,
}

impl ApplicationUpgradeService {
    /// The service a running server holds.
    ///
    /// `profile_dir` is the directory Weaver keeps its own state in; the upgrade
    /// works entirely inside a subdirectory of it, so nothing it writes can
    /// land beside the operator's downloads.
    pub fn new(
        db: Database,
        update_check: UpdateCheckService,
        profile_dir: &Path,
        assessment: InstallationAssessment,
        executable_path: Option<PathBuf>,
    ) -> Self {
        let persisted = load_persisted_run(&db);
        let (state, _rx) = watch::channel(persisted);
        Self {
            inner: Arc::new(Inner {
                db,
                update_check,
                root: profile_dir.join(UPGRADE_DIR_NAME),
                assessment,
                executable_path,
                state,
                admission: Arc::new(Mutex::new(())),
                restart: RwLock::new(None),
                bundle_signature_check: RwLock::new(None),
                run_sequence: AtomicU64::new(0),
            }),
        }
    }

    /// Wire the serve loop's restart controller in.
    ///
    /// Separate from construction because the controller is built after the
    /// schema that holds this service: the run needs it only at the very end.
    pub async fn set_restart_controller(&self, restart: RestartController) {
        *self.inner.restart.write().await = Some(restart);
    }

    /// Replace the staged-bundle signature check. Tests only.
    pub async fn set_bundle_signature_check(
        &self,
        check: application_updater::macos_bundle::BundleSignatureCheck,
    ) {
        *self.inner.bundle_signature_check.write().await = Some(check);
    }

    /// Everything the `applicationUpgradeStatus` query answers.
    pub fn snapshot(&self) -> ApplicationUpgradeSnapshot {
        let update = self.inner.update_check.status();
        let latest_run = self.inner.state.borrow().clone();
        let active_run = latest_run.clone().filter(|run| !run.status.is_terminal());
        ApplicationUpgradeSnapshot {
            current_version: WEAVER_VERSION.to_string(),
            update_tag: update
                .latest_version
                .as_deref()
                .map(release_tag_for_version),
            update_version: update.latest_version,
            update_available: update.update_available,
            installation_kind: self.inner.assessment.kind,
            management_owner: self.inner.assessment.owner,
            eligible: self.inner.assessment.eligible,
            eligibility_reason: self.inner.assessment.reason,
            active_run,
            latest_run,
        }
    }

    /// Receiver seeded with the current run, so a new subscriber sees the state
    /// immediately instead of waiting for the next transition.
    pub fn subscribe(&self) -> watch::Receiver<Option<ApplicationUpgradeRun>> {
        self.inner.state.subscribe()
    }

    /// Admit and begin an upgrade. Returns the accepted run.
    ///
    /// Every refusal happens here, before anything is downloaded: an empty
    /// field, a tag or version the release checker has not published, a version
    /// that is not strictly newer than the running one, an installation this
    /// build must not replace, and a run already in flight.
    pub async fn start(
        &self,
        request: ApplicationUpgradeStartRequest,
    ) -> ApplicationUpgradeResult<ApplicationUpgradeRun> {
        if request.expected_tag.trim().is_empty() {
            return Err(ApplicationUpgradeError::Validation(
                "expectedTag must not be empty".to_string(),
            ));
        }
        if request.expected_version.trim().is_empty() {
            return Err(ApplicationUpgradeError::Validation(
                "expectedVersion must not be empty".to_string(),
            ));
        }

        let update = self.inner.update_check.status();
        if !update.update_available {
            return Err(ApplicationUpgradeError::Validation(
                "no application update is available".to_string(),
            ));
        }
        let latest_version = update.latest_version.clone().ok_or_else(|| {
            ApplicationUpgradeError::Validation("no application update is available".to_string())
        })?;
        if latest_version != request.expected_version {
            return Err(ApplicationUpgradeError::Validation(
                "expectedVersion does not match the available release".to_string(),
            ));
        }
        if release_tag_for_version(&latest_version) != request.expected_tag {
            return Err(ApplicationUpgradeError::Validation(
                "expectedTag does not match the available release".to_string(),
            ));
        }

        let expected_version = Version::parse(&request.expected_version).map_err(|error| {
            ApplicationUpgradeError::Validation(format!(
                "expectedVersion must be valid semver: {error}"
            ))
        })?;
        let running_version = Version::parse(WEAVER_VERSION).map_err(|error| {
            ApplicationUpgradeError::Repository(format!(
                "running application version is invalid: {error}"
            ))
        })?;
        if expected_version <= running_version {
            return Err(ApplicationUpgradeError::Validation(
                "expectedVersion must be strictly newer than the running version".to_string(),
            ));
        }

        if !self.inner.assessment.eligible {
            return Err(ApplicationUpgradeError::Validation(format!(
                "this installation does not upgrade itself: {}",
                self.inner.assessment.reason.as_str()
            )));
        }
        if !eligible_installation_kind(self.inner.assessment.kind) {
            return Err(ApplicationUpgradeError::Validation(
                "application upgrade installation is not eligible".to_string(),
            ));
        }

        // A run that survived a restart — one still waiting on a reboot, or
        // one rehydrated from its journal — holds no admission lock in this
        // process, so it is refused on its published state instead.
        let carried_over = self
            .inner
            .state
            .borrow()
            .as_ref()
            .filter(|run| !run.status.is_terminal())
            .map(|run| (run.run_id.clone(), run.phase.clone()));
        if let Some((run_id, phase)) = carried_over {
            return Err(ApplicationUpgradeError::Validation(format!(
                "an application upgrade is already running (run {run_id}, phase {phase})"
            )));
        }

        // Held for the whole run. A refusal here is the single-flight answer:
        // nothing is queued behind an upgrade.
        let guard = Arc::clone(&self.inner.admission)
            .try_lock_owned()
            .map_err(|_| {
                ApplicationUpgradeError::Validation(
                    "an application upgrade is already running".to_string(),
                )
            })?;

        let request = UpgradeJobRequest {
            expected_tag: request.expected_tag,
            expected_version: request.expected_version,
            installation_kind: self.inner.assessment.kind,
            executable_path: self.inner.executable_path.clone(),
            tray_supervised: self.inner.assessment.tray_supervised,
        };
        let run = ApplicationUpgradeRun::checking(self.next_run_id(), &request);
        self.publish(run.clone());

        let service = self.clone();
        let started = run.clone();
        tokio::spawn(async move {
            service.run_upgrade(started, request, guard).await;
        });
        Ok(run)
    }

    async fn run_upgrade(
        &self,
        run: ApplicationUpgradeRun,
        request: UpgradeJobRequest,
        _admission: OwnedMutexGuard<()>,
    ) {
        if let Err(error) = self.execute(&run, &request).await {
            self.cleanup_staging();
            self.cleanup_bundle_staging(&request);
            self.fail_run(&run, error.to_string());
        }
    }

    async fn execute(
        &self,
        run: &ApplicationUpgradeRun,
        request: &UpgradeJobRequest,
    ) -> ApplicationUpgradeResult<()> {
        let client = application_upgrade_http_client()?;
        // v2 first, v1 only when the v2 asset is absent for this tag. Every
        // other v2 failure is fatal, because a client that downgrades on
        // failure hands an attacker a downgrade oracle.
        let fetched = fetch_upgrade_manifest(&client, &request.expected_tag).await?;
        debug!(
            run_id = %run.run_id,
            tag = %request.expected_tag,
            generation = ?fetched.generation,
            "resolved upgrade manifest generation"
        );
        self.run_pipeline_with_dependencies(
            run,
            request,
            &fetched.manifest,
            UpgradePipelineDependencies {
                client: &client,
                artifact_url_override: None,
                ensure_available_space,
                rename: rename_path,
            },
        )
        .await
    }

    async fn run_pipeline_with_dependencies(
        &self,
        run: &ApplicationUpgradeRun,
        request: &UpgradeJobRequest,
        manifest: &UpgradeManifest,
        dependencies: UpgradePipelineDependencies<'_>,
    ) -> ApplicationUpgradeResult<()> {
        if manifest.tag != request.expected_tag {
            return Err(ApplicationUpgradeError::Validation(
                "upgrade manifest tag does not match expectedTag".to_string(),
            ));
        }
        if manifest.version != request.expected_version {
            return Err(ApplicationUpgradeError::Validation(
                "upgrade manifest version does not match expectedVersion".to_string(),
            ));
        }
        let artifact = select_artifact(manifest, request.installation_kind)?.clone();

        self.advance(run, phases::DOWNLOADING, 0, artifact.size);
        // A bundle is promoted by renaming it inside its own parent directory,
        // so its replacement has to be staged on the same filesystem. Every
        // other installation kind stages under the profile directory.
        let bundle_paths = if request.installation_kind == InstallationKind::MacosAppBundle {
            Some(macos_bundle_upgrade_paths(
                request.installation_kind,
                request.executable_path.as_deref(),
                WEAVER_VERSION,
                &request.expected_version,
            )?)
        } else {
            None
        };
        let staging_dir = bundle_paths
            .as_ref()
            .map_or_else(|| self.staging_dir(), |paths| paths.staging_dir.clone());
        recreate_staging_dir(&staging_dir)?;
        (dependencies.ensure_available_space)(&staging_dir, staging_space_requirement(&artifact))?;
        let download_path = staging_dir.join("artifact");
        let mut progress = RunDownloadProgress { service: self, run };
        download_artifact(
            dependencies.client,
            &artifact,
            dependencies.artifact_url_override,
            &download_path,
            &mut progress,
        )
        .await?;

        self.advance(run, phases::VERIFYING, artifact.size, artifact.size);
        verify_artifact_hash(&download_path, &artifact)?;
        validate_archive_members(&download_path, &artifact)?;

        self.advance(run, phases::STAGING, artifact.size, artifact.size);
        let extracted_dir = staging_dir.join("extracted");
        extract_archive(&download_path, &artifact, &extracted_dir)?;

        self.advance(run, phases::APPLYING, artifact.size, artifact.size);
        #[cfg(windows)]
        {
            return self
                .handoff_windows_upgrade(run, request, &artifact, &extracted_dir, &download_path)
                .await;
        }

        #[cfg(not(windows))]
        if let Some(bundle_paths) = bundle_paths {
            return self
                .promote_macos_bundle_upgrade(run, request, &artifact, &bundle_paths, &dependencies)
                .await;
        }

        #[cfg(not(windows))]
        {
            let paths = portable_upgrade_paths(request, WEAVER_VERSION)?;
            let journal_path = self.journal_path();
            let journal = ApplicationUpgradeJournal {
                schema: JOURNAL_SCHEMA.to_string(),
                run_id: run.run_id.clone(),
                expected_version: request.expected_version.clone(),
                expected_tag: request.expected_tag.clone(),
                executable_path: paths.executable_path.clone(),
                backup_path: paths.backup_path.clone(),
                backup_paths: vec![paths.backup_path.clone()],
                phase: phases::RESTARTING.to_string(),
                helper_error: None,
                written_at: Some(Utc::now()),
                macos_bundle_upgrade: false,
                staging_dir: None,
            };
            // Durable before the binary moves: a crash in between must never
            // leave a promoted executable the next boot has no record of.
            write_journal(&journal_path, &journal)?;
            if let Err(failure) = apply_portable_upgrade(
                &extracted_dir,
                &artifact,
                &paths,
                &request.expected_version,
                dependencies.ensure_available_space,
                dependencies.rename,
            ) {
                let (error, restored) = failure.into_parts();
                let error = map_updater_error(error);
                if restored {
                    if let Err(cleanup_error) = remove_file_if_exists(&journal_path) {
                        warn!(
                            error = %cleanup_error,
                            "failed to remove the application upgrade journal after a restored promotion failure"
                        );
                    }
                } else {
                    tracing::error!(
                        journal_path = %journal_path.display(),
                        backup_path = %paths.backup_path.display(),
                        "portable application upgrade could not restore the previous executable; preserving recovery journal"
                    );
                }
                return Err(error);
            }

            self.advance(run, phases::RESTARTING, artifact.size, artifact.size);
            let restart = match self.restart_controller().await {
                Ok(restart) => restart,
                Err(error) => {
                    return Err(roll_back_portable_promotion(
                        &paths,
                        &journal_path,
                        dependencies.rename,
                        error,
                    ));
                }
            };
            restart.request_restart();
            Ok(())
        }
    }

    /// Swap a freshly extracted `Weaver.app` over the installed one.
    ///
    /// What is left by the time this runs is the part that must not be got
    /// wrong: prove the staged bundle would actually launch, write the durable
    /// journal before anything moves, promote by two renames inside one
    /// directory, and put the previous bundle back on any failure.
    ///
    /// The relaunch is the wrapper's, not this process's: the executable this
    /// server runs from has just been replaced, so re-executing it is not an
    /// option.
    #[cfg(not(windows))]
    async fn promote_macos_bundle_upgrade(
        &self,
        run: &ApplicationUpgradeRun,
        request: &UpgradeJobRequest,
        artifact: &UpgradeArtifact,
        paths: &application_updater::macos_bundle::MacosBundleUpgradePaths,
        dependencies: &UpgradePipelineDependencies<'_>,
    ) -> ApplicationUpgradeResult<()> {
        let journal_path = self.journal_path();
        let journal = ApplicationUpgradeJournal {
            schema: JOURNAL_SCHEMA.to_string(),
            run_id: run.run_id.clone(),
            expected_version: request.expected_version.clone(),
            expected_tag: request.expected_tag.clone(),
            executable_path: paths.bundle_path.clone(),
            backup_path: paths.backup_path.clone(),
            backup_paths: vec![paths.backup_path.clone()],
            phase: phases::RESTARTING.to_string(),
            helper_error: None,
            written_at: Some(Utc::now()),
            macos_bundle_upgrade: true,
            staging_dir: Some(paths.staging_dir.clone()),
        };
        // Durable before anything moves: a crash between the renames must leave
        // the next boot a record of both the bundle and its backup.
        write_journal(&journal_path, &journal)?;

        if let Err(failure) = apply_macos_bundle_upgrade(
            paths,
            self.bundle_signature_check().await,
            dependencies.rename,
        ) {
            let (error, restored) = failure.into_parts();
            let error = map_updater_error(error);
            if restored {
                if let Err(cleanup_error) = remove_file_if_exists(&journal_path) {
                    warn!(
                        error = %cleanup_error,
                        "failed to remove the application upgrade journal after a restored bundle promotion failure"
                    );
                }
            } else {
                tracing::error!(
                    journal_path = %journal_path.display(),
                    backup_path = %paths.backup_path.display(),
                    "application bundle upgrade could not restore the previous bundle; preserving recovery journal"
                );
            }
            return Err(error);
        }

        self.advance(run, phases::RESTARTING, artifact.size, artifact.size);
        let restart = match self.restart_controller().await {
            Ok(restart) => restart,
            Err(error) => {
                return Err(roll_back_macos_bundle_promotion(
                    paths,
                    &journal_path,
                    dependencies.rename,
                    error,
                ));
            }
        };
        restart.request_bundle_relaunch();
        Ok(())
    }

    #[cfg(windows)]
    async fn handoff_windows_upgrade(
        &self,
        run: &ApplicationUpgradeRun,
        request: &UpgradeJobRequest,
        artifact: &UpgradeArtifact,
        extracted_dir: &Path,
        msi_path: &Path,
    ) -> ApplicationUpgradeResult<()> {
        use application_updater::windows_handoff::WindowsUpgradeHandoffInput;

        let executable_path = request
            .executable_path
            .clone()
            .or_else(|| std::env::current_exe().ok())
            .ok_or_else(|| {
                ApplicationUpgradeError::Repository(
                    "failed to resolve the running executable path".to_string(),
                )
            })?;
        let install_dir = executable_path.parent().map(PathBuf::from).ok_or_else(|| {
            ApplicationUpgradeError::Validation(
                "running executable has no parent directory".to_string(),
            )
        })?;
        let direct_relaunch_args = std::env::args_os()
            .skip(1)
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let direct_relaunch_cwd = std::env::current_dir().unwrap_or_else(|_| install_dir.clone());
        let handoff = build_windows_upgrade_handoff(WindowsUpgradeHandoffInput {
            run_id: &run.run_id,
            expected_version: &request.expected_version,
            expected_tag: &request.expected_tag,
            installation_kind: request.installation_kind,
            tray_supervised: request.tray_supervised,
            executable_path: &executable_path,
            install_dir: &install_dir,
            backend_process_id: std::process::id(),
            artifact: Some(artifact),
            extracted_dir: Some(extracted_dir),
            msi_path: Some(msi_path),
            journal_path: self.journal_path(),
            direct_relaunch_args: &direct_relaunch_args,
            direct_relaunch_cwd: &direct_relaunch_cwd,
            current_version: WEAVER_VERSION,
            written_at: Utc::now(),
        })?;
        if let Some(existing_backup) = handoff
            .journal
            .backup_paths
            .iter()
            .find(|path| path.exists())
        {
            return Err(ApplicationUpgradeError::Validation(format!(
                "refusing to overwrite existing application backup '{}'",
                existing_backup.display()
            )));
        }
        write_journal(&handoff.plan.journal_path, &handoff.journal)?;
        self.advance(run, handoff.progress_phase, artifact.size, artifact.size);
        let helper_dir = self.helper_dir();
        let plan_path = helper_dir.join("plan.json");
        let helper_path = helper_dir.join(WEAVER_PRODUCT.windows_helper_executable);
        write_helper_plan(&plan_path, &handoff.plan)?;
        copy_and_spawn_windows_upgrade_helper(&helper_path, &plan_path)?;
        // The helper is detached and waiting for this process to release its own
        // executable; it starts the new build itself.
        self.restart_controller().await?.request_exit();
        Ok(())
    }

    // -- run state ----------------------------------------------------------

    fn next_run_id(&self) -> String {
        let sequence = self.inner.run_sequence.fetch_add(1, Ordering::SeqCst);
        format!("upgrade-{}-{sequence}", epoch_ms_now())
    }

    /// Publish a phase transition and persist it.
    fn advance(
        &self,
        run: &ApplicationUpgradeRun,
        phase: &str,
        downloaded_bytes: u64,
        total_bytes: u64,
    ) {
        let mut next = run.clone();
        next.phase = phase.to_string();
        next.downloaded_bytes = downloaded_bytes;
        next.total_bytes = total_bytes;
        self.publish(next);
    }

    /// Publish download byte counts without a settings write.
    ///
    /// Progress arrives per chunk; a settings row written that often would cost
    /// far more than the number is worth. Phase transitions are what persist.
    fn report_download_progress(
        &self,
        run: &ApplicationUpgradeRun,
        downloaded_bytes: u64,
        total_bytes: u64,
    ) {
        let mut next = run.clone();
        next.phase = phases::DOWNLOADING.to_string();
        next.downloaded_bytes = downloaded_bytes;
        next.total_bytes = total_bytes;
        self.inner.state.send_replace(Some(next));
    }

    fn fail_run(&self, run: &ApplicationUpgradeRun, error: String) {
        let mut next = run.clone();
        next.status = ApplicationUpgradeRunStatus::Failed;
        next.error = Some(error);
        next.completed_at_epoch_ms = Some(epoch_ms_now());
        self.publish(next);
    }

    fn publish(&self, run: ApplicationUpgradeRun) {
        self.inner.state.send_replace(Some(run.clone()));
        self.persist(&run);
    }

    fn persist(&self, run: &ApplicationUpgradeRun) {
        let Ok(encoded) = serde_json::to_string(run) else {
            return;
        };
        // Best-effort: a settings-write failure must not abandon an upgrade that
        // is otherwise proceeding.
        if let Err(error) = self
            .inner
            .db
            .set_setting(APPLICATION_UPGRADE_RUN_SETTING_KEY, &encoded)
        {
            warn!(error = %error, "failed to persist application upgrade run state");
        }
    }

    async fn restart_controller(&self) -> ApplicationUpgradeResult<RestartController> {
        self.inner.restart.read().await.clone().ok_or_else(|| {
            ApplicationUpgradeError::Repository(
                "application upgrade restart controller is not configured".to_string(),
            )
        })
    }

    #[cfg(not(windows))]
    async fn bundle_signature_check(
        &self,
    ) -> application_updater::macos_bundle::BundleSignatureCheck {
        self.inner
            .bundle_signature_check
            .read()
            .await
            .unwrap_or(application_updater::macos_bundle::verify_bundle_signature)
    }

    // -- directories --------------------------------------------------------

    fn staging_dir(&self) -> PathBuf {
        self.inner.root.join("staging")
    }

    fn helper_dir(&self) -> PathBuf {
        self.inner.root.join("helper")
    }

    fn journal_path(&self) -> PathBuf {
        self.inner.root.join("journal.json")
    }

    fn cleanup_staging(&self) {
        if let Err(error) = remove_dir_if_exists(&self.staging_dir()) {
            warn!(error = %error, "failed to clean the application upgrade staging directory");
        }
    }

    /// A bundle upgrade stages beside the installed bundle rather than under
    /// the profile directory, so a failure before promotion would otherwise
    /// leave a full staged copy next to the application. Once promotion has
    /// begun the backup exists, the path computation refuses, and the journal
    /// owns whatever is left.
    fn cleanup_bundle_staging(&self, request: &UpgradeJobRequest) {
        if request.installation_kind != InstallationKind::MacosAppBundle {
            return;
        }
        let Ok(paths) = macos_bundle_upgrade_paths(
            request.installation_kind,
            request.executable_path.as_deref(),
            WEAVER_VERSION,
            &request.expected_version,
        ) else {
            return;
        };
        if let Err(error) = remove_upgrade_owned_directory(&paths.staging_dir) {
            warn!(
                error = %error,
                path = %paths.staging_dir.display(),
                "failed to clean the staged application bundle"
            );
        }
    }

    /// Fail a run this process inherited as `running` but has no journal for.
    ///
    /// Such a run never reached promotion: the process it ran in went away
    /// while it was still downloading, verifying or staging, and nothing was
    /// replaced. Left alone it would sit in the UI as running forever and,
    /// through [`Self::start`], refuse every later attempt.
    fn fail_interrupted_run(&self, journaled_run_id: Option<&str>) {
        let interrupted = self.inner.state.borrow().clone().filter(|run| {
            !run.status.is_terminal() && journaled_run_id != Some(run.run_id.as_str())
        });
        let Some(run) = interrupted else {
            return;
        };
        warn!(
            run_id = %run.run_id,
            phase = %run.phase,
            "application upgrade was interrupted by a restart before it was applied"
        );
        self.fail_run(
            &run,
            "the upgrade was interrupted by a restart before it was applied".to_string(),
        );
        self.cleanup_staging();
    }

    // -- journal recovery ---------------------------------------------------

    /// Finalize the journal an upgrade wrote before it restarted this process.
    ///
    /// Returns the run ids that must stay running because an operating-system
    /// reboot is still outstanding. Called once, early in startup, before the
    /// upgrade surface can accept anything new.
    pub fn finalize_journal(&self) -> ApplicationUpgradeResult<Vec<String>> {
        self.finalize_journal_with_boot_time(None)
    }

    /// [`Self::finalize_journal`] with an injectable operating-system boot time.
    /// Windows hosts supply this from `GetTickCount64`; tests inject a fixed value.
    pub fn finalize_journal_with_boot_time(
        &self,
        boot_time: Option<SystemTime>,
    ) -> ApplicationUpgradeResult<Vec<String>> {
        let journal_path = self.journal_path();
        let Some(journal) = load_journal(&journal_path)? else {
            self.fail_interrupted_run(None);
            return Ok(Vec::new());
        };
        self.fail_interrupted_run(Some(&journal.run_id));
        if journal.schema != JOURNAL_SCHEMA {
            return Err(ApplicationUpgradeError::Validation(format!(
                "unsupported application upgrade journal schema '{}'",
                journal.schema
            )));
        }
        let current_executable = std::env::current_exe().map_err(|error| {
            ApplicationUpgradeError::Repository(format!(
                "failed to resolve running executable: {error}"
            ))
        })?;
        let expected_version_booted = WEAVER_VERSION == journal.expected_version;
        // A replaced application bundle journals the bundle itself, because the
        // bundle is what was renamed; the process that boots afterwards runs the
        // binary inside it, so the comparison is against the running binary's
        // own bundle. Canonicalized either way, or a symlinked layout looks like
        // a boot of the wrong binary.
        let booted_path = if journal.macos_bundle_upgrade {
            application_updater::installation::macos_app_bundle_path(&current_executable)
                .map(Path::to_path_buf)
                .unwrap_or(current_executable)
        } else {
            current_executable
        };
        let expected_executable_booted =
            canonical_path(&booted_path) == canonical_path(&journal.executable_path);

        if journal.phase == phases::REBOOT_REQUIRED {
            if reboot_required_completion_allowed(
                journal.written_at,
                boot_time.map(DateTime::<Utc>::from),
                expected_version_booted,
                expected_executable_booted,
            ) {
                self.complete_journal_run(&journal, &journal_path)?;
                return Ok(Vec::new());
            }
            // The run stays running until the operator reboots, and the
            // in-memory state is rehydrated so a second upgrade cannot start
            // behind the pending one.
            self.rehydrate_running_run(&journal);
            return Ok(vec![journal.run_id]);
        }

        if let Some(error) = journal.helper_error.clone() {
            self.finish_journal_run(&journal, ApplicationUpgradeRunStatus::Failed, Some(error));
            remove_file_if_exists(&journal_path)?;
            remove_dir_if_exists(&self.staging_dir())?;
            remove_dir_if_exists(&self.helper_dir())?;
            return Ok(Vec::new());
        }

        if journal.phase != phases::RESTARTING {
            return Err(ApplicationUpgradeError::Validation(format!(
                "unsupported application upgrade journal phase '{}'",
                journal.phase
            )));
        }

        if expected_version_booted && expected_executable_booted {
            self.complete_journal_run(&journal, &journal_path)?;
            return Ok(Vec::new());
        }

        self.finish_journal_run(
            &journal,
            ApplicationUpgradeRunStatus::Failed,
            Some("upgrade did not boot the expected version; backups preserved".to_string()),
        );
        Ok(Vec::new())
    }

    fn complete_journal_run(
        &self,
        journal: &ApplicationUpgradeJournal,
        journal_path: &Path,
    ) -> ApplicationUpgradeResult<()> {
        self.finish_journal_run(journal, ApplicationUpgradeRunStatus::Completed, None);
        // A replaced application bundle's backup is a directory beside the
        // installed bundle rather than a file under the profile directory, so it
        // goes through the guarded helper that refuses any name the upgrade does
        // not own.
        if journal.macos_bundle_upgrade {
            remove_upgrade_owned_directory(&journal.backup_path)?;
            for backup_path in &journal.backup_paths {
                remove_upgrade_owned_directory(backup_path)?;
            }
            if let Some(staging_dir) = journal.staging_dir.as_deref() {
                remove_upgrade_owned_directory(staging_dir)?;
            }
        } else {
            remove_file_if_exists(&journal.backup_path)?;
            for backup_path in &journal.backup_paths {
                remove_file_if_exists(backup_path)?;
            }
        }
        remove_file_if_exists(journal_path)?;
        remove_dir_if_exists(&self.staging_dir())?;
        remove_dir_if_exists(&self.helper_dir())?;
        Ok(())
    }

    /// Write the journal's outcome onto the persisted run.
    ///
    /// A run that already reached a terminal status was finalized by the
    /// pipeline itself; re-finalizing would rewrite its outcome.
    fn finish_journal_run(
        &self,
        journal: &ApplicationUpgradeJournal,
        status: ApplicationUpgradeRunStatus,
        error: Option<String>,
    ) {
        let mut run = self
            .inner
            .state
            .borrow()
            .clone()
            .filter(|run| run.run_id == journal.run_id)
            .unwrap_or_else(|| ApplicationUpgradeRun::from_journal(journal));
        if run.status.is_terminal() {
            tracing::info!(
                run_id = %run.run_id,
                status = run.status.as_str(),
                "skipping journal finalization for an already finished application upgrade"
            );
            return;
        }
        run.status = status;
        run.phase = journal.phase.clone();
        run.error = error;
        run.completed_at_epoch_ms = Some(epoch_ms_now());
        self.publish(run);
    }

    /// Put a run that survived the restart back into the published state.
    fn rehydrate_running_run(&self, journal: &ApplicationUpgradeJournal) {
        if self
            .inner
            .state
            .borrow()
            .as_ref()
            .is_some_and(|run| run.run_id == journal.run_id && !run.status.is_terminal())
        {
            return;
        }
        self.publish(ApplicationUpgradeRun::from_journal(journal));
    }
}

/// Which layouts this build replaces in place.
fn eligible_installation_kind(kind: InstallationKind) -> bool {
    matches!(
        kind,
        InstallationKind::Portable | InstallationKind::DirectMsi | InstallationKind::MacosAppBundle
    )
}

/// Reports download progress into the published run.
struct RunDownloadProgress<'a> {
    service: &'a ApplicationUpgradeService,
    run: &'a ApplicationUpgradeRun,
}

impl DownloadProgress for RunDownloadProgress<'_> {
    fn report(&mut self, downloaded_bytes: u64, total_bytes: u64) -> ProgressFuture<'_> {
        Box::pin(async move {
            self.service
                .report_download_progress(self.run, downloaded_bytes, total_bytes);
            Ok(())
        })
    }
}

fn load_persisted_run(db: &Database) -> Option<ApplicationUpgradeRun> {
    match db.get_setting(APPLICATION_UPGRADE_RUN_SETTING_KEY) {
        Ok(Some(raw)) => match serde_json::from_str(&raw) {
            Ok(run) => Some(run),
            Err(error) => {
                warn!(error = %error, "ignoring unreadable persisted application upgrade run");
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            warn!(error = %error, "failed to read the persisted application upgrade run");
            None
        }
    }
}

fn epoch_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// ---------------------------------------------------------------------------
// Adapters onto the shared application-upgrade core.
//
// Each of these binds one shared operation to Weaver's product identity and
// maps the shared error onto `ApplicationUpgradeError`. The messages, checks and
// ordering are the shared core's.
// ---------------------------------------------------------------------------

fn application_upgrade_http_client() -> ApplicationUpgradeResult<reqwest::Client> {
    application_updater::pipeline::application_upgrade_http_client(&WEAVER_PRODUCT)
        .map_err(map_updater_error)
}

/// Fetch, verify and validate this release's manifest, preferring v2.
///
/// The generation choice, the 404-only fallback and the refusal to downgrade on
/// any other v2 failure all live in the shared core: none of it is
/// Weaver-specific.
async fn fetch_upgrade_manifest(
    client: &reqwest::Client,
    release_tag: &str,
) -> ApplicationUpgradeResult<FetchedUpgradeManifest> {
    application_updater::pipeline::fetch_upgrade_manifest(&WEAVER_PRODUCT, client, release_tag)
        .await
        .map_err(map_updater_error)
}

fn select_artifact(
    manifest: &UpgradeManifest,
    installation_kind: InstallationKind,
) -> ApplicationUpgradeResult<&UpgradeArtifact> {
    application_updater::pipeline::select_artifact(manifest, installation_kind)
        .map_err(map_updater_error)
}

async fn download_artifact(
    client: &reqwest::Client,
    artifact: &UpgradeArtifact,
    artifact_url_override: Option<&str>,
    destination: &Path,
    progress: &mut dyn DownloadProgress,
) -> ApplicationUpgradeResult<()> {
    application_updater::pipeline::download_artifact(
        client,
        artifact,
        artifact_url_override,
        destination,
        progress,
    )
    .await
    .map_err(map_updater_error)
}

fn verify_artifact_hash(path: &Path, artifact: &UpgradeArtifact) -> ApplicationUpgradeResult<()> {
    application_updater::pipeline::verify_artifact_hash(path, artifact).map_err(map_updater_error)
}

fn validate_archive_members(
    path: &Path,
    artifact: &UpgradeArtifact,
) -> ApplicationUpgradeResult<()> {
    application_updater::pipeline::validate_archive_members(path, artifact)
        .map_err(map_updater_error)
}

fn extract_archive(
    path: &Path,
    artifact: &UpgradeArtifact,
    destination: &Path,
) -> ApplicationUpgradeResult<()> {
    application_updater::pipeline::extract_archive(path, artifact, destination)
        .map_err(map_updater_error)
}

#[cfg_attr(windows, allow(dead_code))]
fn portable_upgrade_paths(
    request: &UpgradeJobRequest,
    current_version: &str,
) -> ApplicationUpgradeResult<PortableUpgradePaths> {
    application_updater::pipeline::portable_upgrade_paths(
        request.installation_kind,
        request.executable_path.as_deref(),
        current_version,
    )
    .map_err(map_updater_error)
}

#[cfg_attr(windows, allow(dead_code))]
fn apply_portable_upgrade(
    extracted_dir: &Path,
    artifact: &UpgradeArtifact,
    paths: &PortableUpgradePaths,
    expected_version: &str,
    ensure_available_space: UpgradeSpaceCheck,
    rename: UpgradeRename,
) -> Result<(), PortablePromotionFailure> {
    application_updater::pipeline::apply_portable_upgrade(
        &WEAVER_PRODUCT,
        extracted_dir,
        artifact,
        paths,
        expected_version,
        |path, required_bytes| {
            ensure_available_space(path, required_bytes).map_err(to_updater_error)
        },
        rename,
    )
}

#[cfg(not(windows))]
fn roll_back_portable_promotion(
    paths: &PortableUpgradePaths,
    journal_path: &Path,
    rename: UpgradeRename,
    error: ApplicationUpgradeError,
) -> ApplicationUpgradeError {
    map_updater_error(application_updater::pipeline::roll_back_portable_promotion(
        paths,
        journal_path,
        rename,
        to_updater_error(error),
    ))
}

/// Remove a directory this upgrade owns, by the shared crate's name guard.
fn remove_upgrade_owned_directory(path: &Path) -> ApplicationUpgradeResult<()> {
    application_updater::macos_bundle::remove_upgrade_owned_directory(&WEAVER_PRODUCT, path)
        .map_err(map_updater_error)
}

#[cfg_attr(windows, allow(dead_code))]
fn macos_bundle_upgrade_paths(
    installation_kind: InstallationKind,
    executable_path: Option<&Path>,
    current_version: &str,
    expected_version: &str,
) -> ApplicationUpgradeResult<application_updater::macos_bundle::MacosBundleUpgradePaths> {
    application_updater::macos_bundle::macos_bundle_upgrade_paths(
        &WEAVER_PRODUCT,
        installation_kind,
        executable_path,
        current_version,
        expected_version,
    )
    .map_err(map_updater_error)
}

#[cfg(not(windows))]
fn apply_macos_bundle_upgrade(
    paths: &application_updater::macos_bundle::MacosBundleUpgradePaths,
    verify_signature: application_updater::macos_bundle::BundleSignatureCheck,
    rename: UpgradeRename,
) -> Result<(), application_updater::macos_bundle::BundlePromotionFailure> {
    application_updater::macos_bundle::apply_macos_bundle_upgrade(paths, verify_signature, rename)
}

#[cfg(not(windows))]
fn roll_back_macos_bundle_promotion(
    paths: &application_updater::macos_bundle::MacosBundleUpgradePaths,
    journal_path: &Path,
    rename: UpgradeRename,
    error: ApplicationUpgradeError,
) -> ApplicationUpgradeError {
    map_updater_error(
        application_updater::macos_bundle::roll_back_macos_bundle_promotion(
            paths,
            journal_path,
            rename,
            to_updater_error(error),
        ),
    )
}

#[cfg(windows)]
fn build_windows_upgrade_handoff(
    input: application_updater::windows_handoff::WindowsUpgradeHandoffInput<'_>,
) -> ApplicationUpgradeResult<application_updater::windows_handoff::WindowsUpgradeHandoff> {
    application_updater::windows_handoff::build_windows_upgrade_handoff(&WEAVER_PRODUCT, input)
        .map_err(map_updater_error)
}

#[cfg(windows)]
fn write_helper_plan(
    path: &Path,
    plan: &application_updater::helper_plan::ApplicationUpgradeHelperPlan,
) -> ApplicationUpgradeResult<()> {
    application_updater::windows_handoff::write_helper_plan(path, plan).map_err(map_updater_error)
}

#[cfg(windows)]
fn copy_and_spawn_windows_upgrade_helper(
    helper_path: &Path,
    plan_path: &Path,
) -> ApplicationUpgradeResult<()> {
    application_updater::windows_handoff::copy_and_spawn_windows_upgrade_helper(
        helper_path,
        plan_path,
    )
    .map_err(map_updater_error)
}

fn reboot_required_completion_allowed(
    written_at: Option<DateTime<Utc>>,
    boot_time: Option<DateTime<Utc>>,
    expected_version_booted: bool,
    expected_executable_booted: bool,
) -> bool {
    application_updater::helper_plan::reboot_required_completion_allowed(
        written_at,
        boot_time,
        expected_version_booted,
        expected_executable_booted,
    )
}

fn recreate_staging_dir(path: &Path) -> ApplicationUpgradeResult<()> {
    application_updater::pipeline::recreate_staging_dir(path).map_err(map_updater_error)
}

fn staging_space_requirement(artifact: &UpgradeArtifact) -> u64 {
    application_updater::pipeline::staging_space_requirement(artifact)
}

/// The free-space admission check, over Weaver's own disk probe.
fn ensure_available_space(path: &Path, required_bytes: u64) -> ApplicationUpgradeResult<()> {
    let space = crate::operations::disk::probe_nearest_disk_space(path).map_err(|error| {
        ApplicationUpgradeError::Repository(format!(
            "failed to inspect upgrade filesystem space: {error}"
        ))
    })?;
    if space.available_bytes < required_bytes {
        return Err(ApplicationUpgradeError::Validation(format!(
            "insufficient free space for application upgrade: need {required_bytes} bytes, have {} bytes",
            space.available_bytes
        )));
    }
    Ok(())
}

fn write_journal(path: &Path, journal: &ApplicationUpgradeJournal) -> ApplicationUpgradeResult<()> {
    application_updater::journal::write_journal(path, journal).map_err(map_updater_error)
}

fn load_journal(path: &Path) -> ApplicationUpgradeResult<Option<ApplicationUpgradeJournal>> {
    application_updater::journal::load_journal(path).map_err(map_updater_error)
}

/// Persist a terminal status observed by the temporary upgrade helper.
pub fn application_upgrade_helper_update_journal(
    path: &Path,
    phase: &str,
    helper_error: Option<String>,
) -> ApplicationUpgradeResult<()> {
    application_updater::journal::application_upgrade_helper_update_journal(
        path,
        phase,
        helper_error,
    )
    .map_err(map_updater_error)
}

fn remove_file_if_exists(path: &Path) -> ApplicationUpgradeResult<()> {
    application_updater::journal::remove_file_if_exists(path).map_err(map_updater_error)
}

fn remove_dir_if_exists(path: &Path) -> ApplicationUpgradeResult<()> {
    application_updater::journal::remove_dir_if_exists(path).map_err(map_updater_error)
}

/// Resolve a path through symlinks, falling back to the path as given.
fn canonical_path(path: &Path) -> PathBuf {
    application_updater::pipeline::canonical_path(path)
}

/// [`fetch_upgrade_manifest`] against a local server with signature
/// verification stubbed, so the fetch order can be exercised without a genuine
/// signed release for every case.
#[cfg(test)]
async fn fetch_upgrade_manifest_with_overrides(
    client: &reqwest::Client,
    release_tag: &str,
    overrides: application_updater::pipeline::UpgradeManifestFetchOverrides<'_>,
) -> ApplicationUpgradeResult<FetchedUpgradeManifest> {
    application_updater::pipeline::fetch_upgrade_manifest_with_overrides(
        &WEAVER_PRODUCT,
        client,
        release_tag,
        overrides,
    )
    .await
    .map_err(map_updater_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::update_check::{FetchOutcome, ReleaseFetcher, ReleaseInfo};
    use application_updater::manifest::{
        UpgradeArchitecture, UpgradeArchive, UpgradeArtifactMember, UpgradeChannel, UpgradePlatform,
    };
    use application_updater::pipeline::{UpgradeManifestFetchOverrides, UpgradeManifestGeneration};
    use std::fs;
    use std::future::Future;
    use std::pin::Pin;
    #[cfg(unix)]
    use std::time::Duration;

    const TEST_TAG: &str = "weaver-v99.0.0";
    const TEST_VERSION: &str = "99.0.0";

    // -- fixtures -----------------------------------------------------------

    /// A release checker that has already found `version`, without a network.
    struct StubReleaseFetcher {
        version: Option<String>,
    }

    impl ReleaseFetcher for StubReleaseFetcher {
        fn fetch_latest<'a>(
            &'a self,
            _etag: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<FetchOutcome, String>> + Send + 'a>> {
            Box::pin(async move {
                Ok(FetchOutcome::Fetched {
                    release: self.version.as_ref().map(|version| ReleaseInfo {
                        version: version.clone(),
                        url: None,
                        published_at_epoch_ms: None,
                    }),
                    etag: None,
                })
            })
        }
    }

    fn portable_assessment() -> InstallationAssessment {
        InstallationAssessment {
            kind: InstallationKind::Portable,
            owner: ManagementOwner::InApp,
            eligible: true,
            reason: EligibilityReason::Eligible,
            tray_supervised: false,
        }
    }

    async fn service_with_update(
        profile_dir: &Path,
        latest_version: Option<&str>,
        assessment: InstallationAssessment,
        executable_path: Option<PathBuf>,
    ) -> ApplicationUpgradeService {
        let db = Database::open_in_memory().expect("in-memory database");
        let update_check = UpdateCheckService::with_fetcher_enabled(
            db.clone(),
            Arc::new(StubReleaseFetcher {
                version: latest_version.map(str::to_string),
            }),
            true,
        );
        let _ = update_check.run_check().await;
        ApplicationUpgradeService::new(db, update_check, profile_dir, assessment, executable_path)
    }

    fn test_request(executable_path: PathBuf) -> UpgradeJobRequest {
        UpgradeJobRequest {
            expected_tag: TEST_TAG.to_string(),
            expected_version: TEST_VERSION.to_string(),
            installation_kind: InstallationKind::Portable,
            executable_path: Some(executable_path),
            tray_supervised: false,
        }
    }

    fn runtime_platform() -> UpgradePlatform {
        match std::env::consts::OS {
            "linux" => UpgradePlatform::Linux,
            "macos" => UpgradePlatform::Darwin,
            "windows" => UpgradePlatform::Windows,
            os => panic!("unsupported upgrade test platform {os}"),
        }
    }

    fn runtime_architecture() -> UpgradeArchitecture {
        match std::env::consts::ARCH {
            "x86_64" => UpgradeArchitecture::X86_64,
            "aarch64" => UpgradeArchitecture::Arm64,
            arch => panic!("unsupported upgrade test architecture {arch}"),
        }
    }

    #[cfg(unix)]
    fn tar_gz(members: &[(&str, &[u8], u32)]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);
        for (path, bytes, mode) in members {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).expect("set archive path");
            header.set_size(bytes.len() as u64);
            header.set_mode(*mode);
            header.set_cksum();
            archive
                .append(&header, *bytes)
                .expect("append archive member");
        }
        archive
            .into_inner()
            .expect("finish archive")
            .finish()
            .expect("finish gzip")
    }

    fn portable_manifest(bytes: &[u8], members: Vec<UpgradeArtifactMember>) -> UpgradeManifest {
        UpgradeManifest {
            schema: super::super::UPGRADE_MANIFEST_SCHEMA_VERSION.to_string(),
            tag: TEST_TAG.to_string(),
            version: TEST_VERSION.to_string(),
            artifacts: vec![UpgradeArtifact {
                platform: runtime_platform(),
                arch: runtime_architecture(),
                channel: UpgradeChannel::Portable,
                asset_name: "weaver-portable.tar.gz".to_string(),
                url: format!(
                    "https://github.com/scryer-media/weaver/releases/download/{TEST_TAG}/weaver-portable.tar.gz"
                ),
                size: bytes.len() as u64,
                blake3: blake3::hash(bytes).to_hex().to_string(),
                archive: UpgradeArchive::TarGz,
                members,
            }],
        }
    }

    /// A one-route local server, so the download path is exercised over real
    /// HTTP without reaching the network.
    async fn local_server(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local port");
        let address = listener.local_addr().expect("local address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{address}")
    }

    /// The upgrade client is `https_only`, which a local test server is not, so
    /// the tests drive the shared pipeline with a plain client — exactly the
    /// client the shipped code would build minus the TLS requirement.
    fn test_http_client() -> reqwest::Client {
        reqwest::Client::new()
    }

    // -- admission ----------------------------------------------------------

    #[tokio::test]
    async fn an_empty_request_field_is_refused_before_anything_is_fetched() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        for (tag, version, expected) in [
            ("  ", TEST_VERSION, "expectedTag must not be empty"),
            (TEST_TAG, " ", "expectedVersion must not be empty"),
        ] {
            let error = service
                .start(ApplicationUpgradeStartRequest {
                    expected_tag: tag.to_string(),
                    expected_version: version.to_string(),
                })
                .await
                .expect_err("an empty field is refused");
            assert_eq!(error.to_string(), expected);
        }
    }

    /// The release checker is the only source of what may be installed: a tag or
    /// version the operator asks for that it has not published is refused, so a
    /// crafted mutation cannot point the upgrade at an arbitrary release.
    #[tokio::test]
    async fn only_the_release_the_checker_found_may_be_installed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;

        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: TEST_TAG.to_string(),
                expected_version: "98.0.0".to_string(),
            })
            .await
            .expect_err("a version the checker did not find is refused");
        assert_eq!(
            error.to_string(),
            "expectedVersion does not match the available release"
        );

        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: "v99.0.0".to_string(),
                expected_version: TEST_VERSION.to_string(),
            })
            .await
            .expect_err("a tag that is not this version's release tag is refused");
        assert_eq!(
            error.to_string(),
            "expectedTag does not match the available release"
        );
    }

    #[tokio::test]
    async fn an_upgrade_is_refused_when_nothing_newer_was_found() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service = service_with_update(temp.path(), None, portable_assessment(), None).await;
        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: TEST_TAG.to_string(),
                expected_version: TEST_VERSION.to_string(),
            })
            .await
            .expect_err("nothing to install");
        assert_eq!(error.to_string(), "no application update is available");
    }

    /// An installation the classifier says is managed by someone else — Docker,
    /// Homebrew, winget, a Windows service — never replaces its own files.
    #[tokio::test]
    async fn an_ineligible_installation_never_upgrades_itself() {
        let temp = tempfile::tempdir().expect("tempdir");
        let assessment = InstallationAssessment {
            kind: InstallationKind::Docker,
            owner: ManagementOwner::Operator,
            eligible: false,
            reason: EligibilityReason::ManagedByDocker,
            tray_supervised: false,
        };
        let service = service_with_update(temp.path(), Some(TEST_VERSION), assessment, None).await;
        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: TEST_TAG.to_string(),
                expected_version: TEST_VERSION.to_string(),
            })
            .await
            .expect_err("a managed installation is refused");
        assert!(
            error.to_string().contains("managed_by_docker"),
            "{error} names the reason"
        );
    }

    /// Eligible by the classifier but a layout this build does not replace in
    /// place: the kind check is separate so a future eligible-but-unsupported
    /// layout cannot slip through.
    #[tokio::test]
    async fn an_eligible_but_unsupported_layout_is_still_refused() {
        let temp = tempfile::tempdir().expect("tempdir");
        let assessment = InstallationAssessment {
            kind: InstallationKind::Unsupported,
            owner: ManagementOwner::InApp,
            eligible: true,
            reason: EligibilityReason::Eligible,
            tray_supervised: false,
        };
        let service = service_with_update(temp.path(), Some(TEST_VERSION), assessment, None).await;
        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: TEST_TAG.to_string(),
                expected_version: TEST_VERSION.to_string(),
            })
            .await
            .expect_err("an unsupported layout is refused");
        assert_eq!(
            error.to_string(),
            "application upgrade installation is not eligible"
        );
    }

    /// The published snapshot carries the tag the mutation must echo back, so the
    /// UI never has to build a release tag itself.
    #[tokio::test]
    async fn the_snapshot_names_the_release_tag_for_the_available_version() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let snapshot = service.snapshot();
        assert_eq!(snapshot.update_version.as_deref(), Some(TEST_VERSION));
        assert_eq!(snapshot.update_tag.as_deref(), Some(TEST_TAG));
        assert!(snapshot.update_available);
        assert!(snapshot.eligible);
        assert_eq!(snapshot.installation_kind, InstallationKind::Portable);
        assert!(snapshot.active_run.is_none());
        assert!(snapshot.latest_run.is_none());
    }

    // -- the portable pipeline ----------------------------------------------

    #[cfg(unix)]
    #[tokio::test]
    async fn the_portable_pipeline_replaces_the_executable_and_asks_for_a_restart() {
        let temp = tempfile::tempdir().expect("tempdir");
        let executable_path = temp.path().join("bin/weaver");
        fs::create_dir_all(executable_path.parent().expect("executable parent"))
            .expect("create executable directory");
        fs::write(&executable_path, b"old executable").expect("write old executable");
        let new_binary = b"new executable";
        let archive = tar_gz(&[("weaver", new_binary, 0o755)]);
        let manifest = portable_manifest(
            &archive,
            vec![UpgradeArtifactMember {
                path: "weaver".to_string(),
                size: new_binary.len() as u64,
                executable: true,
            }],
        );
        let served = archive.clone();
        let base = local_server(axum::Router::new().route(
            "/artifact",
            axum::routing::get(move || {
                let served = served.clone();
                async move { served }
            }),
        ))
        .await;

        let profile_dir = temp.path().join("profile");
        let service = service_with_update(
            &profile_dir,
            Some(TEST_VERSION),
            portable_assessment(),
            Some(executable_path.clone()),
        )
        .await;
        let restart = RestartController::new();
        service.set_restart_controller(restart.clone()).await;

        let request = test_request(executable_path.clone());
        let run = ApplicationUpgradeRun::checking("test-run".to_string(), &request);
        let client = test_http_client();
        service
            .run_pipeline_with_dependencies(
                &run,
                &request,
                &manifest,
                UpgradePipelineDependencies {
                    client: &client,
                    artifact_url_override: Some(&format!("{base}/artifact")),
                    ensure_available_space,
                    rename: rename_path,
                },
            )
            .await
            .expect("the portable pipeline succeeds");

        assert_eq!(
            fs::read(&executable_path).expect("replacement executable"),
            new_binary
        );
        let backup_path = PathBuf::from(format!(
            "{}.pre-upgrade-{WEAVER_VERSION}",
            executable_path.display()
        ));
        assert_eq!(
            fs::read(&backup_path).expect("backup executable"),
            b"old executable"
        );
        // The journal is what the next boot recovers from, so it must name the
        // promoted executable and its backup.
        let journal = load_journal(&service.journal_path())
            .expect("read journal")
            .expect("the promotion wrote a journal");
        assert_eq!(journal.schema, JOURNAL_SCHEMA);
        assert_eq!(journal.phase, phases::RESTARTING);
        assert_eq!(journal.expected_version, TEST_VERSION);
        assert_eq!(journal.executable_path, executable_path);
        assert_eq!(journal.backup_paths, vec![backup_path]);
        assert!(!journal.macos_bundle_upgrade);
        // A portable promotion re-execs in place; it is not a bundle relaunch.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), restart.requested())
                .await
                .expect("the pipeline asked the serve loop to restart"),
            crate::runtime::restart::RestartAction::Restart
        );
    }

    /// Trust-root priming talks to the Sigstore TUF repository, so it may never
    /// finish on an offline host. The upgrade must not wait for it: verification
    /// falls back to the trust snapshot the build embeds.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_pipeline_does_not_wait_for_sigstore_trust_root_priming() {
        let priming = crate::application_upgrade::trust::spawn_trust_root_priming_for_test(|| {
            std::future::pending::<Result<(), String>>()
        });

        let temp = tempfile::tempdir().expect("tempdir");
        let executable_path = temp.path().join("bin/weaver");
        fs::create_dir_all(executable_path.parent().expect("executable parent"))
            .expect("create executable directory");
        fs::write(&executable_path, b"old executable").expect("write old executable");
        let new_binary = b"new executable";
        let archive = tar_gz(&[("weaver", new_binary, 0o755)]);
        let manifest = portable_manifest(
            &archive,
            vec![UpgradeArtifactMember {
                path: "weaver".to_string(),
                size: new_binary.len() as u64,
                executable: true,
            }],
        );
        let served = archive.clone();
        let base = local_server(axum::Router::new().route(
            "/artifact",
            axum::routing::get(move || {
                let served = served.clone();
                async move { served }
            }),
        ))
        .await;

        let profile_dir = temp.path().join("profile");
        let service = service_with_update(
            &profile_dir,
            Some(TEST_VERSION),
            portable_assessment(),
            Some(executable_path.clone()),
        )
        .await;
        service
            .set_restart_controller(RestartController::new())
            .await;

        let request = test_request(executable_path.clone());
        let run = ApplicationUpgradeRun::checking("test-run".to_string(), &request);
        let client = test_http_client();
        service
            .run_pipeline_with_dependencies(
                &run,
                &request,
                &manifest,
                UpgradePipelineDependencies {
                    client: &client,
                    artifact_url_override: Some(&format!("{base}/artifact")),
                    ensure_available_space,
                    rename: rename_path,
                },
            )
            .await
            .expect("the pipeline finishes while priming is still pending");

        assert_eq!(
            fs::read(&executable_path).expect("replacement executable"),
            new_binary
        );
        assert!(
            !priming.is_finished(),
            "the priming task must still be pending, proving nothing awaited it"
        );
        priming.abort();
    }

    /// A pipeline whose manifest disagrees with the admitted request stops before
    /// anything is downloaded: the signed manifest and the request must name the
    /// same release.
    #[tokio::test]
    async fn a_manifest_that_disagrees_with_the_request_is_refused() {
        let temp = tempfile::tempdir().expect("tempdir");
        let executable_path = temp.path().join("weaver");
        fs::write(&executable_path, b"old").expect("write executable");
        let service = service_with_update(
            temp.path(),
            Some(TEST_VERSION),
            portable_assessment(),
            Some(executable_path.clone()),
        )
        .await;
        let request = test_request(executable_path);
        let run = ApplicationUpgradeRun::checking("test-run".to_string(), &request);
        let client = test_http_client();

        let mut manifest = portable_manifest(b"payload", Vec::new());
        manifest.tag = "weaver-v0.0.1".to_string();
        let error = service
            .run_pipeline_with_dependencies(
                &run,
                &request,
                &manifest,
                UpgradePipelineDependencies {
                    client: &client,
                    artifact_url_override: None,
                    ensure_available_space,
                    rename: rename_path,
                },
            )
            .await
            .expect_err("a mismatched tag is refused");
        assert_eq!(
            error.to_string(),
            "upgrade manifest tag does not match expectedTag"
        );

        let mut manifest = portable_manifest(b"payload", Vec::new());
        manifest.version = "1.2.3".to_string();
        let error = service
            .run_pipeline_with_dependencies(
                &run,
                &request,
                &manifest,
                UpgradePipelineDependencies {
                    client: &client,
                    artifact_url_override: None,
                    ensure_available_space,
                    rename: rename_path,
                },
            )
            .await
            .expect_err("a mismatched version is refused");
        assert_eq!(
            error.to_string(),
            "upgrade manifest version does not match expectedVersion"
        );
    }

    // -- manifest fetch order -----------------------------------------------

    /// v2 is preferred, and v1 is read only when the v2 asset is *absent*.
    #[tokio::test]
    async fn the_v2_manifest_is_preferred_over_v1() {
        let temp = tempfile::tempdir().expect("tempdir");
        let _ = temp;
        let v1 = manifest_document(super::super::UPGRADE_MANIFEST_SCHEMA_VERSION, "portable");
        let v2 = manifest_document(super::super::UPGRADE_MANIFEST_V2_SCHEMA_VERSION, "portable");
        let base = manifest_server(Some(v2), Some(v1)).await;
        let fetched = fetch_upgrade_manifest_with_overrides(
            &test_http_client(),
            TEST_TAG,
            UpgradeManifestFetchOverrides {
                asset_base: Some(&base),
                verify_signature: Some(accept_any_signature),
            },
        )
        .await
        .expect("the v2 manifest is read");
        assert_eq!(fetched.generation, UpgradeManifestGeneration::V2);
    }

    #[tokio::test]
    async fn v1_is_read_only_when_the_v2_asset_is_absent() {
        let v1 = manifest_document(super::super::UPGRADE_MANIFEST_SCHEMA_VERSION, "portable");
        let base = manifest_server(None, Some(v1)).await;
        let fetched = fetch_upgrade_manifest_with_overrides(
            &test_http_client(),
            TEST_TAG,
            UpgradeManifestFetchOverrides {
                asset_base: Some(&base),
                verify_signature: Some(accept_any_signature),
            },
        )
        .await
        .expect("the v1 manifest is read");
        assert_eq!(fetched.generation, UpgradeManifestGeneration::V1);
    }

    /// A v2 manifest that exists but does not validate is fatal. Falling back to
    /// v1 on anything but a missing asset would hand an attacker a downgrade
    /// oracle: break v2 and the client reads the older document instead.
    #[tokio::test]
    async fn a_broken_v2_manifest_never_falls_back_to_v1() {
        let v1 = manifest_document(super::super::UPGRADE_MANIFEST_SCHEMA_VERSION, "portable");
        let base = manifest_server(Some("{ not a manifest".to_string()), Some(v1)).await;
        let fetched = fetch_upgrade_manifest_with_overrides(
            &test_http_client(),
            TEST_TAG,
            UpgradeManifestFetchOverrides {
                asset_base: Some(&base),
                verify_signature: Some(accept_any_signature),
            },
        )
        .await;
        assert!(
            fetched.is_err(),
            "a broken v2 manifest is fatal rather than a fallback to v1"
        );
    }

    fn accept_any_signature(
        _manifest: &[u8],
        _bundle: &[u8],
        _tag: &str,
    ) -> Result<(), application_updater::Error> {
        Ok(())
    }

    /// A minimal, valid manifest document for `schema`.
    fn manifest_document(schema: &str, channel: &str) -> String {
        let asset = "weaver-linux-x86_64-portable.tar.gz";
        serde_json::json!({
            "schema": schema,
            "tag": TEST_TAG,
            "version": TEST_VERSION,
            "artifacts": [{
                "platform": "linux",
                "arch": "x86_64",
                "channel": channel,
                "asset_name": asset,
                "url": format!(
                    "https://github.com/scryer-media/weaver/releases/download/{TEST_TAG}/{asset}"
                ),
                "size": 12,
                "blake3": "0".repeat(64),
                "archive": "tar.gz",
                "members": [{ "path": "weaver", "size": 12, "executable": true }],
            }],
        })
        .to_string()
    }

    /// A release-asset server that serves whichever manifests are present and
    /// answers 404 for the rest — which is exactly what a release published
    /// before v2 existed looks like.
    async fn manifest_server(v2: Option<String>, v1: Option<String>) -> url::Url {
        let mut router = axum::Router::new();
        for (asset, body) in [
            (WEAVER_PRODUCT.manifest_v2_asset_name, v2.clone()),
            (
                WEAVER_PRODUCT.manifest_v2_signature_asset_name,
                v2.map(|_| "bundle".to_string()),
            ),
            (WEAVER_PRODUCT.manifest_asset_name, v1.clone()),
            (
                WEAVER_PRODUCT.manifest_signature_asset_name,
                v1.map(|_| "bundle".to_string()),
            ),
        ] {
            if let Some(body) = body {
                router = router.route(
                    &format!("/download/{TEST_TAG}/{asset}"),
                    axum::routing::get(move || {
                        let body = body.clone();
                        async move { body }
                    }),
                );
            }
        }
        let base = local_server(router).await;
        url::Url::parse(&format!("{base}/download/")).expect("a parseable asset base")
    }

    // -- journal recovery ---------------------------------------------------

    fn journal_for(
        run_id: &str,
        expected_version: &str,
        executable_path: PathBuf,
        backup_path: PathBuf,
        phase: &str,
    ) -> ApplicationUpgradeJournal {
        ApplicationUpgradeJournal {
            schema: JOURNAL_SCHEMA.to_string(),
            run_id: run_id.to_string(),
            expected_version: expected_version.to_string(),
            expected_tag: format!("weaver-v{expected_version}"),
            executable_path,
            backup_path: backup_path.clone(),
            backup_paths: vec![backup_path],
            phase: phase.to_string(),
            helper_error: None,
            written_at: Some(Utc::now()),
            macos_bundle_upgrade: false,
            staging_dir: None,
        }
    }

    /// The boot that follows a promotion is what decides the upgrade took: the
    /// running version and the running program file must both be the ones the
    /// journal named. When they are, the backup and the journal go away.
    #[tokio::test]
    async fn a_boot_of_the_expected_build_completes_the_run_and_drops_the_backup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let executable = std::env::current_exe().expect("current executable");
        let backup_path = temp.path().join("weaver.pre-upgrade-old");
        fs::write(&backup_path, b"previous build").expect("write the backup");
        let journal = journal_for(
            "recovered-run",
            WEAVER_VERSION,
            executable,
            backup_path.clone(),
            phases::RESTARTING,
        );
        write_journal(&service.journal_path(), &journal).expect("write journal");

        let awaiting = service.finalize_journal().expect("finalize the journal");
        assert!(awaiting.is_empty(), "nothing is waiting on a reboot");
        let run = service
            .snapshot()
            .latest_run
            .expect("the recovered run is published");
        assert_eq!(run.run_id, "recovered-run");
        assert_eq!(run.status, ApplicationUpgradeRunStatus::Completed);
        assert_eq!(run.error, None);
        assert!(
            !backup_path.exists(),
            "a completed upgrade drops its backup"
        );
        assert!(
            !service.journal_path().exists(),
            "a completed upgrade drops its journal"
        );
    }

    /// A boot that is not the expected build fails the run and keeps the backup:
    /// the operator's way back must survive a failed upgrade.
    #[tokio::test]
    async fn a_boot_of_a_different_build_fails_the_run_and_keeps_the_backup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let backup_path = temp.path().join("weaver.pre-upgrade-old");
        fs::write(&backup_path, b"previous build").expect("write the backup");
        let journal = journal_for(
            "recovered-run",
            "99.99.99",
            temp.path().join("some-other-weaver"),
            backup_path.clone(),
            phases::RESTARTING,
        );
        write_journal(&service.journal_path(), &journal).expect("write journal");

        assert!(service.finalize_journal().expect("finalize").is_empty());
        let run = service.snapshot().latest_run.expect("the run is published");
        assert_eq!(run.status, ApplicationUpgradeRunStatus::Failed);
        assert_eq!(
            run.error.as_deref(),
            Some("upgrade did not boot the expected version; backups preserved")
        );
        assert!(backup_path.exists(), "the way back is preserved");
    }

    /// A helper that recorded a failure fails the run on the next boot and clears
    /// the recovery state it left behind.
    #[tokio::test]
    async fn a_helper_failure_recorded_in_the_journal_fails_the_run() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let mut journal = journal_for(
            "helper-run",
            WEAVER_VERSION,
            std::env::current_exe().expect("current executable"),
            temp.path().join("weaver.pre-upgrade-old"),
            phases::RESTARTING,
        );
        journal.helper_error = Some("the installer refused to run".to_string());
        write_journal(&service.journal_path(), &journal).expect("write journal");

        assert!(service.finalize_journal().expect("finalize").is_empty());
        let run = service.snapshot().latest_run.expect("the run is published");
        assert_eq!(run.status, ApplicationUpgradeRunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("the installer refused to run"));
        assert!(!service.journal_path().exists());
    }

    /// A run waiting on an operating-system reboot stays running across the
    /// restart, and is republished so a second upgrade cannot start behind it.
    #[tokio::test]
    async fn a_run_awaiting_a_reboot_stays_running_and_is_republished() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let journal = journal_for(
            "reboot-run",
            "99.99.99",
            temp.path().join("some-other-weaver"),
            temp.path().join("weaver.pre-upgrade-old"),
            phases::REBOOT_REQUIRED,
        );
        write_journal(&service.journal_path(), &journal).expect("write journal");

        let awaiting = service.finalize_journal().expect("finalize");
        assert_eq!(awaiting, vec!["reboot-run".to_string()]);
        let run = service
            .snapshot()
            .active_run
            .expect("the run is still active");
        assert_eq!(run.run_id, "reboot-run");
        assert_eq!(run.status, ApplicationUpgradeRunStatus::Running);
        assert!(
            service.journal_path().exists(),
            "the journal survives until the reboot happens"
        );
    }

    /// A journal from a schema this build does not know is a refusal, not a
    /// silent skip: something else wrote it and this build must not act on it.
    #[tokio::test]
    async fn an_unknown_journal_schema_is_refused() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let mut journal = journal_for(
            "foreign-run",
            WEAVER_VERSION,
            std::env::current_exe().expect("current executable"),
            temp.path().join("weaver.pre-upgrade-old"),
            phases::RESTARTING,
        );
        journal.schema = "someone.else.journal.v1".to_string();
        write_journal(&service.journal_path(), &journal).expect("write journal");

        let error = service
            .finalize_journal()
            .expect_err("a foreign journal is refused");
        assert!(
            error.to_string().contains("someone.else.journal.v1"),
            "{error} names the schema"
        );
    }

    /// No journal is the ordinary case, and it must be silent.
    #[tokio::test]
    async fn a_boot_with_no_journal_finalizes_nothing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        assert!(service.finalize_journal().expect("finalize").is_empty());
        assert!(service.snapshot().latest_run.is_none());
    }

    /// A run that was still downloading when the process went away has no
    /// journal, because nothing was promoted. The next boot fails it outright:
    /// it must neither show as running forever nor block the next attempt.
    #[tokio::test]
    async fn a_running_run_with_no_journal_is_failed_on_the_next_boot() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let request = test_request(temp.path().join("weaver"));
        let mut run = ApplicationUpgradeRun::checking("interrupted-run".to_string(), &request);
        run.phase = phases::DOWNLOADING.to_string();
        service.publish(run);
        let staging = service.staging_dir();
        fs::create_dir_all(&staging).expect("staging dir");
        fs::write(staging.join("artifact"), b"partial").expect("partial download");

        assert!(service.finalize_journal().expect("finalize").is_empty());
        let snapshot = service.snapshot();
        let failed = snapshot.latest_run.expect("the run is still published");
        assert_eq!(failed.run_id, "interrupted-run");
        assert_eq!(failed.status, ApplicationUpgradeRunStatus::Failed);
        assert!(
            failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("interrupted by a restart")),
            "{:?} names the interruption",
            failed.error
        );
        assert!(snapshot.active_run.is_none(), "a failed run is not active");
        assert!(!staging.exists(), "the partial download is removed");
    }

    /// A run carried over from the previous process — rehydrated from a journal
    /// that is waiting on a reboot — holds no admission lock here, so the
    /// single-flight rule has to come from the published state.
    #[tokio::test]
    async fn a_second_upgrade_is_refused_while_a_carried_over_run_is_still_running() {
        let temp = tempfile::tempdir().expect("tempdir");
        let service =
            service_with_update(temp.path(), Some(TEST_VERSION), portable_assessment(), None).await;
        let request = test_request(temp.path().join("weaver"));
        let mut run = ApplicationUpgradeRun::checking("carried-over".to_string(), &request);
        run.phase = phases::REBOOT_REQUIRED.to_string();
        service.publish(run);

        let error = service
            .start(ApplicationUpgradeStartRequest {
                expected_tag: TEST_TAG.to_string(),
                expected_version: TEST_VERSION.to_string(),
            })
            .await
            .expect_err("a carried-over running run refuses a second start");
        assert_eq!(
            error.to_string(),
            "an application upgrade is already running (run carried-over, phase reboot_required)"
        );
    }

    /// The run survives a restart through the settings row, so the UI that
    /// triggered the upgrade still sees its outcome after the process it
    /// replaced has gone.
    #[tokio::test]
    async fn a_published_run_is_restored_from_settings_on_the_next_start() {
        let temp = tempfile::tempdir().expect("tempdir");
        let db = Database::open_in_memory().expect("in-memory database");
        let update_check = UpdateCheckService::with_fetcher_enabled(
            db.clone(),
            Arc::new(StubReleaseFetcher {
                version: Some(TEST_VERSION.to_string()),
            }),
            true,
        );
        let service = ApplicationUpgradeService::new(
            db.clone(),
            update_check.clone(),
            temp.path(),
            portable_assessment(),
            None,
        );
        let request = test_request(temp.path().join("weaver"));
        let mut run = ApplicationUpgradeRun::checking("persisted-run".to_string(), &request);
        run.status = ApplicationUpgradeRunStatus::Completed;
        service.publish(run.clone());

        let restarted = ApplicationUpgradeService::new(
            db,
            update_check,
            temp.path(),
            portable_assessment(),
            None,
        );
        assert_eq!(restarted.snapshot().latest_run, Some(run));
        assert!(
            restarted.snapshot().active_run.is_none(),
            "a finished run is not active"
        );
    }
}

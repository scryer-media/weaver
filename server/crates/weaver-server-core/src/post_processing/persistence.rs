use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::model::{
    ApprovedFilesystemRoots, ArtifactCondition, AttemptId, AttemptStatus, ExtensionAdapter,
    ExtensionId, ExtensionManifest, ExtensionRevisionId, ExtensionSelection, FrozenPlan,
    FrozenPlanProvenance, FrozenPlanStep, OnFailure, OptionName, OrderedStep, OutcomeImpact,
    PipelineOutcome, PostProcessingSettings, PostProcessingSummary, Profile, ProfileId,
    ResolvedOption, ResolvedOptionValue, RunId, RunStatus, RunWhen, SelectionMode,
    SubmissionPlanSelection, SubmissionPlanSelectionMode, TimeoutPolicy, TrustState,
};
use super::runner::ControlEffects;
use crate::persistence::encryption::{decrypt_value, encrypt_value};
use crate::persistence::sql_runtime::{SqlArg, SqlEngine, SqlRow, SqlRuntime};
use crate::persistence::{Database, StateError};

const GLOBAL_ASSIGNMENT_KEY: &str = "*";
const POST_PROCESSING_SETTINGS_KEY: &str = "post_processing.settings.v1";
pub const MAX_PERSISTED_ATTEMPT_OUTPUT_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_LOGICAL_LINE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalIntent {
    Complete,
    Fail,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostProcessingRunRecord {
    pub run_id: RunId,
    pub job_id: u64,
    pub status: RunStatus,
    pub pipeline_outcome: PipelineOutcome,
    pub summary: PostProcessingSummary,
    pub terminal_intent: TerminalIntent,
    pub plan: FrozenPlan,
    pub rerun_of_run_id: Option<RunId>,
    pub queued_at_epoch_ms: i64,
    pub queue_position: i64,
    pub started_at_epoch_ms: Option<i64>,
    pub finished_at_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostProcessingAttemptRecord {
    pub attempt_id: AttemptId,
    pub run_id: RunId,
    pub step_index: u32,
    pub status: AttemptStatus,
    pub extension_id: ExtensionId,
    pub revision_id: ExtensionRevisionId,
    pub adapter: ExtensionAdapter,
    pub working_directory: Option<String>,
    pub exit_code: Option<i32>,
    pub error_message: Option<String>,
    pub progress: Option<serde_json::Value>,
    pub output_truncated: bool,
    pub queued_at_epoch_ms: i64,
    pub started_at_epoch_ms: Option<i64>,
    pub finished_at_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostProcessingArtifactRecord {
    pub attempt_id: AttemptId,
    pub step_index: u32,
    pub path: PathBuf,
    pub exists: bool,
    pub is_file: bool,
    pub is_directory: bool,
    pub is_symlink: bool,
    pub size_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAttemptEffects {
    weaver_control_effects_v1: ControlEffects,
}

impl PostProcessingAttemptRecord {
    /// Recover every validated effect emitted by the attempt. Older rows stored only the
    /// progress payload, so retain that as a backwards-compatible fallback.
    pub fn control_effects(&self) -> ControlEffects {
        self.progress
            .clone()
            .and_then(|value| serde_json::from_value::<StoredAttemptEffects>(value).ok())
            .map(|stored| stored.weaver_control_effects_v1)
            .unwrap_or_else(|| ControlEffects {
                progress: self.progress.clone(),
                ..ControlEffects::default()
            })
    }

    pub fn reported_progress(&self) -> Option<serde_json::Value> {
        self.control_effects().progress
    }
}

pub(crate) fn encode_control_effects(
    effects: &ControlEffects,
) -> Result<serde_json::Value, StateError> {
    serde_json::to_value(StoredAttemptEffects {
        weaver_control_effects_v1: effects.clone(),
    })
    .map_err(state_err)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    System,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostProcessingLogChunk {
    pub sequence: u64,
    pub stream: LogStream,
    pub payload: Vec<u8>,
    pub created_at_epoch_ms: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PostProcessingLogPage {
    pub chunks: Vec<PostProcessingLogChunk>,
    pub next_cursor: Option<u64>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct PostProcessingMetricsSnapshot {
    pub queue_depth: u64,
    pub active_attempts: u64,
    pub duration_count: u64,
    pub duration_sum_millis: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub skipped: u64,
    pub timed_out: u64,
    pub cancelled: u64,
    pub interrupted: u64,
    pub truncated: u64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExtensionRevisionRecord {
    pub manifest: ExtensionManifest,
    pub trust_state: TrustState,
    pub managed_path: Option<String>,
    pub source_path: Option<String>,
    pub discovered_at_epoch_ms: i64,
    pub approved_at_epoch_ms: Option<i64>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ProfileRecord {
    pub profile: Profile,
    pub enabled: bool,
    pub created_at_epoch_ms: i64,
    pub updated_at_epoch_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPolicy {
    run_when: RunWhen,
    on_failure: OnFailure,
    outcome_impact: OutcomeImpact,
    timeout_policy: TimeoutPolicy,
    approved_roots: ApprovedFilesystemRoots,
    #[serde(default)]
    artifact_condition: ArtifactCondition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPlanStep {
    index: u32,
    revision: super::model::ExtensionRevision,
    policy: StoredPolicy,
    options: Vec<ResolvedOption>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredPlan {
    provenance: FrozenPlanProvenance,
    steps: Vec<StoredPlanStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSecretOption {
    step_index: u32,
    name: OptionName,
    ciphertext: String,
}

#[derive(Debug, Clone)]
struct StoredProfileStepRow {
    index: u32,
    selection_json: String,
    policy_json: String,
    options_json: String,
    secret_options_json: String,
}

fn resolve_manifest_options(
    manifest: &ExtensionManifest,
    supplied: &[ResolvedOption],
) -> Result<Vec<ResolvedOption>, StateError> {
    let mut declarations = HashMap::new();
    for declaration in manifest.options() {
        let key = declaration.name().as_str().to_ascii_lowercase();
        if declarations.insert(key, declaration).is_some() {
            return Err(StateError::Database(
                "extension manifest has ambiguous option names".into(),
            ));
        }
    }

    let mut supplied_by_name = HashMap::new();
    for option in supplied {
        let key = option.name().as_str().to_ascii_lowercase();
        if supplied_by_name.insert(key.clone(), option).is_some() {
            return Err(StateError::Database(
                "post-processing step has duplicate options".into(),
            ));
        }
        let declaration = declarations.get(&key).ok_or_else(|| {
            StateError::Database(format!(
                "post-processing step supplies undeclared option {}",
                option.name().as_str()
            ))
        })?;
        if !option.value().matches_type(declaration.option_type()) {
            return Err(StateError::Database(format!(
                "post-processing option {} has an invalid value",
                declaration.name().as_str()
            )));
        }
    }

    let mut resolved = Vec::with_capacity(manifest.options().len());
    for declaration in manifest.options() {
        let key = declaration.name().as_str().to_ascii_lowercase();
        let value = supplied_by_name
            .get(&key)
            .map(|option| option.value().clone())
            .or_else(|| declaration.default().cloned());
        let Some(value) = value else {
            if declaration.required() {
                return Err(StateError::Database(format!(
                    "required post-processing option {} is missing",
                    declaration.name().as_str()
                )));
            }
            continue;
        };
        resolved.push(ResolvedOption::new(declaration.name().clone(), value));
    }
    Ok(resolved)
}

fn validate_frozen_roots(
    roots: &ApprovedFilesystemRoots,
    settings: &PostProcessingSettings,
) -> Result<(), StateError> {
    let allowed = settings
        .allowed_roots
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if roots
        .as_slice()
        .iter()
        .all(|root| allowed.contains(root.as_str()))
    {
        Ok(())
    } else {
        Err(StateError::Database(
            "post-processing profile uses a filesystem root that is no longer approved".into(),
        ))
    }
}

impl Database {
    pub fn post_processing_settings(&self) -> Result<PostProcessingSettings, StateError> {
        self.get_setting(POST_PROCESSING_SETTINGS_KEY)?
            .map(|raw| from_json(&raw))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub fn save_post_processing_settings(
        &self,
        settings: &PostProcessingSettings,
    ) -> Result<(), StateError> {
        settings.validate().map_err(state_err)?;
        self.set_setting(POST_PROCESSING_SETTINGS_KEY, &to_json(settings)?)
    }

    pub fn upsert_discovered_extension(
        &self,
        manifest: &ExtensionManifest,
        source_path: Option<&str>,
        discovered_at_epoch_ms: i64,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let manifest_json = to_json(manifest)?;
        let revision = manifest.revision();
        let args = vec![
            SqlArg::Text(revision.extension_id().as_str().to_string()),
            SqlArg::Text(revision.revision_id().as_str().to_string()),
            SqlArg::Text(revision.declared_version().as_str().to_string()),
            SqlArg::Text(revision.digest().as_str().to_string()),
            SqlArg::Text(adapter_name(manifest.adapter()).to_string()),
            SqlArg::Text(manifest.display_name().to_string()),
            SqlArg::OptText(
                manifest
                    .compatibility_name()
                    .map(|name| name.as_str().to_string()),
            ),
            SqlArg::Text(manifest.entrypoint().to_string()),
            SqlArg::Text(manifest_json),
            SqlArg::OptText(source_path.map(str::to_string)),
            SqlArg::I64(discovered_at_epoch_ms),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO post_processing_extension_revisions (
                    extension_id, revision_id, declared_version, digest, adapter, display_name,
                    compatibility_name, entrypoint, manifest_json, discovered_source_path,
                    discovered_at_epoch_ms
                 ) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                 ON CONFLICT (extension_id, revision_id) DO UPDATE SET
                    discovered_source_path = excluded.discovered_source_path,
                    discovered_at_epoch_ms = excluded.discovered_at_epoch_ms",
                &args,
            )
            .await?;
            Ok(())
        })
    }

    pub fn approve_extension_revision(
        &self,
        extension_id: &ExtensionId,
        revision_id: &ExtensionRevisionId,
        managed_path: &str,
        approved_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::Text(managed_path.to_string()),
            SqlArg::I64(approved_at_epoch_ms),
            SqlArg::Text(extension_id.as_str().to_string()),
            SqlArg::Text(revision_id.as_str().to_string()),
        ];
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE post_processing_extension_revisions
                    SET trust_state = 'approved', managed_path = {}, approved_at_epoch_ms = {}
                  WHERE extension_id = {} AND revision_id = {}",
                &args,
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn revoke_extension_revision(
        &self,
        extension_id: &ExtensionId,
        revision_id: &ExtensionRevisionId,
    ) -> Result<bool, StateError> {
        self.set_extension_trust_state(extension_id, revision_id, "revoked")
    }

    pub fn disable_extension_revision(
        &self,
        extension_id: &ExtensionId,
        revision_id: &ExtensionRevisionId,
    ) -> Result<bool, StateError> {
        self.set_extension_trust_state(extension_id, revision_id, "unapproved")
    }

    fn set_extension_trust_state(
        &self,
        extension_id: &ExtensionId,
        revision_id: &ExtensionRevisionId,
        state: &'static str,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::Text(state.to_string()),
            SqlArg::Text(extension_id.as_str().to_string()),
            SqlArg::Text(revision_id.as_str().to_string()),
        ];
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE post_processing_extension_revisions SET trust_state = {}
                  WHERE extension_id = {} AND revision_id = {}",
                &args,
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn extension_revision(
        &self,
        extension_id: &ExtensionId,
        revision_id: &ExtensionRevisionId,
    ) -> Result<Option<ExtensionRevisionRecord>, StateError> {
        let datastore = self.datastore();
        let args = vec![
            SqlArg::Text(extension_id.as_str().to_string()),
            SqlArg::Text(revision_id.as_str().to_string()),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT manifest_json, trust_state, managed_path, discovered_source_path,
                        discovered_at_epoch_ms, approved_at_epoch_ms
                   FROM post_processing_extension_revisions
                  WHERE extension_id = {} AND revision_id = {}",
                &args,
            )
            .await?
            .map(extension_record_from_row)
            .transpose()
        })
    }

    pub fn list_extension_revisions(&self) -> Result<Vec<ExtensionRevisionRecord>, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT manifest_json, trust_state, managed_path, discovered_source_path,
                        discovered_at_epoch_ms, approved_at_epoch_ms
                   FROM post_processing_extension_revisions
                  ORDER BY extension_id, discovered_at_epoch_ms DESC",
                &[],
            )
            .await?
            .into_iter()
            .map(extension_record_from_row)
            .collect()
        })
    }

    pub fn save_post_processing_profile(
        &self,
        profile: &Profile,
        enabled: bool,
        now_epoch_ms: i64,
    ) -> Result<(), StateError> {
        let key = self.encryption_key();
        let rows = profile
            .steps()
            .iter()
            .map(|step| profile_step_to_row(step, key))
            .collect::<Result<Vec<_>, _>>()?;
        let profile_id = profile.id().as_str().to_string();
        let name = profile.name().to_string();
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "save_post_processing_profile", |tx| {
                let profile_id = profile_id.clone();
                let name = name.clone();
                let rows = rows.clone();
                Box::pin(async move {
                    tx.execute(
                        "INSERT INTO post_processing_profiles (
                            profile_id, name, enabled, created_at_epoch_ms, updated_at_epoch_ms
                         ) VALUES ({}, {}, {}, {}, {})
                         ON CONFLICT (profile_id) DO UPDATE SET
                            name = excluded.name,
                            enabled = excluded.enabled,
                            updated_at_epoch_ms = excluded.updated_at_epoch_ms",
                        &[
                            SqlArg::Text(profile_id.clone()),
                            SqlArg::Text(name),
                            SqlArg::Bool(enabled),
                            SqlArg::I64(now_epoch_ms),
                            SqlArg::I64(now_epoch_ms),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "DELETE FROM post_processing_profile_steps WHERE profile_id = {}",
                        &[SqlArg::Text(profile_id.clone())],
                    )
                    .await?;
                    for row in rows {
                        tx.execute(
                            "INSERT INTO post_processing_profile_steps (
                                profile_id, step_index, selection_json, policy_json,
                                options_json, secret_options_json
                             ) VALUES ({}, {}, {}, {}, {}, {})",
                            &[
                                SqlArg::Text(profile_id.clone()),
                                SqlArg::I64(i64::from(row.index)),
                                SqlArg::Text(row.selection_json),
                                SqlArg::Text(row.policy_json),
                                SqlArg::Text(row.options_json),
                                SqlArg::Text(row.secret_options_json),
                            ],
                        )
                        .await?;
                    }
                    Ok(())
                })
            })
            .await
        })
    }

    pub fn post_processing_profile(
        &self,
        profile_id: &ProfileId,
    ) -> Result<Option<ProfileRecord>, StateError> {
        let datastore = self.datastore();
        let profile_id = profile_id.as_str().to_string();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            let Some(row) = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT profile_id, name, enabled, created_at_epoch_ms, updated_at_epoch_ms
                   FROM post_processing_profiles WHERE profile_id = {}",
                &[SqlArg::Text(profile_id.clone())],
            )
            .await?
            else {
                return Ok(None);
            };
            let step_rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT step_index, selection_json, policy_json, options_json, secret_options_json
                   FROM post_processing_profile_steps
                  WHERE profile_id = {} ORDER BY step_index",
                &[SqlArg::Text(profile_id)],
            )
            .await?;
            let id = ProfileId::new(row.text("profile_id")?).map_err(state_err)?;
            let mut steps = Vec::with_capacity(step_rows.len());
            for step_row in step_rows {
                steps.push(profile_step_from_row(&step_row, key.as_ref())?);
            }
            let profile = Profile::new(id, row.text("name")?, steps).map_err(state_err)?;
            Ok(Some(ProfileRecord {
                profile,
                enabled: row.bool("enabled")?,
                created_at_epoch_ms: row.i64("created_at_epoch_ms")?,
                updated_at_epoch_ms: row.i64("updated_at_epoch_ms")?,
            }))
        })
    }

    pub fn list_post_processing_profiles(&self) -> Result<Vec<ProfileRecord>, StateError> {
        let datastore = self.datastore();
        let ids = self.run_sql_blocking(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT profile_id FROM post_processing_profiles ORDER BY name, profile_id",
                &[],
            )
            .await?
            .into_iter()
            .map(|row| row.text("profile_id"))
            .collect::<Result<Vec<_>, _>>()
        })?;
        ids.into_iter()
            .map(|id| {
                let id = ProfileId::new(id).map_err(state_err)?;
                self.post_processing_profile(&id)?.ok_or_else(|| {
                    StateError::Database("post-processing profile disappeared during read".into())
                })
            })
            .collect()
    }

    pub fn delete_post_processing_profile(
        &self,
        profile_id: &ProfileId,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let id = profile_id.as_str().to_string();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM post_processing_profiles WHERE profile_id = {}",
                &[SqlArg::Text(id)],
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn assign_global_post_processing_profile(
        &self,
        profile_id: Option<&ProfileId>,
    ) -> Result<(), StateError> {
        self.assign_post_processing_profile("global", GLOBAL_ASSIGNMENT_KEY, profile_id)
    }

    pub fn assign_category_post_processing_profile(
        &self,
        category: &str,
        profile_id: Option<&ProfileId>,
    ) -> Result<(), StateError> {
        if category.trim().is_empty() || category != category.trim() {
            return Err(StateError::Database("invalid category assignment".into()));
        }
        self.assign_post_processing_profile("category", category, profile_id)
    }

    fn assign_post_processing_profile(
        &self,
        scope_kind: &'static str,
        scope_key: &str,
        profile_id: Option<&ProfileId>,
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let scope_key = scope_key.to_string();
        let profile_id = profile_id.map(|id| id.as_str().to_string());
        self.run_sql_blocking(async move {
            match profile_id {
                Some(profile_id) => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "INSERT INTO post_processing_profile_assignments (
                            scope_kind, scope_key, profile_id
                         ) VALUES ({}, {}, {})
                         ON CONFLICT (scope_kind, scope_key) DO UPDATE SET
                            profile_id = excluded.profile_id",
                        &[
                            SqlArg::Text(scope_kind.to_string()),
                            SqlArg::Text(scope_key),
                            SqlArg::Text(profile_id),
                        ],
                    )
                    .await?;
                }
                None => {
                    SqlRuntime::execute(
                        datastore.read_exec(),
                        "DELETE FROM post_processing_profile_assignments
                          WHERE scope_kind = {} AND scope_key = {}",
                        &[
                            SqlArg::Text(scope_kind.to_string()),
                            SqlArg::Text(scope_key),
                        ],
                    )
                    .await?;
                }
            }
            Ok(())
        })
    }

    pub fn freeze_submission_post_processing_plan(
        &self,
        selection: Option<&SubmissionPlanSelection>,
    ) -> Result<Option<FrozenPlan>, StateError> {
        match selection.map(SubmissionPlanSelection::mode) {
            Some(SubmissionPlanSelectionMode::Inherit) | None => Ok(None),
            Some(_) => self.resolve_post_processing_plan(selection, None).map(Some),
        }
    }

    pub fn resolve_post_processing_plan(
        &self,
        selection: Option<&SubmissionPlanSelection>,
        category: Option<&str>,
    ) -> Result<FrozenPlan, StateError> {
        match selection.map(SubmissionPlanSelection::mode) {
            Some(SubmissionPlanSelectionMode::Disabled) => {
                return FrozenPlan::new(FrozenPlanProvenance::Disabled, vec![]).map_err(state_err);
            }
            Some(SubmissionPlanSelectionMode::Profile) => {
                let profile_id = selection
                    .and_then(SubmissionPlanSelection::profile_id)
                    .ok_or_else(|| StateError::Database("missing selected profile".into()))?;
                return self.freeze_profile(profile_id, FrozenPlanProvenance::Explicit);
            }
            Some(SubmissionPlanSelectionMode::Extensions) => {
                let selections = selection
                    .and_then(SubmissionPlanSelection::selected_extensions)
                    .ok_or_else(|| StateError::Database("missing selected extensions".into()))?;
                return self.freeze_explicit_extensions(selections);
            }
            Some(SubmissionPlanSelectionMode::Inherit) | None => {}
        }

        if let Some(category) = category
            && let Some(profile_id) = self.assigned_profile_id("category", category)?
        {
            return self.freeze_profile(
                &profile_id,
                FrozenPlanProvenance::CategoryProfile {
                    profile_id: profile_id.clone(),
                },
            );
        }
        if let Some(profile_id) = self.assigned_profile_id("global", GLOBAL_ASSIGNMENT_KEY)? {
            return self.freeze_profile(
                &profile_id,
                FrozenPlanProvenance::GlobalDefault {
                    profile_id: profile_id.clone(),
                },
            );
        }
        FrozenPlan::new(FrozenPlanProvenance::Empty, vec![]).map_err(state_err)
    }

    fn assigned_profile_id(
        &self,
        scope_kind: &'static str,
        scope_key: &str,
    ) -> Result<Option<ProfileId>, StateError> {
        let datastore = self.datastore();
        let key = scope_key.to_string();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT profile_id FROM post_processing_profile_assignments
                  WHERE scope_kind = {} AND scope_key = {}",
                &[SqlArg::Text(scope_kind.to_string()), SqlArg::Text(key)],
            )
            .await?;
            row.map(|row| ProfileId::new(row.text("profile_id")?).map_err(state_err))
                .transpose()
        })
    }

    fn freeze_profile(
        &self,
        profile_id: &ProfileId,
        provenance: FrozenPlanProvenance,
    ) -> Result<FrozenPlan, StateError> {
        let record = self
            .post_processing_profile(profile_id)?
            .ok_or_else(|| StateError::Database("post-processing profile not found".into()))?;
        if !record.enabled {
            return Err(StateError::Database(
                "post-processing profile is disabled".into(),
            ));
        }
        let settings = self.post_processing_settings()?;
        let mut frozen = Vec::with_capacity(record.profile.steps().len());
        for step in record.profile.steps() {
            validate_frozen_roots(step.approved_roots(), &settings)?;
            let revision = self.resolve_approved_revision_record(step.selection())?;
            let options = resolve_manifest_options(&revision.manifest, step.options())?;
            frozen.push(
                FrozenPlanStep::new(
                    step.index(),
                    revision.manifest.revision().clone(),
                    step.run_when(),
                    step.on_failure(),
                    step.outcome_impact(),
                    step.timeout_policy(),
                    step.approved_roots().clone(),
                    options,
                )
                .map_err(state_err)?
                .with_artifact_condition(step.artifact_condition().clone()),
            );
        }
        FrozenPlan::new(provenance, frozen).map_err(state_err)
    }

    fn freeze_explicit_extensions(
        &self,
        selections: &[ExtensionSelection],
    ) -> Result<FrozenPlan, StateError> {
        let mut steps = Vec::with_capacity(selections.len());
        for (index, selection) in selections.iter().enumerate() {
            let revision = self.resolve_approved_revision_record(selection)?;
            let options = resolve_manifest_options(&revision.manifest, &[])?;
            steps.push(
                FrozenPlanStep::new(
                    index as u32,
                    revision.manifest.revision().clone(),
                    RunWhen::Always,
                    OnFailure::Continue,
                    OutcomeImpact::Warning,
                    TimeoutPolicy::Default24Hours,
                    ApprovedFilesystemRoots::new(vec![]),
                    options,
                )
                .map_err(state_err)?,
            );
        }
        FrozenPlan::new(FrozenPlanProvenance::Explicit, steps).map_err(state_err)
    }

    pub fn rebind_frozen_post_processing_plan(
        &self,
        plan: &FrozenPlan,
    ) -> Result<FrozenPlan, StateError> {
        let settings = self.post_processing_settings()?;
        let mut steps = Vec::with_capacity(plan.steps().len());
        for step in plan.steps() {
            validate_frozen_roots(step.approved_roots(), &settings)?;
            let selection =
                ExtensionSelection::latest_approved(step.revision().extension_id().clone());
            let revision = self.resolve_approved_revision_record(&selection)?;
            let options = resolve_manifest_options(&revision.manifest, step.options())?;
            steps.push(
                FrozenPlanStep::new(
                    step.index(),
                    revision.manifest.revision().clone(),
                    step.run_when(),
                    step.on_failure(),
                    step.outcome_impact(),
                    step.timeout_policy(),
                    step.approved_roots().clone(),
                    options,
                )
                .map_err(state_err)?
                .with_artifact_condition(step.artifact_condition().clone()),
            );
        }
        FrozenPlan::new(plan.provenance().clone(), steps).map_err(state_err)
    }

    fn resolve_approved_revision_record(
        &self,
        selection: &ExtensionSelection,
    ) -> Result<ExtensionRevisionRecord, StateError> {
        let record = match selection.mode() {
            SelectionMode::PinnedRevision => {
                let revision_id = selection.revision_id().ok_or_else(|| {
                    StateError::Database("pinned extension has no revision".into())
                })?;
                self.extension_revision(selection.extension_id(), revision_id)?
            }
            SelectionMode::LatestApproved => {
                self.latest_approved_extension_revision(selection.extension_id())?
            }
        }
        .ok_or_else(|| StateError::Database("approved extension revision not found".into()))?;
        if record.trust_state != TrustState::Approved || record.managed_path.is_none() {
            return Err(StateError::Database(
                "extension revision is not approved and managed".into(),
            ));
        }
        Ok(record)
    }

    fn latest_approved_extension_revision(
        &self,
        extension_id: &ExtensionId,
    ) -> Result<Option<ExtensionRevisionRecord>, StateError> {
        let datastore = self.datastore();
        let id = extension_id.as_str().to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT manifest_json, trust_state, managed_path, discovered_source_path,
                        discovered_at_epoch_ms, approved_at_epoch_ms
                   FROM post_processing_extension_revisions
                  WHERE extension_id = {} AND trust_state = 'approved' AND managed_path IS NOT NULL
                  ORDER BY approved_at_epoch_ms DESC, discovered_at_epoch_ms DESC
                  LIMIT 1",
                &[SqlArg::Text(id)],
            )
            .await?
            .map(extension_record_from_row)
            .transpose()
        })
    }

    pub fn save_frozen_post_processing_plan(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        created_at_epoch_ms: i64,
    ) -> Result<(), StateError> {
        let (plan_json, secret_options_json) = encode_plan(plan, self.encryption_key())?;
        let provenance_json = to_json(plan.provenance())?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO post_processing_job_plans (
                    job_id, provenance_json, plan_json, secret_options_json, created_at_epoch_ms
                 ) VALUES ({}, {}, {}, {}, {})
                 ON CONFLICT (job_id) DO NOTHING",
                &[
                    SqlArg::I64(job_id_i64(job_id)?),
                    SqlArg::Text(provenance_json),
                    SqlArg::Text(plan_json),
                    SqlArg::Text(secret_options_json),
                    SqlArg::I64(created_at_epoch_ms),
                ],
            )
            .await?;
            Ok(())
        })
    }

    pub fn delete_frozen_post_processing_plan(&self, job_id: u64) -> Result<(), StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DELETE FROM post_processing_job_plans WHERE job_id = {}",
                &[SqlArg::I64(job_id_i64(job_id)?)],
            )
            .await?;
            Ok(())
        })
    }

    pub fn replace_frozen_post_processing_plan(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        updated_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let (plan_json, secret_options_json) = encode_plan(plan, self.encryption_key())?;
        let provenance_json = to_json(plan.provenance())?;
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let job_id = job_id_i64(job_id)?;
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE post_processing_job_plans
                    SET provenance_json = {}, plan_json = {}, secret_options_json = {},
                        created_at_epoch_ms = {}
                  WHERE job_id = {}
                    AND EXISTS (SELECT 1 FROM active_jobs WHERE job_id = {})
                    AND NOT EXISTS (
                        SELECT 1 FROM post_processing_runs WHERE job_id = {}
                    )",
                &[
                    SqlArg::Text(provenance_json),
                    SqlArg::Text(plan_json),
                    SqlArg::Text(secret_options_json),
                    SqlArg::I64(updated_at_epoch_ms),
                    SqlArg::I64(job_id),
                    SqlArg::I64(job_id),
                    SqlArg::I64(job_id),
                ],
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn frozen_post_processing_plan(
        &self,
        job_id: u64,
    ) -> Result<Option<FrozenPlan>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT plan_json, secret_options_json FROM post_processing_job_plans
                  WHERE job_id = {}",
                &[SqlArg::I64(job_id_i64(job_id)?)],
            )
            .await?;
            row.map(|row| {
                decode_plan(
                    &row.text("plan_json")?,
                    &row.text("secret_options_json")?,
                    key.as_ref(),
                )
            })
            .transpose()
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_post_processing_run(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        pipeline_outcome: &PipelineOutcome,
        terminal_intent: TerminalIntent,
        rerun_of_run_id: Option<&RunId>,
        queued_at_epoch_ms: i64,
    ) -> Result<RunId, StateError> {
        self.create_post_processing_run_inner(
            job_id,
            plan,
            pipeline_outcome,
            terminal_intent,
            rerun_of_run_id,
            queued_at_epoch_ms,
            false,
        )
    }

    pub fn create_history_post_processing_rerun(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        pipeline_outcome: &PipelineOutcome,
        terminal_intent: TerminalIntent,
        rerun_of_run_id: &RunId,
        queued_at_epoch_ms: i64,
    ) -> Result<RunId, StateError> {
        self.create_post_processing_run_inner(
            job_id,
            plan,
            pipeline_outcome,
            terminal_intent,
            Some(rerun_of_run_id),
            queued_at_epoch_ms,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn create_post_processing_run_inner(
        &self,
        job_id: u64,
        plan: &FrozenPlan,
        pipeline_outcome: &PipelineOutcome,
        terminal_intent: TerminalIntent,
        rerun_of_run_id: Option<&RunId>,
        queued_at_epoch_ms: i64,
        require_history: bool,
    ) -> Result<RunId, StateError> {
        let run_id = generate_run_id()?;
        let (plan_json, secret_options_json) = encode_plan(plan, self.encryption_key())?;
        let pipeline_outcome_json = to_json(pipeline_outcome)?;
        let terminal_intent = terminal_intent_name(terminal_intent).to_string();
        let rerun = rerun_of_run_id.map(|id| id.as_str().to_string());
        let datastore = self.datastore();
        let history_lock_sql = if datastore.engine() == SqlEngine::Postgres {
            "SELECT job_id FROM job_history WHERE job_id = {} FOR UPDATE"
        } else {
            "SELECT job_id FROM job_history WHERE job_id = {}"
        };
        let run_id_text = run_id.as_str().to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "create_post_processing_run", |tx| {
                let run_id = run_id_text.clone();
                let plan_json = plan_json.clone();
                let secret_options_json = secret_options_json.clone();
                let pipeline_outcome_json = pipeline_outcome_json.clone();
                let terminal_intent = terminal_intent.clone();
                let rerun = rerun.clone();
                Box::pin(async move {
                    if require_history
                        && tx
                            .fetch_optional(history_lock_sql, &[SqlArg::I64(job_id_i64(job_id)?)])
                            .await?
                            .is_none()
                    {
                        return Err(StateError::Conflict(
                            "history row disappeared before rerun creation".into(),
                        ));
                    }
                    let queue_position = tx
                        .fetch_optional(
                            "SELECT COALESCE(MAX(queue_position), 0) + 1 AS next_position
                               FROM post_processing_runs",
                            &[],
                        )
                        .await?
                        .ok_or_else(|| {
                            StateError::Database(
                                "failed to allocate post-processing queue position".into(),
                            )
                        })?
                        .i64("next_position")?;
                    tx.execute(
                        "INSERT INTO post_processing_runs (
                            run_id, job_id, status, pipeline_outcome_json, summary,
                            terminal_intent, plan_json, secret_options_json, rerun_of_run_id,
                            queued_at_epoch_ms, queue_position
                         ) VALUES ({}, {}, 'queued', {}, 'not_run', {}, {}, {}, {}, {}, {})",
                        &[
                            SqlArg::Text(run_id.clone()),
                            SqlArg::I64(job_id_i64(job_id)?),
                            SqlArg::Text(pipeline_outcome_json.clone()),
                            SqlArg::Text(terminal_intent),
                            SqlArg::Text(plan_json),
                            SqlArg::Text(secret_options_json),
                            SqlArg::OptText(rerun),
                            SqlArg::I64(queued_at_epoch_ms),
                            SqlArg::I64(queue_position),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE active_jobs
                            SET pipeline_outcome_json = {},
                                post_processing_summary = 'not_run',
                                post_processing_run_id = {}
                          WHERE job_id = {}",
                        &[
                            SqlArg::Text(pipeline_outcome_json.clone()),
                            SqlArg::Text(run_id.clone()),
                            SqlArg::I64(job_id_i64(job_id)?),
                        ],
                    )
                    .await?;
                    tx.execute(
                        "UPDATE job_history
                            SET pipeline_outcome_json = {},
                                post_processing_summary = 'not_run',
                                post_processing_run_id = {}
                          WHERE job_id = {}",
                        &[
                            SqlArg::Text(pipeline_outcome_json),
                            SqlArg::Text(run_id),
                            SqlArg::I64(job_id_i64(job_id)?),
                        ],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })?;
        Ok(run_id)
    }

    pub fn enqueue_post_processing_attempt(
        &self,
        run_id: &RunId,
        step: &FrozenPlanStep,
        adapter: ExtensionAdapter,
        control_token_hash: Option<Vec<u8>>,
        queued_at_epoch_ms: i64,
    ) -> Result<AttemptId, StateError> {
        let attempt_id = generate_attempt_id()?;
        let datastore = self.datastore();
        let revision = step.revision();
        let args = vec![
            SqlArg::Text(attempt_id.as_str().to_string()),
            SqlArg::Text(run_id.as_str().to_string()),
            SqlArg::I64(i64::from(step.index())),
            SqlArg::Text(revision.extension_id().as_str().to_string()),
            SqlArg::Text(revision.revision_id().as_str().to_string()),
            SqlArg::Text(adapter_name(adapter).to_string()),
            SqlArg::OptBytes(control_token_hash),
            SqlArg::I64(queued_at_epoch_ms),
        ];
        self.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "INSERT INTO post_processing_attempts (
                    attempt_id, run_id, step_index, status, extension_id, revision_id,
                    adapter, control_token_hash, queued_at_epoch_ms
                 ) VALUES ({}, {}, {}, 'queued', {}, {}, {}, {}, {})",
                &args,
            )
            .await?;
            Ok(())
        })?;
        Ok(attempt_id)
    }

    pub fn mark_post_processing_attempt_starting(
        &self,
        attempt_id: &AttemptId,
        command: &serde_json::Value,
        working_directory: &str,
        started_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        self.update_attempt_state(
            attempt_id,
            "starting",
            Some(command.clone()),
            Some(working_directory.to_string()),
            Some(started_at_epoch_ms),
            &["queued"],
        )
    }

    pub fn mark_post_processing_attempt_running(
        &self,
        attempt_id: &AttemptId,
    ) -> Result<bool, StateError> {
        self.update_attempt_state(attempt_id, "running", None, None, None, &["starting"])
    }

    pub fn validate_post_processing_control_token(
        &self,
        attempt_id: &AttemptId,
        token: &str,
    ) -> Result<Option<u64>, StateError> {
        let datastore = self.datastore();
        let attempt_id = attempt_id.as_str().to_string();
        let candidate_hash = blake3::hash(token.as_bytes()).as_bytes().to_vec();
        self.run_sql_blocking(async move {
            let Some(row) = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT r.job_id, a.control_token_hash
                   FROM post_processing_attempts a
                   JOIN post_processing_runs r ON r.run_id = a.run_id
                  WHERE a.attempt_id = {}
                    AND a.status IN ('starting', 'running')
                    AND a.control_token_hash IS NOT NULL",
                &[SqlArg::Text(attempt_id)],
            )
            .await?
            else {
                return Ok(None);
            };
            if !constant_time_eq(&row.bytes("control_token_hash")?, &candidate_hash) {
                return Ok(None);
            }
            let job_id = u64::try_from(row.i64("job_id")?)
                .map_err(|_| StateError::Database("stored job id is negative".into()))?;
            Ok(Some(job_id))
        })
    }

    fn update_attempt_state(
        &self,
        attempt_id: &AttemptId,
        status: &'static str,
        command: Option<serde_json::Value>,
        working_directory: Option<String>,
        started_at_epoch_ms: Option<i64>,
        allowed_from: &'static [&'static str],
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let id = attempt_id.as_str().to_string();
        let allowed = allowed_from
            .iter()
            .map(|value| format!("'{value}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let statement = format!(
            "UPDATE post_processing_attempts
                SET status = {{}},
                    command_json = COALESCE({{}}, command_json),
                    working_directory = COALESCE({{}}, working_directory),
                    started_at_epoch_ms = COALESCE({{}}, started_at_epoch_ms)
              WHERE attempt_id = {{}} AND status IN ({allowed})"
        );
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                &statement,
                &[
                    SqlArg::Text(status.to_string()),
                    SqlArg::OptJson(command),
                    SqlArg::OptText(working_directory),
                    SqlArg::OptI64(started_at_epoch_ms),
                    SqlArg::Text(id),
                ],
            )
            .await?;
            Ok(changed == 1)
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_post_processing_attempt(
        &self,
        attempt_id: &AttemptId,
        status: AttemptStatus,
        exit_code: Option<i32>,
        error_message: Option<String>,
        progress: Option<serde_json::Value>,
        finished_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        if matches!(
            status,
            AttemptStatus::Queued | AttemptStatus::Starting | AttemptStatus::Running
        ) {
            return Err(StateError::Database(
                "attempt terminal update requires a terminal status".into(),
            ));
        }
        let datastore = self.datastore();
        let id = attempt_id.as_str().to_string();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE post_processing_attempts
                    SET status = {}, exit_code = {}, error_message = {}, progress_json = {},
                        finished_at_epoch_ms = {}, control_token_hash = NULL
                  WHERE attempt_id = {} AND status IN ('queued', 'starting', 'running')",
                &[
                    SqlArg::Text(attempt_status_name(status).to_string()),
                    SqlArg::OptI32(exit_code),
                    SqlArg::OptText(error_message),
                    SqlArg::OptJson(progress),
                    SqlArg::I64(finished_at_epoch_ms),
                    SqlArg::Text(id),
                ],
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn finish_post_processing_run(
        &self,
        run_id: &RunId,
        status: RunStatus,
        summary: PostProcessingSummary,
        finished_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        if matches!(
            status,
            RunStatus::Queued | RunStatus::Starting | RunStatus::Running
        ) {
            return Err(StateError::Database(
                "run terminal update requires a terminal status".into(),
            ));
        }
        let datastore = self.datastore();
        let id = run_id.as_str().to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "finish_post_processing_run", |tx| {
                let id = id.clone();
                let summary = summary.clone();
                Box::pin(async move {
                    let changed = tx
                        .execute(
                            "UPDATE post_processing_runs
                                SET status = {}, summary = {}, finished_at_epoch_ms = {}
                              WHERE run_id = {} AND status IN ('queued', 'starting', 'running')",
                            &[
                                SqlArg::Text(run_status_name(status).to_string()),
                                SqlArg::Text(summary_name(&summary).to_string()),
                                SqlArg::I64(finished_at_epoch_ms),
                                SqlArg::Text(id.clone()),
                            ],
                        )
                        .await?;
                    if changed == 1 {
                        tx.execute(
                            "UPDATE active_jobs SET post_processing_summary = {}
                              WHERE post_processing_run_id = {}",
                            &[
                                SqlArg::Text(summary_name(&summary).to_string()),
                                SqlArg::Text(id.clone()),
                            ],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE job_history SET post_processing_summary = {}
                              WHERE post_processing_run_id = {}",
                            &[
                                SqlArg::Text(summary_name(&summary).to_string()),
                                SqlArg::Text(id),
                            ],
                        )
                        .await?;
                    }
                    Ok(changed == 1)
                })
            })
            .await
        })
    }

    pub fn mark_post_processing_run_running(
        &self,
        run_id: &RunId,
        started_at_epoch_ms: i64,
    ) -> Result<bool, StateError> {
        let datastore = self.datastore();
        let id = run_id.as_str().to_string();
        self.run_sql_blocking(async move {
            let changed = SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE post_processing_runs
                    SET status = 'running', started_at_epoch_ms = {}
                  WHERE run_id = {} AND status IN ('queued', 'starting')",
                &[SqlArg::I64(started_at_epoch_ms), SqlArg::Text(id)],
            )
            .await?;
            Ok(changed == 1)
        })
    }

    pub fn recover_interrupted_post_processing(
        &self,
        finished_at_epoch_ms: i64,
    ) -> Result<u64, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "recover_interrupted_post_processing",
                |tx| {
                    Box::pin(async move {
                        let attempts = tx
                            .execute(
                                "UPDATE post_processing_attempts
                                    SET status = 'interrupted',
                                        error_message = 'Weaver restarted while the attempt was active',
                                        finished_at_epoch_ms = {}, control_token_hash = NULL
                                  WHERE status IN ('starting', 'running')",
                                &[SqlArg::I64(finished_at_epoch_ms)],
                            )
                            .await?;
                        tx.execute(
                            "UPDATE post_processing_runs
                                SET status = 'interrupted', summary = 'interrupted',
                                    finished_at_epoch_ms = {}
                              WHERE status IN ('starting', 'running')",
                            &[SqlArg::I64(finished_at_epoch_ms)],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE post_processing_runs
                                SET status = 'interrupted', summary = 'interrupted',
                                    finished_at_epoch_ms = {}
                              WHERE status = 'queued'
                                AND NOT EXISTS (
                                    SELECT 1 FROM active_jobs
                                     WHERE active_jobs.post_processing_run_id =
                                           post_processing_runs.run_id
                                )",
                            &[SqlArg::I64(finished_at_epoch_ms)],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE active_jobs SET post_processing_summary = 'interrupted'
                              WHERE post_processing_run_id IN (
                                  SELECT run_id FROM post_processing_runs
                                   WHERE status = 'interrupted'
                              )",
                            &[],
                        )
                        .await?;
                        tx.execute(
                            "UPDATE job_history SET post_processing_summary = 'interrupted'
                              WHERE post_processing_run_id IN (
                                  SELECT run_id FROM post_processing_runs
                                   WHERE status = 'interrupted'
                              )",
                            &[],
                        )
                        .await?;
                        Ok(attempts)
                    })
                },
            )
            .await
        })
    }

    pub fn append_post_processing_log(
        &self,
        attempt_id: &AttemptId,
        stream: LogStream,
        payload: &[u8],
        created_at_epoch_ms: i64,
    ) -> Result<u64, StateError> {
        if payload.len() > MAX_LOGICAL_LINE_BYTES {
            return Err(StateError::Database(
                "post-processing log chunk exceeds logical line limit".into(),
            ));
        }
        let datastore = self.datastore();
        let id = attempt_id.as_str().to_string();
        let payload = payload.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "append_post_processing_log", |tx| {
                let id = id.clone();
                let payload = payload.clone();
                Box::pin(async move {
                    let next = tx
                        .fetch_optional(
                            "SELECT COALESCE(MAX(sequence), -1) + 1 AS next_sequence
                               FROM post_processing_log_chunks WHERE attempt_id = {}",
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?
                        .ok_or_else(|| {
                            StateError::Database("failed to allocate log sequence".into())
                        })?
                        .i64("next_sequence")?;
                    tx.execute(
                        "INSERT INTO post_processing_log_chunks (
                            attempt_id, sequence, stream, payload, byte_count, created_at_epoch_ms
                         ) VALUES ({}, {}, {}, {}, {}, {})",
                        &[
                            SqlArg::Text(id.clone()),
                            SqlArg::I64(next),
                            SqlArg::Text(log_stream_name(stream).to_string()),
                            SqlArg::Bytes(payload.clone()),
                            SqlArg::I64(payload.len() as i64),
                            SqlArg::I64(created_at_epoch_ms),
                        ],
                    )
                    .await?;
                    let rows = tx
                        .fetch_all(
                            "SELECT sequence, byte_count FROM post_processing_log_chunks
                              WHERE attempt_id = {} ORDER BY sequence",
                            &[SqlArg::Text(id.clone())],
                        )
                        .await?;
                    let mut total = rows.iter().try_fold(0_u64, |total, row| {
                        let bytes = u64::try_from(row.i64("byte_count")?)
                            .map_err(|_| StateError::Database("invalid stored log size".into()))?;
                        Ok::<_, StateError>(total.saturating_add(bytes))
                    })?;
                    let mut truncated = false;
                    for row in rows.iter().skip(1) {
                        if total <= MAX_PERSISTED_ATTEMPT_OUTPUT_BYTES {
                            break;
                        }
                        let bytes = u64::try_from(row.i64("byte_count")?)
                            .map_err(|_| StateError::Database("invalid stored log size".into()))?;
                        tx.execute(
                            "DELETE FROM post_processing_log_chunks
                              WHERE attempt_id = {} AND sequence = {}",
                            &[SqlArg::Text(id.clone()), SqlArg::I64(row.i64("sequence")?)],
                        )
                        .await?;
                        total = total.saturating_sub(bytes);
                        truncated = true;
                    }
                    if truncated {
                        tx.execute(
                            "UPDATE post_processing_attempts SET output_truncated = {}
                              WHERE attempt_id = {}",
                            &[SqlArg::Bool(true), SqlArg::Text(id)],
                        )
                        .await?;
                    }
                    u64::try_from(next)
                        .map_err(|_| StateError::Database("invalid log sequence".into()))
                })
            })
            .await
        })
    }

    pub fn post_processing_logs(
        &self,
        attempt_id: &AttemptId,
        after: Option<u64>,
        limit: usize,
    ) -> Result<PostProcessingLogPage, StateError> {
        let limit = limit.clamp(1, 500);
        let datastore = self.datastore();
        let id = attempt_id.as_str().to_string();
        let after = after.map(i64::try_from).transpose().map_err(|_| {
            StateError::Database("post-processing log cursor exceeds SQL range".into())
        })?;
        self.run_sql_blocking(async move {
            let mut rows = SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT sequence, stream, payload, created_at_epoch_ms
                   FROM post_processing_log_chunks
                  WHERE attempt_id = {} AND sequence > {}
                  ORDER BY sequence LIMIT {}",
                &[
                    SqlArg::Text(id.clone()),
                    SqlArg::I64(after.unwrap_or(-1)),
                    SqlArg::I64((limit + 1) as i64),
                ],
            )
            .await?;
            let has_more = rows.len() > limit;
            rows.truncate(limit);
            let chunks = rows
                .into_iter()
                .map(log_chunk_from_row)
                .collect::<Result<Vec<_>, _>>()?;
            let truncated = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT output_truncated FROM post_processing_attempts WHERE attempt_id = {}",
                &[SqlArg::Text(id)],
            )
            .await?
            .map(|row| row.bool("output_truncated"))
            .transpose()?
            .unwrap_or(false);
            Ok(PostProcessingLogPage {
                next_cursor: has_more
                    .then(|| chunks.last().map(|chunk| chunk.sequence))
                    .flatten(),
                chunks,
                truncated,
            })
        })
    }

    pub fn post_processing_run(
        &self,
        run_id: &RunId,
    ) -> Result<Option<PostProcessingRunRecord>, StateError> {
        let datastore = self.datastore();
        let id = run_id.as_str().to_string();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT run_id, job_id, status, pipeline_outcome_json, summary, terminal_intent,
                        plan_json, secret_options_json, rerun_of_run_id, queued_at_epoch_ms,
                        queue_position,
                        started_at_epoch_ms, finished_at_epoch_ms
                   FROM post_processing_runs WHERE run_id = {}",
                &[SqlArg::Text(id)],
            )
            .await?
            .map(|row| run_record_from_row(row, key.as_ref()))
            .transpose()
        })
    }

    pub fn active_job_post_processing_run(
        &self,
        job_id: u64,
    ) -> Result<Option<PostProcessingRunRecord>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT r.run_id, r.job_id, r.status, r.pipeline_outcome_json, r.summary,
                        r.terminal_intent, r.plan_json, r.secret_options_json,
                        r.rerun_of_run_id, r.queued_at_epoch_ms, r.queue_position,
                        r.started_at_epoch_ms,
                        r.finished_at_epoch_ms
                   FROM active_jobs a
                   JOIN post_processing_runs r ON r.run_id = a.post_processing_run_id
                  WHERE a.job_id = {}",
                &[SqlArg::I64(job_id_i64(job_id)?)],
            )
            .await?
            .map(|row| run_record_from_row(row, key.as_ref()))
            .transpose()
        })
    }

    pub fn list_post_processing_runs(
        &self,
        job_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<PostProcessingRunRecord>, StateError> {
        let datastore = self.datastore();
        let key = self.encryption_key().cloned();
        let limit = i64::from(limit.clamp(1, 500));
        self.run_sql_blocking(async move {
            let columns = "run_id, job_id, status, pipeline_outcome_json, summary, terminal_intent,
                           plan_json, secret_options_json, rerun_of_run_id, queued_at_epoch_ms,
                           queue_position,
                           started_at_epoch_ms, finished_at_epoch_ms";
            let rows = if let Some(job_id) = job_id {
                SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT {columns} FROM post_processing_runs
                          WHERE job_id = {{}} ORDER BY queued_at_epoch_ms DESC LIMIT {{}}"
                    ),
                    &[SqlArg::I64(job_id_i64(job_id)?), SqlArg::I64(limit)],
                )
                .await?
            } else {
                SqlRuntime::fetch_all(
                    datastore.read_exec(),
                    &format!(
                        "SELECT {columns} FROM post_processing_runs
                          ORDER BY queued_at_epoch_ms DESC LIMIT {{}}"
                    ),
                    &[SqlArg::I64(limit)],
                )
                .await?
            };
            rows.into_iter()
                .map(|row| run_record_from_row(row, key.as_ref()))
                .collect()
        })
    }

    pub fn reorder_queued_post_processing_runs(
        &self,
        ordered_run_ids: &[String],
    ) -> Result<(), StateError> {
        let datastore = self.datastore();
        let ordered_run_ids = ordered_run_ids.to_vec();
        self.run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(
                &datastore,
                "reorder_queued_post_processing_runs",
                |tx| {
                    let ordered_run_ids = ordered_run_ids.clone();
                    Box::pin(async move {
                        let queued_count = tx
                            .fetch_optional(
                                "SELECT COUNT(*) AS queued_count FROM post_processing_runs
                                  WHERE status = 'queued'",
                                &[],
                            )
                            .await?
                            .ok_or_else(|| {
                                StateError::Database(
                                    "failed to count queued post-processing runs".into(),
                                )
                            })?
                            .i64("queued_count")?;
                        if usize::try_from(queued_count).ok() != Some(ordered_run_ids.len()) {
                            return Err(StateError::Database(
                                "post-processing queue changed while it was reordered".into(),
                            ));
                        }
                        for (position, run_id) in ordered_run_ids.iter().enumerate() {
                            let changed = tx
                                .execute(
                                    "UPDATE post_processing_runs SET queue_position = {}
                                      WHERE run_id = {} AND status = 'queued'",
                                    &[
                                        SqlArg::I64(i64::try_from(position).map_err(|_| {
                                            StateError::Database(
                                                "post-processing queue is too large".into(),
                                            )
                                        })?),
                                        SqlArg::Text(run_id.clone()),
                                    ],
                                )
                                .await?;
                            if changed != 1 {
                                return Err(StateError::Database(
                                    "post-processing queue changed while it was reordered".into(),
                                ));
                            }
                        }
                        Ok(())
                    })
                },
            )
            .await
        })
    }

    pub fn post_processing_attempts(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PostProcessingAttemptRecord>, StateError> {
        let datastore = self.datastore();
        let id = run_id.as_str().to_string();
        self.run_sql_blocking(async move {
            SqlRuntime::fetch_all(
                datastore.read_exec(),
                "SELECT attempt_id, run_id, step_index, status, extension_id, revision_id,
                        adapter, working_directory, exit_code, error_message, progress_json,
                        output_truncated, queued_at_epoch_ms, started_at_epoch_ms,
                        finished_at_epoch_ms
                   FROM post_processing_attempts WHERE run_id = {} ORDER BY step_index",
                &[SqlArg::Text(id)],
            )
            .await?
            .into_iter()
            .map(attempt_record_from_row)
            .collect()
        })
    }

    pub fn post_processing_artifacts(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<PostProcessingArtifactRecord>, StateError> {
        let attempts = self.post_processing_attempts(run_id)?;
        let mut seen = HashSet::new();
        let mut artifacts = Vec::new();
        for attempt in attempts {
            for path in attempt.control_effects().artifacts {
                if !seen.insert(path.clone()) {
                    continue;
                }
                let metadata = std::fs::symlink_metadata(&path).ok();
                artifacts.push(PostProcessingArtifactRecord {
                    attempt_id: attempt.attempt_id.clone(),
                    step_index: attempt.step_index,
                    exists: metadata.is_some(),
                    is_file: metadata.as_ref().is_some_and(std::fs::Metadata::is_file),
                    is_directory: metadata.as_ref().is_some_and(std::fs::Metadata::is_dir),
                    is_symlink: metadata
                        .as_ref()
                        .is_some_and(|metadata| metadata.file_type().is_symlink()),
                    size_bytes: metadata
                        .as_ref()
                        .filter(|metadata| metadata.is_file())
                        .map(std::fs::Metadata::len),
                    path,
                });
            }
        }
        Ok(artifacts)
    }

    pub fn post_processing_metrics_snapshot(
        &self,
    ) -> Result<PostProcessingMetricsSnapshot, StateError> {
        let datastore = self.datastore();
        self.run_sql_blocking(async move {
            let queue_row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT COUNT(*) AS queue_depth FROM post_processing_runs WHERE status = 'queued'",
                &[],
            )
            .await?
            .ok_or_else(|| {
                StateError::Database("post-processing queue metrics returned no row".into())
            })?;
            let row = SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT
                    COALESCE(SUM(CASE WHEN status IN ('starting', 'running') THEN 1 ELSE 0 END), 0)
                      AS active_attempts,
                    COALESCE(SUM(CASE WHEN started_at_epoch_ms IS NOT NULL
                      AND finished_at_epoch_ms IS NOT NULL THEN 1 ELSE 0 END), 0) AS duration_count,
                    COALESCE(SUM(CASE WHEN started_at_epoch_ms IS NOT NULL
                      AND finished_at_epoch_ms IS NOT NULL
                      THEN finished_at_epoch_ms - started_at_epoch_ms ELSE 0 END), 0)
                      AS duration_sum_millis,
                    COALESCE(SUM(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded,
                    COALESCE(SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END), 0) AS failed,
                    COALESCE(SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END), 0) AS skipped,
                    COALESCE(SUM(CASE WHEN status = 'timed_out' THEN 1 ELSE 0 END), 0) AS timed_out,
                    COALESCE(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled,
                    COALESCE(SUM(CASE WHEN status = 'interrupted' THEN 1 ELSE 0 END), 0)
                      AS interrupted,
                    COALESCE(SUM(CASE WHEN output_truncated = TRUE THEN 1 ELSE 0 END), 0)
                      AS truncated
                   FROM post_processing_attempts",
                &[],
            )
            .await?
            .ok_or_else(|| {
                StateError::Database("post-processing attempt metrics returned no row".into())
            })?;
            let metric = |name| {
                u64::try_from(row.i64(name)?)
                    .map_err(|_| StateError::Database(format!("invalid {name} metric")))
            };
            Ok(PostProcessingMetricsSnapshot {
                queue_depth: u64::try_from(queue_row.i64("queue_depth")?)
                    .map_err(|_| StateError::Database("invalid queue_depth metric".into()))?,
                active_attempts: metric("active_attempts")?,
                duration_count: metric("duration_count")?,
                duration_sum_millis: metric("duration_sum_millis")?,
                succeeded: metric("succeeded")?,
                failed: metric("failed")?,
                skipped: metric("skipped")?,
                timed_out: metric("timed_out")?,
                cancelled: metric("cancelled")?,
                interrupted: metric("interrupted")?,
                truncated: metric("truncated")?,
            })
        })
    }
}

fn generate_run_id() -> Result<RunId, StateError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(state_err)?;
    RunId::new(format!("run-{}", hex::encode(bytes))).map_err(state_err)
}

fn generate_attempt_id() -> Result<AttemptId, StateError> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(state_err)?;
    AttemptId::new(format!("attempt-{}", hex::encode(bytes))).map_err(state_err)
}

fn terminal_intent_name(intent: TerminalIntent) -> &'static str {
    match intent {
        TerminalIntent::Complete => "complete",
        TerminalIntent::Fail => "fail",
    }
}

fn parse_terminal_intent(value: &str) -> Result<TerminalIntent, StateError> {
    match value {
        "complete" => Ok(TerminalIntent::Complete),
        "fail" => Ok(TerminalIntent::Fail),
        _ => Err(StateError::Database("invalid terminal intent".into())),
    }
}

fn run_status_name(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Starting => "starting",
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Skipped => "skipped",
        RunStatus::Cancelled => "cancelled",
        RunStatus::Interrupted => "interrupted",
    }
}

fn parse_run_status(value: &str) -> Result<RunStatus, StateError> {
    match value {
        "queued" => Ok(RunStatus::Queued),
        "starting" => Ok(RunStatus::Starting),
        "running" => Ok(RunStatus::Running),
        "succeeded" => Ok(RunStatus::Succeeded),
        "failed" => Ok(RunStatus::Failed),
        "skipped" => Ok(RunStatus::Skipped),
        "cancelled" => Ok(RunStatus::Cancelled),
        "interrupted" => Ok(RunStatus::Interrupted),
        _ => Err(StateError::Database(
            "invalid post-processing run status".into(),
        )),
    }
}

fn attempt_status_name(status: AttemptStatus) -> &'static str {
    match status {
        AttemptStatus::Queued => "queued",
        AttemptStatus::Starting => "starting",
        AttemptStatus::Running => "running",
        AttemptStatus::Succeeded => "succeeded",
        AttemptStatus::Failed => "failed",
        AttemptStatus::TimedOut => "timed_out",
        AttemptStatus::Skipped => "skipped",
        AttemptStatus::Cancelled => "cancelled",
        AttemptStatus::Interrupted => "interrupted",
    }
}

fn parse_attempt_status(value: &str) -> Result<AttemptStatus, StateError> {
    match value {
        "queued" => Ok(AttemptStatus::Queued),
        "starting" => Ok(AttemptStatus::Starting),
        "running" => Ok(AttemptStatus::Running),
        "succeeded" => Ok(AttemptStatus::Succeeded),
        "failed" => Ok(AttemptStatus::Failed),
        "timed_out" => Ok(AttemptStatus::TimedOut),
        "skipped" => Ok(AttemptStatus::Skipped),
        "cancelled" => Ok(AttemptStatus::Cancelled),
        "interrupted" => Ok(AttemptStatus::Interrupted),
        _ => Err(StateError::Database(
            "invalid post-processing attempt status".into(),
        )),
    }
}

fn summary_name(summary: &PostProcessingSummary) -> &'static str {
    match summary {
        PostProcessingSummary::NotRun => "not_run",
        PostProcessingSummary::Succeeded => "succeeded",
        PostProcessingSummary::Warning => "warning",
        PostProcessingSummary::Failed => "failed",
        PostProcessingSummary::Cancelled => "cancelled",
        PostProcessingSummary::Interrupted => "interrupted",
    }
}

fn parse_summary(value: &str) -> Result<PostProcessingSummary, StateError> {
    match value {
        "not_run" => Ok(PostProcessingSummary::NotRun),
        "succeeded" => Ok(PostProcessingSummary::Succeeded),
        "warning" => Ok(PostProcessingSummary::Warning),
        "failed" => Ok(PostProcessingSummary::Failed),
        "cancelled" => Ok(PostProcessingSummary::Cancelled),
        "interrupted" => Ok(PostProcessingSummary::Interrupted),
        _ => Err(StateError::Database(
            "invalid post-processing summary".into(),
        )),
    }
}

fn parse_adapter(value: &str) -> Result<ExtensionAdapter, StateError> {
    match value {
        "native" => Ok(ExtensionAdapter::Native),
        "sabnzbd" => Ok(ExtensionAdapter::Sabnzbd),
        "webhook" => Ok(ExtensionAdapter::Webhook),
        "nzbget" => Ok(ExtensionAdapter::Nzbget),
        _ => Err(StateError::Database("invalid extension adapter".into())),
    }
}

fn log_stream_name(stream: LogStream) -> &'static str {
    match stream {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
        LogStream::System => "system",
    }
}

fn parse_log_stream(value: &str) -> Result<LogStream, StateError> {
    match value {
        "stdout" => Ok(LogStream::Stdout),
        "stderr" => Ok(LogStream::Stderr),
        "system" => Ok(LogStream::System),
        _ => Err(StateError::Database("invalid log stream".into())),
    }
}

fn run_record_from_row(
    row: SqlRow,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<PostProcessingRunRecord, StateError> {
    let job_id = u64::try_from(row.i64("job_id")?)
        .map_err(|_| StateError::Database("invalid post-processing job id".into()))?;
    Ok(PostProcessingRunRecord {
        run_id: RunId::new(row.text("run_id")?).map_err(state_err)?,
        job_id,
        status: parse_run_status(&row.text("status")?)?,
        pipeline_outcome: from_json(&row.text("pipeline_outcome_json")?)?,
        summary: parse_summary(&row.text("summary")?)?,
        terminal_intent: parse_terminal_intent(&row.text("terminal_intent")?)?,
        plan: decode_plan(
            &row.text("plan_json")?,
            &row.text("secret_options_json")?,
            key,
        )?,
        rerun_of_run_id: row
            .opt_text("rerun_of_run_id")?
            .map(RunId::new)
            .transpose()
            .map_err(state_err)?,
        queued_at_epoch_ms: row.i64("queued_at_epoch_ms")?,
        queue_position: row.i64("queue_position")?,
        started_at_epoch_ms: row.opt_i64("started_at_epoch_ms")?,
        finished_at_epoch_ms: row.opt_i64("finished_at_epoch_ms")?,
    })
}

fn attempt_record_from_row(row: SqlRow) -> Result<PostProcessingAttemptRecord, StateError> {
    Ok(PostProcessingAttemptRecord {
        attempt_id: AttemptId::new(row.text("attempt_id")?).map_err(state_err)?,
        run_id: RunId::new(row.text("run_id")?).map_err(state_err)?,
        step_index: u32::try_from(row.i64("step_index")?)
            .map_err(|_| StateError::Database("invalid attempt step index".into()))?,
        status: parse_attempt_status(&row.text("status")?)?,
        extension_id: ExtensionId::new(row.text("extension_id")?).map_err(state_err)?,
        revision_id: ExtensionRevisionId::new(row.text("revision_id")?).map_err(state_err)?,
        adapter: parse_adapter(&row.text("adapter")?)?,
        working_directory: row.opt_text("working_directory")?,
        exit_code: row.opt_i32("exit_code")?,
        error_message: row.opt_text("error_message")?,
        progress: row.opt_json("progress_json")?,
        output_truncated: row.bool("output_truncated")?,
        queued_at_epoch_ms: row.i64("queued_at_epoch_ms")?,
        started_at_epoch_ms: row.opt_i64("started_at_epoch_ms")?,
        finished_at_epoch_ms: row.opt_i64("finished_at_epoch_ms")?,
    })
}

fn log_chunk_from_row(row: SqlRow) -> Result<PostProcessingLogChunk, StateError> {
    Ok(PostProcessingLogChunk {
        sequence: u64::try_from(row.i64("sequence")?)
            .map_err(|_| StateError::Database("invalid log sequence".into()))?,
        stream: parse_log_stream(&row.text("stream")?)?,
        payload: row.bytes("payload")?,
        created_at_epoch_ms: row.i64("created_at_epoch_ms")?,
    })
}

fn extension_record_from_row(row: SqlRow) -> Result<ExtensionRevisionRecord, StateError> {
    let manifest: ExtensionManifest = from_json(&row.text("manifest_json")?)?;
    Ok(ExtensionRevisionRecord {
        manifest,
        trust_state: parse_trust_state(&row.text("trust_state")?)?,
        managed_path: row.opt_text("managed_path")?,
        source_path: row.opt_text("discovered_source_path")?,
        discovered_at_epoch_ms: row.i64("discovered_at_epoch_ms")?,
        approved_at_epoch_ms: row.opt_i64("approved_at_epoch_ms")?,
    })
}

fn profile_step_to_row(
    step: &OrderedStep,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<StoredProfileStepRow, StateError> {
    let (options, secrets) = encode_options(step.index(), step.options(), key)?;
    Ok(StoredProfileStepRow {
        index: step.index(),
        selection_json: to_json(step.selection())?,
        policy_json: to_json(&StoredPolicy {
            run_when: step.run_when(),
            on_failure: step.on_failure(),
            outcome_impact: step.outcome_impact(),
            timeout_policy: step.timeout_policy(),
            approved_roots: step.approved_roots().clone(),
            artifact_condition: step.artifact_condition().clone(),
        })?,
        options_json: to_json(&options)?,
        secret_options_json: to_json(&secrets)?,
    })
}

fn profile_step_from_row(
    row: &SqlRow,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<OrderedStep, StateError> {
    let index = u32::try_from(row.i64("step_index")?)
        .map_err(|_| StateError::Database("invalid profile step index".into()))?;
    let selection: ExtensionSelection = from_json(&row.text("selection_json")?)?;
    let policy: StoredPolicy = from_json(&row.text("policy_json")?)?;
    let mut options: Vec<ResolvedOption> = from_json(&row.text("options_json")?)?;
    options.extend(decode_options(
        index,
        &row.text("secret_options_json")?,
        key,
    )?);
    let StoredPolicy {
        run_when,
        on_failure,
        outcome_impact,
        timeout_policy,
        approved_roots,
        artifact_condition,
    } = policy;
    OrderedStep::new(
        index,
        selection,
        run_when,
        on_failure,
        outcome_impact,
        timeout_policy,
        approved_roots,
        options,
    )
    .map_err(state_err)
    .map(|step| step.with_artifact_condition(artifact_condition))
}

fn encode_plan(
    plan: &FrozenPlan,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<(String, String), StateError> {
    let mut secrets = Vec::new();
    let mut steps = Vec::with_capacity(plan.steps().len());
    for step in plan.steps() {
        let (options, step_secrets) = encode_options(step.index(), step.options(), key)?;
        secrets.extend(step_secrets);
        steps.push(StoredPlanStep {
            index: step.index(),
            revision: step.revision().clone(),
            policy: StoredPolicy {
                run_when: step.run_when(),
                on_failure: step.on_failure(),
                outcome_impact: step.outcome_impact(),
                timeout_policy: step.timeout_policy(),
                approved_roots: step.approved_roots().clone(),
                artifact_condition: step.artifact_condition().clone(),
            },
            options,
        });
    }
    Ok((
        to_json(&StoredPlan {
            provenance: plan.provenance().clone(),
            steps,
        })?,
        to_json(&secrets)?,
    ))
}

fn decode_plan(
    plan_json: &str,
    secret_options_json: &str,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<FrozenPlan, StateError> {
    let stored: StoredPlan = from_json(plan_json)?;
    let encrypted: Vec<StoredSecretOption> = from_json(secret_options_json)?;
    let mut steps = Vec::with_capacity(stored.steps.len());
    for stored_step in stored.steps {
        let mut options = stored_step.options;
        for secret in encrypted
            .iter()
            .filter(|secret| secret.step_index == stored_step.index)
        {
            let key = key.ok_or_else(|| {
                StateError::Database("encryption key is required to load secret options".into())
            })?;
            let value = decrypt_value(key, &secret.ciphertext).map_err(state_err)?;
            options.push(ResolvedOption::new(
                secret.name.clone(),
                ResolvedOptionValue::Secret(super::model::SecretOptionValue::for_execution(value)),
            ));
        }
        let StoredPolicy {
            run_when,
            on_failure,
            outcome_impact,
            timeout_policy,
            approved_roots,
            artifact_condition,
        } = stored_step.policy;
        steps.push(
            FrozenPlanStep::new(
                stored_step.index,
                stored_step.revision,
                run_when,
                on_failure,
                outcome_impact,
                timeout_policy,
                approved_roots,
                options,
            )
            .map_err(state_err)?
            .with_artifact_condition(artifact_condition),
        );
    }
    FrozenPlan::new(stored.provenance, steps).map_err(state_err)
}

fn encode_options(
    step_index: u32,
    options: &[ResolvedOption],
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<(Vec<ResolvedOption>, Vec<StoredSecretOption>), StateError> {
    let mut plain = Vec::new();
    let mut secrets = Vec::new();
    for option in options {
        match option.value() {
            ResolvedOptionValue::Secret(value) => {
                let key = key.ok_or_else(|| {
                    StateError::Database(
                        "encryption key is required to store secret options".into(),
                    )
                })?;
                let ciphertext =
                    encrypt_value(key, value.expose_for_execution()).map_err(state_err)?;
                secrets.push(StoredSecretOption {
                    step_index,
                    name: option.name().clone(),
                    ciphertext,
                });
            }
            _ => plain.push(option.clone()),
        }
    }
    Ok((plain, secrets))
}

fn decode_options(
    step_index: u32,
    json: &str,
    key: Option<&crate::persistence::encryption::EncryptionKey>,
) -> Result<Vec<ResolvedOption>, StateError> {
    let secrets: Vec<StoredSecretOption> = from_json(json)?;
    secrets
        .into_iter()
        .filter(|secret| secret.step_index == step_index)
        .map(|secret| {
            let key = key.ok_or_else(|| {
                StateError::Database("encryption key is required to load secret options".into())
            })?;
            let plaintext = decrypt_value(key, &secret.ciphertext).map_err(state_err)?;
            Ok(ResolvedOption::new(
                secret.name,
                ResolvedOptionValue::Secret(super::model::SecretOptionValue::for_execution(
                    plaintext,
                )),
            ))
        })
        .collect()
}

fn adapter_name(adapter: ExtensionAdapter) -> &'static str {
    match adapter {
        ExtensionAdapter::Native => "native",
        ExtensionAdapter::Sabnzbd => "sabnzbd",
        ExtensionAdapter::Nzbget => "nzbget",
        ExtensionAdapter::Webhook => "webhook",
    }
}

fn parse_trust_state(value: &str) -> Result<TrustState, StateError> {
    match value {
        "unapproved" => Ok(TrustState::Unapproved),
        "approved" => Ok(TrustState::Approved),
        "revoked" => Ok(TrustState::Revoked),
        _ => Err(StateError::Database(
            "invalid stored extension trust state".into(),
        )),
    }
}

fn job_id_i64(job_id: u64) -> Result<i64, StateError> {
    i64::try_from(job_id).map_err(|_| StateError::Database("job id exceeds SQL range".into()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn to_json<T: Serialize>(value: &T) -> Result<String, StateError> {
    serde_json::to_string(value).map_err(state_err)
}

fn from_json<T: for<'de> Deserialize<'de>>(value: &str) -> Result<T, StateError> {
    serde_json::from_str(value).map_err(state_err)
}

fn state_err(error: impl std::fmt::Display) -> StateError {
    StateError::Database(error.to_string())
}

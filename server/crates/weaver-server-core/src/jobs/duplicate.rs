//! Stable duplicate-detection identities and admission policy.
//!
//! These values deliberately describe a validated [`JobSpec`] instead of the
//! submitted XML bytes.  The raw NZB hash remains an archival integrity value;
//! it is not reused as a duplicate identity.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{LazyLock, Mutex};

use serde::{Deserialize, Serialize};
use unicode_normalization::UnicodeNormalization;

use super::{JobId, JobSpec};

pub const FINGERPRINT_VERSION: u16 = 1;

static DUPLICATE_ADMISSION_METRICS: LazyLock<Mutex<BTreeMap<(&'static str, &'static str), u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
static SEMANTIC_DUPLICATE_LIFECYCLE_METRICS: LazyLock<Mutex<BTreeMap<&'static str, u64>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateAdmissionMetric {
    pub origin: &'static str,
    pub status: &'static str,
    pub count: u64,
}

/// Fixed semantic-arbitration lifecycle events. These are intentionally a
/// closed allowlist so Prometheus never receives a release name, key, digest,
/// job identifier, or caller identity as a label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticDuplicateLifecycleEvent {
    Park,
    Supersede,
    Promote,
    PromotionRecovery,
    PromotionRetry,
    PromotionFailure,
    PromotionNoFallback,
}

impl SemanticDuplicateLifecycleEvent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Park => "park",
            Self::Supersede => "supersede",
            Self::Promote => "promote",
            Self::PromotionRecovery => "promotion_recovery",
            Self::PromotionRetry => "promotion_retry",
            Self::PromotionFailure => "promotion_failure",
            Self::PromotionNoFallback => "promotion_no_fallback",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticDuplicateLifecycleMetric {
    pub event: &'static str,
    pub count: u64,
}

pub fn record_duplicate_admission_metric(origin: &SubmissionOrigin, status: &'static str) {
    let mut metrics = DUPLICATE_ADMISSION_METRICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *metrics.entry((origin.as_str(), status)).or_insert(0) += 1;
}

pub fn duplicate_admission_metrics_snapshot() -> Vec<DuplicateAdmissionMetric> {
    DUPLICATE_ADMISSION_METRICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .map(|(&(origin, status), &count)| DuplicateAdmissionMetric {
            origin,
            status,
            count,
        })
        .collect()
}

pub fn record_semantic_duplicate_lifecycle_metric(event: SemanticDuplicateLifecycleEvent) {
    let mut metrics = SEMANTIC_DUPLICATE_LIFECYCLE_METRICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *metrics.entry(event.as_str()).or_insert(0) += 1;
}

pub fn semantic_duplicate_lifecycle_metrics_snapshot() -> Vec<SemanticDuplicateLifecycleMetric> {
    SEMANTIC_DUPLICATE_LIFECYCLE_METRICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .map(|(&event, &count)| SemanticDuplicateLifecycleMetric { event, count })
        .collect()
}

/// The independently versioned shapes used to compare two validated NZBs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FingerprintKind {
    StrictArticleLayout,
    ArticleLayout,
    ArticleSet,
    NormalizedName,
}

impl FingerprintKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StrictArticleLayout => "strict-article-layout",
            Self::ArticleLayout => "article-layout",
            Self::ArticleSet => "article-set",
            Self::NormalizedName => "normalized-name",
        }
    }

    pub const fn domain(self) -> &'static str {
        match self {
            Self::StrictArticleLayout => "weaver.job-fingerprint/strict-article-layout/v1",
            Self::ArticleLayout => "weaver.job-fingerprint/article-layout/v1",
            Self::ArticleSet => "weaver.job-fingerprint/article-set/v1",
            Self::NormalizedName => "weaver.job-fingerprint/normalized-name/v1",
        }
    }

    pub const fn all() -> [Self; 4] {
        [
            Self::StrictArticleLayout,
            Self::ArticleLayout,
            Self::ArticleSet,
            Self::NormalizedName,
        ]
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "strict-article-layout" => Some(Self::StrictArticleLayout),
            "article-layout" => Some(Self::ArticleLayout),
            "article-set" => Some(Self::ArticleSet),
            "normalized-name" => Some(Self::NormalizedName),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobFingerprint {
    pub kind: FingerprintKind,
    pub version: u16,
    pub digest: [u8; 32],
}

impl JobFingerprint {
    pub fn from_validated_spec(spec: &JobSpec) -> Vec<Self> {
        let normalized_name = normalize_name(&spec.name);
        vec![
            Self::strict_article_layout(spec),
            Self::article_layout(spec),
            Self::article_set(spec),
            Self::normalized_name(&normalized_name),
        ]
    }

    fn strict_article_layout(spec: &JobSpec) -> Self {
        let mut roots = spec
            .files
            .iter()
            .map(|file| {
                let mut hasher =
                    framed_hasher("weaver.job-fingerprint/strict-article-layout/file/v1");
                write_count(&mut hasher, file.segments.len());
                for segment in &file.segments {
                    write_string(&mut hasher, &segment.message_id);
                    // The parser validates and normalizes this u32 declared
                    // byte count. Encode it as a fixed-width u64 to keep the
                    // layout forward-compatible without platform dependence.
                    hasher.update(&u64::from(segment.bytes).to_be_bytes());
                }
                *hasher.finalize().as_bytes()
            })
            .collect::<Vec<_>>();
        roots.sort_unstable();

        let mut hasher = framed_hasher(FingerprintKind::StrictArticleLayout.domain());
        write_count(&mut hasher, roots.len());
        for root in roots {
            hasher.update(&root);
        }
        Self::new(FingerprintKind::StrictArticleLayout, hasher)
    }

    fn article_layout(spec: &JobSpec) -> Self {
        let mut roots = spec
            .files
            .iter()
            .map(|file| {
                let mut hasher = framed_hasher("weaver.job-fingerprint/article-layout/file/v1");
                write_count(&mut hasher, file.segments.len());
                for segment in &file.segments {
                    write_string(&mut hasher, &segment.message_id);
                }
                *hasher.finalize().as_bytes()
            })
            .collect::<Vec<_>>();
        roots.sort_unstable();

        let mut hasher = framed_hasher(FingerprintKind::ArticleLayout.domain());
        write_count(&mut hasher, roots.len());
        for root in roots {
            hasher.update(&root);
        }
        Self::new(FingerprintKind::ArticleLayout, hasher)
    }

    fn article_set(spec: &JobSpec) -> Self {
        let ids = spec
            .files
            .iter()
            .flat_map(|file| {
                file.segments
                    .iter()
                    .map(|segment| segment.message_id.as_str())
            })
            .collect::<BTreeSet<_>>();
        let mut hasher = framed_hasher(FingerprintKind::ArticleSet.domain());
        write_count(&mut hasher, ids.len());
        for message_id in ids {
            write_string(&mut hasher, message_id);
        }
        Self::new(FingerprintKind::ArticleSet, hasher)
    }

    fn normalized_name(name: &str) -> Self {
        let mut hasher = framed_hasher(FingerprintKind::NormalizedName.domain());
        write_string(&mut hasher, name);
        Self::new(FingerprintKind::NormalizedName, hasher)
    }

    fn new(kind: FingerprintKind, hasher: blake3::Hasher) -> Self {
        Self {
            kind,
            version: FINGERPRINT_VERSION,
            digest: *hasher.finalize().as_bytes(),
        }
    }
}

fn framed_hasher(domain: &str) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new();
    // The fixed prefix prevents an accidental collision with another framed
    // BLAKE3 protocol that happens to reuse a domain string.
    hasher.update(b"weaver.fingerprint.frame\0");
    write_string(&mut hasher, domain);
    hasher
}

fn write_count(hasher: &mut blake3::Hasher, count: usize) {
    hasher.update(&(count as u64).to_be_bytes());
}

fn write_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

pub fn normalize_name(name: &str) -> String {
    name.chars()
        .flat_map(char::to_lowercase)
        .map(|ch| if ch.is_alphanumeric() { ch } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DuplicateAction {
    Accept,
    Warn,
    Pause,
    Block,
}

impl DuplicateAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Warn => "warn",
            Self::Pause => "pause",
            Self::Block => "block",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "accept" => Some(Self::Accept),
            "warn" => Some(Self::Warn),
            "pause" => Some(Self::Pause),
            "block" => Some(Self::Block),
            _ => None,
        }
    }

    fn severity(self) -> u8 {
        match self {
            Self::Accept => 0,
            Self::Warn => 1,
            Self::Pause => 2,
            Self::Block => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateMode {
    /// Apply only the independent article-fingerprint policy.
    Enforce,
    /// Arbitrate candidates sharing an explicit source-supplied semantic key.
    Score,
    /// Do not group candidates, while retaining article-fingerprint policy.
    All,
    /// Bypass semantic and fingerprint policy, never caller idempotency.
    Force,
}

impl DuplicateMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enforce => "enforce",
            Self::Score => "score",
            Self::All => "all",
            Self::Force => "force",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "enforce" | "normal" => Some(Self::Enforce),
            "score" => Some(Self::Score),
            "all" => Some(Self::All),
            "force" => Some(Self::Force),
            _ => None,
        }
    }

    pub const fn enables_semantic_arbitration(self) -> bool {
        matches!(self, Self::Score)
    }
}

/// A source-supplied grouping key and score. The key is deliberately separate
/// from caller-scoped idempotency: multiple independently submitted releases
/// can legitimately compete for one semantic group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticDuplicate {
    pub normalized_key: String,
    pub score: i64,
}

impl SemanticDuplicate {
    pub fn from_source(key: impl AsRef<str>, score: i64) -> Option<Self> {
        normalize_semantic_duplicate_key(key.as_ref()).map(|normalized_key| Self {
            normalized_key,
            score,
        })
    }
}

/// Normalizes a semantic source key using Unicode NFKC followed by Unicode
/// lowercase folding. Empty keys have no semantic authority.
pub fn normalize_semantic_duplicate_key(value: &str) -> Option<String> {
    let normalized = value.trim().nfkc().collect::<String>();
    let normalized = unicase::UniCase::new(normalized).to_folded_case();
    let normalized = normalized.trim();
    (!normalized.is_empty()).then(|| normalized.to_string())
}

/// Durable classification for a terminal SCORE candidate. The raw pipeline
/// error remains in history; this small taxonomy is the only policy input for
/// automatic candidate promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SemanticTerminalCause {
    Success,
    MissingArticlesOrLowHealth,
    DecodeOrCorruptData,
    RepairFailure,
    ArchiveOrUnpackFailure,
    PasswordFailure,
    DiskOrPermissionFailure,
    ServerAuthQuotaOrOutage,
    Shutdown,
    UserCancelled,
    UnknownFailure,
}

impl SemanticTerminalCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::MissingArticlesOrLowHealth => "missing-articles-or-low-health",
            Self::DecodeOrCorruptData => "decode-or-corrupt-data",
            Self::RepairFailure => "repair-failure",
            Self::ArchiveOrUnpackFailure => "archive-or-unpack-failure",
            Self::PasswordFailure => "password-failure",
            Self::DiskOrPermissionFailure => "disk-or-permission-failure",
            Self::ServerAuthQuotaOrOutage => "server-auth-quota-or-outage",
            Self::Shutdown => "shutdown",
            Self::UserCancelled => "user-cancelled",
            Self::UnknownFailure => "unknown-failure",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "missing-articles-or-low-health" => Some(Self::MissingArticlesOrLowHealth),
            "decode-or-corrupt-data" => Some(Self::DecodeOrCorruptData),
            "repair-failure" => Some(Self::RepairFailure),
            "archive-or-unpack-failure" => Some(Self::ArchiveOrUnpackFailure),
            "password-failure" => Some(Self::PasswordFailure),
            "disk-or-permission-failure" => Some(Self::DiskOrPermissionFailure),
            "server-auth-quota-or-outage" => Some(Self::ServerAuthQuotaOrOutage),
            "shutdown" => Some(Self::Shutdown),
            "user-cancelled" => Some(Self::UserCancelled),
            "unknown-failure" => Some(Self::UnknownFailure),
            _ => None,
        }
    }

    pub const fn is_promotable(self) -> bool {
        matches!(
            self,
            Self::MissingArticlesOrLowHealth
                | Self::DecodeOrCorruptData
                | Self::RepairFailure
                | Self::ArchiveOrUnpackFailure
                | Self::PasswordFailure
        )
    }
}

/// Classifies the terminal pipeline result once at the history boundary. This
/// intentionally does not become a caller-controlled string protocol.
pub fn classify_semantic_terminal_cause(
    status: &str,
    raw_error: Option<&str>,
    health: u32,
    failed_bytes: u64,
) -> SemanticTerminalCause {
    match status {
        "complete" | "succeeded" => return SemanticTerminalCause::Success,
        "cancelled" => return SemanticTerminalCause::UserCancelled,
        _ => {}
    }

    let error = raw_error.unwrap_or_default().to_ascii_lowercase();
    let contains = |needles: &[&str]| needles.iter().any(|needle| error.contains(needle));
    if contains(&["shutdown", "shutting down", "interrupted by signal"]) {
        SemanticTerminalCause::Shutdown
    } else if contains(&[
        "permission denied",
        "read-only",
        "no space",
        "disk full",
        "write error",
        "move error",
        "rename error",
        "i/o error",
    ]) {
        SemanticTerminalCause::DiskOrPermissionFailure
    } else if contains(&[
        "authentication",
        "authorization",
        "unauthorized",
        "access denied",
        "quota",
        "outage",
        "server unavailable",
        "connection refused",
        "connection reset",
        "connection timed out",
        "connection timeout",
        "timed out",
        "timeout",
    ]) {
        SemanticTerminalCause::ServerAuthQuotaOrOutage
    } else if contains(&["password", "passphrase", "encrypted archive"]) {
        SemanticTerminalCause::PasswordFailure
    } else if contains(&["par2", "repair failed", "repair error"]) {
        SemanticTerminalCause::RepairFailure
    } else if contains(&["unpack", "extract", "archive", "unrar", "7z", "zip"]) {
        SemanticTerminalCause::ArchiveOrUnpackFailure
    } else if contains(&["decode", "yenc", "corrupt", "crc mismatch", "checksum"]) {
        SemanticTerminalCause::DecodeOrCorruptData
    } else if failed_bytes > 0
        || health < 1_000
        || contains(&[
            "missing article",
            "article not found",
            "incomplete",
            "low health",
        ])
    {
        SemanticTerminalCause::MissingArticlesOrLowHealth
    } else {
        SemanticTerminalCause::UnknownFailure
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DuplicateJobLifecycle {
    Reserved,
    Active,
    Parked,
    Succeeded,
    Failed,
    Cancelled,
}

impl DuplicateJobLifecycle {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Active => "active",
            Self::Parked => "parked",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "reserved" => Some(Self::Reserved),
            "active" => Some(Self::Active),
            "parked" => Some(Self::Parked),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub const fn is_active_or_success(self) -> bool {
        matches!(self, Self::Reserved | Self::Active | Self::Succeeded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateJobSnapshot {
    pub job_id: JobId,
    pub lifecycle: DuplicateJobLifecycle,
    pub normalized_name: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub reservation_expires_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateMatch {
    pub job_id: JobId,
    pub fingerprint: JobFingerprint,
    pub lifecycle: DuplicateJobLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateDecision {
    pub action: DuplicateAction,
    pub matches: Vec<DuplicateMatch>,
    pub force_bypassed: bool,
}

impl DuplicateDecision {
    pub fn accept() -> Self {
        Self {
            action: DuplicateAction::Accept,
            matches: Vec::new(),
            force_bypassed: false,
        }
    }
}

/// Default policy accepted for the first duplicate-detection rollout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuplicatePolicy {
    #[serde(default = "default_strict_active_or_success_action")]
    pub strict_active_or_success: DuplicateAction,
    #[serde(default = "default_strict_failed_or_cancelled_action")]
    pub strict_failed_or_cancelled: DuplicateAction,
    #[serde(default = "default_article_layout_active_or_success_action")]
    pub article_layout_active_or_success: DuplicateAction,
    #[serde(default = "default_article_layout_failed_or_cancelled_action")]
    pub article_layout_failed_or_cancelled: DuplicateAction,
    #[serde(default = "default_warn_action")]
    pub article_set: DuplicateAction,
    #[serde(default = "default_warn_action")]
    pub normalized_name: DuplicateAction,
}

impl Default for DuplicatePolicy {
    fn default() -> Self {
        Self {
            strict_active_or_success: default_strict_active_or_success_action(),
            strict_failed_or_cancelled: default_strict_failed_or_cancelled_action(),
            article_layout_active_or_success: default_article_layout_active_or_success_action(),
            article_layout_failed_or_cancelled: default_article_layout_failed_or_cancelled_action(),
            article_set: default_warn_action(),
            normalized_name: default_warn_action(),
        }
    }
}

impl DuplicatePolicy {
    pub fn action_for(
        self,
        kind: FingerprintKind,
        lifecycle: DuplicateJobLifecycle,
    ) -> DuplicateAction {
        match kind {
            FingerprintKind::StrictArticleLayout if lifecycle.is_active_or_success() => {
                self.strict_active_or_success
            }
            FingerprintKind::StrictArticleLayout => self.strict_failed_or_cancelled,
            FingerprintKind::ArticleLayout if lifecycle.is_active_or_success() => {
                self.article_layout_active_or_success
            }
            FingerprintKind::ArticleLayout => self.article_layout_failed_or_cancelled,
            FingerprintKind::ArticleSet => self.article_set,
            FingerprintKind::NormalizedName => self.normalized_name,
        }
    }

    pub fn decide(self, mode: DuplicateMode, matches: Vec<DuplicateMatch>) -> DuplicateDecision {
        if matches.is_empty() {
            return DuplicateDecision::accept();
        }
        if matches!(mode, DuplicateMode::Force) {
            return DuplicateDecision {
                action: DuplicateAction::Accept,
                matches,
                force_bypassed: true,
            };
        }

        let action = matches
            .iter()
            .map(|candidate| self.action_for(candidate.fingerprint.kind, candidate.lifecycle))
            .max_by_key(|action| action.severity())
            .unwrap_or(DuplicateAction::Accept);
        DuplicateDecision {
            action,
            matches,
            force_bypassed: false,
        }
    }
}

const fn default_strict_active_or_success_action() -> DuplicateAction {
    DuplicateAction::Block
}

const fn default_strict_failed_or_cancelled_action() -> DuplicateAction {
    DuplicateAction::Warn
}

const fn default_article_layout_active_or_success_action() -> DuplicateAction {
    DuplicateAction::Pause
}

const fn default_article_layout_failed_or_cancelled_action() -> DuplicateAction {
    DuplicateAction::Warn
}

const fn default_warn_action() -> DuplicateAction {
    DuplicateAction::Warn
}

/// Classifies the submitting boundary without making external transport text an
/// untyped policy input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionOrigin {
    Api,
    NzbGet,
    Rss,
    WatchFolder,
    Cli,
    InternalRedownload,
    Recovery,
}

impl SubmissionOrigin {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::NzbGet => "nzbget",
            Self::Rss => "rss",
            Self::WatchFolder => "watch-folder",
            Self::Cli => "cli",
            Self::InternalRedownload => "internal-redownload",
            Self::Recovery => "recovery",
        }
    }

    pub const fn bypasses_duplicate_policy(&self) -> bool {
        matches!(self, Self::InternalRedownload | Self::Recovery)
    }
}

/// An idempotency key only has meaning within the submitting caller's scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerScopedIdempotency {
    pub caller: String,
    pub key: String,
}

impl CallerScopedIdempotency {
    pub fn new(caller: impl Into<String>, key: impl Into<String>) -> Option<Self> {
        let caller = caller.into();
        let key = key.into();
        (!caller.trim().is_empty() && !key.trim().is_empty()).then_some(Self { caller, key })
    }
}

#[derive(Debug, Clone)]
pub struct FingerprintEvidence {
    pub raw_job_hash: [u8; 32],
    pub normalized_name: String,
    pub fingerprints: Vec<JobFingerprint>,
}

impl FingerprintEvidence {
    pub fn from_validated_spec(spec: &JobSpec, raw_job_hash: [u8; 32]) -> Self {
        Self {
            raw_job_hash,
            normalized_name: normalize_name(&spec.name),
            fingerprints: JobFingerprint::from_validated_spec(spec),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jobs::{FileSpec, SegmentSpec};
    use weaver_model::files::FileRole;

    fn spec(files: Vec<Vec<(&str, u32)>>) -> JobSpec {
        JobSpec {
            name: "A.Release-Name".to_string(),
            password: None,
            files: files
                .into_iter()
                .enumerate()
                .map(|(file_index, segments)| FileSpec {
                    filename: format!("file-{file_index}.rar"),
                    role: FileRole::Unknown,
                    groups: vec![],
                    posted_at_epoch: None,
                    segments: segments
                        .into_iter()
                        .enumerate()
                        .map(|(ordinal, (message_id, bytes))| SegmentSpec {
                            ordinal: ordinal as u32,
                            article_number: ordinal as u32 + 1,
                            bytes,
                            message_id: message_id.to_string(),
                        })
                        .collect(),
                })
                .collect(),
            total_bytes: 0,
            category: None,
            metadata: vec![],
        }
    }

    fn fingerprints(spec: &JobSpec) -> Vec<JobFingerprint> {
        JobFingerprint::from_validated_spec(spec)
    }

    #[test]
    fn strict_layout_uses_bytes_but_layout_does_not() {
        let original = spec(vec![vec![("Case@Id", 10), ("two@Id", 20)]]);
        let changed_bytes = spec(vec![vec![("Case@Id", 11), ("two@Id", 20)]]);
        let a = fingerprints(&original);
        let b = fingerprints(&changed_bytes);
        assert_ne!(a[0], b[0]);
        assert_eq!(a[1], b[1]);
        assert_eq!(a[2], b[2]);
    }

    #[test]
    fn file_order_is_ignored_but_file_multiplicity_is_not() {
        let first = spec(vec![vec![("one@Id", 1)], vec![("two@Id", 2)]]);
        let reordered = spec(vec![vec![("two@Id", 2)], vec![("one@Id", 1)]]);
        let duplicated = spec(vec![vec![("one@Id", 1)], vec![("one@Id", 1)]]);
        assert_eq!(fingerprints(&first)[0], fingerprints(&reordered)[0]);
        assert_ne!(fingerprints(&first)[0], fingerprints(&duplicated)[0]);
    }

    #[test]
    fn article_set_is_order_independent_and_deduplicated() {
        let first = spec(vec![vec![("one@Id", 1), ("two@Id", 2)]]);
        let reordered = spec(vec![
            vec![("two@Id", 2)],
            vec![("one@Id", 1), ("one@Id", 1)],
        ]);
        assert_eq!(fingerprints(&first)[2], fingerprints(&reordered)[2]);
    }

    #[test]
    fn semantic_key_uses_nfkc_and_full_unicode_case_folding() {
        assert_eq!(
            normalize_semantic_duplicate_key("  ＳＴＲＡＳＳＥ  "),
            normalize_semantic_duplicate_key("Straße")
        );
        assert_eq!(
            normalize_semantic_duplicate_key("ΟΣ"),
            normalize_semantic_duplicate_key("οσ")
        );
        assert_eq!(
            normalize_semantic_duplicate_key("ΟΣ"),
            normalize_semantic_duplicate_key("ος")
        );
        assert_eq!(normalize_semantic_duplicate_key(" \u{3000} "), None);
    }

    #[test]
    fn policy_matches_accepted_status_matrix() {
        let policy = DuplicatePolicy::default();
        assert_eq!(
            policy.action_for(
                FingerprintKind::StrictArticleLayout,
                DuplicateJobLifecycle::Active
            ),
            DuplicateAction::Block
        );
        assert_eq!(
            policy.action_for(
                FingerprintKind::StrictArticleLayout,
                DuplicateJobLifecycle::Succeeded
            ),
            DuplicateAction::Block
        );
        assert_eq!(
            policy.action_for(
                FingerprintKind::StrictArticleLayout,
                DuplicateJobLifecycle::Failed
            ),
            DuplicateAction::Warn
        );
        assert_eq!(
            policy.action_for(
                FingerprintKind::ArticleLayout,
                DuplicateJobLifecycle::Active
            ),
            DuplicateAction::Pause
        );
        assert_eq!(
            policy.action_for(
                FingerprintKind::ArticleSet,
                DuplicateJobLifecycle::Succeeded
            ),
            DuplicateAction::Warn
        );
    }
}

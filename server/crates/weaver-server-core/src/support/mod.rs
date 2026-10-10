// Support reports: an NZB analysis plus what Weaver did with the job.
//
// The NZB half is [`weaver_nzb::analysis`]. This half appends the job's
// outcome from state Weaver already keeps, under the same rule: nothing in
// a report names the job, a file, a path, a server host or a password.
// Every string is a constant from a fixed list or a generated token.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;

use serde::Serialize;
use weaver_nzb::analysis::{NzbReport, analyze};

use crate::history::JobEvent;
use crate::ingest::decode_persisted_nzb_bytes;
use crate::jobs::model::TerminalDiscardKind;
use crate::jobs::server_attribution::{JobServerContribution, contributions_from_storage};
use crate::jobs::support_facts::JobSupportFacts;
use crate::persistence::Database;
use crate::{JobHistoryRow, JobId, JobInfo, SchedulerHandle, StateError};

// A report in both forms: text to paste, JSON to attach.
#[derive(Debug, Clone)]
pub struct SupportReport {
    pub text: String,
    pub json: String,
}

#[derive(Debug, thiserror::Error)]
pub enum SupportReportError {
    #[error("job {0} not found")]
    NotFound(u64),
    #[error("the NZB for job {0} is no longer stored")]
    NzbUnavailable(u64),
    #[error("the NZB could not be read: {0}")]
    Parse(String),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("support report task failed: {0}")]
    Task(String),
}

// Where the report was made. Every field is a fixed name.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ReportEnvironment {
    pub weaver_version: &'static str,
    pub datastore: &'static str,
    pub os: &'static str,
    pub arch: &'static str,
    pub hardware_profile: Option<&'static str>,
}

impl ReportEnvironment {
    pub fn current(db: &Database, handle: &SchedulerHandle) -> Self {
        Self {
            weaver_version: env!("CARGO_PKG_VERSION"),
            datastore: db.engine_name(),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            hardware_profile: handle
                .hardware_profile_in_force()
                .map(|profile| profile.active.as_str()),
        }
    }
}

// Analyze NZB bytes that nobody submitted: the standalone analyzer.
pub fn analyze_nzb_bytes(
    xml: &[u8],
    environment: &ReportEnvironment,
    now_epoch_secs: u64,
) -> Result<SupportReport, SupportReportError> {
    let nzb = analyze_xml(xml, environment, now_epoch_secs)?;
    Ok(render(&nzb, None, environment))
}

// The support report for one job, live or finished.
pub async fn job_support_report(
    db: &Database,
    handle: &SchedulerHandle,
    job_id: u64,
) -> Result<SupportReport, SupportReportError> {
    let environment = ReportEnvironment::current(db, handle);
    let live = handle.get_job(JobId(job_id)).ok();
    let db = db.clone();
    tokio::task::spawn_blocking(move || {
        build_job_support_report(&db, live, job_id, &environment, now_epoch_secs())
    })
    .await
    .map_err(|error| SupportReportError::Task(error.to_string()))?
}

// The blocking body of [`job_support_report`], with every input explicit.
pub fn build_job_support_report(
    db: &Database,
    live: Option<JobInfo>,
    job_id: u64,
    environment: &ReportEnvironment,
    now_epoch_secs: u64,
) -> Result<SupportReport, SupportReportError> {
    let (job, stored) = match live {
        Some(info) => {
            let stored = db.load_active_job_persisted_nzb(JobId(job_id))?;
            (JobSection::from_live(&info), stored)
        }
        None => {
            let row = db
                .get_job_history(job_id)?
                .ok_or(SupportReportError::NotFound(job_id))?;
            let stored = db.load_history_job_persisted_nzb(job_id)?;
            (JobSection::from_history(&row), stored)
        }
    };
    let mut job = job;
    job.apply_events(&db.get_job_events(job_id)?);
    job.apply_support_facts(&db.load_job_support_facts(JobId(job_id))?, now_epoch_secs);

    let (path, compressed) = stored.ok_or(SupportReportError::NzbUnavailable(job_id))?;
    let raw = match compressed {
        Some(bytes) => bytes,
        None => std::fs::read(&path).map_err(|_| SupportReportError::NzbUnavailable(job_id))?,
    };
    let xml = decode_persisted_nzb_bytes(&raw)
        .map_err(|error| SupportReportError::Parse(error.to_string()))?;
    let nzb = analyze_xml(&xml, environment, now_epoch_secs)?;
    Ok(render(&nzb, Some(&job), environment))
}

fn analyze_xml(
    xml: &[u8],
    environment: &ReportEnvironment,
    now_epoch_secs: u64,
) -> Result<NzbReport, SupportReportError> {
    let (nzb, diagnostics) = weaver_nzb::parse_nzb_with_diagnostics(xml)
        .map_err(|error| SupportReportError::Parse(error.to_string()))?;
    Ok(analyze(&nzb, &diagnostics, now_epoch_secs).with_weaver_version(environment.weaver_version))
}

// The wall clock, for callers that analyze "as of now".
pub fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

#[derive(Serialize)]
struct CombinedReport<'a> {
    nzb: &'a NzbReport,
    job: Option<&'a JobSection>,
    environment: &'a ReportEnvironment,
}

fn render(
    nzb: &NzbReport,
    job: Option<&JobSection>,
    environment: &ReportEnvironment,
) -> SupportReport {
    let combined = CombinedReport {
        nzb,
        job,
        environment,
    };
    let json = serde_json::to_string_pretty(&combined).expect("report types always serialize");
    let mut text = nzb.to_text();
    if let Some(job) = job {
        job.render(&mut text);
    }
    let _ = writeln!(
        text,
        "env weaver {}  db {}  os {}/{}  profile {}",
        environment.weaver_version,
        environment.datastore,
        environment.os,
        environment.arch,
        environment.hardware_profile.unwrap_or("-"),
    );
    SupportReport { text, json }
}

// Where the job stands now, from the queue or from history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobSource {
    Queue,
    History,
}

// A failure message reduced to its cause. The message itself can name files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureVerdict {
    Cancelled,
    Password,
    Par3,
    Repair,
    Extraction,
    Disk,
    Script,
    Checksum,
    MissingArticles,
    Duplicate,
    Other,
}

impl FailureVerdict {
    pub fn classify(message: &str) -> Self {
        let lower = message.to_ascii_lowercase();
        let any = |needles: &[&str]| needles.iter().any(|needle| lower.contains(needle));
        if any(&["cancel"]) {
            Self::Cancelled
        } else if any(&["password", "encrypted", "decrypt"]) {
            Self::Password
        } else if any(&["par3"]) {
            Self::Par3
        } else if any(&["par2", "repair", "recovery"]) {
            Self::Repair
        } else if any(&["extract", "unrar", "7z", "archive", "unpack"]) {
            Self::Extraction
        } else if any(&["no space", "disk", "write", "permission", "read-only"]) {
            Self::Disk
        } else if any(&["script"]) {
            Self::Script
        } else if any(&["crc", "checksum", "hash mismatch"]) {
            Self::Checksum
        } else if any(&[
            "missing",
            "not found",
            "430",
            "incomplete",
            "health",
            "unavailable",
        ]) {
            Self::MissingArticles
        } else if any(&["duplicate"]) {
            Self::Duplicate
        } else {
            Self::Other
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Password => "password",
            Self::Par3 => "par3",
            Self::Repair => "repair",
            Self::Extraction => "extraction",
            Self::Disk => "disk",
            Self::Script => "script",
            Self::Checksum => "checksum",
            Self::MissingArticles => "missing_articles",
            Self::Duplicate => "duplicate",
            Self::Other => "other",
        }
    }
}

// A server's stand-in: `s3` is configured server id 3, never its host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerToken(u32);

impl std::fmt::Display for ServerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

impl Serialize for ServerToken {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderOutcome {
    pub server: ServerToken,
    pub articles: u32,
    pub wire_bytes: u64,
}

// The job's first direct-store demotion.
//
// `reason` is the demotion's stable label. A stored label that is not made
// of lowercase letters, digits and underscores is read back as `unknown`, so
// the row cannot carry text into the report.
#[derive(Debug, Clone, Serialize)]
pub struct DemotionOutcome {
    pub reason: String,
    pub stage: &'static str,
    pub at_epoch_secs: u64,
    pub age_secs: u64,
    pub sets: u32,
}

// A gap's position: the NZB report's file token and the segment's place in
// that file's segment list, counted from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GapToken {
    file: u32,
    segment: u32,
}

impl std::fmt::Display for GapToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "f{:02}#{}", self.file, self.segment)
    }
}

impl Serialize for GapToken {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ServerGapOutcome {
    pub server: ServerToken,
    pub refused: u32,
}

// The job's articles that never arrived.
#[derive(Debug, Clone, Serialize)]
pub struct GapOutcome {
    // Articles no server had.
    pub missing: u32,
    // Segments whose retries or decodes ran out.
    pub failed: u32,
    // Per server, how many of those it was asked for and refused.
    pub servers: Vec<ServerGapOutcome>,
    // The first gaps booked, in position order.
    pub sample: Vec<GapToken>,
}

impl DemotionOutcome {
    fn of(facts: &JobSupportFacts, now_epoch_secs: u64) -> Option<Self> {
        let demotion = facts.demotion.as_ref()?;
        Some(Self {
            reason: demotion.reason.clone(),
            stage: known_status(&demotion.stage),
            at_epoch_secs: demotion.at_epoch_secs,
            age_secs: now_epoch_secs.saturating_sub(demotion.at_epoch_secs),
            sets: demotion.sets,
        })
    }
}

impl GapOutcome {
    fn of(facts: &JobSupportFacts) -> Option<Self> {
        let gaps = &facts.gaps;
        if gaps.is_empty() {
            return None;
        }
        let mut servers: Vec<ServerGapOutcome> = gaps
            .servers
            .iter()
            .map(|entry| ServerGapOutcome {
                server: ServerToken(entry.0),
                refused: entry.1,
            })
            .collect();
        servers.sort_by_key(|server| std::cmp::Reverse(server.refused));
        Some(Self {
            missing: gaps.missing,
            failed: gaps.failed,
            servers,
            sample: gaps
                .sorted_sample()
                .into_iter()
                .map(|position| GapToken {
                    file: position.0.saturating_add(1),
                    segment: position.1.saturating_add(1),
                })
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StageOutcome {
    NotRun,
    Started,
    Complete,
    Failed,
}

impl StageOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRun => "not run",
            Self::Started => "started",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
}

// How the output directory is written, never what it says.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PathKind {
    None,
    Absolute,
    WindowsDrive,
    Unc,
    Relative,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct OutputShape {
    pub kind: PathKind,
    pub components: u32,
}

impl OutputShape {
    fn of(path: Option<&str>) -> Self {
        let Some(path) = path.map(str::trim).filter(|path| !path.is_empty()) else {
            return Self {
                kind: PathKind::None,
                components: 0,
            };
        };
        let bytes = path.as_bytes();
        let kind = if path.starts_with("\\\\") || path.starts_with("//") {
            PathKind::Unc
        } else if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
            PathKind::WindowsDrive
        } else if path.starts_with('/') {
            PathKind::Absolute
        } else {
            PathKind::Relative
        };
        let components = path
            .split(['/', '\\'])
            .filter(|part| !part.is_empty())
            .count();
        Self {
            kind,
            components: u32::try_from(components).unwrap_or(u32::MAX),
        }
    }

    fn as_text(self) -> String {
        let kind = match self.kind {
            PathKind::None => return "none".to_string(),
            PathKind::Absolute => "absolute",
            PathKind::WindowsDrive => "drive",
            PathKind::Unc => "unc",
            PathKind::Relative => "relative",
        };
        format!("{kind}, {} parts", self.components)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DiscardCount {
    pub kind: &'static str,
    pub files: u32,
    pub bytes: u64,
}

// What the job did, from state Weaver already keeps.
#[derive(Debug, Clone, Serialize)]
pub struct JobSection {
    pub source: JobSource,
    pub status: &'static str,
    pub download_state: Option<&'static str>,
    pub post_state: Option<&'static str>,
    pub run_state: Option<&'static str>,
    pub failure: Option<FailureVerdict>,
    // 0-1000.
    pub health: u32,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub failed_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    pub total_files: Option<u32>,
    pub completed_files: Option<u32>,
    pub remaining_par_files: Option<u32>,
    pub discards: Vec<DiscardCount>,
    pub providers: Vec<ProviderOutcome>,
    pub demotion: Option<DemotionOutcome>,
    pub gaps: Option<GapOutcome>,
    pub verification_runs: u32,
    pub repair: StageOutcome,
    pub extraction: StageOutcome,
    pub extraction_member_failures: u32,
    pub files_reported_missing: u32,
    pub output: OutputShape,
    pub category_set: bool,
    pub password_supplied: bool,
    pub run_secs: Option<u64>,
}

// Statuses a job can be stored with. Anything else reads as `other`.
const KNOWN_STATUSES: &[&str] = &[
    "awaiting_queue_scripts",
    "queued",
    "downloading",
    "checking",
    "verifying",
    "queued_repair",
    "repairing",
    "queued_extract",
    "extracting",
    "moving",
    "queued_post_processing",
    "post_processing",
    "complete",
    "failed",
    "paused",
    "cancelled",
];

fn known_status(status: &str) -> &'static str {
    KNOWN_STATUSES
        .iter()
        .find(|known| **known == status)
        .copied()
        .unwrap_or("other")
}

fn providers(contributions: &[JobServerContribution]) -> Vec<ProviderOutcome> {
    let mut providers: Vec<ProviderOutcome> = contributions
        .iter()
        .map(|contribution| ProviderOutcome {
            server: ServerToken(contribution.server_id),
            articles: contribution.articles,
            wire_bytes: contribution.wire_bytes,
        })
        .collect();
    providers.sort_by_key(|provider| std::cmp::Reverse(provider.articles));
    providers
}

impl JobSection {
    fn empty(source: JobSource, status: &'static str) -> Self {
        Self {
            source,
            status,
            download_state: None,
            post_state: None,
            run_state: None,
            failure: None,
            health: 0,
            total_bytes: 0,
            downloaded_bytes: 0,
            failed_bytes: 0,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            total_files: None,
            completed_files: None,
            remaining_par_files: None,
            discards: Vec::new(),
            providers: Vec::new(),
            demotion: None,
            gaps: None,
            verification_runs: 0,
            repair: StageOutcome::NotRun,
            extraction: StageOutcome::NotRun,
            extraction_member_failures: 0,
            files_reported_missing: 0,
            output: OutputShape::of(None),
            category_set: false,
            password_supplied: false,
            run_secs: None,
        }
    }

    pub fn from_live(info: &JobInfo) -> Self {
        let mut section = Self::empty(JobSource::Queue, info.status.persisted_status());
        section.download_state = Some(info.download_state.as_str());
        section.post_state = Some(info.post_state.as_str());
        section.run_state = Some(info.run_state.as_str());
        section.failure = info.error.as_deref().map(FailureVerdict::classify);
        section.health = info.health;
        section.total_bytes = info.total_bytes;
        section.downloaded_bytes = info.downloaded_bytes;
        section.failed_bytes = info.failed_bytes;
        section.optional_recovery_bytes = info.optional_recovery_bytes;
        section.optional_recovery_downloaded_bytes = info.optional_recovery_downloaded_bytes;
        section.total_files = Some(info.total_files);
        section.completed_files = Some(info.completed_files);
        section.remaining_par_files = Some(info.remaining_par_files);
        for discard in &info.terminal_discards {
            let kind: TerminalDiscardKind = discard.kind;
            match section
                .discards
                .iter_mut()
                .find(|count| count.kind == kind.as_str())
            {
                Some(count) => {
                    count.files += 1;
                    count.bytes += discard.bytes;
                }
                None => section.discards.push(DiscardCount {
                    kind: kind.as_str(),
                    files: 1,
                    bytes: discard.bytes,
                }),
            }
        }
        section.providers = providers(&info.server_attribution);
        section.output = OutputShape::of(info.output_dir.as_deref());
        section.category_set = info.category.as_deref().is_some_and(|c| !c.is_empty());
        section.password_supplied = info.password.as_deref().is_some_and(|p| !p.is_empty());
        section
    }

    pub fn from_history(row: &JobHistoryRow) -> Self {
        let mut section = Self::empty(JobSource::History, known_status(&row.status));
        section.failure = match (row.status.as_str(), row.error_message.as_deref()) {
            ("cancelled", _) => Some(FailureVerdict::Cancelled),
            ("failed", message) => Some(FailureVerdict::classify(message.unwrap_or(""))),
            (_, Some(message)) if !message.trim().is_empty() => {
                Some(FailureVerdict::classify(message))
            }
            _ => None,
        };
        section.health = row.health;
        section.total_bytes = row.total_bytes;
        section.downloaded_bytes = row.downloaded_bytes;
        section.failed_bytes = row.failed_bytes;
        section.optional_recovery_bytes = row.optional_recovery_bytes;
        section.optional_recovery_downloaded_bytes = row.optional_recovery_downloaded_bytes;
        section.providers = providers(&contributions_from_storage(
            row.server_attribution.as_deref(),
        ));
        section.output = OutputShape::of(row.output_dir.as_deref());
        section.category_set = row.category.as_deref().is_some_and(|c| !c.is_empty());
        section.run_secs = (row.completed_at >= row.created_at && row.created_at > 0)
            .then(|| u64::try_from(row.completed_at - row.created_at).unwrap_or(0));
        section
    }

    // Fold the job's recorded events into stage outcomes. Only the event
    // kinds are read; their messages can name files.
    pub fn apply_events(&mut self, events: &[JobEvent]) {
        for event in events {
            match event.kind.as_str() {
                "JobVerificationComplete" | "Par3VerificationComplete" => {
                    self.verification_runs += 1;
                }
                "RepairStarted" => self.repair = advance(self.repair, StageOutcome::Started),
                "RepairComplete" => self.repair = advance(self.repair, StageOutcome::Complete),
                "RepairFailed" => self.repair = advance(self.repair, StageOutcome::Failed),
                "ExtractionReady" | "ExtractionMemberStarted" => {
                    self.extraction = advance(self.extraction, StageOutcome::Started);
                }
                "ExtractionComplete" => {
                    self.extraction = advance(self.extraction, StageOutcome::Complete);
                }
                "ExtractionFailed" => {
                    self.extraction = advance(self.extraction, StageOutcome::Failed);
                }
                "ExtractionMemberFailed" => self.extraction_member_failures += 1,
                "FileMissing" => self.files_reported_missing += 1,
                _ => {}
            }
        }
    }

    // The job's stored demotion and article-gap summary.
    pub fn apply_support_facts(&mut self, facts: &JobSupportFacts, now_epoch_secs: u64) {
        self.demotion = DemotionOutcome::of(facts, now_epoch_secs);
        self.gaps = GapOutcome::of(facts);
    }

    fn render(&self, text: &mut String) {
        let source = match self.source {
            JobSource::Queue => "queue",
            JobSource::History => "history",
        };
        let _ = writeln!(text, "job ({source})");
        let mut status = format!("  status {}", self.status);
        if let Some(failure) = self.failure {
            let _ = write!(status, "  verdict {}", failure.as_str());
        }
        let _ = write!(
            status,
            "  health {}.{}%",
            self.health / 10,
            self.health % 10
        );
        if let Some(secs) = self.run_secs {
            let _ = write!(status, "  ran {secs}s");
        }
        let _ = writeln!(text, "{status}");
        if let (Some(download), Some(post), Some(run)) =
            (self.download_state, self.post_state, self.run_state)
        {
            let _ = writeln!(text, "  states download {download}  post {post}  run {run}");
        }
        let _ = writeln!(
            text,
            "  bytes {} of {}  failed {}  recovery {} of {}",
            human_bytes(self.downloaded_bytes),
            human_bytes(self.total_bytes),
            human_bytes(self.failed_bytes),
            human_bytes(self.optional_recovery_downloaded_bytes),
            human_bytes(self.optional_recovery_bytes),
        );
        if let (Some(total), Some(done), Some(par_left)) = (
            self.total_files,
            self.completed_files,
            self.remaining_par_files,
        ) {
            let mut files = format!("  files {done}/{total} done  par left {par_left}");
            for discard in &self.discards {
                let _ = write!(
                    files,
                    "  discard {} x{} {}",
                    discard.kind,
                    discard.files,
                    human_bytes(discard.bytes)
                );
            }
            let _ = writeln!(text, "{}", clip(&files));
        }
        if self.providers.is_empty() {
            let _ = writeln!(text, "  servers: none attributed");
        } else {
            let mut servers = String::from("  servers");
            for provider in self.providers.iter().take(MAX_SERVERS) {
                let _ = write!(
                    servers,
                    "  {} {} art {}",
                    provider.server,
                    provider.articles,
                    human_bytes(provider.wire_bytes)
                );
            }
            if self.providers.len() > MAX_SERVERS {
                let _ = write!(servers, "  +{}", self.providers.len() - MAX_SERVERS);
            }
            let _ = writeln!(text, "{}", clip(&servers));
        }
        if let Some(demotion) = &self.demotion {
            let _ = writeln!(
                text,
                "{}",
                clip(&format!(
                    "  demoted {}  in {}  sets {}  {} ago",
                    demotion.reason,
                    demotion.stage,
                    demotion.sets,
                    human_age(demotion.age_secs)
                ))
            );
        }
        if let Some(gaps) = &self.gaps {
            let mut line = format!("  gaps missing {}  failed {}", gaps.missing, gaps.failed);
            if !gaps.servers.is_empty() {
                line.push_str("  refused");
                for server in gaps.servers.iter().take(MAX_SERVERS) {
                    let _ = write!(line, " {} {}", server.server, server.refused);
                }
                if gaps.servers.len() > MAX_SERVERS {
                    let _ = write!(line, " +{}", gaps.servers.len() - MAX_SERVERS);
                }
            }
            let _ = writeln!(text, "{}", clip(&line));
            if !gaps.sample.is_empty() {
                // Wrapped rather than clipped: the sample is the part a
                // reader looks at to see where the gaps cluster.
                let mut words: Vec<String> = gaps.sample.iter().map(ToString::to_string).collect();
                let total = u64::from(gaps.missing) + u64::from(gaps.failed);
                if total > gaps.sample.len() as u64 {
                    words.push(format!("(first {} of {total})", gaps.sample.len()));
                }
                let mut line = String::from("  gap at");
                for word in words {
                    if line.len() + 1 + word.len() > MAX_COLUMNS {
                        let _ = writeln!(text, "{line}");
                        line = String::from("        ");
                    }
                    line.push(' ');
                    line.push_str(&word);
                }
                let _ = writeln!(text, "{line}");
            }
        }
        let _ = writeln!(
            text,
            "  verify {}  repair {}  extract {}  member fails {}  missing {}",
            self.verification_runs,
            self.repair.as_str(),
            self.extraction.as_str(),
            self.extraction_member_failures,
            self.files_reported_missing,
        );
        let _ = writeln!(
            text,
            "  output {}  category {}  password {}",
            self.output.as_text(),
            yes_no(self.category_set),
            yes_no(self.password_supplied),
        );
    }
}

// A failure or completion is final for the run it ends; a later start
// begins a new run.
fn advance(current: StageOutcome, next: StageOutcome) -> StageOutcome {
    match (current, next) {
        (_, StageOutcome::Started) => StageOutcome::Started,
        (_, outcome) => outcome,
    }
}

const MAX_SERVERS: usize = 3;
const MAX_COLUMNS: usize = 80;

fn clip(line: &str) -> String {
    if line.len() > MAX_COLUMNS {
        format!("{}...", &line[..MAX_COLUMNS - 3])
    } else {
        line.to_string()
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.1} {}", UNITS[unit])
}

fn human_age(secs: u64) -> String {
    match secs {
        0..60 => format!("{secs}s"),
        60..3_600 => format!("{}m", secs / 60),
        3_600..86_400 => format!("{}h", secs / 3_600),
        _ => format!("{}d", secs / 86_400),
    }
}

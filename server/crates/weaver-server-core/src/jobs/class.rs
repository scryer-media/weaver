// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The classes of job the queue routes by: one object per protocol, which
//! alone answers for its jobs. The queue, the job layer and the intakes ask
//! the registry for a job's class and hand the class the work; none of them
//! names a class. A new class is one `Protocol` variant, its row in the
//! protocol table, one `JobClass` and one call to `JobClasses::seat`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use crate::ingest::Kind;
use crate::jobs::error::SchedulerError;
use crate::jobs::handle::JobInfo;
use crate::jobs::handle::{CancellationOrigin, QueueAction};
use crate::jobs::ids::JobId;
use crate::jobs::model::Protocol;
use crate::pipeline::queue::Queue;
use crate::settings::SeedingPolicy;

/// One `T` for each protocol, indexed by protocol: a protocol added to the
/// table adds its entry here with no site of its own.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ByProtocol<T>([T; Protocol::COUNT]);

impl<T> ByProtocol<T> {
    /// The entries in the table's order, one per protocol.
    pub const fn new(entries: [T; Protocol::COUNT]) -> Self {
        Self(entries)
    }

    /// Each protocol with its entry, in the table's order.
    pub fn iter(&self) -> impl Iterator<Item = (Protocol, &T)> {
        Protocol::ALL.iter().copied().zip(&self.0)
    }
}

/// Each protocol with its entry, in the table's order, taken.
impl<T> IntoIterator for ByProtocol<T> {
    type Item = (Protocol, T);
    type IntoIter = std::iter::Zip<
        std::iter::Copied<std::slice::Iter<'static, Protocol>>,
        std::array::IntoIter<T, { Protocol::COUNT }>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        Protocol::ALL.iter().copied().zip(self.0)
    }
}

impl<T> std::ops::Index<Protocol> for ByProtocol<T> {
    type Output = T;

    fn index(&self, protocol: Protocol) -> &T {
        &self.0[protocol.index()]
    }
}

impl<T> std::ops::IndexMut<Protocol> for ByProtocol<T> {
    fn index_mut(&mut self, protocol: Protocol) -> &mut T {
        &mut self.0[protocol.index()]
    }
}

/// The class whose jobs are the pipeline's own rows: every job no other
/// class published.
pub const PIPELINE: Protocol = Protocol::Nzb;

/// The rates a class's jobs move at, one per direction: their side of the
/// download split, and the whole upload limit, since usenet uploads
/// nothing. `None` is no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassRates {
    pub download: Option<u64>,
    pub upload: Option<u64>,
}

/// A class's side of the global limits.
pub trait ClassRate: Send + Sync {
    /// Measure the class: what it received at since it was last measured,
    /// in bytes per second. Its own shares follow it.
    fn sample(&self) -> u64;
    /// Put the class's download and upload rates in force.
    fn set_rates(&self, rates: ClassRates);
}

/// What an intake hands a class to add: the payload as classified and what
/// the intake asks of the job. NZB intake stays on each intake's own path.
#[derive(Debug, Clone)]
pub struct Submission {
    pub kind: Kind,
    pub bytes: Vec<u8>,
    pub category: Option<String>,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<String>,
    pub paused: bool,
    pub forced: bool,
    // Whether its archives are extracted; `None` inherits.
    pub unpack: Option<bool>,
    pub policy_override: Option<SeedingPolicy>,
}

impl Submission {
    /// A submission of `kind`, holding `bytes`, with nothing else asked.
    pub fn of(kind: Kind, bytes: Vec<u8>) -> Self {
        Self {
            kind,
            bytes,
            category: None,
            metadata: Vec::new(),
            tags: Vec::new(),
            paused: false,
            forced: false,
            unpack: None,
            policy_override: None,
        }
    }
}

/// Why a class refused an add, as its callers tell refusals apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalReason {
    /// No class is running for the payload's protocol.
    NotRunning,
    /// The class runs and takes no new jobs now; one may be added later.
    Off,
    /// The payload cannot be read as the class's.
    Invalid,
    /// The class's admission refused it.
    Refused,
    /// The same job is still being removed; it may be added again once
    /// that ends.
    Removing,
    /// The job is already in the queue, as this id.
    Duplicate(JobId),
    /// Nothing can carry it now.
    NoNetwork,
    /// The class could not record it.
    State,
    /// The class runs and takes no jobs from the intakes at all.
    NoIntake,
}

/// A class's refusal of an add, in the class's own words.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{text}")]
pub struct AddRefusal {
    pub reason: RefusalReason,
    pub text: String,
}

impl AddRefusal {
    /// The refusal of an add to `protocol` while no class runs for it.
    pub fn not_running(protocol: Protocol) -> Self {
        Self {
            reason: RefusalReason::NotRunning,
            text: format!("the {} is not running", protocol.names().runner),
        }
    }

    /// The refusal of an add to `protocol` whose class runs and takes no
    /// jobs from the intakes.
    pub fn no_intake(protocol: Protocol) -> Self {
        Self {
            reason: RefusalReason::NoIntake,
            text: format!(
                "the {} takes no new jobs from here",
                protocol.names().runner
            ),
        }
    }

    /// Whether the same add may be taken later, as it stands.
    pub const fn transient(&self) -> bool {
        match self.reason {
            RefusalReason::NotRunning
            | RefusalReason::Off
            | RefusalReason::NoNetwork
            | RefusalReason::State => true,
            RefusalReason::Invalid
            | RefusalReason::Refused
            | RefusalReason::Removing
            | RefusalReason::Duplicate(_)
            | RefusalReason::NoIntake => false,
        }
    }
}

/// How a class takes new jobs from the intakes.
#[async_trait::async_trait]
pub trait ClassIntake: Send + Sync {
    /// Whether the class takes new jobs now, and its refusal when it does
    /// not.
    fn takes_new(&self) -> Result<(), AddRefusal>;
    /// Add the job `submission` asks for.
    async fn add(&self, submission: Submission) -> Result<JobId, AddRefusal>;
}

/// One class of job. Each job id has one class: the one that published it,
/// or else the pipeline.
#[async_trait::async_trait]
pub trait JobClass: Send + Sync {
    fn protocol(&self) -> Protocol;

    /// Every queue action of one request on this class's jobs, as one call:
    /// each job answered on its own.
    async fn act(
        &self,
        actions: Vec<(JobId, QueueAction)>,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>>;

    /// The class's side of the global limits, when it is measured this way.
    fn rate(&self) -> Option<&dyn ClassRate> {
        None
    }

    /// How the class takes new jobs from the intakes, when it takes them
    /// through the registry.
    fn intake(&self) -> Option<&dyn ClassIntake> {
        None
    }

    /// A WireGuard tunnel wakes the class to give back what it owes there,
    /// as another class presses for bytes this one holds past its share.
    /// Answered without waiting: on its own turn the class reads what it
    /// owes then, `ask.owed`, and closes what it can of that less what it
    /// has already marked to close, never a busy connection; the rest comes
    /// back as its sockets close by themselves. An answer that finds nothing
    /// it can give says so, through `ask.owed`.
    fn give_back(&self, ask: TunnelGiveBack) {
        let _ = ask;
    }
}

/// A WireGuard tunnel's wake for a class to give back its sockets there.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TunnelGiveBack {
    /// The tunnel: its egress and the hops up to and including it, as a
    /// socket's `DialPath` names them.
    pub egress: u32,
    pub hops: Vec<u32>,
    /// The torrent sessions whose legs run over the tunnel.
    pub torrent_routes: Vec<crate::torrents::sessions::RouteKey>,
    /// What the class owes the tunnel, read when it acts.
    pub owed: Owed,
}

impl TunnelGiveBack {
    /// Whether a socket dialled over `path` sits in the tunnel itself: on
    /// its egress, with the tunnel its last hop.
    pub fn holds(&self, path: &weaver_tunnel::pipe::DialPath) -> bool {
        self.rides(path.egress, &path.proxies)
    }

    /// Whether a socket on `egress` through `proxies` sits in the tunnel.
    pub fn rides(&self, egress: u32, proxies: &[u32]) -> bool {
        egress == self.egress && proxies == self.hops
    }
}

/// What a class owes a tunnel: the bytes it holds there past what it is
/// given while another class presses, read at the moment it is acted on.
/// A level, never an amount handed over once: an answer, or a connection
/// marked to close for one, reads it again when it acts.
#[derive(Clone)]
pub struct Owed {
    owed: Arc<dyn Fn() -> u64 + Send + Sync>,
    dry: Arc<dyn Fn() + Send + Sync>,
}

impl Owed {
    /// `owed` reads the level; `dry` tells the tunnel an answer found
    /// nothing it could give.
    pub fn new(
        owed: impl Fn() -> u64 + Send + Sync + 'static,
        dry: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            owed: Arc::new(owed),
            dry: Arc::new(dry),
        }
    }

    /// The bytes owed now.
    pub fn bytes(&self) -> u64 {
        (self.owed)()
    }

    /// Whether anything is owed now.
    pub fn stands(&self) -> bool {
        self.bytes() > 0
    }

    /// The answer found nothing it could give: the class is not woken again
    /// until its own holdings on the tunnel change.
    pub fn dry(&self) {
        (self.dry)()
    }

    /// Whether anything is owed, as a connection marked to close reads it.
    pub fn standing(&self) -> Arc<dyn Fn() -> bool + Send + Sync> {
        let owed = Arc::clone(&self.owed);
        Arc::new(move || owed() > 0)
    }
}

impl std::fmt::Debug for Owed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Owed")
    }
}

impl PartialEq for Owed {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.owed, &other.owed)
    }
}

impl Eq for Owed {}

/// Where an intake hands a payload.
pub enum Route<'a> {
    /// The pipeline's own path: an NZB, or a URL not fetched yet.
    Pipeline,
    /// The intake of the class the payload belongs to.
    Class(&'a dyn ClassIntake),
}

/// The classes in force, one per protocol, each seated once at start and
/// never replaced.
#[derive(Clone, Default)]
pub struct JobClasses(Arc<ByProtocol<OnceLock<Arc<dyn JobClass>>>>);

impl JobClasses {
    /// Seat `class` for its protocol. A protocol keeps the first class
    /// seated for it.
    pub fn seat(&self, class: Arc<dyn JobClass>) {
        let protocol = class.protocol();
        if self.0[protocol].set(class).is_err() {
            tracing::warn!(
                protocol = protocol.as_str(),
                "a class is already seated for this protocol"
            );
        }
    }

    pub fn get(&self, protocol: Protocol) -> Option<&Arc<dyn JobClass>> {
        self.0[protocol].get()
    }

    /// Every class seated, in the protocol table's order.
    pub fn iter(&self) -> impl Iterator<Item = &Arc<dyn JobClass>> {
        self.0.iter().filter_map(|(_, class)| class.get())
    }

    /// Where an intake hands a payload of `kind`: the pipeline's own path,
    /// or the intake of the class it belongs to, or that class's refusal
    /// while it is not running or takes no jobs from the intakes. The one
    /// place an intake's payload is told apart from the pipeline's.
    pub fn route(&self, kind: &Kind) -> Result<Route<'_>, AddRefusal> {
        self.route_to(kind.protocol())
    }

    /// Where an intake hands a payload of `protocol`, before its bytes are
    /// read: what [`Self::route`] answers for a payload of it.
    pub fn route_to(&self, protocol: Option<Protocol>) -> Result<Route<'_>, AddRefusal> {
        let protocol = match protocol {
            Some(protocol) if protocol != PIPELINE => protocol,
            // A URL is fetched on the pipeline's path before it is a payload.
            Some(_) | None => return Ok(Route::Pipeline),
        };
        let class = self
            .get(protocol)
            .ok_or_else(|| AddRefusal::not_running(protocol))?;
        class
            .intake()
            .map(Route::Class)
            .ok_or_else(|| AddRefusal::no_intake(protocol))
    }
}

/// The pipeline as a class: its jobs are the queue's own rows, and every
/// action on them is a queue intent, grouped by kind.
pub struct PipelineClass(pub Arc<Queue>);

#[async_trait::async_trait]
impl JobClass for PipelineClass {
    fn protocol(&self) -> Protocol {
        PIPELINE
    }

    async fn act(
        &self,
        actions: Vec<(JobId, QueueAction)>,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
        let queue = &self.0;
        let mut paused = Vec::new();
        let mut resumed = Vec::new();
        let mut cancels: Vec<(CancellationOrigin, BTreeSet<JobId>)> = Vec::new();
        let mut updates = Vec::new();
        let mut moves = Vec::new();
        for (job_id, action) in actions {
            match action {
                QueueAction::Pause => paused.push(job_id),
                QueueAction::Resume => resumed.push(job_id),
                QueueAction::Cancel(origin) => {
                    match cancels.iter_mut().find(|(was, _)| *was == origin) {
                        Some((_, job_ids)) => {
                            job_ids.insert(job_id);
                        }
                        None => cancels.push((origin, BTreeSet::from([job_id]))),
                    }
                }
                QueueAction::Update(update) => updates.push((job_id, update)),
                QueueAction::Move(target) => moves.push((job_id, target)),
            }
        }
        let mut outcome = BTreeMap::new();
        if !paused.is_empty() {
            outcome.extend(queue.set_jobs_paused(paused, true));
        }
        if !resumed.is_empty() {
            outcome.extend(queue.set_jobs_paused(resumed, false));
        }
        for (origin, job_ids) in cancels {
            outcome.extend(queue.cancel_jobs(job_ids, origin));
        }
        if !updates.is_empty() {
            outcome.extend(queue.update_jobs(updates));
        }
        if let Some(&(first, _)) = moves.first() {
            // The moves are one change, applied whole or not at all: a
            // failure is the answer of the job it names, or of the first.
            let job_ids: Vec<JobId> = moves.iter().map(|(job_id, _)| *job_id).collect();
            match queue.reorder_jobs(moves) {
                Ok(()) => outcome.extend(job_ids.into_iter().map(|job_id| (job_id, Ok(())))),
                Err(error) => {
                    let named = match &error {
                        SchedulerError::JobNotFound(job_id) if job_ids.contains(job_id) => *job_id,
                        _ => first,
                    };
                    outcome.insert(named, Err(error));
                }
            }
        }
        outcome
    }

    // The pipeline answers on its own turn: the ask waits in the queue for
    // it, and the turn retires idle connections on the tunnel.
    fn give_back(&self, ask: TunnelGiveBack) {
        self.0.write_intent(|inner| {
            inner.socket_give_backs.push(ask);
            crate::pipeline::queue::Change::unchanged(())
        });
    }
}

/// The rows a class publishes beside the pipeline's, in the class's order,
/// with each one's position, so asking whose a job is, or reading one,
/// costs no walk of them. Changed only through its own methods, which keep
/// the positions in step.
#[derive(Debug, Default)]
pub struct OwnedRows {
    rows: Vec<JobInfo>,
    positions: std::collections::HashMap<JobId, usize>,
}

impl OwnedRows {
    pub fn rows(&self) -> &[JobInfo] {
        &self.rows
    }

    /// Replaces the rows whole, and the position of each.
    pub fn replace(&mut self, rows: Vec<JobInfo>) {
        self.rows = rows;
        self.positions = self
            .rows
            .iter()
            .enumerate()
            .map(|(position, job)| (job.job_id, position))
            .collect();
    }

    /// Puts `job`, new among the rows, at its place in their order, by when
    /// it was added; only the positions after it move.
    pub fn insert(&mut self, job: JobInfo) {
        let key = (job.created_at_epoch_ms, job.job_id);
        let at = self
            .rows
            .partition_point(|was| (was.created_at_epoch_ms, was.job_id) <= key);
        self.positions.insert(job.job_id, at);
        self.rows.insert(at, job);
        for (position, later) in self.rows.iter().enumerate().skip(at + 1) {
            self.positions.insert(later.job_id, position);
        }
    }

    /// Puts `job` in place of the row at `at`, which has its id.
    pub fn set(&mut self, at: usize, job: JobInfo) {
        debug_assert_eq!(self.rows[at].job_id, job.job_id);
        self.rows[at] = job;
    }

    /// Takes the row `job_id` out, if it is one; only the positions after
    /// it move.
    pub fn remove(&mut self, job_id: JobId) -> Option<JobInfo> {
        let at = self.positions.remove(&job_id)?;
        let job = self.rows.remove(at);
        for (position, later) in self.rows.iter().enumerate().skip(at) {
            self.positions.insert(later.job_id, position);
        }
        Some(job)
    }

    /// Where `job_id` sits among the rows, if it is one.
    pub fn position(&self, job_id: JobId) -> Option<usize> {
        self.positions.get(&job_id).copied()
    }

    /// The row `job_id`, if it is one: one probe.
    pub fn get(&self, job_id: JobId) -> Option<&JobInfo> {
        self.position(job_id).map(|at| &self.rows[at])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bandwidth::ScheduleAction;
    use crate::jobs::handle::{HoldOwner, HoldScope};
    use crate::jobs::model::HeldClass;

    // With no class seated, every payload a class owns is refused as not
    // running, in the class's words, and may be taken later; what no class
    // owns goes on the pipeline's path. Every intake (RSS, the watch
    // folder, GraphQL's submit and staging) asks this one question.
    #[test]
    fn every_class_payload_is_refused_as_not_running_while_no_class_is_seated() {
        let classes = JobClasses::default();
        let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_owned();
        for kind in [Kind::Metainfo, Kind::Magnet(magnet)] {
            let refusal = match classes.route(&kind) {
                Err(refusal) => refusal,
                Ok(_) => panic!("{kind:?} was not refused"),
            };
            assert_eq!(refusal.reason, RefusalReason::NotRunning);
            assert_eq!(refusal.text, "the torrent client is not running");
            assert!(refusal.transient());
        }
        let url = Kind::Url(reqwest::Url::parse("https://example.invalid/a.nzb").unwrap());
        for kind in [Kind::Nzb, url] {
            assert!(
                matches!(classes.route(&kind), Ok(Route::Pipeline)),
                "{kind:?}"
            );
        }
    }

    // A class seated with no intake refuses an add in its own words, for
    // good: it is running, so "not running" would not be true.
    #[test]
    fn a_seated_class_with_no_intake_refuses_as_taking_no_jobs() {
        struct Quiet;
        #[async_trait::async_trait]
        impl JobClass for Quiet {
            fn protocol(&self) -> Protocol {
                Protocol::Torrent
            }
            async fn act(
                &self,
                actions: Vec<(JobId, QueueAction)>,
            ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
                actions
                    .into_iter()
                    .map(|(job_id, _)| (job_id, Ok(())))
                    .collect()
            }
        }
        let classes = JobClasses::default();
        classes.seat(Arc::new(Quiet));
        let refusal = match classes.route(&Kind::Metainfo) {
            Err(refusal) => refusal,
            Ok(_) => panic!("a class with no intake took an add"),
        };
        assert_eq!(refusal, AddRefusal::no_intake(Protocol::Torrent));
        assert_eq!(
            refusal.text,
            "the torrent client takes no new jobs from here"
        );
        assert!(!refusal.transient());
        assert!(matches!(classes.route(&Kind::Nzb), Ok(Route::Pipeline)));
    }

    // The words and keys each class owns, as they are shown and stored.
    #[test]
    fn each_class_owns_its_words_and_keys() {
        let nzb = Protocol::Nzb.names();
        assert_eq!(
            (nzb.key, nzb.runner, nzb.disabled_code),
            ("nzb", "download pipeline", "NZB_DISABLED")
        );
        assert!(nzb.hold.is_none());
        assert!(nzb.live.is_none());
        assert_eq!(HeldClass::of(Protocol::Nzb), None);

        let torrent = Protocol::Torrent.names();
        assert_eq!(
            (torrent.key, torrent.runner, torrent.disabled_code),
            ("torrent", "torrent client", "TORRENTS_DISABLED")
        );
        let hold = torrent.hold.expect("torrents hold apart");
        assert_eq!(hold.scope, HoldScope::Everything);
        assert_eq!(hold.setting, "torrent_paused");
        assert!(torrent.live.is_some());
        assert_eq!(
            HeldClass::of(Protocol::Torrent),
            Some(crate::torrents::CLASS)
        );
        assert_eq!(
            HeldClass::all().collect::<Vec<_>>(),
            vec![crate::torrents::CLASS]
        );
        assert_eq!(
            AddRefusal::not_running(Protocol::Torrent).text,
            "the torrent client is not running"
        );
        assert_eq!(
            HoldOwner::kept()
                .map(|(_, setting)| setting)
                .collect::<Vec<_>>(),
            vec!["global_paused", "torrent_paused"]
        );

        for (action, stored) in [
            (
                ScheduleAction::PauseClass {
                    class: crate::torrents::CLASS,
                },
                r#"{"type":"pause_class","class":"torrent"}"#,
            ),
            (
                ScheduleAction::ResumeClass {
                    class: crate::torrents::CLASS,
                },
                r#"{"type":"resume_class","class":"torrent"}"#,
            ),
        ] {
            assert_eq!(serde_json::to_string(&action).unwrap(), stored);
            assert_eq!(
                serde_json::from_str::<ScheduleAction>(stored).unwrap(),
                action
            );
        }
        // A pause of a class with no hold, or of no class, is never read in.
        for stored in [
            r#"{"type":"pause_class","class":"nzb"}"#,
            r#"{"type":"resume_class","class":"nzb"}"#,
            r#"{"type":"pause_class","class":"http"}"#,
        ] {
            assert!(
                serde_json::from_str::<ScheduleAction>(stored).is_err(),
                "{stored}"
            );
        }
    }

    // Every protocol's entry is at its index, and the index is its place in
    // `ALL`, so a per-protocol table reads the right row.
    #[test]
    fn every_protocol_is_indexed_at_its_place_in_the_table() {
        for (at, protocol) in Protocol::ALL.iter().enumerate() {
            assert_eq!(protocol.index(), at);
        }
        let mut keys = ByProtocol::<&str>::default();
        for protocol in Protocol::ALL {
            keys[*protocol] = protocol.as_str();
        }
        for (protocol, key) in keys.iter() {
            assert_eq!(*key, protocol.as_str());
        }
    }
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The usenet feed's word on whether a post is worth fetching, from the NZB
// and the servers alone. Whatever consumes the bytes is not asked: the feed
// keeps streaming or gives the post up on what it can see itself.
//
// Three things weigh a post. The first-article sample: one article per file
// the post names as payload, the cheapest sign of whether the post is still
// there. The post's health, the share of its bytes that has not been lost,
// against the critical line its declared recovery sets, as NZBGet draws it:
// a post that names no recovery is held to 85%, and the more recovery it
// names the lower the line. And the STAT probe: once the losses take the
// shape of a release nobody uploaded, a task asks the servers whether a
// stride of the post's articles exists, by STAT and never by BODY, and the
// share it finds gone, taken over the whole payload, is a projected loss the
// health is weighed against as well as against what it has lost so far, so a
// dead release fails before a gigabyte of missing articles proves it one at
// a time. A projection is a health signal and never a ledger entry; the
// job's failed bytes stay the sum of what it lost. The job's status is not
// touched while a round is out.
//
// The recovery a post declares is what its NZB names: PAR2 files, PAR3
// files and RAR recovery volumes, each by its name. A RAR recovery record
// inside a volume is invisible here; the 85% line is what leaves room for
// it.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};
use weaver_model::files::FileRole;

use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec};
use crate::pipeline::download::queue::DownloadWork;
use crate::pipeline::{Pipeline, SegmentTerminalState, health_milli};

// The fewest files a first-article verdict is worth taking. One article per
// file is a sample, and a sample of a handful of files says very little: a
// two-file post whose first file is missing is an ordinary damaged post, not
// a dead one.
pub(crate) const FIRST_ARTICLE_GATE_MIN_FILES: usize = 10;

// How much of the sample has to be lost before the post is called dead, in
// hundredths. Short of this the post's health rules.
pub(crate) const FIRST_ARTICLE_GATE_MISSING_PCT: usize = 80;

// The least failed bytes a clean round waits for before the next.
pub(crate) const REARM_MIN_BYTES: u64 = 128 * 1024 * 1024;
// A clean round also waits for this share of the payload to fail.
pub(crate) const REARM_PAYLOAD_DIVISOR: u64 = 200;

// How far ahead of the delivered payload the losses must run before damage
// confined to a single file is allowed to look like a dead release.
//
// One withheld volume in an otherwise healthy posting is a hole recovery
// covers, not a release nobody uploaded, and sampling the rest of it answers
// a question no one asked. A release that really is gone looks different:
// almost nothing lands while the failures pile up.
pub(crate) const DEAD_RELEASE_LANDED_DIVISOR: u64 = 4;

// Articles asked of the servers in one STAT batch.
const BATCH_SIZE: usize = 50;
// Batches between progress updates within a round.
const UPDATE_INTERVAL: usize = 10;
// Updates the mail holds before a round waits for the pipeline to hear one.
const MAIL_CAPACITY: usize = 64;

// What a round asks the servers: whether each article exists on any of
// them. An error is a batch that could not settle, never a verdict that the
// articles are gone.
pub(crate) trait Stat: Send + Sync {
    fn stat_many<'a>(
        &'a self,
        message_ids: &'a [&'a str],
    ) -> Pin<Box<dyn Future<Output = Option<Vec<bool>>> + Send + 'a>>;
}

impl Stat for weaver_nntp::NntpClient {
    fn stat_many<'a>(
        &'a self,
        message_ids: &'a [&'a str],
    ) -> Pin<Box<dyn Future<Output = Option<Vec<bool>>> + Send + 'a>> {
        Box::pin(async move {
            weaver_nntp::NntpClient::stat_many(self, message_ids)
                .await
                .ok()
        })
    }
}

// What a round found so far, or in all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeUpdate {
    pub(crate) job_id: JobId,
    pub(crate) round: u32,
    // Articles a batch answered for.
    pub(crate) total: usize,
    // Of those, the ones no server holds.
    pub(crate) missed: usize,
    // Articles left unanswered by a batch that could not settle.
    pub(crate) unverified: usize,
    pub(crate) done: bool,
    // The round is done and nothing in it answered.
    pub(crate) inconclusive: bool,
}

// Running tally over one round's batches.
//
// A batch can come back without an answer: a transport failure, or no
// connection free while the download lanes hold every permit. That costs
// the round coverage, not its verdict: the batches that did answer are still
// evidence about the release, and a sample that is smaller than intended is
// what `total` already stands for. Only a round that answered for nothing at
// all has no verdict to give.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeTally {
    pub(crate) checked: usize,
    pub(crate) missed: usize,
    pub(crate) unverified: usize,
    pub(crate) unverified_batches: usize,
}

impl ProbeTally {
    // Fold in a batch that answered.
    pub(crate) fn record_answered(&mut self, exists: &[bool]) {
        for found in exists {
            self.checked = self.checked.saturating_add(1);
            if !found {
                self.missed = self.missed.saturating_add(1);
            }
        }
    }

    // Fold in a batch that could not settle, so the round goes on with the
    // articles it could not reach counted against its coverage.
    pub(crate) fn record_unsettled(&mut self, batch_len: usize) {
        self.unverified_batches = self.unverified_batches.saturating_add(1);
        self.unverified = self.unverified.saturating_add(batch_len);
    }

    // The update the round reports with what it has so far.
    pub(crate) fn update(&self, job_id: JobId, round: u32, done: bool) -> ProbeUpdate {
        ProbeUpdate {
            job_id,
            round,
            total: self.checked,
            missed: self.missed,
            unverified: self.unverified,
            done,
            inconclusive: done && self.checked == 0,
        }
    }
}

// Where a job's probe stands between rounds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProbeState {
    // The round out now, if one is.
    pub(crate) out: Option<u32>,
    // The rounds armed so far; the next round is numbered with it.
    pub(crate) rounds: u32,
    // The failing files the last round was armed on: another file failing
    // re-arms the next one, and a second lost article in a file already
    // known to be failing does not.
    pub(crate) failing_files: usize,
    // The failed bytes at which a round is armed again.
    pub(crate) next_failed_bytes: u64,
    // The largest loss a round projected over the payload.
    pub(crate) projected: u64,
    // What the round that projected it found: the articles it found gone,
    // of those it asked that answered.
    pub(crate) sampled: Option<(usize, usize)>,
}

impl ProbeState {
    // The failed bytes the post's health is weighed against: what it lost,
    // or what a round projected it will, whichever is larger.
    pub(crate) fn decided(&self, failed: u64) -> u64 {
        failed.max(self.projected)
    }
}

// The probes of every graph job, and the mail their rounds speak through.
pub(crate) struct Probes {
    sender: mpsc::Sender<ProbeUpdate>,
    receiver: mpsc::Receiver<ProbeUpdate>,
    pub(crate) states: HashMap<JobId, ProbeState>,
    // The posts the feed gave up, in its words.
    pub(crate) dead: HashMap<JobId, String>,
    // Who answers a round's STAT batches, when not the pipeline's own
    // client.
    pub(crate) stat: Option<Arc<dyn Stat>>,
}

impl Default for Probes {
    fn default() -> Self {
        let (sender, receiver) = mpsc::channel(MAIL_CAPACITY);
        Self {
            sender,
            receiver,
            states: HashMap::new(),
            dead: HashMap::new(),
            stat: None,
        }
    }
}

impl Probes {
    pub(crate) fn sender(&self) -> mpsc::Sender<ProbeUpdate> {
        self.sender.clone()
    }

    pub(crate) async fn recv(&mut self) -> Option<ProbeUpdate> {
        self.receiver.recv().await
    }

    // Whether a round is out for the job.
    pub(crate) fn out(&self, job_id: JobId) -> bool {
        self.states
            .get(&job_id)
            .is_some_and(|state| state.out.is_some())
    }

    // The job no longer downloads, so no round rules on it again: its probe
    // goes, and the post's verdict, if the feed gave one, stays.
    pub(crate) fn retire(&mut self, job_id: JobId) {
        self.states.remove(&job_id);
    }

    // The job has gone: the feed keeps nothing of it.
    pub(crate) fn forget(&mut self, job_id: JobId) {
        self.states.remove(&job_id);
        self.dead.remove(&job_id);
    }
}

// Whether the post names the file a recovery volume: PAR2 or PAR3 blocks,
// or a RAR recovery volume. An index names the payload; it is no volume of
// recovery blocks.
pub(crate) fn declares_recovery(file: &FileSpec) -> bool {
    match file.role {
        FileRole::Par2 { is_index, .. } | FileRole::Par3 { is_index } => !is_index,
        _ => file.filename.to_ascii_lowercase().ends_with(".rev"),
    }
}

// Whether a file's name makes it a piece of something else: a RAR volume, a
// 7z or zip archive or one of its volumes, a split part, or recovery data,
// classified as the feed classifies the post's files. Whole or not, such a
// file is never one the user opens as it was posted. A compressed stream or
// a tar is not one: posted whole, it is a file as it stands, whatever
// opening it gave.
pub(crate) fn names_a_piece(name: &str) -> bool {
    let role = FileRole::from_filename(name);
    name.to_ascii_lowercase().ends_with(".rev")
        || matches!(
            role,
            FileRole::Par2 { .. }
                | FileRole::Par3 { .. }
                | FileRole::RarVolume { .. }
                | FileRole::SevenZipArchive
                | FileRole::SevenZipSplit { .. }
                | FileRole::ZipArchive
                | FileRole::SplitFile { .. }
        )
}

// The bytes of every file the post names as recovery data, its indexes
// among them: what the critical line is drawn from.
pub(crate) fn declared_recovery_bytes(spec: &JobSpec) -> u64 {
    spec.files
        .iter()
        .filter(|file| {
            matches!(file.role, FileRole::Par2 { .. } | FileRole::Par3 { .. })
                || declares_recovery(file)
        })
        .flat_map(|file| file.segments.iter())
        .map(|segment| u64::from(segment.bytes))
        .sum()
}

// Where one payload file's first article stands in the sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Head {
    // Every server failed it.
    Lost,
    // In hand, or ended some other way that is no loss.
    Settled,
    // Still to come.
    Outstanding,
}

// The sample's word on the post.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Gate {
    // The lost reach the failure share of the whole sample: how many of the
    // sampled files every server failed the first article of, of how many.
    Dead { missing: usize, total: usize },
    // The sample is too small to rule, or can no longer reach its share
    // whatever its outstanding first articles do: the post's health speaks.
    Open,
    // Short of its share with first articles still to come that could take
    // it there: nothing speaks for the post yet.
    Undecided,
}

// The sample's verdict on `heads`, one per payload file. Only a loss counts
// toward the share, and an article still to come can only add to it, so a
// sample that reaches the share with articles outstanding has the verdict
// the complete sample would have, and one that cannot reach it even were
// every outstanding article lost has let the post through.
pub(crate) fn first_article_gate(heads: impl IntoIterator<Item = Head>) -> Gate {
    let mut total = 0;
    let mut lost = 0;
    let mut outstanding = 0;
    for head in heads {
        total += 1;
        match head {
            Head::Lost => lost += 1,
            Head::Outstanding => outstanding += 1,
            Head::Settled => {}
        }
    }
    let reaches = |lost: usize| lost * 100 >= total * FIRST_ARTICLE_GATE_MISSING_PCT;
    if total < FIRST_ARTICLE_GATE_MIN_FILES {
        Gate::Open
    } else if reaches(lost) {
        Gate::Dead {
            missing: lost,
            total,
        }
    } else if reaches(lost + outstanding) {
        Gate::Undecided
    } else {
        Gate::Open
    }
}

pub(crate) fn first_article_verdict_error(missing: usize, total: usize) -> String {
    format!("aborted: {missing} of {total} first articles are missing, the post cannot complete")
}

// The message ids a round samples from: every article of every file that
// counts toward the post's health, in post order.
pub(crate) fn candidates(spec: &JobSpec) -> Vec<String> {
    spec.files
        .iter()
        .filter(|file| file.role.counts_toward_health())
        .flat_map(|file| {
            file.segments
                .iter()
                .map(|segment| segment.message_ids.in_use().to_string())
        })
        .collect()
}

// The candidates a round asks for: an even stride of at least ten, or one
// in twelve and a half, offset by the round so repeat rounds widen coverage
// instead of asking the same articles again.
pub(crate) fn sample_indices(total: usize, round: u32) -> Vec<usize> {
    if total == 0 {
        return Vec::new();
    }
    let count = (total * 8 / 100).max(10).min(total);
    let stride = (total / count).max(1);
    let offset = if stride > 1 {
        round as usize % stride
    } else {
        0
    };
    (offset..total).step_by(stride).collect()
}

// The loss a round projects over the payload: the share of what it asked
// that is gone, taken over every byte that counts toward health.
pub(crate) fn projected(tracked_bytes: u64, total: usize, missed: usize) -> Option<u64> {
    if total == 0 || missed == 0 || tracked_bytes == 0 {
        return None;
    }
    Some((u128::from(tracked_bytes) * missed as u128 / total as u128) as u64)
}

// Whether the damage has the shape a probe exists to catch.
//
// The probe's job is to abandon a release nobody posted before a gigabyte
// proves it. Two shapes qualify: losses spanning more than one file, since a
// posting coming apart in several places is unlikely to be coming apart in
// only those; and losses that dwarf what has landed, since the job is trying
// and getting nothing back, whether or not it has reached a second file yet.
// One file failing while the rest of the posting arrives is neither. That is
// a hole, and the recovery set decides what to do about it.
pub(crate) fn looks_like_a_dead_release(
    downloaded: u64,
    failing_files: usize,
    failed: u64,
) -> bool {
    failing_files > 1 || downloaded.saturating_mul(DEAD_RELEASE_LANDED_DIVISOR) < failed
}

// The failed bytes at which the next round is armed. A round that found
// articles missing, or answered nothing, arms the next on the next lost
// byte; a clean one waits for a material share of the payload more to fail,
// and never past the bytes that take the post below critical.
pub(crate) fn next_failed_bytes(
    decided: u64,
    tracked_bytes: u64,
    critical_failed_bytes: u64,
    missed: usize,
    inconclusive: bool,
) -> u64 {
    let immediate = decided.saturating_add(1);
    if inconclusive || missed > 0 {
        return immediate;
    }
    let delta = (tracked_bytes / REARM_PAYLOAD_DIVISOR)
        .max(REARM_MIN_BYTES)
        .max(1);
    decided
        .saturating_add(delta)
        .min(critical_failed_bytes.saturating_add(1))
        .max(immediate)
}

// One round: the sampled articles asked in batches, a progress update every
// few batches, and the tally sent when every batch has answered or failed to.
pub(crate) async fn sample(
    stat: Arc<dyn Stat>,
    job_id: JobId,
    round: u32,
    probes: Vec<String>,
    mail: mpsc::Sender<ProbeUpdate>,
) {
    let mut tally = ProbeTally::default();
    let mut since_update = 0usize;
    for (batch_index, batch) in probes.chunks(BATCH_SIZE).enumerate() {
        let ids: Vec<&str> = batch.iter().map(String::as_str).collect();
        match stat.stat_many(&ids).await {
            Some(exists) if exists.len() == batch.len() => tally.record_answered(&exists),
            _ => {
                // One batch that could not settle is a hole in the sample,
                // not a verdict about the release: the remaining batches are
                // still worth asking, and what they answer stands.
                tally.record_unsettled(batch.len());
                warn!(
                    job_id = job_id.0,
                    round,
                    batch_index,
                    batch_len = batch.len(),
                    "health probe: STAT batch inconclusive, skipping batch"
                );
            }
        }
        since_update += 1;
        if since_update >= UPDATE_INTERVAL {
            since_update = 0;
            if mail.send(tally.update(job_id, round, false)).await.is_err() {
                return;
            }
        }
    }
    info!(
        job_id = job_id.0,
        round,
        checked = tally.checked,
        missed = tally.missed,
        unverified = tally.unverified,
        "health probe complete"
    );
    let _ = mail.send(tally.update(job_id, round, true)).await;
}

impl Pipeline {
    // The first article of each file the post names, its first segment:
    // lost when every server failed it, settled when the root holds it or
    // it ended some other way, and otherwise still to come.
    fn first_article(&self, job_id: JobId, index: u32, file: &FileSpec) -> Head {
        let Some(first) = file.segments.iter().map(|segment| segment.ordinal).min() else {
            return Head::Settled;
        };
        let segment = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: index,
            },
            segment_number: first,
        };
        match self.segment_terminal_states.get(&segment) {
            Some(SegmentTerminalState::Missing) => Head::Lost,
            Some(_) => Head::Settled,
            None if self.graph_article_given_up(segment) => Head::Lost,
            None if self.graph_article_settled(segment) => Head::Settled,
            None => Head::Outstanding,
        }
    }

    // The files the post names as payload that lost at least one article.
    // A lost recovery volume is not damage to the release.
    fn failing_files(&self, job_id: JobId, spec: &JobSpec) -> usize {
        let failing: BTreeSet<u32> = self
            .segment_terminal_states
            .iter()
            .filter(|(segment, state)| {
                segment.file_id.job_id == job_id && **state == SegmentTerminalState::Missing
            })
            .map(|(segment, _)| segment.file_id.file_index)
            .collect();
        failing
            .into_iter()
            .filter(|index| {
                spec.files.get(*index as usize).is_some_and(|file| {
                    file.role.counts_toward_health() && !declares_recovery(file)
                })
            })
            .count()
    }

    // Why the post is dead, if it is, in the feed's own words. The
    // first-article sample rules first, and until it has, nothing does:
    // past its share the post is dead, unless the recovery files whose
    // first articles have not been lost outweigh the payload files the
    // sample lost; short of it with first articles still to come that could
    // take it there, the post has no verdict yet. Only a post the sample let
    // through is weighed by its health, at the failed bytes it lost or a
    // round projected it will, against its critical line.
    pub(in crate::pipeline) fn post_health_verdict(&self, job_id: JobId) -> Option<String> {
        if self.post_probes.dead.contains_key(&job_id) {
            return None;
        }
        let state = self.jobs.get(&job_id)?;
        let spec = &state.spec;
        let heads: Vec<(u32, &FileSpec, Head)> = spec
            .files
            .iter()
            .zip(0u32..)
            .filter(|(file, _)| !file.segments.is_empty())
            .map(|(file, index)| (index, file, self.first_article(job_id, index, file)))
            .collect();
        let payload = heads.iter().filter(|(_, file, _)| !declares_recovery(file));
        match first_article_gate(payload.clone().map(|head| head.2)) {
            Gate::Undecided => return None,
            Gate::Open => {}
            Gate::Dead { missing, total } => {
                let bytes = |file: &FileSpec| -> u64 {
                    file.segments
                        .iter()
                        .map(|segment| u64::from(segment.bytes))
                        .sum()
                };
                let lost: u64 = payload
                    .filter(|(_, _, head)| *head == Head::Lost)
                    .map(|(_, file, _)| bytes(file))
                    .sum();
                let reachable: u64 = heads
                    .iter()
                    .filter(|(_, file, head)| declares_recovery(file) && *head != Head::Lost)
                    .map(|(_, file, _)| bytes(file))
                    .sum();
                if lost > reachable {
                    return Some(first_article_verdict_error(missing, total));
                }
            }
        }
        let probe = self
            .post_probes
            .states
            .get(&job_id)
            .copied()
            .unwrap_or_default();
        let total = spec.total_bytes;
        let lost = self.missing_bytes(job_id);
        let failed = probe.decided(lost);
        let health = health_milli(total, failed);
        let critical = Self::critical_health_milli(total, declared_recovery_bytes(spec));
        if !below_critical(health, critical) {
            return None;
        }
        let below = format!(
            "health {:.1}% below critical {:.1}%",
            f64::from(health) / 10.0,
            f64::from(critical) / 10.0
        );
        // The probe's projection, when it is what takes the post there, is
        // named with what the round found.
        Some(match probe.sampled {
            Some((missed, asked)) if probe.projected >= lost => {
                format!("health probe: {missed} of {asked} samples missing; {below}")
            }
            _ => below,
        })
    }

    // Arm a round once the post's losses look like a dead release and a
    // round could still change the answer: the post is not yet healthy past
    // doubt, its losses are not past critical by more than one byte, and
    // the losses span files or outrun what landed. A round is armed again
    // on more failed bytes than the last one waited for, or on another file
    // failing.
    pub(in crate::pipeline) fn arm_post_probe(&mut self, job_id: JobId) {
        if !self.nzb_admitted.contains(&job_id) || !self.graph_jobs.contains_key(&job_id) {
            return;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let total = state.spec.total_bytes;
        if total == 0 || !state.status.in_progress() {
            return;
        }
        if self.post_probes.out(job_id) || self.post_probes.dead.contains_key(&job_id) {
            return;
        }
        let probe = self
            .post_probes
            .states
            .get(&job_id)
            .copied()
            .unwrap_or_default();
        let failing_files = self.failing_files(job_id, &state.spec);
        let decided = probe.decided(self.missing_bytes(job_id));
        let health = health_milli(total, decided);
        let critical = Self::critical_health_milli(total, declared_recovery_bytes(&state.spec));
        let critical_failed = critical_failed_bytes(total, critical);
        let rearmed = decided >= probe.next_failed_bytes || failing_files > probe.failing_files;
        let needed = health < 980
            && rearmed
            && (!below_critical(health, critical) || decided <= critical_failed.saturating_add(1))
            && looks_like_a_dead_release(state.downloaded_bytes, failing_files, decided);
        if !needed {
            return;
        }
        let candidates = candidates(&state.spec);
        let round = probe.rounds;
        let entry = self.post_probes.states.entry(job_id).or_default();
        entry.rounds = round.wrapping_add(1);
        entry.failing_files = failing_files;
        if candidates.is_empty() {
            entry.next_failed_bytes = decided.saturating_add(1);
            return;
        }
        entry.out = Some(round);
        let probes: Vec<String> = sample_indices(candidates.len(), round)
            .into_iter()
            .map(|index| candidates[index].clone())
            .collect();
        let stat = self
            .post_probes
            .stat
            .clone()
            .unwrap_or_else(|| Arc::clone(&self.nntp) as Arc<dyn Stat>);
        info!(
            job_id = job_id.0,
            round,
            probes = probes.len(),
            candidates = candidates.len(),
            failing_files,
            failed_bytes = decided,
            "health probe armed: STAT sampling"
        );
        tokio::spawn(sample(
            stat,
            job_id,
            round,
            probes,
            self.post_probes.sender(),
        ));
    }

    // What a round said. Only the round the job waits on is heard: a job
    // that has gone or has moved past downloading forgets its probe. A done
    // round folds its projection into the post's health, sets when the next
    // round is armed, and gives the post its verdict again.
    pub(in crate::pipeline) fn post_probe_heard(&mut self, update: ProbeUpdate) {
        let job_id = update.job_id;
        if !update.done
            || self
                .post_probes
                .states
                .get(&job_id)
                .is_none_or(|probe| probe.out != Some(update.round))
        {
            return;
        }
        let Some(state) = self
            .jobs
            .get(&job_id)
            .filter(|state| state.status.in_progress())
            .filter(|_| self.graph_jobs.contains_key(&job_id))
        else {
            self.post_probes.retire(job_id);
            return;
        };
        let total = state.spec.total_bytes;
        let recovery = declared_recovery_bytes(&state.spec);
        let tracked = total.saturating_sub(recovery);
        let critical = Self::critical_health_milli(total, recovery);
        let failed = self.missing_bytes(job_id);
        let Some(probe) = self.post_probes.states.get_mut(&job_id) else {
            return;
        };
        probe.out = None;
        if !update.inconclusive
            && let Some(projected) = projected(tracked, update.total, update.missed)
            && projected > probe.projected
        {
            probe.projected = projected;
            probe.sampled = Some((update.missed, update.total));
        }
        probe.next_failed_bytes = next_failed_bytes(
            probe.decided(failed),
            tracked,
            critical_failed_bytes(total, critical),
            update.missed,
            update.inconclusive,
        );
        if let Some(error) = self.post_health_verdict(job_id) {
            self.stop_feeding_post(job_id, error);
        }
    }

    // Every server failed an article of the post, and the root has heard
    // the refusal. The feed weighs the post on it: a job that failed on the
    // loss is done with. Otherwise a post the feed already gave up loses the
    // files that lost an article, a post the feed's word says is dead is
    // given up, a job its rulings already leave short fails as the server
    // decides, and a post that is none of those may be one nobody uploaded,
    // which a probe finds out.
    pub(in crate::pipeline) fn post_lost(&mut self, job_id: JobId) {
        if !self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.status.in_progress())
        {
            return;
        }
        let dead = self.post_health_verdict(job_id);
        self.abandon_dead_files(job_id);
        if let Some(error) = dead {
            self.stop_feeding_post(job_id, error);
        } else if !self.fail_graph_job_ruled_short(job_id) {
            self.arm_post_probe(job_id);
        }
    }

    // The bytes of the post the servers said are gone. An article no
    // server may fetch, or one the feed stopped asking for, is not one.
    fn missing_bytes(&self, job_id: JobId) -> u64 {
        self.segment_terminal_states
            .iter()
            .filter(|(segment, state)| {
                segment.file_id.job_id == job_id && **state == SegmentTerminalState::Missing
            })
            .map(|(segment, _)| self.health_counted_segment_bytes(*segment))
            .sum()
    }

    // The feed gives the post up. Below its recovery line nothing can
    // rebuild a file that lost an article, so the feed stops asking for
    // those files; the others are still fetched and may yet arrive whole.
    // The job then ends as its rulings say, partial when a file is whole,
    // and otherwise failed in the feed's words.
    pub(in crate::pipeline) fn stop_feeding_post(&mut self, job_id: JobId, error: String) {
        if self.post_probes.dead.contains_key(&job_id) {
            return;
        }
        warn!(job_id = job_id.0, reason = %error, "post given up: files that lost an article are no longer fetched");
        self.post_probes.dead.insert(job_id, error);
        self.abandon_dead_files(job_id);
    }

    // A dead post's file with a missing article is abandoned: every article
    // of it still queued is refused to the root. Called whenever an article
    // of the post is lost, so a file whose first loss lands after the post
    // was given up is abandoned then.
    pub(in crate::pipeline) fn abandon_dead_files(&mut self, job_id: JobId) {
        if !self.post_probes.dead.contains_key(&job_id) {
            return;
        }
        let lost: BTreeSet<u32> = self
            .segment_terminal_states
            .iter()
            .filter(|(segment, state)| {
                segment.file_id.job_id == job_id && **state == SegmentTerminalState::Missing
            })
            .map(|(segment, _)| segment.file_id.file_index)
            .collect();
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        let lost_file = |work: &DownloadWork| lost.contains(&work.segment_id.file_id.file_index);
        let mut abandoned = state.download_queue.extract_matching(lost_file);
        abandoned.extend(state.recovery_queue.extract_matching(lost_file));
        if abandoned.is_empty() {
            return;
        }
        for work in abandoned {
            self.book_terminal_segment(work.segment_id, SegmentTerminalState::Abandoned);
        }
        self.update_queue_metrics();
        self.maybe_finish_download_pass(job_id);
    }
}

// Whether a post at `health` is dead against its `critical` line. At the
// line itself it is still healthy; only below it is it dead.
fn below_critical(health: u32, critical: u32) -> bool {
    health < critical
}

// The failed bytes past which the post's health is below `critical`.
fn critical_failed_bytes(total: u64, critical: u32) -> u64 {
    total.saturating_sub((u128::from(total) * u128::from(critical) / 1000) as u64)
}

#[cfg(test)]
mod tests;

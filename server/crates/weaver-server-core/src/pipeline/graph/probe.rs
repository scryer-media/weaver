// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The health probe beside a graph job's root. Once the post's losses take the
// shape of a release nobody uploaded, a task asks the servers whether a
// stride of the post's articles exists, by STAT and never by BODY, and says
// what share of them is gone. That share, taken over the whole payload, is a
// projected loss: the post's health is weighed against it as well as against
// what it has lost so far, so a dead release fails before a gigabyte of
// missing articles proves it one at a time. A projection is a health signal
// and never a ledger entry; the job's failed bytes stay the sum of what it
// lost. The job's status is not touched while a round is out.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::jobs::ids::JobId;
use crate::jobs::model::JobSpec;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::{Pipeline, health_milli};

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
    // Arm a round once the post's losses look like a dead release and a
    // round could still change the answer: the post is not yet healthy past
    // doubt, its losses are not past critical by more than one byte, the
    // recovery it can reach does not already cover them, and the losses span
    // files or outrun what landed. A round is armed again on more failed
    // bytes than the last one waited for, or on another file failing.
    pub(in crate::pipeline) fn arm_graph_probe(&mut self, job_id: JobId) {
        if !self.nzb_admitted.contains(&job_id) {
            return;
        }
        let (Some(graph), Some(state)) = (self.graph_jobs.get(&job_id), self.jobs.get(&job_id))
        else {
            return;
        };
        let total = state.spec.total_bytes;
        if total == 0 || !RootPhase::may_replace(&state.status) {
            return;
        }
        if self.graph_probes.out(job_id) {
            return;
        }
        let probe = self
            .graph_probes
            .states
            .get(&job_id)
            .copied()
            .unwrap_or_default();
        let failing_files = graph.failing_files();
        let decided = probe.decided(self.settled_failed_bytes(job_id, total));
        let health = health_milli(total, decided);
        let critical = Self::critical_health_milli(total, state.par2_bytes);
        let critical_failed = critical_failed_bytes(total, critical);
        let rearmed = decided >= probe.next_failed_bytes || failing_files > probe.failing_files;
        let needed = health < 980
            && rearmed
            && (health > critical || decided <= critical_failed.saturating_add(1))
            && !graph.recovery_covers(decided, failing_files)
            && looks_like_a_dead_release(state.downloaded_bytes, failing_files, decided);
        if !needed {
            return;
        }
        let candidates = candidates(&state.spec);
        let round = probe.rounds;
        let entry = self.graph_probes.states.entry(job_id).or_default();
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
            .graph_probes
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
            self.graph_probes.sender(),
        ));
    }

    // What a round said. Only the round the job waits on is heard: a job
    // that has gone or has moved past downloading forgets its probe. A done
    // round folds its projection into the post's health, sets when the next
    // round is armed, and gives the post its verdict again.
    pub(in crate::pipeline) fn graph_probe_heard(&mut self, update: ProbeUpdate) {
        let job_id = update.job_id;
        if !update.done
            || self
                .graph_probes
                .states
                .get(&job_id)
                .is_none_or(|probe| probe.out != Some(update.round))
        {
            return;
        }
        let Some(state) = self
            .jobs
            .get(&job_id)
            .filter(|state| RootPhase::may_replace(&state.status))
            .filter(|_| self.graph_jobs.contains_key(&job_id))
        else {
            self.graph_probes.states.remove(&job_id);
            return;
        };
        let total = state.spec.total_bytes;
        let tracked = total.saturating_sub(state.par2_bytes);
        let critical = Self::critical_health_milli(total, state.par2_bytes);
        let failed = self.settled_failed_bytes(job_id, total);
        let Some(probe) = self.graph_probes.states.get_mut(&job_id) else {
            return;
        };
        probe.out = None;
        if !update.inconclusive {
            if let Some(projected) = projected(tracked, update.total, update.missed)
                && projected > probe.projected
            {
                probe.projected = projected;
                probe.sampled = Some((update.missed, update.total));
            }
        }
        probe.next_failed_bytes = next_failed_bytes(
            probe.decided(failed),
            tracked,
            critical_failed_bytes(total, critical),
            update.missed,
            update.inconclusive,
        );
        if let Some(error) = self.graph_health_verdict(job_id) {
            self.fail_job_on_health(job_id, error);
        }
    }

    // Why the post is dead, if it is: the first-article sample's verdict,
    // then a recovery set short of blocks for what nothing can bring whole,
    // then a round's projection. Each is weighed against the failed bytes
    // the post lost or a round projected it will.
    pub(in crate::pipeline) fn graph_health_verdict(&self, job_id: JobId) -> Option<String> {
        let graph = self.graph_jobs.get(&job_id)?;
        if let Some(dead) = graph.first_articles_dead() {
            return Some(dead);
        }
        let state = self.jobs.get(&job_id)?;
        let probe = self
            .graph_probes
            .states
            .get(&job_id)
            .copied()
            .unwrap_or_default();
        let total = state.spec.total_bytes;
        let failed = probe.decided(self.settled_failed_bytes(job_id, total));
        let health = health_milli(total, failed);
        let critical = Self::critical_health_milli(total, state.par2_bytes);
        let below = format!(
            "health {:.1}% below critical {:.1}%",
            f64::from(health) / 10.0,
            f64::from(critical) / 10.0
        );
        // A recovery set short of blocks for what it covers fails the post
        // in the set's own words, saying where its health stands when that
        // is at or below critical. With none short, the post fails when its
        // health is at or below critical and the recovery it may still
        // obtain cannot cover its losses; a post that declares no recovery,
        // by the PAR2 bytes its NZB lists or a PAR2 or PAR3 file it names,
        // has none to obtain.
        let below_critical = health <= critical;
        let declares = state.par2_bytes > 0 || graph.declares_recovery();
        let may_cover = |reachable| declares && graph.recovery_may_cover(failed, reachable);
        if let Some(shortfall) = graph.shortfall() {
            match shortfall.sets {
                Some(sets) if below_critical => return Some(format!("{sets}; {below}")),
                Some(sets) => return Some(sets),
                None if below_critical && !may_cover(shortfall.reachable) => {
                    return Some(below);
                }
                None => {}
            }
        }
        // A round's projection is the post's verdict once the
        // first-article sample has ruled, when it takes the post's health
        // to or below critical and past what recovery it may still obtain
        // can cover: what the servers no longer hold, nothing can rebuild.
        let (missed, asked) = probe.sampled?;
        let covered = may_cover(graph.recovery_reachable());
        (graph.sampled() && below_critical && !covered)
            .then(|| format!("health probe: {missed} of {asked} samples missing; {below}"))
    }
}

// The failed bytes past which the post's health is below `critical`.
fn critical_failed_bytes(total: u64, critical: u32) -> u64 {
    total.saturating_sub((u128::from(total) * u128::from(critical) / 1000) as u64)
}

#[cfg(test)]
mod tests;

// Two facts about a job that a support reader needs and nothing else keeps.
//
// A direct-store demotion and a job's article gaps are both logged as they
// happen, and both are gone once the log rotates: the per-article events
// that describe gaps are deliberately never recorded as job events, because
// a job can raise thousands of them. This is the bounded summary that stands
// in for them.
//
// Everything here is reporting only. Nothing in the scheduling, failover,
// repair or extraction path reads it, and its size is fixed whatever the job
// does: one demotion record of two short identifiers and a timestamp, four
// counters, at most [`MAX_GAP_SERVERS`] server counts and at most
// [`MAX_GAP_SAMPLE`] segment positions. It names no file, subject or
// message-id; a position is a file's index in the NZB and a segment number.

use serde::{Deserialize, Serialize};

// How many gap positions the summary keeps: the first ones booked.
pub const MAX_GAP_SAMPLE: usize = 16;

// The most servers one job's gaps are counted against. Matches the cap on
// the per-job server attribution, for the same reason.
pub const MAX_GAP_SERVERS: usize = 32;

// The longest identifier a stored record may carry. Every identifier written
// is a short static label; anything longer was not written by this code.
const MAX_IDENTIFIER_LEN: usize = 48;

// Why a gap became terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapKind {
    // Every server that could be asked said the article is not there.
    Missing,
    // The article was asked for until the retry or decode budget ran out.
    Failed,
}

// The first direct-store demotion a job took, and how many sets followed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemotionFact {
    // The demotion reason's stable label, such as `par2_damaged`.
    #[serde(rename = "r")]
    pub reason: String,
    // The job's status when it happened, such as `downloading`.
    #[serde(rename = "s")]
    pub stage: String,
    // When it happened, in seconds since the Unix epoch.
    #[serde(rename = "t")]
    pub at_epoch_secs: u64,
    // How many of the job's sets were demoted, this one included.
    #[serde(rename = "n", default = "one")]
    pub sets: u32,
}

fn one() -> u32 {
    1
}

// A position in the job: the file's index in the NZB and the segment number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct GapPosition(pub u32, pub u32);

// Gaps booked against one server, by its durable id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerGaps(pub u32, pub u32);

// The job's articles that reached a terminal state without arriving.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArticleGapSummary {
    // Articles no server had.
    #[serde(rename = "m", default)]
    pub missing: u32,
    // Segments that failed permanently after their retries or decodes ran out.
    #[serde(rename = "f", default)]
    pub failed: u32,
    // Per server: how many of those gaps it was asked for and refused.
    // An article several servers refused counts once against each.
    #[serde(rename = "s", default, skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<ServerGaps>,
    // The first gaps booked, in booking order.
    #[serde(rename = "i", default, skip_serializing_if = "Vec::is_empty")]
    pub sample: Vec<GapPosition>,
}

impl ArticleGapSummary {
    pub fn is_empty(&self) -> bool {
        self.missing == 0 && self.failed == 0 && self.servers.is_empty() && self.sample.is_empty()
    }

    // The sample in position order, which is how gaps are read for clustering.
    pub fn sorted_sample(&self) -> Vec<GapPosition> {
        let mut sample = self.sample.clone();
        sample.sort_unstable();
        sample
    }
}

// The per-job record, stored as compact JSON on the job's active and history
// rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSupportFacts {
    #[serde(rename = "d", default, skip_serializing_if = "Option::is_none")]
    pub demotion: Option<DemotionFact>,
    #[serde(
        rename = "g",
        default,
        skip_serializing_if = "ArticleGapSummary::is_empty"
    )]
    pub gaps: ArticleGapSummary,
    // Read back from a previous run. A restarted download re-asks for every
    // article it does not hold and books each gap again, so the first gap
    // booked after a restore starts the summary over instead of counting the
    // same gaps twice. A job restored past its download keeps what it read.
    #[serde(skip)]
    restored_gaps: bool,
}

impl JobSupportFacts {
    pub fn is_empty(&self) -> bool {
        self.demotion.is_none() && self.gaps.is_empty()
    }

    // Record a set's demotion. The first one is kept, since it is the one
    // that explains the job; later ones only add to the count.
    pub fn note_demotion(&mut self, reason: &str, stage: &str, at_epoch_secs: u64) {
        match &mut self.demotion {
            Some(existing) => existing.sets = existing.sets.saturating_add(1),
            None => {
                self.demotion = Some(DemotionFact {
                    reason: identifier(reason),
                    stage: identifier(stage),
                    at_epoch_secs,
                    sets: 1,
                });
            }
        }
    }

    // Record one gap on its pending-to-terminal edge.
    pub fn note_gap(&mut self, kind: GapKind, position: GapPosition) {
        self.start_fresh_after_restore();
        let gaps = &mut self.gaps;
        match kind {
            GapKind::Missing => gaps.missing = gaps.missing.saturating_add(1),
            GapKind::Failed => gaps.failed = gaps.failed.saturating_add(1),
        }
        if gaps.sample.len() < MAX_GAP_SAMPLE && !gaps.sample.contains(&position) {
            gaps.sample.push(position);
        }
    }

    // Charge one gap to each server that refused it.
    pub fn note_gap_servers(&mut self, server_ids: impl IntoIterator<Item = u32>) {
        self.start_fresh_after_restore();
        for server_id in server_ids {
            let servers = &mut self.gaps.servers;
            if let Some(entry) = servers.iter_mut().find(|entry| entry.0 == server_id) {
                entry.1 = entry.1.saturating_add(1);
            } else if servers.len() < MAX_GAP_SERVERS {
                servers.push(ServerGaps(server_id, 1));
            }
        }
    }

    // Undo one gap a verified late replacement filled. The server counts
    // stay: those servers did refuse it.
    pub fn forget_gap(&mut self, kind: GapKind, position: GapPosition) {
        let gaps = &mut self.gaps;
        match kind {
            GapKind::Missing => gaps.missing = gaps.missing.saturating_sub(1),
            GapKind::Failed => gaps.failed = gaps.failed.saturating_sub(1),
        }
        gaps.sample.retain(|entry| *entry != position);
    }

    // Drop the gaps, when the job's terminal segment states are dropped.
    pub fn clear_gaps(&mut self) {
        self.gaps = ArticleGapSummary::default();
        self.restored_gaps = false;
    }

    fn start_fresh_after_restore(&mut self) {
        if self.restored_gaps {
            self.gaps = ArticleGapSummary::default();
            self.restored_gaps = false;
        }
    }

    // The compact JSON for the job's row, or `None` when there is nothing to
    // say and the column stays NULL.
    pub fn to_storage_json(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }
        serde_json::to_string(self).ok()
    }

    // Read a stored record. One written by a newer version, or corrupted in
    // place, reads as nothing rather than failing the report it belongs to,
    // and the caps hold whatever the row says.
    pub fn from_storage_json(raw: &str) -> Self {
        let mut facts: Self = serde_json::from_str(raw).unwrap_or_default();
        if let Some(demotion) = &mut facts.demotion {
            demotion.reason = identifier(&demotion.reason);
            demotion.stage = identifier(&demotion.stage);
        }
        facts.gaps.servers.truncate(MAX_GAP_SERVERS);
        facts.gaps.sample.truncate(MAX_GAP_SAMPLE);
        facts.restored_gaps = !facts.gaps.is_empty();
        facts
    }

    // Read an optional stored column.
    pub fn from_storage(raw: Option<&str>) -> Self {
        raw.map(Self::from_storage_json).unwrap_or_default()
    }
}

// Keep a label only when it is made of what a label is made of. Anything
// else reads as `unknown`, so a row edited by hand cannot carry text into a
// report.
fn identifier(raw: &str) -> String {
    let is_label = !raw.is_empty()
        && raw.len() <= MAX_IDENTIFIER_LEN
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if is_label {
        raw.to_string()
    } else {
        "unknown".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_demotion_is_kept_and_later_ones_are_counted() {
        let mut facts = JobSupportFacts::default();
        facts.note_demotion("par2_damaged", "downloading", 100);
        facts.note_demotion("holds_budget", "repairing", 200);
        assert_eq!(
            facts.demotion,
            Some(DemotionFact {
                reason: "par2_damaged".into(),
                stage: "downloading".into(),
                at_epoch_secs: 100,
                sets: 2,
            })
        );
    }

    #[test]
    fn gaps_are_counted_by_kind_and_sampled_up_to_the_cap() {
        let mut facts = JobSupportFacts::default();
        for segment in 1..=40 {
            facts.note_gap(GapKind::Missing, GapPosition(2, segment));
        }
        facts.note_gap(GapKind::Failed, GapPosition(0, 7));
        assert_eq!(facts.gaps.missing, 40);
        assert_eq!(facts.gaps.failed, 1);
        assert_eq!(facts.gaps.sample.len(), MAX_GAP_SAMPLE);
        assert_eq!(facts.gaps.sample[0], GapPosition(2, 1));
        assert_eq!(facts.gaps.sorted_sample()[0], GapPosition(2, 1));
    }

    #[test]
    fn servers_are_counted_once_per_refused_gap_and_capped() {
        let mut facts = JobSupportFacts::default();
        facts.note_gap_servers([7, 9]);
        facts.note_gap_servers([7]);
        assert_eq!(facts.gaps.servers, vec![ServerGaps(7, 2), ServerGaps(9, 1)]);
        facts.note_gap_servers(0..100);
        assert_eq!(facts.gaps.servers.len(), MAX_GAP_SERVERS);
        assert_eq!(facts.gaps.servers[0], ServerGaps(7, 3));
    }

    #[test]
    fn a_filled_gap_comes_off_the_count_and_the_sample() {
        let mut facts = JobSupportFacts::default();
        facts.note_gap(GapKind::Failed, GapPosition(1, 3));
        facts.note_gap(GapKind::Failed, GapPosition(1, 4));
        facts.forget_gap(GapKind::Failed, GapPosition(1, 3));
        assert_eq!(facts.gaps.failed, 1);
        assert_eq!(facts.gaps.sample, vec![GapPosition(1, 4)]);
    }

    #[test]
    fn storage_round_trips_in_the_compact_shape() {
        let mut facts = JobSupportFacts::default();
        assert!(facts.to_storage_json().is_none());
        facts.note_demotion("member_checksum_mismatch", "downloading", 1_790_000_000);
        facts.note_gap(GapKind::Missing, GapPosition(3, 17));
        facts.note_gap_servers([4]);
        let stored = facts.to_storage_json().unwrap();
        assert_eq!(
            stored,
            r#"{"d":{"r":"member_checksum_mismatch","s":"downloading","t":1790000000,"n":1},"g":{"m":1,"f":0,"s":[[4,1]],"i":[[3,17]]}}"#
        );
        let read = JobSupportFacts::from_storage_json(&stored);
        assert_eq!(read.demotion, facts.demotion);
        assert_eq!(read.gaps, facts.gaps);
    }

    #[test]
    fn a_restarted_download_rebooks_gaps_instead_of_doubling_them() {
        let mut before = JobSupportFacts::default();
        before.note_demotion("par2_damaged", "downloading", 1);
        before.note_gap(GapKind::Missing, GapPosition(0, 1));
        before.note_gap(GapKind::Missing, GapPosition(0, 2));
        let mut restored = JobSupportFacts::from_storage(before.to_storage_json().as_deref());
        assert_eq!(restored.gaps.missing, 2);
        restored.note_gap(GapKind::Missing, GapPosition(0, 1));
        assert_eq!(restored.gaps.missing, 1);
        assert_eq!(restored.gaps.sample, vec![GapPosition(0, 1)]);
        assert!(restored.demotion.is_some());
    }

    #[test]
    fn a_hand_edited_row_cannot_carry_text_or_grow_past_the_caps() {
        let sample: Vec<String> = (0..50).map(|n| format!("[0,{n}]")).collect();
        let raw = format!(
            r#"{{"d":{{"r":"Secret Name.part01.rar","s":"","t":5}},"g":{{"m":1,"i":[{}]}}}}"#,
            sample.join(",")
        );
        let read = JobSupportFacts::from_storage_json(&raw);
        let demotion = read.demotion.unwrap();
        assert_eq!(demotion.reason, "unknown");
        assert_eq!(demotion.stage, "unknown");
        assert_eq!(demotion.sets, 1);
        assert_eq!(read.gaps.sample.len(), MAX_GAP_SAMPLE);
        assert_eq!(
            JobSupportFacts::from_storage_json("not json"),
            JobSupportFacts::default()
        );
    }
}

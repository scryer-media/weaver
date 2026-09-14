//! Per-cohort recovery planning.
//!
//! An interleaved set's global recovery indices are partitioned by
//! `index % cohorts`. A block from the wrong cohort cannot repair this one's
//! losses, so a window that fetches "any recovery article" can spend the whole
//! byte budget on parity that will never be used. Everything here works from
//! the engine's own `RecoveryRequirement` values; nothing infers capacity from
//! a name, and a name is only ever used to *exclude* a carrier whose advertised
//! span provably holds no admissible index.

use super::outcome::CohortDeficit;
use par3_rs::session::RecoveryRequirement;
use std::ops::Range;

/// One deficient cohort's admissible index span, with the indices already held
/// discounted. A cohort in surplus never appears here.
#[derive(Debug, Clone)]
pub(in crate::pipeline) struct CohortWindow {
    pub deficit: CohortDeficit,
    /// Admissible global recovery-index span for the whole cohort.
    pub indices: Range<u64>,
    /// Distinct compatible indices already held, inside `indices`.
    held: Vec<u64>,
    /// Admissible indices in `indices` that are not yet held.
    pub remaining: u64,
}

impl CohortWindow {
    /// Whether any index this cohort still admits falls inside `span`.
    pub fn admits_span(&self, span: &Range<u64>) -> bool {
        let overlap = intersect(&self.indices, span);
        let admissible = congruent_count(&overlap, self.deficit.cohort, self.deficit.cohorts);
        let held = self
            .held
            .iter()
            .filter(|index| overlap.contains(index))
            .count() as u64;
        admissible > held
    }

    /// Whether this cohort's own span can no longer supply what it still needs.
    pub fn is_exhausted(&self) -> bool {
        self.deficit.additional != 0 && self.remaining < self.deficit.additional
    }
}

/// The deficient cohorts of every retained assessment, and what they need.
#[derive(Debug, Default, Clone)]
pub(in crate::pipeline) struct CohortPlan {
    pub windows: Vec<CohortWindow>,
    /// Sum of `additional * block_size` over the deficient cohorts.
    pub needed_bytes: u64,
    /// Whether any retained view still lacks authenticated metadata.
    pub metadata_incomplete: bool,
    /// Whether any retained view was seen at all.
    pub views: usize,
}

impl CohortPlan {
    pub fn push_view(&mut self, requirements: &[RecoveryRequirement], block_size: u64) {
        self.views += 1;
        for need in requirements {
            if need.additional == 0 {
                continue;
            }
            self.needed_bytes = self
                .needed_bytes
                .saturating_add(need.additional.saturating_mul(block_size));
            let held: Vec<u64> = need
                .available
                .iter()
                .copied()
                .filter(|index| {
                    need.recovery_indices.contains(index)
                        && need.cohorts != 0
                        && index % need.cohorts == need.cohort
                })
                .collect();
            let admissible = congruent_count(&need.recovery_indices, need.cohort, need.cohorts);
            self.windows.push(CohortWindow {
                deficit: CohortDeficit::from_requirement(need),
                indices: need.recovery_indices.clone(),
                remaining: admissible.saturating_sub(held.len() as u64),
                held,
            });
        }
    }

    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// True when no deficient cohort admits an index inside `span`, so a
    /// carrier advertising exactly that span cannot help any of them.
    pub fn excludes_span(&self, span: &Range<u64>) -> bool {
        !self.is_empty() && !self.windows.iter().any(|window| window.admits_span(span))
    }

    /// Deficits whose own admissible span is already spent.
    pub fn exhausted(&self) -> Vec<CohortDeficit> {
        self.windows
            .iter()
            .filter(|window| window.is_exhausted())
            .map(|window| window.deficit)
            .collect()
    }

    pub fn deficits(&self) -> Vec<CohortDeficit> {
        self.windows.iter().map(|window| window.deficit).collect()
    }
}

fn intersect(left: &Range<u64>, right: &Range<u64>) -> Range<u64> {
    let start = left.start.max(right.start);
    let end = left.end.min(right.end);
    start..end.max(start)
}

/// Count the indices in `range` congruent to `cohort` modulo `cohorts`.
fn congruent_count(range: &Range<u64>, cohort: u64, cohorts: u64) -> u64 {
    if cohorts == 0 || cohort >= cohorts || range.start >= range.end {
        return 0;
    }
    let below = |limit: u64| {
        let whole = limit / cohorts;
        let rest = limit % cohorts;
        whole + u64::from(cohort < rest)
    };
    below(range.end).saturating_sub(below(range.start))
}

/// The global recovery-index span a PAR3 volume name advertises, when it
/// follows the `<base>.vol<start>+<count>.par3` convention.
///
/// A name is a hint, never a proof: a parsed span may only exclude a carrier
/// from a window, and an unparsable name always stays eligible.
pub(in crate::pipeline) fn volume_span(name: &str) -> Option<Range<u64>> {
    let lower = name.to_ascii_lowercase();
    let (_, suffix) = lower.strip_suffix(".par3")?.rsplit_once(".vol")?;
    let (start, count) = suffix.split_once('+')?;
    let start: u64 = start.parse().ok()?;
    let count: u64 = count.parse().ok()?;
    let end = start.checked_add(count)?;
    (end > start).then_some(start..end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn requirement(
        cohort: u64,
        cohorts: u64,
        span: Range<u64>,
        held: &[u64],
        additional: u64,
    ) -> RecoveryRequirement {
        RecoveryRequirement {
            matrix: [cohort as u8; 16],
            cohort,
            cohorts,
            recovery_indices: span,
            lost: additional,
            available: held.to_vec(),
            additional,
        }
    }

    #[test]
    fn congruent_counting_matches_a_direct_scan() {
        for cohorts in 1..5u64 {
            for cohort in 0..cohorts {
                for start in 0..7u64 {
                    for end in start..12u64 {
                        let direct = (start..end)
                            .filter(|index| index % cohorts == cohort)
                            .count() as u64;
                        assert_eq!(
                            congruent_count(&(start..end), cohort, cohorts),
                            direct,
                            "cohort {cohort}/{cohorts} over {start}..{end}"
                        );
                    }
                }
            }
        }
        assert_eq!(congruent_count(&(0..10), 0, 0), 0);
        assert_eq!(congruent_count(&(0..10), 3, 2), 0);
    }

    #[test]
    fn only_deficient_cohorts_enter_the_plan_and_carry_their_own_bytes() {
        let mut plan = CohortPlan::default();
        plan.push_view(
            &[
                requirement(0, 2, 0..8, &[0, 2], 0),
                requirement(1, 2, 0..8, &[1], 1),
            ],
            4096,
        );
        assert_eq!(plan.windows.len(), 1);
        assert_eq!(plan.windows[0].deficit.cohort, 1);
        assert_eq!(plan.needed_bytes, 4096);
        // Cohort 1 admits 1, 3, 5, 7 and already holds 1.
        assert_eq!(plan.windows[0].remaining, 3);
        assert!(!plan.windows[0].is_exhausted());
    }

    #[test]
    fn a_span_holding_only_surplus_indices_is_excluded() {
        let mut plan = CohortPlan::default();
        plan.push_view(
            &[
                requirement(0, 2, 0..8, &[], 0),
                requirement(1, 2, 0..8, &[1, 3], 1),
            ],
            1024,
        );
        // vol4+1 publishes index 4 only: cohort 0, which is in surplus.
        assert!(plan.excludes_span(&(4..5)));
        // vol5+1 publishes index 5: cohort 1, still wanted.
        assert!(!plan.excludes_span(&(5..6)));
        // An index the cohort already holds is not wanted again.
        assert!(plan.excludes_span(&(3..4)));
        // Outside the admissible span entirely.
        assert!(plan.excludes_span(&(90..99)));
    }

    #[test]
    fn a_cohort_short_of_its_own_span_is_exhausted() {
        let mut plan = CohortPlan::default();
        // Cohort 1 of 2 over 0..4 admits 1 and 3; it holds both and still
        // needs one more, so nothing published anywhere can close it.
        plan.push_view(&[requirement(1, 2, 0..4, &[1, 3], 1)], 512);
        assert_eq!(plan.windows[0].remaining, 0);
        assert!(plan.windows[0].is_exhausted());
        assert_eq!(plan.exhausted().len(), 1);
        assert_eq!(plan.exhausted()[0].cohort, 1);
    }

    /// Deliverable: a two-cohort set, one short by exactly one block and one
    /// in surplus, requests only the deficient cohort's admissible indices,
    /// asks for exactly that deficit in bytes, and — once that cohort has
    /// outrun its own span — names the cohort in an `Unrecoverable` verdict.
    #[test]
    fn a_two_cohort_set_requests_only_the_short_cohorts_own_indices() {
        const BLOCK: u64 = 2048;
        let mut plan = CohortPlan::default();
        plan.push_view(
            &[
                // Cohort 0 admits 0, 2, 4, 6, 8 and holds three of them with
                // nothing further to ask for: it is in surplus.
                requirement(0, 2, 0..10, &[0, 2, 4], 0),
                // Cohort 1 admits 1, 3, 5, 7, 9, holds two, and is short by
                // exactly one block.
                requirement(1, 2, 0..10, &[1, 3], 1),
            ],
            BLOCK,
        );

        // (a) Only the deficient cohort is planned for, and only spans holding
        // an index it still admits survive the filter. Each name here is one
        // volume a real acquisition pass would weigh.
        assert_eq!(plan.windows.len(), 1);
        assert_eq!(plan.windows[0].deficit.cohort, 1);
        let admitted: Vec<&str> = [
            "set.vol0+1.par3",  // index 0   -> cohort 0, in surplus
            "set.vol1+1.par3",  // index 1   -> cohort 1, already held
            "set.vol2+1.par3",  // index 2   -> cohort 0, in surplus
            "set.vol4+1.par3",  // index 4   -> cohort 0, in surplus
            "set.vol5+1.par3",  // index 5   -> cohort 1, wanted
            "set.vol7+2.par3",  // 7, 8      -> 7 is cohort 1, wanted
            "set.vol9+1.par3",  // index 9   -> cohort 1, wanted
            "set.vol20+4.par3", // outside the admissible span entirely
            "set.par3",         // advertises nothing: never excluded
        ]
        .into_iter()
        .filter(|name| volume_span(name).is_none_or(|span| !plan.excludes_span(&span)))
        .collect();
        assert_eq!(
            admitted,
            vec![
                "set.vol5+1.par3",
                "set.vol7+2.par3",
                "set.vol9+1.par3",
                "set.par3"
            ],
            "a window must not spend its budget on a cohort that is not short"
        );

        // (b) The bytes asked for are exactly the one short cohort's deficit;
        // the surplus cohort contributes nothing.
        assert_eq!(plan.needed_bytes, BLOCK);

        // (c) Once the short cohort has consumed its own admissible span, the
        // verdict is Unrecoverable and it names that cohort, not the other.
        let mut spent = CohortPlan::default();
        spent.push_view(
            &[
                requirement(0, 2, 0..10, &[0, 2, 4], 0),
                requirement(1, 2, 0..10, &[1, 3, 5, 7, 9], 1),
            ],
            BLOCK,
        );
        let exhausted = spent.exhausted();
        assert_eq!(exhausted.len(), 1);
        assert_eq!(exhausted[0].cohort, 1);
        let verdict = super::super::outcome::Par3Outcome::Unrecoverable { cohorts: exhausted };
        let message = verdict.to_string();
        assert!(message.contains("cohort 1/2"), "{message}");
        assert!(!message.contains("cohort 0/2"), "{message}");
        assert!(message.contains("short 1"), "{message}");
    }

    #[test]
    fn volume_names_advertise_a_span_only_when_they_follow_the_convention() {
        assert_eq!(volume_span("set.vol0+4.par3"), Some(0..4));
        assert_eq!(volume_span("SET.VOL12+3.PAR3"), Some(12..15));
        assert_eq!(volume_span("archive.part1.vol7+1.par3"), Some(7..8));
        assert_eq!(volume_span("set.par3"), None);
        assert_eq!(volume_span("set.vol0+0.par3"), None);
        assert_eq!(volume_span("set.vol0-4.par3"), None);
        assert_eq!(volume_span("set.volx+4.par3"), None);
        assert_eq!(volume_span("set.vol0+4.par2"), None);
    }
}

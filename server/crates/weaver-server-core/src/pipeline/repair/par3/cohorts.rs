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

/// One deficient cohort's admissible index span and the exact indices the
/// engine says are still to be asked for. A cohort in surplus never appears
/// here.
#[derive(Debug, Clone)]
pub(in crate::pipeline) struct CohortWindow {
    pub deficit: CohortDeficit,
    /// Admissible global recovery-index span for the whole cohort.
    pub indices: Range<u64>,
    /// Exactly the indices this cohort still has to fetch: neither already
    /// available nor already declared in flight. The engine derives these, so
    /// nothing here re-counts congruence or re-subtracts what is held.
    next: Vec<u64>,
    /// Distinct compatible indices already held, inside `indices`. Carrier
    /// exclusion needs these because a carrier may publish any admissible
    /// index, not only the lowest ones the engine would ask for next.
    held: Vec<u64>,
    /// What still has to be asked for, as the engine counts it. This is
    /// `additional` less whatever acquisition has already declared in flight,
    /// so a reassessment mid-fetch never asks for the same index twice.
    outstanding: u64,
}

impl CohortWindow {
    /// Whether a carrier advertising `span` could supply anything this cohort
    /// still wants. The engine names the indices it would ask for next, but a
    /// carrier is free to publish any admissible index, so this walks the
    /// overlap by congruence and stops at the first one not already held.
    /// A cohort whose whole deficit is in flight wants nothing.
    pub fn admits_span(&self, span: &Range<u64>) -> bool {
        if self.outstanding == 0 || self.deficit.cohorts == 0 {
            return false;
        }
        let overlap = intersect(&self.indices, span);
        let Some(mut index) = first_congruent(&overlap, self.deficit.cohort, self.deficit.cohorts)
        else {
            return false;
        };
        // `held` is the handful of indices already in hand, so the first
        // candidate that is not one of them ends this walk immediately.
        while index < overlap.end {
            if !self.held.contains(&index) {
                return true;
            }
            let Some(next) = index.checked_add(self.deficit.cohorts) else {
                return false;
            };
            index = next;
        }
        false
    }

    /// Whether this cohort's own span can no longer supply what it still needs.
    /// The engine stops generating indices at the end of the admissible span,
    /// so a short list is its exhaustion signal.
    pub fn is_exhausted(&self) -> bool {
        self.outstanding != 0 && self.remaining() < self.outstanding
    }

    /// The indices this cohort still has to fetch, lowest first. The engine
    /// derives these; the coordinator reads them straight off the requirement
    /// when it declares a window, so this accessor exists for the tests that
    /// pin the derivation.
    #[cfg(test)]
    pub fn next_indices(&self) -> &[u64] {
        &self.next
    }

    /// How many indices this cohort still has to fetch.
    pub fn remaining(&self) -> u64 {
        self.next.len() as u64
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
            // Only what is not yet spoken for is asked for again: a cohort
            // whose whole deficit is in flight contributes no bytes and admits
            // no carrier until those articles land or fail.
            self.needed_bytes = self
                .needed_bytes
                .saturating_add(need.outstanding.saturating_mul(block_size));
            self.windows.push(CohortWindow {
                deficit: CohortDeficit::from_requirement(need),
                indices: need.recovery_indices.clone(),
                next: need.next_indices.clone(),
                held: need
                    .available
                    .iter()
                    .copied()
                    .filter(|index| {
                        need.recovery_indices.contains(index)
                            && need.cohorts != 0
                            && index % need.cohorts == need.cohort
                    })
                    .collect(),
                outstanding: need.outstanding,
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

/// The indices a window may declare in flight: for each selected carrier,
/// the lowest `admitted` still-wanted indices inside its advertised span,
/// where `admitted` is how many of that carrier's articles the window took.
///
/// A carrier's span is an upper bound on what it holds, never a promise of
/// what one window fetches from it. Declaring the whole span for a 32-article
/// window would tell the engine every remaining index is on its way, its
/// `outstanding` would drop to zero, and the next reassessment would find
/// nothing left to ask for while most of the span was never requested. The
/// count may still overshoot by the carrier's non-recovery packets; the
/// window retracts everything it declared when it drains, so an overshoot
/// costs one reassessment, never a block.
pub(in crate::pipeline) fn declarable_indices(
    next: &[u64],
    carriers: &[(Range<u64>, usize)],
) -> Vec<u64> {
    let mut declared: Vec<u64> = Vec::new();
    for (span, admitted) in carriers {
        let fresh: Vec<u64> = next
            .iter()
            .copied()
            .filter(|index| span.contains(index) && !declared.contains(index))
            .take(*admitted)
            .collect();
        declared.extend(fresh);
    }
    declared.sort_unstable();
    declared
}

fn intersect(left: &Range<u64>, right: &Range<u64>) -> Range<u64> {
    let start = left.start.max(right.start);
    let end = left.end.min(right.end);
    start..end.max(start)
}

/// The lowest index in `range` congruent to `cohort` modulo `cohorts`.
fn first_congruent(range: &Range<u64>, cohort: u64, cohorts: u64) -> Option<u64> {
    if cohorts == 0 || cohort >= cohorts || range.start >= range.end {
        return None;
    }
    let rest = range.start % cohorts;
    let step = if rest <= cohort {
        cohort - rest
    } else {
        cohorts - rest + cohort
    };
    let index = range.start.checked_add(step)?;
    (index < range.end).then_some(index)
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
        requirement_in_flight(cohort, cohorts, span, held, additional, &[])
    }

    /// Mirrors what the engine derives: `outstanding` is the deficit less what
    /// the host has declared in flight, and `next_indices` is exactly that
    /// many admissible indices that are neither held nor in flight.
    fn requirement_in_flight(
        cohort: u64,
        cohorts: u64,
        span: Range<u64>,
        held: &[u64],
        additional: u64,
        in_flight: &[u64],
    ) -> RecoveryRequirement {
        let outstanding = additional.saturating_sub(in_flight.len() as u64);
        let next_indices: Vec<u64> = span
            .clone()
            .filter(|index| {
                cohorts != 0
                    && index % cohorts == cohort
                    && !held.contains(index)
                    && !in_flight.contains(index)
            })
            .take(outstanding as usize)
            .collect();
        RecoveryRequirement {
            matrix: [cohort as u8; 16],
            cohort,
            cohorts,
            recovery_indices: span,
            lost: additional,
            available: held.to_vec(),
            additional,
            in_flight: in_flight.len() as u64,
            outstanding,
            next_indices,
        }
    }

    #[test]
    fn the_first_admissible_index_matches_a_direct_scan() {
        for cohorts in 1..5u64 {
            for cohort in 0..cohorts {
                for start in 0..7u64 {
                    for end in start..12u64 {
                        let direct = (start..end).find(|index| index % cohorts == cohort);
                        assert_eq!(
                            first_congruent(&(start..end), cohort, cohorts),
                            direct,
                            "cohort {cohort}/{cohorts} over {start}..{end}"
                        );
                    }
                }
            }
        }
        assert_eq!(first_congruent(&(0..10), 0, 0), None);
        assert_eq!(first_congruent(&(0..10), 3, 2), None);
    }

    /// Deliverable: a reassessment taken while articles are in flight asks for
    /// nothing that was already requested. The engine subtracts what the host
    /// declared, and the plan spends only what is left.
    #[test]
    fn indices_already_in_flight_are_never_requested_again() {
        const BLOCK: u64 = 1024;
        let mut plan = CohortPlan::default();
        // Cohort 1 of 2 over 0..10 admits 1, 3, 5, 7, 9. It holds 1, is short
        // two blocks, and has already declared index 3 in flight.
        plan.push_view(&[requirement_in_flight(1, 2, 0..10, &[1], 2, &[3])], BLOCK);
        assert_eq!(plan.needed_bytes, BLOCK, "only the unspoken-for block");
        assert_eq!(plan.windows[0].next_indices(), [5]);
        assert_eq!(plan.windows[0].remaining(), 1);
        assert!(!plan.windows[0].is_exhausted());

        // Declaring the rest in flight leaves nothing to ask for, and no
        // carrier is worth fetching until those articles land or fail.
        let mut spoken = CohortPlan::default();
        spoken.push_view(
            &[requirement_in_flight(1, 2, 0..10, &[1], 2, &[3, 5])],
            BLOCK,
        );
        assert_eq!(spoken.needed_bytes, 0);
        assert!(spoken.windows[0].next_indices().is_empty());
        assert!(spoken.excludes_span(&(0..10)));
        assert!(
            !spoken.windows[0].is_exhausted(),
            "fully spoken for is not the same as out of indices"
        );
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
        // Cohort 1 admits 1, 3, 5, 7, already holds 1, and is short one: the
        // engine names index 3 as the one still to ask for.
        assert_eq!(plan.windows[0].next_indices(), [3]);
        assert_eq!(plan.windows[0].remaining(), 1);
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
        assert_eq!(plan.windows[0].remaining(), 0);
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
    fn a_window_declares_only_as_many_indices_as_it_admitted_per_carrier() {
        // 271 still wanted, all inside one 512-index carrier: a 32-article
        // window declares 32, not the whole span.
        let next: Vec<u64> = (511..782).collect();
        let declared = declarable_indices(&next, &[(511..1023, 32)]);
        assert_eq!(declared, (511..543).collect::<Vec<_>>());

        // Two carriers in one window each contribute their own admitted
        // count, indices outside every span are never declared, and a carrier
        // with more articles than wanted indices declares only what exists.
        let next: Vec<u64> = vec![1, 3, 5, 7, 9, 11, 40, 41];
        let declared = declarable_indices(&next, &[(0..8, 2), (8..12, 5), (20..30, 3)]);
        assert_eq!(declared, vec![1, 3, 9, 11]);

        // No carrier, or no admitted articles, declares nothing.
        assert!(declarable_indices(&next, &[]).is_empty());
        assert!(declarable_indices(&next, &[(0..100, 0)]).is_empty());
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

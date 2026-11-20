// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Two independent archive sets in one job, under the graph engine.
//
// Each set is an archive of its own, so nothing a schedule does to one set
// is a reason for the other to deliver less. Every member of both sets is
// wanted. A set delivers its member whole unless the schedule starves one
// of its slots, or loses one with no recovery posted; then it delivers
// nothing, and the other set still delivers whole. So a starved slot in one
// set leaves the job Partial, and in both, Failed. Neither set is
// encrypted, so a wrong password changes nothing. A member assembled from
// the other set's volumes is not its own bytes, so the rule set's byte
// check is the never-merge check.
use super::*;

// A two-set layout of the archive matrix.
#[derive(Clone, Copy, Debug)]
pub enum TwoSets {
    // Two single-volume stored sets: the first in slots 0 and 1, the second
    // in slots 2 and 3.
    Stored,
    // Two two-volume stored sets under hex names, posted interleaved: the
    // first set's volumes in slots 0 and 2, the second's in slots 1 and 3.
    Hex(HexTwoSets),
}

impl TwoSets {
    // The set the article in schedule slot `slot` belongs to.
    fn set_of(self, slot: u32) -> usize {
        match self {
            Self::Stored => slot as usize / 2,
            Self::Hex(_) => slot as usize % 2,
        }
    }

    fn fixture(self) -> GraphFixture {
        let (names, payloads, volumes, spec) = match self {
            Self::Stored => two_set_fixture(),
            Self::Hex(format) => hex_two_set_fixture(format),
        };
        let options = ScheduleOptions::MATRIX;
        let members: BTreeMap<String, Vec<u8>> = names
            .iter()
            .map(|name| name.to_string())
            .zip(payloads)
            .collect();
        let sets = names.map(str::to_string);
        let deliverable: Deliverable = Box::new(move |_opens, interruption| {
            let mask = interruption.loss().map_or(0, |(mask, _)| mask);
            let unmendable = interruption.fails() || options.recovery == RecoveryFormat::Absent;
            let lost: BTreeSet<usize> = (0..4)
                .filter(|slot| unmendable && mask & (1 << slot) != 0)
                .map(|slot| self.set_of(slot))
                .collect();
            Leaves {
                deliverable: sets
                    .iter()
                    .enumerate()
                    .filter(|(set, _)| !lost.contains(set))
                    .map(|(_, name)| name.clone())
                    .collect(),
                mended: lost.is_empty(),
                files: None,
            }
        });
        GraphFixture {
            label: format!("{self:?}"),
            options,
            spec,
            volumes,
            described: None,
            members,
            needs_password: false,
            deliverable,
        }
    }
}

pub async fn two_set_campaign(layout: TwoSets, selection: Selection) {
    graph_fixture_campaign(layout.fixture(), selection).await;
}

// One named arrival order, uninterrupted, under its smoke replay index.
pub async fn two_set_order(layout: TwoSets, order: &[(u32, u32)]) {
    let schedule = (order.to_vec(), Interruption::None);
    let case = schedules()
        .into_iter()
        .position(|smoke| smoke == schedule)
        .expect("the order is a smoke schedule");
    assert_cells(&graph_fixture_cases(&layout.fixture(), [(case, schedule)]).await);
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR3 recovery: the archive matrix's loss cases with a PAR3 set posted in
// place of PAR2.
//
// Every case loses at least one article, and the PAR3 set covers each loss
// a starved schedule does not make, so the member is delivered whole. A
// starved schedule delivers only what the post still holds whole and fails
// the job as a repair when that is nothing.
use super::super::repair::{Fixture, LOSS_CASES, Slice, campaign_cases, carries_loss, fixture};
use super::*;

pub async fn graph_par3_campaign(format: Format, slice: Slice) {
    let Fixture {
        spec,
        volumes,
        described,
        members,
        ..
    } = fixture(format);
    let options = ScheduleOptions {
        recovery: RecoveryFormat::Par3,
        ..ScheduleOptions::MATRIX
    };
    let members: BTreeMap<String, Vec<u8>> = members
        .into_iter()
        .map(|(name, bytes)| (name.to_string(), bytes))
        .collect();
    let fixture = GraphFixture {
        label: format!("par3 {format:?}"),
        options,
        deliverable: rar_post(&volumes, &spec, &members, options),
        needs_password: spec.password.is_some(),
        spec,
        volumes,
        described,
        members,
    };
    let cases = campaign_cases(slice, carries_loss, LOSS_CASES);
    assert_cells(&graph_fixture_cases(&fixture, cases).await);
}

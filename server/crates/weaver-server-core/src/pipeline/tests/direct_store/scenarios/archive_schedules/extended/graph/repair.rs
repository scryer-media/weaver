// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// PAR3 recovery: the archive matrix's loss cases with a PAR3 set posted in
// place of PAR2.
//
// Every case loses at least one article, and the PAR3 set covers each loss
// a starved schedule does not make, so the member is delivered whole. A
// starved schedule delivers only what the post still holds whole and fails
// the job as a repair when that is nothing.
use super::super::repair::{
    Fixture, LOSS_CASES, Slice, campaign_cases, carries_loss, fixture, par3_shards,
};
use super::*;

async fn graph_par3_campaign(format: Format, slice: Slice) {
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

macro_rules! graph_par3_smoke {
    ($($name:ident $format:ident),+ $(,)?) => {
        $(
            #[tokio::test]
            async fn $name() {
                graph_par3_campaign(Format::$format, Slice::Smoke).await;
            }
        )+
    };
}

graph_par3_smoke!(
    par3_rar4_loss_smoke Rar4,
    par3_rar5_loss_smoke Rar5,
    par3_rar4_encrypted_loss_smoke Rar4Encrypted,
    par3_rar5_encrypted_loss_smoke Rar5Encrypted,
    par3_rar4_four_volume_loss_smoke Rar4FourVolumes,
    par3_rar5_four_volume_loss_smoke Rar5FourVolumes,
    par3_rar5_obfuscated_loss_smoke Rar5Obfuscated,
);

par3_shards!(combined_par3_rar4, graph_par3_campaign, [Format::Rar4]);
par3_shards!(combined_par3_rar5, graph_par3_campaign, [Format::Rar5]);
par3_shards!(
    combined_par3_rar4_encrypted,
    graph_par3_campaign,
    [Format::Rar4Encrypted]
);
par3_shards!(
    combined_par3_rar5_encrypted,
    graph_par3_campaign,
    [Format::Rar5Encrypted]
);
par3_shards!(
    combined_par3_rar4_four_volume,
    graph_par3_campaign,
    [Format::Rar4FourVolumes]
);
par3_shards!(
    combined_par3_rar5_four_volume,
    graph_par3_campaign,
    [Format::Rar5FourVolumes]
);
par3_shards!(
    combined_par3_rar5_obfuscated,
    graph_par3_campaign,
    [Format::Rar5Obfuscated]
);

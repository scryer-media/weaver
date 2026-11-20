// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The PAR3 axis beyond the archive matrix proper: the fixtures its loss
// cases run over, and the slices and shards that cut them.
use super::*;

// Combined cases that carry a loss, and so a recovery set.
pub(super) const LOSS_CASES: usize = 4743;

// A job and everything a schedule over it must deliver.
pub(super) struct Fixture {
    pub spec: JobSpec,
    pub volumes: Vec<(String, Vec<u8>)>,
    pub described: Option<Vec<String>>,
    pub members: Vec<(&'static str, Vec<u8>)>,
}

// The fixture the archive matrix posts for `format`.
pub(super) fn fixture(format: Format) -> Fixture {
    let name = "nested/feature.mkv";
    let password = "moonlit-harbour";
    let length = match format {
        // A described volume is bound by the fingerprint of its first 16 KiB,
        // which its offset-zero article has to cover whole.
        Format::Rar5Obfuscated => 70_001,
        _ => 6001,
    };
    let payload: Vec<u8> = (0..length)
        .map(|n| ((n * 7 + n / 251) % 253) as u8)
        .collect();
    let count = format.volume_count();
    let volumes = match format {
        Format::Rar4 | Format::Rar4FourVolumes => {
            single_member_rar4_store_set(name, &payload, count)
        }
        Format::Rar5 | Format::Rar5FourVolumes => single_member_store_set(name, &payload, count),
        Format::Rar5Obfuscated => obfuscate_volumes(&single_member_store_set(name, &payload, 2)),
        Format::Rar4Encrypted => {
            encrypted_rar4_store_set(name, &payload, count, password, Some(TEST_RAR4_SALT))
        }
        Format::Rar5Encrypted => {
            encrypted_store_set(name, &payload, 2, password, Some(password), false)
        }
        other => unreachable!("{other:?} has no repair campaign"),
    };
    let described = matches!(format, Format::Rar5Obfuscated).then(|| {
        single_member_store_set(name, &payload, 2)
            .into_iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>()
    });
    let mut spec = direct_store_job_spec_with_articles("Archive schedules", &volumes, 4 / count);
    spec.password = matches!(format, Format::Rar4Encrypted | Format::Rar5Encrypted)
        .then(|| password.to_string());
    Fixture {
        spec,
        volumes,
        described,
        members: vec![(name, payload)],
    }
}

// Which slice of a campaign one test runs.
#[derive(Clone, Copy, Debug)]
pub(super) enum Slice {
    // The default suite's sample, drawn from the smoke schedules.
    Smoke,
    // One of `of` shards of the combined cases the campaign keeps.
    Shard { shard: usize, of: usize },
}

// The cases a campaign keeps, by the replay index of the matrix they are
// drawn from: `WEAVER_ARCHIVE_SCHEDULE_CASE` replays a smoke case and
// `WEAVER_ARCHIVE_COMBINED_CASE` a combined one.
pub(super) fn campaign_cases(
    slice: Slice,
    keep: fn(Interruption) -> bool,
    pinned: usize,
) -> Vec<(usize, Schedule)> {
    match slice {
        Slice::Smoke => schedules()
            .into_iter()
            .enumerate()
            .filter(|(_, (_, interruption))| keep(*interruption))
            .collect(),
        Slice::Shard { shard, of } => {
            let cases: Vec<_> = combined_schedule_cases()
                .into_iter()
                .filter(|(_, (_, interruption))| keep(*interruption))
                .collect();
            if std::env::var_os("WEAVER_ARCHIVE_COMBINED_CASE").is_none() {
                assert_eq!(cases.len(), pinned);
            }
            cases
                .into_iter()
                .filter(|(case, _)| case % of == shard)
                .collect()
        }
    }
}

pub(super) fn carries_loss(interruption: Interruption) -> bool {
    interruption.loss().is_some()
}

#[test]
fn par3_campaign_case_count() {
    if std::env::var_os("WEAVER_ARCHIVE_COMBINED_CASE").is_some() {
        return;
    }
    let cases = combined_schedule_cases();
    assert_eq!(cases.len(), 6543);
    let losses = cases
        .iter()
        .filter(|(_, (_, interruption))| carries_loss(*interruption))
        .count();
    assert_eq!(losses, LOSS_CASES);
}

// Shards cut each campaign to well under a thousand cases, so no test
// approaches the runner's per-test limit. Each shard calls `$run` with the
// bracketed arguments and its slice.
macro_rules! par3_shards {
    ($module:ident, $run:path, $args:tt) => {
        mod $module {
            use super::*;
            par3_shards!(@each $run, $args, 8;
                shard_0 0, shard_1 1, shard_2 2, shard_3 3,
                shard_4 4, shard_5 5, shard_6 6, shard_7 7);
        }
    };
    (@each $run:path, $args:tt, $of:literal; $($name:ident $shard:literal),+) => {
        $(
            #[tokio::test]
            #[ignore = "opt-in extended archive matrix; run with the archive-matrix-extended Nextest profile and --run-ignored all"]
            async fn $name() {
                par3_shards!(@call $run, $args, Slice::Shard { shard: $shard, of: $of });
            }
        )+
    };
    (@call $run:path, [$($arg:expr),*], $selection:expr) => {
        $run($($arg,)* $selection).await
    };
}
pub(super) use par3_shards;

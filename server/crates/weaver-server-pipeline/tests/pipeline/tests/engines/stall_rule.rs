// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that waits with nothing pending ends. A PAR2 set packed inside a
// stored RAR, beside the file it covers, loses an article of that file and
// every block-carrying article of its own volumes: the set inside rules
// that it waits for blocks, the archive holds the volumes open until the
// job is quiet, and the job is not quiet while a set waits. Nothing more can
// arrive, so the job's objects are told so, and the job ends from what it
// delivered, under the status every job ends in. A run that waits on its
// shelf for input no event brings has that input ended. The rule never
// fires while something the job is owed can still come: a pause of
// post-processing, a repair out on its thread, recovery still being
// fetched, an article still out.

use super::harness::*;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

use weaver_server_pipeline::pipeline::tests::engines::stall_fixtures::{
    STALL_ENDED, STALLED, blocked_for_good_and_handed_on, in_progress, stall_lines,
    stalled_job_gives_its_admission_to_the_next, under_paused_post_processing,
    waiting_on_articles_still_out, warnings, with_its_repair_out,
};

const FRAME: usize = 1024;

// The stored RAR `lantern.rar` holding `lantern.mkv` and a PAR2 index and
// two volumes over it, posted in kilobyte articles. One article of the
// file is lost, and every article lying wholly inside either volume.
fn par2_inside_a_short_archive() -> Scenario {
    let mkv = payload(48 * FRAME, 201);
    let par2 = par2_volumes_sliced("lantern", &[("lantern.mkv", &mkv)], FRAME as u64, 4, 2);
    let mut entries: Vec<(&str, Vec<u8>)> = vec![("lantern.mkv", mkv.clone())];
    entries.extend(
        par2.iter()
            .map(|post| (post.name.as_str(), post.bytes.clone())),
    );
    let rar = multi_member_store_set(&entries, 1).remove(0).1;
    let find = |needle: &[u8]| {
        rar.windows(needle.len())
            .position(|window| window == needle)
            .unwrap()
    };
    let mut lost = vec![(find(&mkv) / FRAME + 5) as u32];
    for volume in &par2[1..] {
        let start = find(&volume.bytes);
        let end = start + volume.bytes.len();
        lost.extend((start.div_ceil(FRAME)..end / FRAME).map(|ordinal| ordinal as u32));
    }
    lost.into_iter().fold(
        Scenario::new(vec![Post::plain("lantern.rar", rar)]).framed(FRAME),
        |scenario, ordinal| scenario.lost(0, ordinal),
    )
}

// A plain file and a PAR2 index and volume over it, one article of the
// file lost: the set rebuilds it from its volume.
fn repairable() -> Scenario {
    let file = payload(16 * ARTICLE, 202);
    let mut posts = vec![Post::plain("ember.mkv", file.clone())];
    posts.extend(par2_set("ember", &[("ember.mkv", &file)], 2));
    Scenario::new(posts).lost(0, 3)
}

#[tokio::test]
async fn a_set_waiting_inside_an_archive_short_of_its_volumes_ends() {
    let (lines, guard) = warnings();
    let outcome = try_run(&par2_inside_a_short_archive()).await;
    drop(guard);
    let outcome = outcome
        .unwrap_or_else(|stall| panic!("job stalled at {:?}: {}", stall.status, stall.state));
    // The quiet ends it under the status rules: the file in the archive
    // lost an article its set cannot rebuild, so nothing was delivered.
    assert!(
        matches!(&outcome.status, Some(JobStatus::Failed { error }) if error.starts_with("no file was delivered whole")),
        "{:?}",
        outcome.status
    );
    assert!(
        lines.of(STALL_ENDED).is_empty(),
        "{:?}",
        stall_lines(&lines)
    );
    let told = lines.of(STALLED);
    assert_eq!(told.len(), 1, "{:?}", stall_lines(&lines));
    assert!(
        told[0].contains(&format!("job_id={}", JOB.0)) && told[0].contains("waiting for recovery"),
        "{told:?}"
    );
}

// With one NZB admitted at a time, the stalled job ends as any failed job
// does and gives its admission to the job queued behind it.
#[tokio::test]
async fn a_stalled_job_gives_its_admission_to_the_next() {
    stalled_job_gives_its_admission_to_the_next(&par2_inside_a_short_archive()).await;
}

// While post-processing is paused the job holds its slots for nothing:
// it waits, and the rule leaves it. Once it resumes, the job ends.
#[tokio::test]
async fn a_job_under_paused_post_processing_is_not_stalled() {
    let (paused, status, stalled) =
        under_paused_post_processing(&par2_inside_a_short_archive()).await;
    assert!(paused.is_empty(), "{paused:?}");
    assert!(
        matches!(status, Some(JobStatus::Failed { .. })),
        "{status:?}"
    );
    assert_eq!(stalled.len(), 1, "{stalled:?}");
}

// A repair out on its thread speaks through the mail: gates run while it
// is out do not end the job, which then completes repaired.
#[tokio::test]
async fn a_job_whose_repair_is_out_is_not_stalled() {
    let (status, lines) = with_its_repair_out(&repairable()).await;
    assert!(lines.is_empty(), "{lines:?}");
    assert_eq!(status, Some(JobStatus::Complete));
}

// The core release of `scene_zips`: zips holding RAR volumes whose member
// is a zip, under a clean set. A seam on the job withholds what decides
// that the bytes its volumes lack are lost for good, so the run reading
// the inner archive waits on its shelf for a change nothing will make. The
// rule ends the input of that run, as a short payload ends; the job ends
// as the ordinary lost path ends it, Complete, and gives its admission to
// the job behind it. The job hears of a run beginning to wait at most once
// for each gate turn that asked.
#[tokio::test]
async fn a_run_blocked_for_good_has_its_input_ended_and_the_job_ends() {
    use super::scene_zips::{CORE, INNER, deflated_zip, inner, nested_core};
    let zipped = deflated_zip(&[(INNER, inner())]);
    let scenario = nested_core((format!("{CORE}.zip").as_str(), zipped), true, true);
    let ended = blocked_for_good_and_handed_on(&scenario).await;
    assert_eq!(ended.status, Some(JobStatus::Complete));
    assert!(!ended.input_ended.is_empty(), "no run was blocked for good");
    assert!(
        ended
            .input_ended
            .iter()
            .all(|line| line.contains(&format!("job_id={}", JOB.0))),
        "{:?}",
        ended.input_ended
    );
    assert!(
        !ended.lines.iter().any(|line| line.contains(STALL_ENDED)),
        "{:?}",
        ended.lines
    );
    assert!(
        ended.told <= ended.asks,
        "{} notices for {} asking turns",
        ended.told,
        ended.asks
    );
}

// The compressed four-volume RAR set, every article of its third volume
// out with the server: the run reads the first two and waits on its shelf
// for the third. Nothing asks to hear of the wait while articles are out,
// and the job is not stalled.
#[tokio::test]
async fn a_run_waiting_for_articles_still_out_is_not_stalled() {
    let posts = super::rar::lz_four()
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect();
    let scenario = Scenario::new(posts);
    let (lines, guard) = warnings();
    let held = waiting_on_articles_still_out(&scenario, 2).await;
    drop(guard);
    assert!(in_progress(&held.report.status), "{:?}", held.report.status);
    assert!(stall_lines(&lines).is_empty(), "{:?}", stall_lines(&lines));
    assert_eq!(held.status, Some(JobStatus::Complete));
}

// The set's recovery volume, asked for once the file lost an article, is
// out with the server: the job waits on it, and completes once it lands.
#[tokio::test]
async fn a_job_fetching_its_recovery_is_not_stalled() {
    let scenario = repairable();
    let (lines, guard) = warnings();
    let held = run_holding(&scenario, 2).await;
    drop(guard);
    assert!(in_progress(&held.report.status), "{:?}", held.report.status);
    assert!(stall_lines(&lines).is_empty(), "{:?}", stall_lines(&lines));
    assert_eq!(held.status, Some(JobStatus::Complete));
}

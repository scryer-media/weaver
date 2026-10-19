// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A verdict on a member's bytes is void once its set repairs them. An
// archive posted with bytes spoiled before it was cut into articles arrives
// whole by the articles' checksums, so its engine reads it at once and
// rules on the damage before the PAR2 set beside it has arrived. The set
// then rules and repairs it, and the archive is read again as the repaired
// bytes say: every member is delivered whole.

use std::collections::BTreeMap;

use sevenz_turbo::encoder_options::Lzma2Options;
use sha2::Digest;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{MemberId, NodeId, Why};

use super::harness::{
    ARTICLE, JOB, Post, Scenario, admit, collect, finished, par2_volumes, par2_volumes_sliced,
    serve,
};
use super::rar::delivered;
use super::sevenz;
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::{
    debug_job_state, hear_graph_mail, job_status_for_assert, pump_pipeline_runtime_queues,
    rar5_fixture_bytes, retire_pipeline_database,
};

const MIXED: &str = "rar5_multifile_lz.rar";
// A byte inside the archive's headers: the engine's read of it ends in an
// error that names no volume as damaged, so it refuses the archive.
const FLIPPED: usize = 38;
const FRAME: usize = 16;

// The archive as posted, a byte flipped, then an index and a volume made
// over it as it was before the flip.
fn posts() -> Vec<Post> {
    let original = rar5_fixture_bytes(MIXED);
    let mut posted = original.clone();
    posted[FLIPPED] ^= 0xff;
    let mut posts = vec![Post::plain("mixed.rar", posted)];
    posts.extend(par2_volumes("mixed", &[("mixed.rar", &original)], 2, 1));
    posts
}

fn articles(post: &Post) -> u32 {
    post.bytes.len().div_ceil(FRAME) as u32
}

// Hear the job's runs until the archive's `volumes` are refused for its
// damage.
async fn until_refused_unrepairable(pipeline: &mut Pipeline, volumes: u32) {
    loop {
        let writer = job(pipeline).writer();
        if (0..volumes)
            .all(|volume| writer.refused(NodeId::ROOT, MemberId(volume)) == Some(Why::Unrepairable))
        {
            break;
        }
        assert!(
            job(pipeline).runs().live(),
            "no run is out and nothing was refused: {}",
            debug_job_state(pipeline, JOB)
        );
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
        pump_pipeline_runtime_queues(pipeline).await;
    }
}

// Run the completion gate and hear every run until the job finishes.
pub(super) async fn drive_to_finish(pipeline: &mut Pipeline, scenario: &Scenario) {
    let mut idle = 0;
    while !finished(pipeline) {
        let mut moved = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.download_queue.pop())
        {
            moved = true;
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve(pipeline, scenario, article).await;
        }
        if finished(pipeline) {
            break;
        }
        pipeline.check_job_completion(JOB).await;
        pump_pipeline_runtime_queues(pipeline).await;
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pump_pipeline_runtime_queues(pipeline).await;
        }
        if !finished(pipeline)
            && pipeline
                .graph_jobs
                .get(&JOB)
                .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            moved = true;
        }
        idle = if moved { 0 } else { idle + 1 };
        assert!(
            idle < 2 || finished(pipeline),
            "stalled at {:?}: {}",
            job_status_for_assert(pipeline, JOB),
            debug_job_state(pipeline, JOB)
        );
    }
}

#[tokio::test]
async fn a_member_its_engine_refused_before_its_set_repaired_it_is_read_again_under_graph() {
    let posts = posts();
    let (archive, index, volume) = (
        articles(&posts[0]),
        articles(&posts[1]),
        articles(&posts[2]),
    );
    let scenario = Scenario::new(posts).framed(FRAME);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario, Engine::Graph).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
        .is_some()
    {}

    // The archive alone: the engine refuses it before any set is in.
    for ordinal in 0..archive {
        serve(&mut pipeline, &scenario, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    until_refused_unrepairable(&mut pipeline, 1).await;
    assert!(
        pipeline.graph_jobs[&JOB].recovery_summary().is_empty(),
        "no set is in"
    );

    // The set arrives, rules, and repairs the flipped slice: the repair goes
    // out in the step its last block arrives, and what it came to is heard.
    let set = (0..index)
        .map(|ordinal| (1, ordinal))
        .chain((0..volume).map(|ordinal| (2, ordinal)));
    for article in set {
        serve(&mut pipeline, &scenario, article).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    hear_graph_mail(&mut pipeline, JOB).await;
    assert_eq!(
        outcomes(&pipeline),
        [Some(RepairOutcome::Repaired { slices: 1 })]
    );

    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        ["hello.txt", "second.txt", "zeros_64k.bin"]
            .map(delivered)
            .to_vec()
    );
    assert!(files[&delivered("zeros_64k.bin")] == vec![0u8; 64 * 1024]);
}

// Every article of the posts at `files`, in order.
pub(super) async fn serve_posts(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    files: std::ops::Range<u32>,
) {
    for file in files {
        let post = &scenario.posts[file as usize];
        for ordinal in 0..post.bytes.len().div_ceil(scenario.article) as u32 {
            serve(pipeline, scenario, (file, ordinal)).await;
            pump_pipeline_runtime_queues(pipeline).await;
        }
    }
}

// Hear the job's runs until none is out.
async fn until_no_run(pipeline: &mut Pipeline) {
    while pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.runs().live())
    {
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
    }
}

fn job(pipeline: &Pipeline) -> &crate::pipeline::graph::job::GraphJob {
    pipeline.graph_jobs.get(&JOB).unwrap_or_else(|| {
        panic!(
            "{:?}: {}",
            job_status_for_assert(pipeline, JOB),
            debug_job_state(pipeline, JOB)
        )
    })
}

// The set's outcomes as the job reports them.
fn outcomes(pipeline: &Pipeline) -> Vec<Option<RepairOutcome>> {
    job(pipeline)
        .recovery_summary()
        .iter()
        .map(|set| set.outcome)
        .collect()
}

const FOUR: [&str; 4] = [
    "rar5_lz_four.part01.rar",
    "rar5_lz_four.part02.rar",
    "rar5_lz_four.part03.rar",
    "rar5_lz_four.part04.rar",
];
const SLICE: usize = 1024;
// Bytes inside the second volume's file header.
const ZEROED: std::ops::Range<usize> = 60..80;

fn profile(name: &str) -> Vec<u8> {
    std::fs::read(format!(
        "{}/tests/fixtures/extraction_profiles/{name}",
        env!("CARGO_MANIFEST_DIR")
    ))
    .unwrap()
}

// A compressed four-volume RAR5 set with bytes of its second volume zeroed
// before posting, an index and a volume made over it as it was.
fn four_volumes_zeroed() -> Vec<Post> {
    let originals: Vec<Vec<u8>> = FOUR.iter().map(|name| profile(name)).collect();
    let mut posts: Vec<Post> = FOUR
        .iter()
        .zip(&originals)
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    posts[1].bytes[ZEROED].fill(0);
    let files: Vec<(&str, &[u8])> = FOUR
        .iter()
        .copied()
        .zip(originals.iter().map(Vec::as_slice))
        .collect();
    posts.extend(par2_volumes_sliced(
        "rar5_lz_four",
        &files,
        SLICE as u64,
        2,
        1,
    ));
    posts
}

// Every volume of the set is refused for the zeroed bytes before the PAR2
// set is in. The set's repair rewrites the second volume, and a new store
// is offered all four again: the volumes it was not yet handed go on from
// their own files, as they arrived, and the member decodes whole.
#[tokio::test]
async fn a_four_volume_rar_refused_before_its_set_repaired_one_volume_is_read_again_under_graph() {
    let scenario = Scenario::new(four_volumes_zeroed()).framed(SLICE);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario, Engine::Graph).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
        .is_some()
    {}

    serve_posts(&mut pipeline, &scenario, 0..4).await;
    until_refused_unrepairable(&mut pipeline, 4).await;

    serve_posts(&mut pipeline, &scenario, 4..6).await;
    hear_graph_mail(&mut pipeline, JOB).await;
    assert_eq!(
        outcomes(&pipeline),
        [Some(RepairOutcome::Repaired { slices: 1 })]
    );

    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        [delivered("compressible.txt")]
    );
    let oracle: serde_json::Value =
        serde_json::from_str(&String::from_utf8(profile("expected.json")).unwrap()).unwrap();
    let expected = &oracle["archives"]["rar5_lz_four"]["compressible.txt"];
    assert_eq!(
        hex::encode(sha2::Sha256::digest(&files[&delivered("compressible.txt")])),
        expected["sha256"].as_str().unwrap()
    );
}

// A header-enciphered LZMA2 7z split in three, one slice of its second
// volume zeroed before posting, and an index and a volume made over the
// volumes as they were. The engine, opened by the job's password, reads the
// zeroed slice before the set is in and takes the bytes it cannot decode
// for a wrong password. The set repairs the slice, the engine reads the
// repaired bytes through on the same password, and nothing is left asking.
#[tokio::test]
async fn a_header_enciphered_split_7z_whose_engine_read_damage_before_its_set_repaired_it_delivers_under_graph()
 {
    let archive = sevenz::sealed_header(Lzma2Options::from_level(1).into());
    let volumes = sevenz::split(&archive, 3);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let set = par2_volumes("lantern", &files, 2, 1);
    let mut posts = volumes;
    posts[1].bytes[ARTICLE..2 * ARTICLE].fill(0);
    posts.extend(set);
    let scenario = Scenario::new(posts).password(sevenz::SECRET);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario, Engine::Graph).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
        .is_some()
    {}

    serve_posts(&mut pipeline, &scenario, 0..3).await;
    until_no_run(&mut pipeline).await;
    serve_posts(&mut pipeline, &scenario, 3..5).await;
    hear_graph_mail(&mut pipeline, JOB).await;
    assert_eq!(
        outcomes(&pipeline),
        [Some(RepairOutcome::Repaired { slices: 1 })]
    );

    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    let expected = sevenz::members();
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>()
    );
    for (name, bytes) in expected {
        assert!(files[&delivered(name)] == bytes, "{name} bytes");
    }
}

// The type of the second member's header: the engine delivers the first
// member whole, then its read of the second ends in an error that names no
// volume as damaged, so it refuses the archive. A byte the header's fields
// read differently instead fails the member's checksum, which is damage
// the engine waits on a repair for, not a refusal.
const FLIPPED_SECOND: usize = 88;

// Every member file of every namespace below the root, by path: the live
// ones and those of the objects that read the archive and were buried.
fn extracted(pipeline: &Pipeline) -> Vec<std::path::PathBuf> {
    let job = job(pipeline);
    let tree = job.tree();
    tree.live()
        .into_iter()
        .filter(|node| *node != NodeId::ROOT)
        .chain(tree.kept(NodeId::ROOT, MemberId(0)))
        .flat_map(|node| job.writer().files(node).map(|(_, path)| path.to_path_buf()))
        .collect()
}

// A member delivered whole before its archive was refused is made over when
// the set repairs the archive and a new reader reads it again, as a buried
// node's members are: it is delivered once, under its own name, with the
// bytes it had.
#[tokio::test]
async fn a_member_delivered_whole_before_its_archive_was_refused_is_made_over_once_under_graph() {
    let original = rar5_fixture_bytes(MIXED);
    let mut posted = original.clone();
    posted[FLIPPED_SECOND] ^= 0xff;
    let mut posts = vec![Post::plain("mixed.rar", posted)];
    posts.extend(par2_volumes("mixed", &[("mixed.rar", &original)], 2, 1));
    let scenario = Scenario::new(posts).framed(FRAME);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario, Engine::Graph).await;
    while pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.download_queue.pop())
        .is_some()
    {}

    serve_posts(&mut pipeline, &scenario, 0..1).await;
    until_refused_unrepairable(&mut pipeline, 1).await;
    let first = extracted(&pipeline);
    assert!(
        first.len() == 1 && first[0].ends_with("hello.txt"),
        "{first:?}"
    );
    let delivered_first = std::fs::read(&first[0]).unwrap();

    serve_posts(&mut pipeline, &scenario, 1..3).await;
    hear_graph_mail(&mut pipeline, JOB).await;
    assert_eq!(
        outcomes(&pipeline),
        [Some(RepairOutcome::Repaired { slices: 1 })]
    );

    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        ["hello.txt", "second.txt", "zeros_64k.bin"]
            .map(delivered)
            .to_vec()
    );
    assert!(files[&delivered("hello.txt")] == delivered_first);
    assert!(files[&delivered("zeros_64k.bin")] == vec![0u8; 64 * 1024]);
}

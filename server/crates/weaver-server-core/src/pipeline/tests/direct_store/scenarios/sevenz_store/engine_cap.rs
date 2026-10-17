// The job's extraction concurrency bounds its live child engines. A child past
// the bound keeps landing its bytes in its parent's member, and its engine
// starts when a live one finishes.

use super::*;
use crate::pipeline::direct_store::nested::NodeMode;
use crate::pipeline::direct_store::plan::{SetFormat, StreamKind};
use std::collections::BTreeSet;
use std::io::Write;

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut writer = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

const NOTES: &[u8] = b"the stored sibling\n";

// What one capped job did: every child engine seen live and the most seen at
// once, whether a child was ever held, the root graph's nodes, and the output.
#[derive(Default)]
struct Engines {
    peak: usize,
    started: BTreeSet<u32>,
    held: bool,
    nodes: Vec<(String, SetFormat, NodeMode)>,
}

impl Engines {
    fn observe(&mut self, pipeline: &Pipeline, job: JobId) {
        if let Some(runtime) = pipeline.archive_runtime.get(&job) {
            let live = runtime.live_child_engines();
            self.peak = self.peak.max(live.len());
            self.started.extend(live.iter().map(|(_, node)| *node));
            self.held |= !runtime.held_child_engines().is_empty();
        }
        if let Some(graph) = pipeline
            .direct_store
            .set(job, 0)
            .and_then(|set| set.nested_graph())
        {
            self.nodes = graph
                .nodes
                .iter()
                .map(|node| {
                    let plan = node.router.plan();
                    (plan.set_name.clone(), plan.format, node.mode)
                })
                .collect();
        }
    }
}

struct CappedRun {
    engines: Engines,
    pipeline: Pipeline,
    output: PathBuf,
    _temp: tempfile::TempDir,
}

// Routes a stored 7z root holding `members` with the job's extraction
// concurrency at one, then drives its extractions to a terminal status,
// observing the live engines after every step that can start or finish one.
async fn run_capped(job: JobId, members: Vec<Entry>) -> CappedRun {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline
        .tuner
        .set_profile_tuning(crate::runtime::ProfileTuning {
            max_concurrent_extractions: 1,
            ..pipeline.tuner.profile_tuning()
        });
    assert_eq!(pipeline.tuner.max_concurrent_extractions(), 1);
    // A stored sibling beside the children, so the root installs a member of
    // its own.
    let mut entries = vec![Entry::file("notes.txt", NOTES.to_vec())];
    entries.extend(members);
    let root = build_7z(&entries, EncoderMethod::COPY, None);
    let volumes = split_volumes(&root, 1);
    let spec = sevenz_job_spec(&volumes, 2);
    insert_active_job(&mut pipeline, job, spec.clone()).await;
    let mut engines = Engines::default();
    for segment in 0..2 {
        submit_volume_article(&mut pipeline, job, &volumes, 0, segment).await;
        engines.observe(&pipeline, job);
    }
    pipeline.check_job_completion(job).await;
    engines.observe(&pipeline, job);
    drain_rar_refreshes(&mut pipeline).await;
    engines.observe(&pipeline, job);
    loop {
        pump_pipeline_runtime_queues(&mut pipeline).await;
        engines.observe(&pipeline, job);
        match job_status_for_assert(&pipeline, job) {
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. }) => break,
            Some(JobStatus::Moving) => {
                let done = pipeline.move_done_rx.recv().await.unwrap();
                pipeline.handle_move_to_complete_done(done).await;
            }
            _ => {
                let done = next_extraction_done(&mut pipeline).await;
                pipeline.handle_extraction_done(done).await;
            }
        }
        engines.observe(&pipeline, job);
    }
    CappedRun {
        engines,
        output: complete_dir.join(crate::jobs::working_dir::sanitize_dirname(&spec.name)),
        pipeline,
        _temp: temp,
    }
}

fn assert_completed(run: &CappedRun, job: JobId) {
    assert!(
        matches!(
            job_status_for_assert(&run.pipeline, job),
            Some(JobStatus::Complete)
        ),
        "{}",
        debug_job_state(&run.pipeline, job)
    );
    assert_eq!(std::fs::read(run.output.join("notes.txt")).unwrap(), NOTES);
}

#[tokio::test]
async fn a_stored_root_runs_its_xz_children_one_engine_at_a_time() {
    let job = JobId(862201);
    let leaves: Vec<_> = (0..3u8).map(|seed| payload(seed + 61, 65_537)).collect();
    let names = ["first.bin", "second.bin", "third.bin"];
    let run = run_capped(
        job,
        vec![
            Entry::file("first.bin.xz", xz(&leaves[0])),
            Entry::file("second.bin.xz", xz(&leaves[1])),
            Entry::file("third.bin.xz", xz(&leaves[2])),
        ],
    )
    .await;
    assert_completed(&run, job);
    let xz = SetFormat::Stream(StreamKind::Xz);
    assert_eq!(
        run.engines.nodes,
        [
            ("first.bin.xz".to_owned(), xz, NodeMode::Engine),
            ("second.bin.xz".to_owned(), xz, NodeMode::Engine),
            ("third.bin.xz".to_owned(), xz, NodeMode::Engine),
        ]
    );
    assert_eq!(run.engines.peak, 1, "one engine slot");
    assert!(run.engines.held, "two children waited for the slot");
    for (name, leaf) in names.iter().zip(&leaves) {
        assert_eq!(&std::fs::read(run.output.join(name)).unwrap(), leaf);
        assert!(!run.output.join(format!("{name}.xz")).exists());
    }
    let runtime = run.pipeline.archive_runtime.get(&job);
    assert!(
        runtime.is_none_or(|runtime| runtime.live_child_engines().is_empty()
            && runtime.held_child_engines().is_empty())
    );
}

// A stored 7z beside the xz children routes directly as its bytes land. It
// takes no engine slot, so the bound neither holds it nor counts it.
#[tokio::test]
async fn a_direct_store_sibling_streams_past_the_engine_bound() {
    let job = JobId(862202);
    let stored_leaf = payload(71, 65_537);
    let inner = build_7z(
        &[Entry::file("stored.bin", stored_leaf.clone())],
        EncoderMethod::COPY,
        None,
    );
    let leaves: Vec<_> = (0..2u8).map(|seed| payload(seed + 81, 65_537)).collect();
    let run = run_capped(
        job,
        vec![
            Entry::file("first.bin.xz", xz(&leaves[0])),
            Entry::file("inner.7z", inner),
            Entry::file("second.bin.xz", xz(&leaves[1])),
        ],
    )
    .await;
    assert_completed(&run, job);
    let xz = SetFormat::Stream(StreamKind::Xz);
    assert_eq!(
        run.engines.nodes,
        [
            ("first.bin.xz".to_owned(), xz, NodeMode::Engine),
            ("inner.7z".to_owned(), SetFormat::SevenZip, NodeMode::Direct),
            ("second.bin.xz".to_owned(), xz, NodeMode::Engine),
        ]
    );
    assert_eq!(run.engines.peak, 1, "one engine slot");
    assert!(run.engines.held, "an xz child waited for the slot");
    assert!(
        !run.engines.started.contains(&2),
        "the direct child never took a slot"
    );
    assert_eq!(
        std::fs::read(run.output.join("stored.bin")).unwrap(),
        stored_leaf
    );
    for (name, leaf) in ["first.bin", "second.bin"].iter().zip(&leaves) {
        assert_eq!(&std::fs::read(run.output.join(name)).unwrap(), leaf);
    }
    assert!(!run.output.join("inner.7z").exists());
}

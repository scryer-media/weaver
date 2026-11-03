// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A sealed RAR set's embedded records read their volumes under the candidate
// the store opened the headers on, which the store hands each record once.
// A record whose repair comes back after the loop moved something it read
// is rebuilt from its facts, which keep no candidate: the rebuilt record is
// handed the candidate before it hears anything, so it rules under it as the
// lent one did.

use std::collections::BTreeMap;

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, GraphView, JOB, Post, Scenario, admit, collect, finished, observe, payload, serve,
};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::direct_store::protected_sealed_rar4_store_set;
use crate::pipeline::tests::direct_store::scenarios::protected_header_encrypted_store_set;
use crate::pipeline::tests::{
    job_status_for_assert, pump_pipeline_runtime_queues, retire_pipeline_database,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-lantern-secret";

fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The last article of the post at `file`.
fn last_article(scenario: &Scenario, file: u32) -> (u32, u32) {
    let post = &scenario.posts[file as usize];
    (file, post.bytes.len().div_ceil(scenario.article) as u32 - 1)
}

fn repair_out(pipeline: &Pipeline) -> bool {
    pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.repair_out())
}

struct Settled {
    status: Option<JobStatus>,
    files: BTreeMap<String, Vec<u8>>,
    asked: BTreeMap<(u32, u32), usize>,
    graph: GraphView,
}

// The job served one article at a time, `held` kept out as a worker keeps
// an article it asked for, until a repair is out on its thread. Then `held`
// lands, so the volume it ends settles while the repair is out, and the
// repair is heard back after. Panics when no repair went out while `held`
// was kept.
async fn run_settling_while_lent(scenario: &Scenario, held: (u32, u32)) -> Settled {
    let (_temp_dir, mut pipeline, complete_dir) = admit(scenario).await;
    let mut asked = BTreeMap::new();
    let mut graph = GraphView::default();
    let mut reads = 0;
    let mut kept = None;
    let mut landed = false;
    let mut quiet = 0;
    loop {
        let mut served = 0;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&JOB)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            *asked.entry(article).or_insert(0usize) += 1;
            if article == held && !landed {
                *pipeline.active_downloads_by_job.entry(JOB).or_insert(0) += 1;
                kept = Some(work);
                continue;
            }
            serve(&mut pipeline, scenario, article).await;
            served += 1;
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            observe(&pipeline, &mut graph, &mut reads);
        }
        // The repair is out and has not been heard: the held article lands
        // now, under it.
        if kept.is_some() && repair_out(&pipeline) {
            kept = None;
            landed = true;
            pipeline.active_downloads_by_job.insert(JOB, 0);
            serve(&mut pipeline, scenario, held).await;
            if pipeline.jobs.contains_key(&JOB) && pipeline.job_decode_stage_drained(JOB) {
                pipeline.maybe_finish_download_pass(JOB);
            }
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        if finished(&pipeline) {
            break;
        }
        if pipeline.jobs.contains_key(&JOB) {
            pipeline.check_job_completion(JOB).await;
        }
        pump_pipeline_runtime_queues(&mut pipeline).await;
        observe(&pipeline, &mut graph, &mut reads);
        if finished(&pipeline) {
            break;
        }
        if kept.is_some() && repair_out(&pipeline) {
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            observe(&pipeline, &mut graph, &mut reads);
            continue;
        }
        if served == 0 {
            quiet += 1;
            assert!(
                quiet < 3,
                "the job stalled at {:?} with {held:?} {}",
                job_status_for_assert(&pipeline, JOB),
                if landed { "landed" } else { "still held" },
            );
        } else {
            quiet = 0;
        }
    }
    assert!(landed, "no repair went out while {held:?} was held");
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Settled {
        status,
        files,
        asked,
        graph,
    }
}

// The first volume's record repaired the damaged unit; the second found its
// volume whole. The member is delivered whole and nothing was asked twice.
fn assert_repaired_under_the_candidate(settled: &Settled, body: &[u8]) {
    assert_eq!(settled.status, Some(JobStatus::Complete));
    assert_eq!(
        settled.files.keys().cloned().collect::<Vec<_>>(),
        vec![format!("lantern beacon/{MEMBER}")]
    );
    assert!(settled.files[&format!("lantern beacon/{MEMBER}")] == body);
    assert_eq!(
        settled.graph.rulings,
        vec![
            (
                "rar_record".to_string(),
                Some(RepairOutcome::Repaired { slices: 1 })
            ),
            ("rar_record".to_string(), Some(RepairOutcome::Clean)),
        ]
    );
    let again: Vec<_> = settled
        .asked
        .iter()
        .filter(|(_, asked)| **asked > 1)
        .collect();
    assert!(again.is_empty(), "{again:?}");
}

// RAR4 `-hp -rr`: two volumes, each with a record of one parity sector, one
// article of the first served damaged. The second volume's last article
// lands while the first volume's record is out repairing, so the record
// comes back to a loop that moved what it read and is rebuilt from its
// facts.
#[tokio::test]
async fn a_sealed_record_rebuilt_from_its_facts_on_return_rules_under_the_candidate_its_store_handed()
 {
    let body = payload(7 * ARTICLE + 13, 51);
    let set = protected_sealed_rar4_store_set(MEMBER, &body, 2, 1, SECRET);
    let scenario = Scenario::new(posts(set)).damaged(0, 2).password(SECRET);
    let held = last_article(&scenario, 1);
    let settled = run_settling_while_lent(&scenario, held).await;
    assert_repaired_under_the_candidate(&settled, &body);
}

// RAR5 `-hp -rr`: the same ordering, the damaged article wholly in the
// first volume's cipher bytes, so the enciphered headers still open. Its
// record is found without the candidate, so the rebuilt record rules the
// same handed or not; the row holds the RAR5 kind to the same outcome.
#[tokio::test]
async fn a_sealed_rar5_record_rebuilt_from_its_facts_on_return_rules_under_the_candidate_its_store_handed()
 {
    let body = payload(7 * ARTICLE + 13, 51);
    let set = protected_header_encrypted_store_set(MEMBER, &body, 2, SECRET, 1);
    let layout = unrar_rs::locate_recovery_record(&mut std::io::Cursor::new(&set[0].1), None)
        .unwrap()
        .unwrap();
    let in_data = (layout.protected_size / ARTICLE as u64 - 1) as u32;
    let scenario = Scenario::new(posts(set))
        .damaged(0, in_data)
        .password(SECRET);
    let held = last_article(&scenario, 1);
    let settled = run_settling_while_lent(&scenario, held).await;
    assert_repaired_under_the_candidate(&settled, &body);
}

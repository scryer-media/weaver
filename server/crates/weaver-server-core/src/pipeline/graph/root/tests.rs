// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{
    Builder, ByteRange, Delivery, Emit, Extent, Kind, MemberId, NodeId, Object, ObjectId,
    Placement, Pull, Seek, Standing, VolumeIndex, Weight, Why,
};
use weaver_model::files::FileRole;

use super::intake::Decoded;
use super::phase::{self, Activity, RootPhase};
use super::seams::apply_moves;
use super::source::{Class, Move};
use super::window::{WINDOW_IN_ORDER_VOLUMES, WINDOW_VOLUMES};
use super::{ArticleRoot, Ask};
use crate::jobs::ids::{JobId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, JobStatus, SegmentSpec};
use crate::operations::disk::CapacityReader;
use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
use crate::pipeline::graph::Engine;
use crate::pipeline::graph::checkpoint;
use crate::pipeline::graph::engine::{RunMail, Runs};
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::tests::{
    insert_active_job, insert_active_job_on, new_direct_pipeline, retire_pipeline_database,
    sample_nzb_zstd,
};
use crate::pipeline::{
    DecodeResult, DecodedChunk, SegmentEncoding, SegmentSource, SegmentTerminalState,
    YencLayoutAssertions,
};
use weaver_graph::recovery::RepairOutcome;

const ZIP_FRONT: &[u8] = b"PK\x03\x04";

// A stand-in for a format's object: it asks for what it was told to until
// it is answered, and remembers the answers.
struct Probe {
    wants: Vec<Seek>,
    answers: Arc<Mutex<Vec<Pull>>>,
}

impl Object for Probe {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Zip
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    // Its whole volume in, the probe announces one member it never closes,
    // the way an object still at work on its last bytes looks.
    fn volume_closed(&mut self, _: VolumeIndex, out: &mut Vec<Emit>) {
        out.push(Emit::Member {
            member: MemberId(0),
            name: "inner.bin".to_string(),
            length: None,
        });
    }
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    // Quiet with the member it announced still open, the probe refuses
    // itself as a format object would: it never mapped what it named.
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if standing.unplaced() {
            out.push(Emit::Refuse(Why::Unmapped));
        }
    }
    fn wants(&self, out: &mut Vec<Seek>) {
        out.extend_from_slice(&self.wants);
    }
    // A seek answered is wanted no more.
    fn pulled(&mut self, seek: Seek, pull: Pull, _: &mut Vec<Emit>) {
        self.wants.retain(|want| *want != seek);
        self.answers.lock().unwrap().push(pull);
    }
    fn locate(&self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Placement>) {}
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

struct Probes {
    wants: Vec<Seek>,
    answers: Arc<Mutex<Vec<Pull>>>,
}

impl Builder for Probes {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::Zip).then(|| {
            Box::new(Probe {
                wants: std::mem::take(&mut self.wants),
                answers: Arc::clone(&self.answers),
            }) as Box<dyn Object>
        })
    }

    fn read(
        &self,
        _: &weaver_graph::Tree,
        _: weaver_graph::NodeId,
        _: weaver_graph::VolumeIndex,
        _: &weaver_graph::Placement,
        _: &mut Vec<u8>,
    ) -> bool {
        false
    }
}

pub(in crate::pipeline::graph) fn file(name: &str, sizes: &[u32]) -> RootFile {
    RootFile {
        name: name.to_string(),
        articles: sizes
            .iter()
            .enumerate()
            .map(|(ordinal, bytes)| (ordinal as u32, *bytes))
            .collect(),
    }
}

// A job whose files go to a fresh directory, kept for as long as the
// returned handle lives.
pub(in crate::pipeline::graph) fn job_with(
    files: &[RootFile],
    wants: Vec<Seek>,
) -> (GraphJob, tempfile::TempDir, Arc<Mutex<Vec<Pull>>>) {
    let answers = Arc::new(Mutex::new(Vec::new()));
    let builder = Probes {
        wants,
        answers: Arc::clone(&answers),
    };
    let dir = tempfile::tempdir().unwrap();
    let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }));
    let writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        Arc::clone(&accountant),
        CapacityReader::unknown(),
    );
    let runs = Runs::new(1, 1, RunMail::default().sender(), accountant);
    let job = GraphJob::new(files, Box::new(builder), writer, runs, u32::MAX);
    (job, dir, answers)
}

pub(in crate::pipeline::graph) fn decoded(
    volume: u32,
    ordinal: u32,
    offset: u64,
    bytes: &[u8],
    length: Option<u64>,
) -> Decoded {
    Decoded {
        volume: VolumeIndex(volume),
        ordinal,
        offset,
        bytes: Bytes::copy_from_slice(bytes),
        intact: true,
        positioned: true,
        length,
        segments: Vec::new(),
    }
}

// The front of a zip member, padded to `len`.
fn zip_front(len: usize) -> Vec<u8> {
    let mut bytes = ZIP_FRONT.to_vec();
    bytes.resize(len, 0);
    bytes
}

fn damaged_ranges(job: &GraphJob, member: u32) -> Vec<ByteRange> {
    job.tree()
        .namespace(NodeId::ROOT)
        .and_then(|namespace| namespace.get(MemberId(member)))
        .map(|member| member.coverage.damaged_ranges().collect())
        .unwrap_or_default()
}

fn data_bid(volume: u32, weight: u32, bidder: u32, in_order: bool) -> Ask {
    Ask {
        seek: Seek::data(VolumeIndex(volume), ByteRange::new(0, 100), Weight(weight)),
        bidder: NodeId(bidder),
        in_order,
    }
}

fn windowed(moves: &[Move]) -> Vec<u32> {
    let mut volumes: Vec<u32> = moves
        .iter()
        .filter(|found| matches!(found.class, Class::Window(_)))
        .map(|found| found.volume.0)
        .collect();
    volumes.dedup();
    volumes
}

fn spec(name: &str, files: Vec<(String, FileRole, u32)>) -> JobSpec {
    JobSpec {
        name: name.to_string(),
        password: None,
        total_bytes: files
            .iter()
            .map(|(_, _, segments)| u64::from(*segments) * 64)
            .sum(),
        category: None,
        metadata: vec![],
        files: files
            .into_iter()
            .enumerate()
            .map(|(index, (filename, role, segments))| FileSpec {
                filename,
                role,
                groups: vec!["alt.binaries.test".to_string()],
                posted_at_epoch: None,
                segments: (0..segments)
                    .map(|ordinal| SegmentSpec {
                        ordinal,
                        article_number: ordinal,
                        bytes: 64,
                        message_ids: crate::jobs::MessageIds::new(vec![format!(
                            "lantern-{index}-{ordinal}@example.com"
                        )]),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn recovery_role() -> FileRole {
    FileRole::Par2 {
        is_index: false,
        recovery_block_count: 4,
    }
}

fn segment_id(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

// Stamp a row graph behind the switch's back, the way a row written by a
// process with the switch on reads after a restart with it off.
pub(in crate::pipeline::graph) fn stamp_graph(db: &crate::Database, job_id: JobId) {
    let datastore = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::execute(
            datastore.read_exec(),
            "UPDATE active_jobs SET engine = {} WHERE job_id = {}",
            &[
                SqlArg::Text(Engine::Graph.as_str().to_string()),
                SqlArg::I64(job_id.0 as i64),
            ],
        )
        .await?;
        Ok(())
    })
    .unwrap();
}

#[test]
fn the_phase_is_the_first_state_that_holds() {
    for activity in [
        Activity::Idle,
        Activity::Verifying,
        Activity::Fetching,
        Activity::Repairing,
    ] {
        for extracting in [false, true] {
            assert_eq!(
                RootPhase::derive(true, activity, extracting),
                RootPhase::Downloading
            );
        }
    }
    for extracting in [false, true] {
        assert_eq!(
            RootPhase::derive(false, Activity::Repairing, extracting),
            RootPhase::Repairing
        );
        assert_eq!(
            RootPhase::derive(false, Activity::Verifying, extracting),
            RootPhase::Verifying
        );
        assert_eq!(
            RootPhase::derive(false, Activity::Fetching, extracting),
            RootPhase::Downloading
        );
    }
    assert_eq!(
        RootPhase::derive(false, Activity::Idle, true),
        RootPhase::Extracting
    );
    assert_eq!(
        RootPhase::derive(false, Activity::Idle, false),
        RootPhase::Done
    );
    assert_eq!(phase::activity([], false), Activity::Idle);
    assert_eq!(
        phase::activity([Some(RepairOutcome::Clean), None], false),
        Activity::Verifying
    );
    assert_eq!(
        phase::activity([None, Some(RepairOutcome::Waiting)], false),
        Activity::Fetching
    );
    assert_eq!(
        phase::activity([None, Some(RepairOutcome::Waiting)], true),
        Activity::Repairing
    );
    assert_eq!(RootPhase::Done.job_status(), None);
    assert_eq!(
        RootPhase::Extracting.job_status(),
        Some(JobStatus::Extracting)
    );
    assert!(!RootPhase::may_replace(&JobStatus::Paused));
}

#[test]
fn a_graph_job_reports_each_phase_as_its_tree_moves() {
    // inner.zip in two articles, the second held back. The zip object
    // announces its member from the first; with bytes still to fetch the
    // root reports downloading, since that phase wins over extracting.
    let files = [file("inner.zip", &[100, 100]), file("plain.bin", &[100])];
    let (mut job, _dir, _) = job_with(&files, Vec::new());
    assert_eq!(job.phase_change(), Some(RootPhase::Downloading));
    assert_eq!(job.phase_change(), None);

    job.intake(
        decoded(1, 0, 0, &[7; 90], Some(90)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    assert_eq!(job.phase(), RootPhase::Downloading);

    job.intake(
        decoded(0, 0, 0, &zip_front(90), Some(180)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    assert_eq!(job.phase(), RootPhase::Downloading);

    // Every volume in and the probe never closes the member it announced:
    // the object refuses itself as unmapped once the job is quiet, in the
    // settle that closes the last volume, and the job is done.
    job.intake(
        decoded(0, 1, 90, &[0; 90], Some(180)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    assert_eq!(job.phase(), RootPhase::Done);
    assert_eq!(
        job.writer().refused(NodeId::ROOT, MemberId(0)),
        Some(Why::Unmapped)
    );

    let (mut plain, _plain_dir, _) = job_with(&[file("plain.bin", &[100])], Vec::new());
    plain
        .intake(
            decoded(0, 0, 0, &[7; 90], Some(90)),
            &mut RepairSlots::default(),
            1,
        )
        .unwrap();
    assert_eq!(plain.phase(), RootPhase::Done);
}

#[test]
fn a_data_seek_moves_the_articles_that_cover_it_into_the_window() {
    let wants = vec![Seek::data(
        VolumeIndex(0),
        ByteRange::new(150, 160),
        Weight(1),
    )];
    let (mut job, _dir, answers) = job_with(&[file("inner.zip", &[100, 100, 100])], wants);
    // The first article binds the zip object and says how long the file is,
    // which pins where the second article's bytes can lie.
    job.intake(
        decoded(0, 0, 0, &zip_front(90), Some(270)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();

    let moves = job.seek_round(|_| true).unwrap();

    // The seek moves its article; the bootstrap leads the bound set's
    // first volume, so the rest of it follows into the window.
    assert_eq!(
        moves,
        vec![
            Move {
                volume: VolumeIndex(0),
                ordinal: 1,
                class: Class::Window(0),
            },
            Move {
                volume: VolumeIndex(0),
                ordinal: 2,
                class: Class::Window(0),
            }
        ]
    );
    assert_eq!(*answers.lock().unwrap(), vec![Pull::Moved(1)]);
}

#[test]
fn the_window_holds_eight_volumes_and_four_for_an_in_order_reader() {
    let volumes: Vec<RootFile> = (0..12)
        .map(|volume| file(&format!("part{volume}.bin"), &[100]))
        .collect();

    let mut root = ArticleRoot::new(&volumes);
    let asks: Vec<Ask> = (0..12)
        .map(|volume| data_bid(volume, volume + 1, 1, false))
        .collect();
    let answers = root.answer(&asks, |_| true);
    assert_eq!(windowed(&root.map.take_moves()), (0..8).collect::<Vec<_>>());
    assert_eq!(
        answers
            .iter()
            .filter(|pull| **pull == Pull::Moved(1))
            .count(),
        WINDOW_VOLUMES
    );

    let mut root = ArticleRoot::new(&volumes);
    let asks: Vec<Ask> = (0..6)
        .map(|volume| data_bid(volume, volume + 1, 1, true))
        .collect();
    root.answer(&asks, |_| true);
    assert_eq!(
        windowed(&root.map.take_moves()).len(),
        WINDOW_IN_ORDER_VOLUMES
    );

    // A volume with nothing queued (all of it in flight) takes no place.
    let mut root = ArticleRoot::new(&volumes);
    let asks: Vec<Ask> = (0..12)
        .map(|volume| data_bid(volume, volume + 1, 1, false))
        .collect();
    root.answer(&asks, |volume| volume != VolumeIndex(2));
    assert_eq!(
        windowed(&root.map.take_moves()),
        vec![0, 1, 3, 4, 5, 6, 7, 8]
    );
}

#[test]
fn a_bid_the_full_window_had_no_room_for_is_drawn_when_asked_again_once_a_place_frees() {
    let volumes: Vec<RootFile> = (0..12)
        .map(|volume| file(&format!("part{volume}.bin"), &[100]))
        .collect();
    let mut root = ArticleRoot::new(&volumes);
    let asks: Vec<Ask> = (0..12)
        .map(|volume| data_bid(volume, volume + 1, 1, false))
        .collect();
    root.answer(&asks, |_| true);
    assert_eq!(windowed(&root.map.take_moves()), (0..8).collect::<Vec<_>>());

    // The window keeps no bid: a round that asks nothing draws nothing.
    root.answer(&[], |volume| volume.0 >= 8);
    assert!(windowed(&root.map.take_moves()).is_empty());

    // The bidder asks again, and the volumes that were drawn have nothing
    // left queued: the rest take their places.
    root.answer(&asks, |volume| volume.0 >= 8);
    assert_eq!(windowed(&root.map.take_moves()), vec![8, 9, 10, 11]);
}

#[tokio::test]
async fn a_recovery_seek_lands_in_the_promoted_class() {
    // Past the 60 bytes that arrived: the second article holds it.
    let wants = vec![Seek::recovery(
        VolumeIndex(0),
        ByteRange::new(64, 65),
        Weight(1),
    )];
    let (mut job, _dir, answers) = job_with(
        // A recovery volume by its name, not an index the root asks for
        // first.
        &[
            file("inner.zip", &[64, 64]),
            file("blocks.vol0+1.par2", &[64]),
        ],
        wants,
    );
    job.intake(
        decoded(0, 0, 0, &zip_front(60), Some(120)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    let moves = job.seek_round(|_| true).unwrap();
    assert_eq!(*answers.lock().unwrap(), vec![Pull::Moved(1)]);
    assert!(!moves.is_empty());
    assert!(moves.iter().all(|found| found.class == Class::Recovery));

    // In the queue, a promoted article leaves wherever the recovery volume
    // waited and leads everything else.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_001);
    let spec = spec(
        "lantern promoted",
        vec![
            ("lantern.bin".to_string(), FileRole::Standalone, 4),
            ("lantern.vol0+4.par2".to_string(), recovery_role(), 2),
        ],
    );
    insert_active_job(&mut pipeline, job_id, spec).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let parked = segment_id(job_id, 1, 1);
    let waiting = |state: &crate::jobs::model::JobState| {
        state
            .download_queue
            .count_matching(|work| work.segment_id == parked)
            + state
                .recovery_queue
                .count_matching(|work| work.segment_id == parked)
    };
    assert_eq!(waiting(state), 1);
    apply_moves(
        state,
        &[Move {
            volume: VolumeIndex(1),
            ordinal: 1,
            class: Class::Recovery,
        }],
    );
    assert_eq!(waiting(state), 1);
    let first = state.download_queue.pop().unwrap();
    assert_eq!(first.segment_id, parked);
    assert!(first.completion_critical && first.is_recovery);
}

#[tokio::test]
async fn a_window_move_leads_the_ordinary_articles_of_other_files() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_002);
    let files = (0..3)
        .map(|index| (format!("lantern.{index:03}"), FileRole::Unknown, 3))
        .collect();
    insert_active_job(&mut pipeline, job_id, spec("lantern window", files)).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    apply_moves(
        state,
        &[2, 1].map(|ordinal| Move {
            volume: VolumeIndex(2),
            ordinal,
            class: Class::Window(0),
        }),
    );
    let mut order = Vec::new();
    while let Some(work) = state.download_queue.pop() {
        if work.segment_id.segment_number > 0 {
            order.push(work.segment_id);
        }
    }
    assert_eq!(
        &order[..2],
        &[segment_id(job_id, 2, 1), segment_id(job_id, 2, 2)]
    );
}

#[tokio::test]
async fn the_first_article_of_every_member_is_requested_before_ordinary_articles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_003);
    // More members than the classic sample takes, and a recovery volume.
    let mut files: Vec<(String, FileRole, u32)> = (0..40)
        .map(|index| {
            (
                format!("lantern.part{index:02}.bin"),
                FileRole::Standalone,
                3,
            )
        })
        .collect();
    files.push(("lantern.vol0+4.par2".to_string(), recovery_role(), 3));
    let members = files.len();
    insert_active_job_on(
        &mut pipeline,
        job_id,
        spec("lantern sample", files),
        Engine::Graph,
    )
    .await;
    pipeline.admit_graph_job(job_id, Engine::Graph);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let leading: Vec<SegmentId> = (0..members)
        .map(|_| state.download_queue.pop().unwrap().segment_id)
        .collect();
    assert!(leading.iter().all(|id| id.segment_number == 0));
    let sampled: HashSet<u32> = leading.iter().map(|id| id.file_id.file_index).collect();
    assert_eq!(sampled.len(), members);
    assert_ne!(
        state
            .download_queue
            .pop()
            .unwrap()
            .segment_id
            .segment_number,
        0
    );
}

// Every file role the classic engine ranks or parks, in an order no role
// ranking would keep.
fn ranked_roles() -> Vec<(String, FileRole, u32)> {
    vec![
        ("lantern.vol0+4.par2".to_string(), recovery_role(), 2),
        ("lantern.bin".to_string(), FileRole::Unknown, 2),
        (
            "lantern.part2.rar".to_string(),
            FileRole::RarVolume { volume_number: 1 },
            2,
        ),
        (
            "lantern.part1.rar".to_string(),
            FileRole::RarVolume { volume_number: 0 },
            3,
        ),
        ("lantern.7z".to_string(), FileRole::SevenZipArchive, 2),
        (
            "lantern.par2".to_string(),
            FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0,
            },
            1,
        ),
    ]
}

#[tokio::test]
async fn a_graph_job_is_queued_in_posted_order_at_one_neutral_priority() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_010);
    let files = ranked_roles();
    let posted: Vec<SegmentId> = files
        .iter()
        .enumerate()
        .flat_map(|(index, (_, _, segments))| {
            (0..*segments).map(move |ordinal| segment_id(job_id, index as u32, ordinal))
        })
        .collect();
    insert_active_job_on(
        &mut pipeline,
        job_id,
        spec("lantern posted", files),
        Engine::Graph,
    )
    .await;

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    // Nothing is parked by its name: the recovery volume waits in the
    // download queue with everything else, and nothing marks it recovery.
    assert_eq!(state.recovery_queue.count_matching(|_| true), 0);
    let mut queued = Vec::new();
    while let Some(work) = state.download_queue.pop() {
        assert_eq!(work.priority, crate::pipeline::graph::NEUTRAL_PRIORITY);
        assert!(!work.is_recovery);
        assert!(!work.completion_critical);
        queued.push(work.segment_id);
    }
    assert_eq!(queued, posted);
}

#[tokio::test]
async fn a_requeued_graph_article_comes_back_at_the_roots_priority_not_its_roles() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_011);
    insert_active_job_on(
        &mut pipeline,
        job_id,
        spec("lantern requeued", ranked_roles()),
        Engine::Graph,
    )
    .await;
    pipeline.admit_graph_job(job_id, Engine::Graph);
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    while state.download_queue.pop().is_some() {}

    // The first RAR volume's role ranks it ahead of everything, and a
    // retry rebuilt from the post says so; the root holds it ordinary.
    let first_volume = segment_id(job_id, 3, 1);
    let retry = pipeline
        .download_retry_work(first_volume, 1, Vec::new())
        .unwrap();
    assert_eq!(
        retry.priority,
        FileRole::RarVolume { volume_number: 0 }.download_priority()
    );
    pipeline.enqueue_download_work(retry);

    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let requeued = state.download_queue.pop().unwrap();
    assert_eq!(requeued.segment_id, first_volume);
    assert_eq!(requeued.priority, crate::pipeline::graph::NEUTRAL_PRIORITY);
    assert!(state.download_queue.pop().is_none());
}

#[tokio::test]
async fn a_lost_article_marks_its_range_damaged() {
    // Both edges known: the range is exact.
    let (mut job, _dir, _) = job_with(&[file("plain.bin", &[100, 100, 100])], Vec::new());
    job.intake(
        decoded(0, 0, 0, &[7; 90], Some(270)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    job.intake(
        decoded(0, 2, 180, &[7; 90], Some(270)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    job.lost(VolumeIndex(0), 1, &mut RepairSlots::default(), 1)
        .unwrap();
    assert_eq!(damaged_ranges(&job, 0), vec![ByteRange::new(90, 180)]);

    // An unknown end: once nothing else can arrive, the widest bound.
    let (mut job, _dir, _) = job_with(&[file("plain.bin", &[100, 100])], Vec::new());
    job.intake(
        decoded(0, 0, 0, &[7; 90], None),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    job.lost(VolumeIndex(0), 1, &mut RepairSlots::default(), 1)
        .unwrap();
    assert_eq!(damaged_ranges(&job, 0), vec![ByteRange::new(90, 190)]);

    // A seek into a lost range is answered Lost.
    let mut root = ArticleRoot::new(&[file("plain.bin", &[100, 100, 100])]);
    root.receive(decoded(0, 0, 0, &[7; 90], Some(270)));
    root.receive(decoded(0, 2, 180, &[7; 90], Some(270)));
    root.map.lost(VolumeIndex(0), 1);
    assert_eq!(
        root.map.seek(
            VolumeIndex(0),
            ByteRange::new(100, 120),
            Some(Class::Critical)
        ),
        Pull::Lost
    );

    // Through the download path: every server failing it is the edge. The
    // second file keeps the job healthy, so the loss does not fail it.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_004);
    let spec = spec(
        "lantern lost",
        vec![
            ("lantern.bin".to_string(), FileRole::Standalone, 1),
            ("lantern.dat".to_string(), FileRole::Standalone, 19),
        ],
    );
    insert_active_job_on(&mut pipeline, job_id, spec, Engine::Graph).await;
    pipeline.admit_graph_job(job_id, Engine::Graph);
    pipeline.book_terminal_segment(segment_id(job_id, 0, 0), SegmentTerminalState::Missing);
    assert_eq!(
        damaged_ranges(&pipeline.graph_jobs[&job_id], 0),
        vec![ByteRange::new(0, 64)]
    );
}

#[tokio::test]
async fn a_damaged_yenc_article_is_delivered_as_damaged() {
    let (mut job, dir, _) = job_with(&[file("plain.bin", &[100])], Vec::new());
    let mut article = decoded(0, 0, 0, &[7; 90], Some(90));
    article.intact = false;
    job.intake(article, &mut RepairSlots::default(), 1).unwrap();
    assert_eq!(damaged_ranges(&job, 0), vec![ByteRange::new(0, 90)]);
    assert_eq!(
        std::fs::read(dir.path().join("plain.bin")).unwrap(),
        vec![7; 90],
        "damaged bytes are written all the same"
    );

    // Through the decode path: a checksum mismatch is damage.
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(91_005);
    let spec = spec(
        "lantern damaged",
        vec![("lantern.bin".to_string(), FileRole::Standalone, 1)],
    );
    insert_active_job_on(&mut pipeline, job_id, spec, Engine::Graph).await;
    pipeline.admit_graph_job(job_id, Engine::Graph);
    let body = vec![3u8; 64];
    pipeline
        .handle_decode_success(
            DecodeResult {
                segment_id: segment_id(job_id, 0, 0),
                raw_size: 64,
                encoding: SegmentEncoding::Yenc,
                yenc_layout: YencLayoutAssertions {
                    file_size: 64,
                    part: Some(1),
                    total: None,
                    begin: Some(1),
                    end: Some(64),
                },
                crc_valid: false,
                part_crc_verified: false,
                part_crc: par2_rs::checksum::crc32(&body),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(body),
                yenc_name: "lantern.bin".to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            },
            SegmentSource {
                source_server_idx: Some(0),
                exclude_servers: Vec::new(),
            },
        )
        .await;
    assert_eq!(
        damaged_ranges(&pipeline.graph_jobs[&job_id], 0),
        vec![ByteRange::new(0, 64)]
    );
}

#[test]
fn a_uu_root_answers_only_for_its_front() {
    let mut root = ArticleRoot::new(&[file("plain.bin", &[100, 100, 100])]);
    let mut article = decoded(0, 0, 0, &[7; 60], None);
    article.positioned = false;
    root.receive(article);
    assert_eq!(
        root.map
            .seek(VolumeIndex(0), ByteRange::new(0, 40), Some(Class::Critical)),
        Pull::Moved(0)
    );
    assert_eq!(
        root.map.seek(
            VolumeIndex(0),
            ByteRange::new(150, 160),
            Some(Class::Critical)
        ),
        Pull::Unseekable
    );
    assert!(root.map.take_moves().is_empty());
}

type Held = std::collections::BTreeMap<u64, Arc<[u8]>>;

fn held(parts: Vec<weaver_graph::Part>) -> Held {
    parts
        .into_iter()
        .map(|part| (part.key, part.bytes))
        .collect()
}

// The root encoded into `encoded`, its parts brought up to date in `held`,
// and both the same as a fresh encoding of the whole root.
fn encodes_whole(
    root: &ArticleRoot,
    encoded: &mut super::facts::Encoded,
    held: &mut Held,
    step: usize,
) {
    use super::facts::{Encoded, Parts};
    let encoding = root.encode(encoded, 9, 2, Some(RootPhase::Done));
    match encoding.parts {
        Parts::All(parts) => *held = self::held(parts),
        Parts::Changed(parts) => {
            for part in parts {
                match part.bytes {
                    Some(bytes) => held.insert(part.key, bytes),
                    None => held.remove(&part.key),
                };
            }
        }
    }
    let whole = root.encode(&mut Encoded::default(), 9, 2, Some(RootPhase::Done));
    assert_eq!(
        (encoding.version, &encoding.bytes),
        (whole.version, &whole.bytes),
        "step {step}"
    );
    let Parts::All(parts) = whole.parts else {
        panic!("a fresh encoding is whole");
    };
    assert_eq!(*held, self::held(parts), "step {step}");
}

fn with_segments(mut article: Decoded, segments: &[(u64, u64, u32)]) -> Decoded {
    article.segments = segments
        .iter()
        .map(|(start, end, crc32)| weaver_graph::Segment {
            range: ByteRange::new(*start, *end),
            crc32: *crc32,
            verified: false,
        })
        .collect();
    article
}

// Reading only the chunks the root marked gives the encoding of the whole
// root after every kind of change: arrivals out of order, one again at
// another start, two at one start and one of them taken back, checksums
// that replace others, losses, damage, raises, a length, and a volume
// placed only in order.
#[test]
fn an_encoding_of_what_changed_is_the_encoding_of_the_whole() {
    let sizes = [70; 40];
    let mut root = ArticleRoot::new(&[file("a.bin", &sizes), file("b.bin", &sizes)]);
    let mut encoded = super::facts::Encoded::default();
    let mut held = Held::new();
    let a = VolumeIndex(0);
    encodes_whole(&root, &mut encoded, &mut held, 0);
    let steps: Vec<Box<dyn Fn(&mut ArticleRoot)>> = vec![
        Box::new(|root| {
            root.receive(with_segments(
                decoded(0, 5, 300, &[1; 60], None),
                &[(300, 330, 1), (330, 360, 2)],
            ));
        }),
        Box::new(|root| {
            root.receive(with_segments(
                decoded(0, 20, 1200, &[2; 60], None),
                &[(1200, 1260, 3)],
            ));
        }),
        Box::new(|root| {
            root.receive(with_segments(
                decoded(0, 17, 1000, &[3; 60], None),
                &[(1000, 1060, 4)],
            ));
        }),
        Box::new(|root| {
            root.receive(with_segments(
                decoded(0, 20, 1100, &[4; 60], None),
                &[(1100, 1160, 5)],
            ));
        }),
        Box::new(|root| {
            root.receive(with_segments(
                decoded(0, 33, 300, &[5; 60], None),
                &[(300, 360, 6)],
            ));
        }),
        Box::new(move |root| root.map.lost(a, 2)),
        Box::new(move |root| root.map.lost(a, 3)),
        Box::new(|root| {
            root.map.seek(
                VolumeIndex(1),
                ByteRange::new(0, 500),
                Some(Class::Critical),
            );
        }),
        Box::new(|root| {
            root.damage();
        }),
        Box::new(move |root| root.map.unarrive(a, 33)),
        Box::new(|root| {
            root.receive(with_segments(
                decoded(1, 0, 0, &[6; 60], Some(2_400)),
                &[(0, 30, 7), (30, 60, 8)],
            ));
        }),
        Box::new(|root| {
            let mut article = decoded(1, 1, 60, &[7; 60], None);
            article.positioned = false;
            root.receive(article);
        }),
        Box::new(|root| {
            root.map.sample();
            root.map
                .seek(a, ByteRange::new(2_000, u64::MAX), Some(Class::Parked));
            root.map.queue(a, ByteRange::new(0, 2_000));
        }),
        Box::new(|root| root.map.arrived_before(|_, ordinal| ordinal % 2 == 0)),
    ];
    for (step, change) in steps.iter().enumerate() {
        change(&mut root);
        encodes_whole(&root, &mut encoded, &mut held, step + 1);
    }
}

// One arrival costs an encoding the parts it touched, whatever the size of
// the job.
#[test]
fn an_arrival_changes_the_same_parts_of_a_small_job_and_a_large_one() {
    use super::facts::{Encoded, Parts};
    let changed = |articles: usize| {
        let mut root = ArticleRoot::new(&[file("a.bin", &vec![70; articles])]);
        let mut encoded = Encoded::default();
        root.encode(&mut encoded, 9, 2, None);
        root.receive(with_segments(
            decoded(0, 5, 300, &[1; 60], None),
            &[(300, 330, 1), (330, 360, 2)],
        ));
        let Parts::Changed(parts) = root.encode(&mut encoded, 9, 2, None).parts else {
            panic!("an encoding after the first is of what changed");
        };
        parts
    };
    let small = changed(40);
    assert!(!small.is_empty() && small.len() <= 3, "{small:?}");
    assert_eq!(changed(400), small);
}

// With the switch unset a new job is a graph job, row and runtime both; a
// switch naming classic opts it into the classic pipeline. The decision is
// driven through `Engine::parse`, never the process variable.
#[tokio::test]
async fn admission_with_the_switch_unset_stamps_graph_and_classic_is_the_opt_in() {
    for (switch, engine, job_id) in [
        (None, Engine::Graph, JobId(91_008)),
        (Some("classic"), Engine::Classic, JobId(91_009)),
    ] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let mut resources = pipeline.admission_resources();
        resources.engine = Engine::parse(switch);
        let prepared = crate::pipeline::Pipeline::prepare_add(
            &resources,
            job_id,
            spec(
                "lantern default",
                vec![("lantern.bin".to_string(), FileRole::Standalone, 2)],
            ),
            PathBuf::from("lantern-default.nzb"),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
        pipeline.install_prepared_add(job_id, prepared);
        assert_eq!(
            pipeline.db.load_active_job_engine(job_id).unwrap(),
            Some(engine),
            "{switch:?}"
        );
        assert_eq!(
            pipeline.graph_jobs.contains_key(&job_id),
            engine == Engine::Graph,
            "{switch:?}"
        );
        retire_pipeline_database(pipeline).await;
    }
}

#[tokio::test]
async fn admission_stamps_the_switch_and_a_restore_keeps_the_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(91_006);
    let restored_id = JobId(91_007);
    let lantern = spec(
        "lantern stamp",
        vec![("lantern.bin".to_string(), FileRole::Standalone, 2)],
    );
    let working_dir: PathBuf = {
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline
            .add_job(
                job_id,
                lantern.clone(),
                PathBuf::from("lantern-stamp.nzb"),
                sample_nzb_zstd(),
                crate::jobs::AddJobOptions::default(),
            )
            .await
            .unwrap();
        let switch = Engine::from_environment();
        assert_eq!(
            pipeline.db.load_active_job_engine(job_id).unwrap(),
            Some(switch)
        );
        assert_eq!(
            pipeline.graph_jobs.contains_key(&job_id),
            switch == Engine::Graph
        );

        // A row stamped graph, whatever the switch says, with the checkpoint
        // its admission took.
        let working_dir = insert_active_job(&mut pipeline, restored_id, lantern.clone()).await;
        stamp_graph(&pipeline.db, restored_id);
        let (admitted, _files, _) = job_with(&[file("lantern.bin", &[64, 64])], Vec::new());
        let pending = checkpoint::Pending::default();
        assert!(pending.put(&admitted));
        pipeline
            .db
            .save_graph_checkpoint(restored_id, Arc::new(pending.take().unwrap()))
            .unwrap();
        retire_pipeline_database(pipeline).await;
        working_dir
    };

    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id: restored_id,
            job_hash: [0; 32],
            spec: lantern,
            complete_files: HashSet::new(),
            file_progress: Default::default(),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            extracted_members: HashSet::new(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir,
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&restored_id));
    assert_eq!(
        pipeline.db.load_active_job_engine(restored_id).unwrap(),
        Some(Engine::Graph)
    );
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(
        pipeline
            .db
            .load_active_job_root_phase(restored_id)
            .unwrap()
            .as_deref(),
        Some("downloading")
    );
}

// A deferral lowers only articles still in the normal order: one already
// raised keeps its class, a later raise takes a parked one, and a deferral
// over a lost article or a missing volume is never Lost to the asker.
#[test]
fn a_deferral_never_lowers_a_raised_article_and_is_never_lost() {
    let mut root = ArticleRoot::new(&[file("plain.bin", &[100, 100, 100, 100])]);
    let volume = VolumeIndex(0);
    // The first article gives the positions of the rest.
    root.receive(decoded(0, 0, 0, &[7; 100], Some(400)));
    assert_eq!(
        root.map
            .seek(volume, ByteRange::new(100, 200), Some(Class::Recovery)),
        Pull::Moved(1)
    );
    assert_eq!(
        root.map
            .seek(volume, ByteRange::new(0, u64::MAX), Some(Class::Parked)),
        Pull::Moved(2)
    );
    assert_eq!(root.map.class(volume, 1), Some(Class::Recovery));
    for ordinal in 2..4 {
        assert_eq!(root.map.class(volume, ordinal), Some(Class::Parked));
    }
    assert_eq!(
        root.map
            .seek(volume, ByteRange::new(200, 300), Some(Class::Recovery)),
        Pull::Moved(1)
    );
    assert_eq!(root.map.class(volume, 2), Some(Class::Recovery));

    root.map.lost(volume, 3);
    assert_eq!(
        root.map
            .seek(volume, ByteRange::new(0, u64::MAX), Some(Class::Parked)),
        Pull::Moved(0)
    );
    assert_eq!(
        root.map
            .seek(VolumeIndex(9), ByteRange::new(0, 100), Some(Class::Parked)),
        Pull::Moved(0)
    );
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A member no object owns, judged past the hole where its front was lost,
// is read back a piece at a time (`Effect::ReadOn`). The loop never waits
// for the writer to put those bytes down: the read is owed while writes to
// them are out, a thread waits for them, and the read is made as its mail
// is heard.

use super::*;
use crate::pipeline::graph::root::phase::RootPhase;
use weaver_server_disk::operations::disk::volume::{LoopSite, LoopWait};

// Articles of this size, recovery blocks of `BLOCK`: the clean bytes past
// the volume's lost first article begin inside a recovery packet, and the
// next packet starts past the first `FIRST_BYTES` of them, so the set's
// prospect is owed the rest, read on from the volume's file.
const SIZE: usize = 64 * 1024;
const BLOCK: u64 = 1024 * 1024;
// The data file's lost article, and the posts' places.
const LOST: u32 = 5;
const INDEX: u32 = 1;
const VOLUME: u32 = 2;
const FRONTLESS: MemberId = MemberId(VOLUME);

// Article `ordinal` of file `index`, cut at `SIZE`, as the decoder hands it.
async fn arrive(pipeline: &mut Pipeline, job_id: JobId, posts: &[Post], index: u32, ordinal: u32) {
    let post = &posts[index as usize];
    let body = post.articles(SIZE).nth(ordinal as usize).unwrap().to_vec();
    let len = body.len() as u64;
    let offset = u64::from(ordinal) * SIZE as u64;
    let mut result = decoded_article(job_id, posts, index, 0, body, true);
    result.segment_id = segment_id(job_id, index, ordinal);
    result.yenc_layout.part = Some(ordinal + 1);
    result.yenc_layout.total = Some(post.articles(SIZE).count() as u32);
    result.yenc_layout.begin = Some(offset + 1);
    result.yenc_layout.end = Some(offset + len);
    result.segments[0].file_offset = offset;
    let credit = pipeline.begin_hand_on_for_test(segment_id(job_id, index, ordinal), len);
    pipeline
        .handle_decode_success(
            result,
            SegmentSource {
                servers: crate::pipeline::PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(0),
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
    pipeline.end_hand_on_for_test(job_id, credit);
}

// The data file, the set's index, and one volume of three blocks renamed
// to a name that says nothing of it.
fn frontless_posts() -> Vec<Post> {
    let data = payload(4 * BLOCK as usize, 7);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(
        crate::pipeline::tests::engines::harness::par2_volumes_sliced(
            "lantern",
            &[("lantern.bin", &data)],
            BLOCK,
            3,
            1,
        ),
    );
    let bytes = std::mem::take(&mut posts[VOLUME as usize].bytes);
    posts[VOLUME as usize] = Post {
        bytes,
        ..Post::plain("k3x9q0.bin", Vec::new())
    };
    posts.push(Post::plain("notes.txt", payload(512, 3)));
    posts
}

// The place of the small plain file posted last, of one article.
fn notes(posts: &[Post]) -> u32 {
    posts.len() as u32 - 1
}

// How often the loop stood still to read on for an object.
fn read_on_waits(volume: &Volume) -> u64 {
    volume.writer.loop_waits_by_site[LoopWait::Read as usize][LoopSite::ReadOn as usize]
        .load(Ordering::Relaxed)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pressure {
    // The volume's thread stands still from before the bytes the read is
    // asked for are written until after it is asked.
    Held,
    // Every write the read meets has returned before it is asked.
    Settled,
}

struct Frontless {
    pipeline: Pipeline,
    job_id: JobId,
    posts: Vec<Post>,
    working_dir: PathBuf,
    volume: Arc<Volume>,
    hold: Option<Hold>,
    // The volume's first article still to come after the ninth.
    rest: u32,
    // How often the loop stood still for the volume before it was held.
    before: u64,
    _threads: threads::Threads,
}

// What a run of the job ends with: its status, whether the data file is
// the post, the files left in its folder, and what its recovery set ruled.
#[derive(Debug, PartialEq, Eq)]
struct Ended {
    status: Option<JobStatus>,
    whole: bool,
    files: Vec<String>,
    judged: String,
}

impl Frontless {
    // The index and the data file arrive, one data article lost; the
    // volume loses its first article, and its second and its fourth to
    // ninth arrive; the third arrives last, and the run past the hole
    // reaches the bytes after it, which the set's prospect is owed. With
    // `early`, the volume's articles past the ninth but its last arrive
    // before any of the fourth to ninth.
    async fn asked(
        temp_dir: &tempfile::TempDir,
        job_id: JobId,
        pressure: Pressure,
        early: bool,
    ) -> Self {
        Self::asked_with(temp_dir, job_id, pressure, early, true).await
    }

    // `asked`, with the small plain file arriving first, or left to come.
    async fn asked_with(
        temp_dir: &tempfile::TempDir,
        job_id: JobId,
        pressure: Pressure,
        early: bool,
        notes_first: bool,
    ) -> Self {
        let threads = threads::real();
        let posts = frontless_posts();
        let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(temp_dir).await;
        let working_dir =
            insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, SIZE)).await;
        let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
        let volume = Arc::clone(disk.working().volume());
        for ordinal in 0..posts[INDEX as usize].articles(SIZE).count() as u32 {
            arrive(&mut pipeline, job_id, &posts, INDEX, ordinal).await;
        }
        if notes_first {
            arrive(&mut pipeline, job_id, &posts, notes(&posts), 0).await;
        }
        for ordinal in 0..posts[0].articles(SIZE).count() as u32 {
            if ordinal == LOST {
                pipeline.book_terminal_segment(
                    segment_id(job_id, 0, LOST),
                    SegmentTerminalState::Missing,
                );
            } else {
                arrive(&mut pipeline, job_id, &posts, 0, ordinal).await;
            }
        }
        pipeline
            .book_terminal_segment(segment_id(job_id, VOLUME, 0), SegmentTerminalState::Missing);
        arrive(&mut pipeline, job_id, &posts, VOLUME, 1).await;
        let count = posts[VOLUME as usize].articles(SIZE).count() as u32;
        let rest = if early { count - 1 } else { 9 };
        for ordinal in 9..rest {
            arrive(&mut pipeline, job_id, &posts, VOLUME, ordinal).await;
        }
        disk.barrier().wait_returned();
        let before = volume.writer.loop_waits.load(Ordering::Relaxed);
        let hold = (pressure == Pressure::Held).then(|| pipeline.volume_writers.hold(&volume));
        for ordinal in 3..=8 {
            arrive(&mut pipeline, job_id, &posts, VOLUME, ordinal).await;
        }
        if pressure == Pressure::Settled {
            disk.barrier().wait_returned();
        }
        let waits = read_on_waits(&volume);
        arrive(&mut pipeline, job_id, &posts, VOLUME, 2).await;
        let job = &pipeline.graph_jobs[&job_id];
        match pressure {
            Pressure::Held => {
                assert!(job.writer().reads_owed(), "the read was made on the loop");
                assert!(job.reads_aside(), "no thread waits for the read's writes");
                assert_eq!(volume.writer.loop_waits.load(Ordering::Relaxed), before);
            }
            Pressure::Settled => {
                assert!(!job.writer().reads_owed(), "a settled read was owed");
                assert!(!job.reads_aside(), "a thread waits for a settled read");
            }
        }
        assert_eq!(read_on_waits(&volume), waits);
        Self {
            pipeline,
            job_id,
            posts,
            working_dir,
            volume,
            hold,
            rest,
            before,
            _threads: threads,
        }
    }

    fn job(&self) -> &crate::pipeline::graph::job::GraphJob {
        &self.pipeline.graph_jobs[&self.job_id]
    }

    // The loop never stood still for the volume while it was held: a wait
    // then would never have returned.
    fn still(&self) {
        assert_eq!(
            self.volume.writer.loop_waits.load(Ordering::Relaxed),
            self.before
        );
    }

    fn owes(&self) -> bool {
        self.job().writer().reads_owed() || self.job().reads_aside()
    }

    // The volume's thread goes on, and the job's mail is heard until no
    // read is owed and no thread waits for one.
    async fn free(&mut self) {
        if let Some(hold) = self.hold.take() {
            hold.open();
        }
        while self.owes() {
            let said = self.pipeline.graph_runs.recv().await.unwrap();
            self.pipeline.graph_run_said(said);
        }
    }

    // The volume's last articles arrive, or are lost, and the job's mail
    // is heard until it ends.
    async fn ends(mut self, rest: bool) -> Ended {
        self.free().await;
        let count = self.posts[VOLUME as usize].articles(SIZE).count() as u32;
        let posts = std::mem::take(&mut self.posts);
        for ordinal in self.rest..count {
            if rest {
                arrive(&mut self.pipeline, self.job_id, &posts, VOLUME, ordinal).await;
            } else {
                self.pipeline.book_terminal_segment(
                    segment_id(self.job_id, VOLUME, ordinal),
                    SegmentTerminalState::Missing,
                );
            }
        }
        self.posts = posts;
        self.settled().await
    }

    // The job's mail heard until the job ends, and what it ended with.
    async fn settled(mut self) -> Ended {
        self.free().await;
        hear_repair(&mut self.pipeline, self.job_id).await;
        let status = self.pipeline.graph_terminal_status(self.job_id);
        let whole = std::fs::read(self.working_dir.join("lantern.bin"))
            .is_ok_and(|bytes| bytes == self.posts[0].bytes);
        let mut files: Vec<String> = std::fs::read_dir(&self.working_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name != weaver_graph::names::INTERNAL_DIR)
            .collect();
        files.sort();
        let job = self.job();
        let summary = job.recovery_summary();
        let rulings: Vec<_> = job
            .tree()
            .live()
            .into_iter()
            .filter_map(|node| job.tree().recovery(node))
            .flat_map(|recovery| {
                let mut sets = Vec::new();
                recovery.sets(&mut sets);
                sets
            })
            .collect();
        let judged = format!("{summary:?} {rulings:?}");
        retire_pipeline_database(self.pipeline).await;
        Ended {
            status,
            whole,
            files,
            judged,
        }
    }
}

// The read on is asked while the volume's thread stands still with the
// bytes it reads still out: it is owed, the loop never stands still for
// it, and it is made once the thread that waited aside for them mails the
// job. The set judges the volume, the job repairs and ends, as when the
// bytes were down before the read was asked.
#[tokio::test]
async fn a_read_on_meeting_writes_out_is_made_after_its_wait_and_ends_the_same() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_990), Pressure::Held, false).await;
    run.free().await;
    assert_eq!(
        read_on_waits(&run.volume),
        0,
        "the loop read on for the writer"
    );
    let pressed = run.ends(true).await;

    let free_dir = tempfile::tempdir().unwrap();
    let free = Frontless::asked(&free_dir, JobId(92_991), Pressure::Settled, false)
        .await
        .ends(true)
        .await;
    assert_eq!(free.status, Some(JobStatus::Complete), "{free:?}");
    assert!(free.whole, "{free:?}");
    assert_eq!(pressed, free);
}

// The bytes the read on is asked for are down: it is made at once, and no
// thread is started for it.
#[tokio::test]
async fn a_read_on_whose_bytes_are_down_is_made_at_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let run = Frontless::asked(&temp_dir, JobId(92_992), Pressure::Settled, false).await;
    let ended = run.ends(true).await;
    assert_eq!(ended.status, Some(JobStatus::Complete), "{ended:?}");
}

// The bytes are written again between the waiting thread's mail and the
// loop hearing it: the read is owed again, waited for again aside, and
// made once that wait mails the job. The job ends as without pressure.
#[tokio::test]
async fn a_write_over_an_owed_read_before_its_mail_is_heard_owes_it_again() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_993), Pressure::Held, false).await;
    run.hold.take().unwrap().open();
    let said = loop {
        let said = run.pipeline.graph_runs.recv().await.unwrap();
        if matches!(said.heard, Heard::ReadSettled) {
            break said;
        }
        run.pipeline.graph_run_said(said);
    };
    run.hold = Some(run.pipeline.volume_writers.hold(&run.volume));
    let again = run.posts[VOLUME as usize]
        .articles(SIZE)
        .nth(4)
        .unwrap()
        .to_vec();
    run.pipeline
        .graph_jobs
        .get_mut(&run.job_id)
        .unwrap()
        .write_again_for_test(FRONTLESS, 4 * SIZE as u64, again);
    run.pipeline.graph_run_said(said);
    assert!(
        run.job().writer().reads_owed(),
        "made over a write still out"
    );
    assert!(run.job().reads_aside(), "owed again with no thread waiting");
    run.still();
    run.free().await;
    assert_eq!(read_on_waits(&run.volume), 0);
    let pressed = run.ends(true).await;

    let free_dir = tempfile::tempdir().unwrap();
    let free = Frontless::asked(&free_dir, JobId(92_994), Pressure::Settled, false)
        .await
        .ends(true)
        .await;
    assert_eq!(pressed, free);
}

// The volume's last articles are lost while the read is owed: the volume's
// close waits behind the read, the job neither quiets, gives up nor ends
// while it is owed, and once it is made the job ends as one whose read
// was made at once.
#[tokio::test]
async fn a_close_behind_an_owed_read_ends_as_without_pressure() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_995), Pressure::Held, true).await;
    let last = run.posts[VOLUME as usize].articles(SIZE).count() as u32 - 1;
    let posts = std::mem::take(&mut run.posts);
    arrive(&mut run.pipeline, run.job_id, &posts, VOLUME, last).await;
    run.posts = posts;
    run.rest = last + 1;
    assert!(run.job().writer().reads_owed());
    assert!(
        run.job().writer().closes_owed(),
        "the volume closed past its read"
    );
    assert_ne!(
        run.job().phase(),
        RootPhase::Done,
        "the job ended past its read"
    );
    run.still();
    let pressed = run.settled().await;

    let free_dir = tempfile::tempdir().unwrap();
    let free = Frontless::asked(&free_dir, JobId(92_996), Pressure::Settled, true)
        .await
        .ends(true)
        .await;
    assert_eq!(free.status, Some(JobStatus::Complete), "{free:?}");
    assert_eq!(pressed, free);
}

// The waiting thread's mail under a number other than the one the job has
// out is not heard: the read stays owed and its thread waits on. The
// number out still answers it.
#[tokio::test]
async fn a_read_settled_under_another_number_is_dropped() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_997), Pressure::Held, false).await;
    run.pipeline
        .graph_run_said(crate::pipeline::graph::engine::Said {
            scope: run.job_id.0,
            node: weaver_graph::NodeId::ROOT,
            generation: u64::MAX,
            heard: Heard::ReadSettled,
        });
    assert!(run.job().writer().reads_owed(), "made on a stale mail");
    assert!(run.job().reads_aside());
    run.still();
    run.free().await;
    let ended = run.ends(true).await;
    assert_eq!(ended.status, Some(JobStatus::Complete), "{ended:?}");
}

// A job cancelled with a read owed and its thread waiting aside: the read
// goes with the job, the thread ends as the volume lets go of the job's
// files, and nothing of the job is left counted on the volume.
#[tokio::test]
async fn a_job_cancelled_with_a_read_owed_ends_and_leaves_nothing_counted() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_998), Pressure::Held, false).await;
    let job_id = run.job_id;
    let handle = crate::SchedulerHandle::new(
        run.pipeline.event_tx.clone(),
        run.pipeline.shared_state.clone(),
    );
    handle.cancel_job(job_id).await.unwrap();
    run.pipeline.apply_queue_intents().await;
    match run
        .pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        crate::pipeline::TerminalPostProcessingEvent::CancelJobsDone(done) => {
            run.pipeline.handle_cancel_jobs_done(done)
        }
        _ => panic!("expected cancellation archive reply"),
    }
    assert!(run.pipeline.retiring_graph_jobs.contains_key(&job_id));
    run.still();
    run.hold.take().unwrap().open();
    hear_retirement(&mut run.pipeline, job_id).await;
    assert!(!run.pipeline.jobs.contains_key(&job_id));
    assert_eq!(run.volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(run.volume.unwritten_bytes(), 0);
    assert_eq!(read_on_waits(&run.volume), 0);
    retire_pipeline_database(run.pipeline).await;
}

// Once the owner is stopping, a wait aside ends at once with the writes
// still out: its mail leaves the read owed and sends no wait again, so the
// stop's drain is not mailed round and round until the writes return.
#[tokio::test]
async fn a_read_owed_at_a_stop_sends_no_wait_again() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run = Frontless::asked(&temp_dir, JobId(92_999), Pressure::Held, false).await;
    run.pipeline.volume_writers.stop_waits();
    loop {
        let said = run.pipeline.graph_runs.recv().await.unwrap();
        let settled = matches!(said.heard, Heard::ReadSettled);
        run.pipeline.graph_run_said(said);
        if settled {
            break;
        }
    }
    let owed = run.job().writer().reads_owed();
    let aside = run.job().reads_aside();
    let waits = run.volume.writer.loop_waits.load(Ordering::Relaxed);
    run.hold.take().unwrap().open();
    assert!(owed, "a read made over writes still out");
    assert!(!aside, "a wait sent again at the stop");
    assert_eq!(waits, run.before);
    retire_pipeline_database(run.pipeline).await;
}

// A member closes while the volume's close is owed behind its read: its
// close is owed behind that one, and both are made in the order they came
// once the read is made. The job ends as without pressure.
#[tokio::test]
async fn a_close_behind_an_owed_close_is_owed_in_turn() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut run =
        Frontless::asked_with(&temp_dir, JobId(92_989), Pressure::Held, true, false).await;
    let last = run.posts[VOLUME as usize].articles(SIZE).count() as u32 - 1;
    let posts = std::mem::take(&mut run.posts);
    arrive(&mut run.pipeline, run.job_id, &posts, VOLUME, last).await;
    assert!(run.job().writer().close_owed_of(FRONTLESS));
    let index = notes(&posts);
    arrive(&mut run.pipeline, run.job_id, &posts, index, 0).await;
    run.posts = posts;
    run.rest = last + 1;
    let owed = run.job().writer().close_owed_of(MemberId(index));
    if !owed {
        run.hold.take().unwrap().open();
    }
    assert!(owed, "closed ahead of an older close");
    run.still();
    let pressed = run.settled().await;

    let free_dir = tempfile::tempdir().unwrap();
    let free = Frontless::asked(&free_dir, JobId(92_988), Pressure::Settled, true)
        .await
        .ends(true)
        .await;
    assert_eq!(free.status, Some(JobStatus::Complete), "{free:?}");
    assert_eq!(pressed, free);
}

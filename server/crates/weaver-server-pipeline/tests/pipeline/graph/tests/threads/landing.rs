// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each article's bytes count on the volume they land on, from its lease to
// its write: a full volume stops what is leased for it, never the loop, and
// never counts past its limit.

use super::*;
use weaver_server_disk::operations::disk::volume::{LoopSite, LoopWait, Reservation};

// A job of `posts` whose working directory and delivered files lie on two
// volumes, each sized at `figure`: the job, where its files are delivered,
// and its working and output volumes.
async fn two_volume_job(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posts: &[Post],
    figure: u64,
) -> (Pipeline, PathBuf, Arc<Volume>, Arc<Volume>) {
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        intermediate,
        complete,
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        0,
        None,
    )
    .await;
    pipeline.volume_buffers.set_figure(figure, u64::MAX);
    pipeline.config.write().await.categories = vec![crate::categories::CategoryConfig {
        id: 0,
        name: "movies".to_string(),
        dest_dir: Some(movies.display().to_string()),
        aliases: String::new(),
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
    }];
    let mut spec = spec("lantern joined", posts, ARTICLE);
    spec.category = Some("movies".to_string());
    let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
    pipeline
        .add_job(
            job_id,
            spec,
            PathBuf::from("lantern.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    let staging_dir = crate::pipeline::queue::staging_dir_for(&movies, job_id);
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let working = Arc::clone(disk.working().volume());
    let output = Arc::clone(disk.under(&staging_dir).volume());
    assert!(!Arc::ptr_eq(&working, &output));
    (pipeline, staging_dir, working, output)
}

fn file(job_id: JobId, file_index: u32) -> NzbFileId {
    NzbFileId { job_id, file_index }
}

fn body(posts: &[Post], index: u32, ordinal: u32) -> Vec<u8> {
    posts[index as usize]
        .articles(ARTICLE)
        .nth(ordinal as usize)
        .unwrap()
        .to_vec()
}

// Article `ordinal` of file `index` is leased, arrives and is handed on, as
// a lane's article is: false, with nothing done, when its volume refuses
// the lease.
async fn take(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
) -> bool {
    let segment = segment_id(job_id, index, ordinal);
    if !pipeline.lease_for_test(segment, ARTICLE as u32) {
        return false;
    }
    let body = body(posts, index, ordinal);
    pipeline.arrive_for_test(segment, ARTICLE as u32, body.len() as u64);
    assert!(
        pipeline
            .hand_on_for_test(decoded_article(job_id, posts, index, ordinal, body, true))
            .await,
        "a leased article is handed on at once"
    );
    true
}

// Take article `ordinal` of file `index` once its volume has room: each
// refusal waits for the volume's room wake, which dispatch would wait for.
// A volume that fell to its room line since the refusal asks no wake, and
// below that line its next lease is granted: only its writer moves it, and
// only down.
async fn take_when_room(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
) {
    while !take(pipeline, job_id, posts, index, ordinal).await {
        if !pipeline.ask_room_wake_for_test(job_id) {
            assert!(
                take(pipeline, job_id, posts, index, ordinal).await,
                "a volume at its room line grants the next lease"
            );
            return;
        }
        pipeline.hear_volume_room_for_test().await;
    }
}

// Article `ordinal` of file `index` arrives counted on `on`, as one leased
// while it was thought to land there, and the loop hands it on: false when
// it parked for room where it lands.
async fn take_counted_on(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    on: &Arc<Volume>,
    index: u32,
    ordinal: u32,
) -> bool {
    let body = body(posts, index, ordinal);
    pipeline.arrive_on_for_test(job_id, on, body.len() as u64);
    pipeline
        .hand_on_for_test(decoded_article(job_id, posts, index, ordinal, body, true))
        .await
}

// The volume never counted past its hard limit.
fn within_limit(volume: &Volume) {
    let (_, hard) = volume.buffer_limits();
    assert!(
        volume.peak_bytes() <= hard,
        "{} counted past {hard}",
        volume.peak_bytes()
    );
}

// The loop never waited for room on the volume, held or not: every write
// it queued was paid for when its article was handed on.
fn no_budget_waits(volume: &Volume) {
    assert_eq!(
        volume.writer.loop_waits_by_kind[LoopWait::Budget as usize].load(Ordering::Relaxed),
        0
    );
}

fn nothing_waits_counted(pipeline: &Pipeline, job_id: JobId) {
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    assert!(
        pipeline
            .volume_arrived_by_job
            .get(&job_id)
            .is_none_or(|holds| holds.bytes() == 0)
    );
}

// A plain file onto a small output volume whose thread stands still: each
// article is counted where it lands, the loop never waits for the volume,
// dispatch stops at its lease limit and goes on at its room wake, and the
// file is whole.
#[tokio::test]
async fn a_plain_file_onto_a_full_output_volume_stops_dispatch_not_the_loop() {
    const ARTICLES: u32 = 24;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_500);
    let data = payload(ARTICLES as usize * ARTICLE, 8);
    let posts = [Post::plain("lantern.bin", data.clone())];
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    let mut ordinal = 0;
    while ordinal < ARTICLES && take(&mut pipeline, job_id, &posts, 0, ordinal).await {
        ordinal += 1;
        within_limit(&working);
        within_limit(&output);
    }
    assert!(ordinal > 1 && ordinal < ARTICLES, "{ordinal}");
    // The member was decided on its first articles: the rest land on
    // output, and what they lease stops there.
    assert!(Arc::ptr_eq(
        &pipeline.landing_volume_for_test(file(job_id, 0)).unwrap(),
        &output
    ));
    assert!(output.unwritten_bytes() >= output.lease_limit());
    assert_eq!(
        output.buffered_bytes.load(Ordering::Relaxed),
        u64::from(ordinal) * ARTICLE as u64
    );
    assert_eq!(working.buffered_bytes.load(Ordering::Relaxed), 0);
    // Dispatch is refused for the job, with a wake asked.
    assert!(pipeline.volume_refuses_dispatch_for_test(job_id));
    no_loop_waits(&working);
    no_loop_waits(&output);
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);

    for hold in &holds {
        hold.open();
    }
    pipeline.hear_volume_room_for_test().await;
    // Heard at the room line: the next article is leased at once.
    assert!(take(&mut pipeline, job_id, &posts, 0, ordinal).await);
    for ordinal in ordinal + 1..ARTICLES {
        take_when_room(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    no_budget_waits(&working);
    no_budget_waits(&output);
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// Articles counted on the wrong volume, both ways: one counted on output
// for a member that turns out to land on working moves there; ones counted
// on working that land on a full output move while it has room and then
// park, in order, until its room wake. The loop never waits, no volume
// counts past its limit, and both files are whole.
#[tokio::test]
async fn an_article_counted_on_the_other_volume_moves_where_it_fits_or_parks() {
    const ARTICLES: u32 = 24;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_501);
    let (data, small) = (
        payload(ARTICLES as usize * ARTICLE, 9),
        payload(2 * ARTICLE, 10),
    );
    let posts = [
        Post::plain("lantern.bin", data.clone()),
        Post::plain("lantern.dat", small.clone()),
    ];
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    assert_eq!(output.buffer_limits().1, 16 * ARTICLE as u64);
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];

    // Output to working: the member is not decided, so it lands on
    // working, and what was counted on output moves there.
    assert!(Arc::ptr_eq(
        &pipeline.landing_volume_for_test(file(job_id, 1)).unwrap(),
        &working
    ));
    assert!(take_counted_on(&mut pipeline, job_id, &posts, &output, 1, 0).await);
    nothing_waits_counted(&pipeline, job_id);
    assert!(take(&mut pipeline, job_id, &posts, 1, 1).await);

    // The big file fills output to its lease limit.
    let mut ordinal = 0;
    while take(&mut pipeline, job_id, &posts, 0, ordinal).await {
        ordinal += 1;
    }
    assert!(ordinal > 0 && ordinal + 2 < ARTICLES, "{ordinal}");
    assert!(output.unwritten_bytes() >= output.lease_limit());

    // Working to output: what fits below the hard limit moves, the next
    // does not and parks, and the one after parks behind it.
    let mut moved = 0;
    while take_counted_on(&mut pipeline, job_id, &posts, &working, 0, ordinal).await {
        moved += 1;
        ordinal += 1;
        within_limit(&output);
    }
    assert!(moved > 0);
    assert!(ordinal + 1 < ARTICLES, "{ordinal}");
    assert_eq!(pipeline.parked_download_results_for_test(), 1);
    assert!(output.unwritten_bytes() + ARTICLE as u64 > output.buffer_limits().1);
    assert!(!take_counted_on(&mut pipeline, job_id, &posts, &working, 0, ordinal + 1).await);
    assert_eq!(pipeline.parked_download_results_for_test(), 2);
    // What parked still counts on working, where it was counted.
    assert_eq!(working.unwritten_bytes(), 2 * ARTICLE as u64);
    no_loop_waits(&working);
    no_loop_waits(&output);
    within_limit(&working);
    within_limit(&output);

    for hold in &holds {
        hold.open();
    }
    let mut handed = 0;
    while pipeline.parked_download_results_for_test() > 0 {
        pipeline.hear_volume_room_for_test().await;
        handed += pipeline.hand_on_parked_for_test().await;
    }
    assert_eq!(handed, 2);
    for ordinal in ordinal + 2..ARTICLES {
        take_when_room(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert_eq!(read(&staging_dir.join("lantern.dat")), small);
    no_budget_waits(&working);
    no_budget_waits(&output);
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// An archive's volumes land on working, as they did: every article counts
// and is written there, output takes nothing from them, and the run's
// output is whole.
#[tokio::test]
async fn an_archives_volumes_count_on_working_as_before() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_502);
    let body = payload(3 * ARTICLE + 5, 11);
    let posts = vec![echo_volume(0, &body)];
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 64 * ARTICLE as u64).await;
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            ..Echoes::default()
        }));
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    let articles = posts[0].articles(ARTICLE).count() as u32;
    for ordinal in 0..articles {
        assert!(take(&mut pipeline, job_id, &posts, 0, ordinal).await);
        assert!(Arc::ptr_eq(
            &pipeline.landing_volume_for_test(file(job_id, 0)).unwrap(),
            &working
        ));
    }
    assert_eq!(
        working.buffered_bytes.load(Ordering::Relaxed),
        posts[0].bytes.len() as u64
    );
    no_loop_waits(&working);
    no_loop_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    for hold in &holds {
        hold.open();
    }
    drain_runs(&mut pipeline, job_id).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("echo-0.mkv")), body);
    no_budget_waits(&working);
    within_limit(&working);
    within_limit(&output);
    retire_pipeline_database(pipeline).await;
}

// A job on one volume counts each article there from lease to write, as it
// did: the volume holds every leased article until its lease limit.
#[tokio::test]
async fn a_single_volume_job_counts_each_article_as_before() {
    const ARTICLES: u32 = 24;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_503);
    let data = payload(ARTICLES as usize * ARTICLE, 12);
    let posts = [Post::plain("lantern.bin", data.clone())];
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    pipeline
        .volume_buffers
        .set_figure(16 * ARTICLE as u64, u64::MAX);
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let (volume, hold) = held(&pipeline, job_id);
    let mut ordinal = 0;
    while take(&mut pipeline, job_id, &posts, 0, ordinal).await {
        ordinal += 1;
        assert!(Arc::ptr_eq(
            &pipeline.landing_volume_for_test(file(job_id, 0)).unwrap(),
            &volume
        ));
        within_limit(&volume);
    }
    // Leased to the lease limit, a write piece under the hard limit.
    assert!(ordinal > 0 && ordinal < ARTICLES, "{ordinal}");
    assert!(volume.unwritten_bytes() >= volume.lease_limit());
    assert!(pipeline.volume_refuses_dispatch_for_test(job_id));
    no_loop_waits(&volume);
    within_limit(&volume);
    nothing_waits_counted(&pipeline, job_id);
    hold.open();
    for ordinal in ordinal..ARTICLES {
        take_when_room(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(read(&working_dir.join("lantern.bin")), data);
    no_budget_waits(&volume);
    within_limit(&volume);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// The job's plain file is taken until its member is decided onto output:
// the next ordinal.
async fn decide_onto_output(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    output: &Arc<Volume>,
) -> u32 {
    let mut ordinal = 0;
    while !Arc::ptr_eq(
        &pipeline.landing_volume_for_test(file(job_id, 0)).unwrap(),
        output,
    ) {
        assert!(take(pipeline, job_id, posts, 0, ordinal).await);
        ordinal += 1;
    }
    ordinal
}

// A plain file fills a held output volume to its lease limit, then two of
// its articles counted on working arrive for output: the first parks for
// output's room, the second behind it. Both still count on working.
async fn park_two_on_output(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    working: &Arc<Volume>,
    output: &Arc<Volume>,
) {
    let mut ordinal = 0;
    while take(pipeline, job_id, posts, 0, ordinal).await {
        ordinal += 1;
    }
    assert!(output.unwritten_bytes() >= output.lease_limit());
    while take_counted_on(pipeline, job_id, posts, working, 0, ordinal).await {
        ordinal += 1;
    }
    assert!(!take_counted_on(pipeline, job_id, posts, working, 0, ordinal + 1).await);
    assert_eq!(pipeline.parked_download_results_for_test(), 2);
    assert_eq!(pipeline.parked_fronts_on_for_test(output), 1);
    assert_eq!(pipeline.parked_fronts_on_for_test(working), 0);
    assert_eq!(working.unwritten_bytes(), 2 * ARTICLE as u64);
    assert_eq!(
        working.arrived_bytes.load(Ordering::SeqCst),
        2 * ARTICLE as u64
    );
}

// Nothing of the job waits for room or is counted for it as arrived: no
// parked result, no hold, nothing booked as waiting, and working, where the
// parked results were counted, counts none of them.
fn parked_gone_with_job(
    pipeline: &Pipeline,
    job_id: JobId,
    working: &Volume,
    output: &Arc<Volume>,
) {
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    assert_eq!(pipeline.parked_fronts_on_for_test(output), 0);
    assert!(!pipeline.volume_arrived_by_job.contains_key(&job_id));
    assert!(!pipeline.results_waiting_for_test(job_id));
    assert_eq!(working.unwritten_bytes(), 0);
    assert_eq!(working.arrived_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(output.arrived_bytes.load(Ordering::SeqCst), 0);
}

// Room on a volume no front waits on retries nothing: the front parked on
// output does not fit there, so neither it nor the result behind it is
// looked at. Once output has room, its front goes and the one behind
// follows, each tried once.
#[tokio::test]
async fn a_room_wake_retries_only_the_fronts_that_fit_their_volume() {
    const ARTICLES: u32 = 24;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_504);
    let posts = [Post::plain(
        "lantern.bin",
        payload(ARTICLES as usize * ARTICLE, 13),
    )];
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    park_two_on_output(&mut pipeline, job_id, &posts, &working, &output).await;
    for _ in 0..3 {
        pipeline.volume_room_taken_for_test();
        assert_eq!(pipeline.hand_on_parked_for_test().await, 0);
    }
    assert_eq!(pipeline.parked_results_tried_for_test(), 0);
    assert_eq!(pipeline.parked_download_results_for_test(), 2);
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);

    for hold in &holds {
        hold.open();
    }
    let mut handed = 0;
    while pipeline.parked_download_results_for_test() > 0 {
        pipeline.hear_volume_room_for_test().await;
        handed += pipeline.hand_on_parked_for_test().await;
    }
    assert_eq!(handed, 2);
    assert_eq!(pipeline.parked_results_tried_for_test(), 2);
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// A job retired with results parked lets them go: none stays parked, and
// what they counted on working is given back there at once, with no room
// wake needed.
#[tokio::test]
async fn a_retired_job_lets_its_parked_results_go() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_505);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 14))];
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    park_two_on_output(&mut pipeline, job_id, &posts, &working, &output).await;
    pipeline.retire_graph_job(job_id);
    parked_gone_with_job(&pipeline, job_id, &working, &output);
    for hold in &holds {
        hold.open();
    }
    retire_pipeline_database(pipeline).await;
}

// The same for a job that fails while its results are parked.
#[tokio::test]
async fn a_failed_job_lets_its_parked_results_go() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_506);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 15))];
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    park_two_on_output(&mut pipeline, job_id, &posts, &working, &output).await;
    pipeline.fail_job(job_id, "the lantern went out".to_string());
    parked_gone_with_job(&pipeline, job_id, &working, &output);
    for hold in &holds {
        hold.open();
    }
    retire_pipeline_database(pipeline).await;
}

// A front parks on output while its writer holds a little and leases hold
// most of it, above the resume line. Once the writer has written what it
// holds the front fits, and the room wake comes there, though the count
// never falls to the resume line.
#[tokio::test]
async fn a_parked_front_wakes_when_the_writer_has_written_what_it_holds() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_507);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 16))];
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    // The member is decided onto output, and both volumes write it all.
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    let hold = pipeline.volume_writers.hold(&output);

    // A lease above the resume line, granted alone on the empty volume.
    let (_, hard) = output.buffer_limits();
    let leased = 14 * ARTICLE as u64 + ARTICLE as u64 / 2;
    assert!(leased > output.resume_line() && leased < hard);
    assert!(pipeline.lease_for_test(segment_id(job_id, 0, next), leased as u32));
    // One article the writer holds.
    assert!(take_counted_on(&mut pipeline, job_id, &posts, &output, 0, next + 1).await);
    assert_eq!(output.buffered_bytes.load(Ordering::SeqCst), ARTICLE as u64);
    // The next does not fit and parks.
    assert!(!take_counted_on(&mut pipeline, job_id, &posts, &working, 0, next + 2).await);
    assert_eq!(pipeline.parked_download_results_for_test(), 1);

    hold.open();
    pipeline.hear_volume_room_for_test().await;
    assert!(output.unwritten_bytes() > output.resume_line());
    assert_eq!(pipeline.hand_on_parked_for_test().await, 1);
    // The lease arrives, and nothing is left waiting.
    let body = body(&posts, 0, next);
    pipeline.arrive_for_test(
        segment_id(job_id, 0, next),
        leased as u32,
        body.len() as u64,
    );
    assert!(
        pipeline
            .hand_on_for_test(decoded_article(job_id, &posts, 0, next, body, true))
            .await
    );
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// Bytes the job holds on a volume it does not write to stay counted there
// when more of it arrives: output takes none of them at once, and only what
// fits moves there as the loop hands each article on.
#[tokio::test]
async fn bytes_held_on_a_volume_the_job_left_move_only_where_they_fit() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_508);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 17))];
    let (mut pipeline, _, _, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let left = Arc::new(Volume::new(None, "left".to_string()));
    left.set_buffer_limits(64 * ARTICLE as u64, 64 * ARTICLE as u64);
    let (_, hard) = output.buffer_limits();
    // More than output's whole limit, counted where the job no longer
    // writes.
    let away = hard + 4 * ARTICLE as u64;
    pipeline.arrive_on_for_test(job_id, &left, away);
    let body = body(&posts, 0, next);
    pipeline.arrive_on_for_test(job_id, &output, body.len() as u64);
    assert_eq!(left.unwritten_bytes(), away);
    assert!(output.unwritten_bytes() <= hard);
    within_limit(&output);
    assert!(
        pipeline
            .hand_on_for_test(decoded_article(job_id, &posts, 0, next, body, true))
            .await
    );
    within_limit(&output);
    assert_eq!(left.unwritten_bytes(), away);
    // Once nothing of the job waits, they are given back where they count.
    pipeline.let_go_result_for_test(job_id, away);
    assert_eq!(left.unwritten_bytes(), 0);
    assert_eq!(left.arrived_bytes.load(Ordering::SeqCst), 0);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// An extraction's thread stands for room on output beside a lease for an
// article that lands there. The lease is settled to what arrived, as a
// lane's article is: that gives most of it back, and the thread is woken
// and granted its room, with nothing written and no owner asked.
#[tokio::test]
async fn a_producer_standing_on_output_wakes_when_a_leased_article_gives_back() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_509);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 18))];
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    // The job's own way to write to output, as its extraction counts there.
    let producer = Arc::clone(disk.under(&staging_dir));
    assert!(Arc::ptr_eq(producer.volume(), &output));
    let hold = pipeline.volume_writers.hold(&output);

    // A lease that leaves no room for the producer's output beside it.
    let (_, hard) = output.buffer_limits();
    let leased = 14 * ARTICLE as u64;
    let wanted = 4 * ARTICLE as u64;
    assert!(leased + wanted > hard);
    assert!(pipeline.lease_for_test(segment_id(job_id, 0, next), leased as u32));
    let standing =
        std::thread::spawn(move || Credit::reserve([&producer], wanted, Stand::UntilStop));
    hold.wait_below_waiters(1);
    assert_eq!(
        output.unwritten_bytes(),
        leased,
        "the producer counted nothing"
    );

    // The article arrives smaller than its lease: what it does not need is
    // given back, and the standing thread is woken by that alone.
    let body = body(&posts, 0, next);
    pipeline.arrive_for_test(
        segment_id(job_id, 0, next),
        leased as u32,
        body.len() as u64,
    );
    let credit = standing.join().unwrap();
    assert!(credit.covers(wanted));
    assert_eq!(output.unwritten_bytes(), body.len() as u64 + wanted);
    assert_eq!(
        output.buffered_bytes.load(Ordering::SeqCst),
        0,
        "nothing was written"
    );
    drop(credit);
    hold.open();
    assert!(
        pipeline
            .hand_on_for_test(decoded_article(job_id, &posts, 0, next, body, true))
            .await
    );
    within_limit(&working);
    within_limit(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// Article `ordinal` of file `index` is leased, arrives and is handed on:
// `None`, with nothing done, when its volume refuses the lease, and
// otherwise whether it was handed on rather than parked.
async fn offer(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
) -> Option<bool> {
    let segment = segment_id(job_id, index, ordinal);
    if !pipeline.lease_for_test(segment, ARTICLE as u32) {
        return None;
    }
    let body = body(posts, index, ordinal);
    pipeline.arrive_for_test(segment, ARTICLE as u32, body.len() as u64);
    Some(
        pipeline
            .hand_on_for_test(decoded_article(job_id, posts, index, ordinal, body, true))
            .await,
    )
}

// Hear room wakes and hand parked results on until none is parked: how
// many were handed on.
async fn hand_on_all_parked(pipeline: &mut Pipeline) -> usize {
    let mut handed = 0;
    while pipeline.parked_download_results_for_test() > 0 {
        pipeline.hear_volume_room_for_test().await;
        handed += pipeline.hand_on_parked_for_test().await;
    }
    handed
}

// Take article `ordinal` of file `index` however the volumes stand: a
// refused lease waits for the room wake, and a parked result for its room.
async fn take_through(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    index: u32,
    ordinal: u32,
) {
    loop {
        match offer(pipeline, job_id, posts, index, ordinal).await {
            Some(true) => return,
            Some(false) => {
                hand_on_all_parked(pipeline).await;
                return;
            }
            None if !pipeline.ask_room_wake_for_test(job_id) => {
                assert!(
                    offer(pipeline, job_id, posts, index, ordinal)
                        .await
                        .is_some(),
                    "a volume at its room line grants the next lease"
                );
                hand_on_all_parked(pipeline).await;
                return;
            }
            None => pipeline.hear_volume_room_for_test().await,
        }
    }
}

fn articles(posts: &[Post], index: u32) -> u32 {
    posts[index as usize].articles(ARTICLE).count() as u32
}

// A plain file, then an archive of `volumes`: a stored RAR5 set of one
// member, `inside.mkv`, or a one-volume set whose member is compressed.
fn archive_posts(compressed: bool, seed: u8) -> (Vec<Post>, Vec<u8>) {
    use crate::pipeline::tests::{
        TEST_RAR5_SIG, build_test_rar_compressed_file_header, build_test_rar_end_header,
        build_test_rar_main_header, test_rar_compression_info,
    };
    let member = payload(6 * ARTICLE + 7, seed);
    let mut posts = vec![Post::plain("lantern.bin", payload(24 * ARTICLE, seed + 1))];
    if compressed {
        let mut bytes = TEST_RAR5_SIG.to_vec();
        bytes.extend_from_slice(&build_test_rar_main_header(0, None));
        bytes.extend_from_slice(&build_test_rar_compressed_file_header(
            "inside.mkv",
            0,
            member.len() as u64,
            member.len() as u64,
            Some(par2_rs::checksum::crc32(&member)),
            test_rar_compression_info(1, false),
        ));
        bytes.extend_from_slice(&member);
        bytes.extend_from_slice(&build_test_rar_end_header(false));
        posts.push(Post::plain("silver.horizon.rar", bytes));
    } else {
        posts.extend(
            crate::pipeline::tests::direct_store::single_member_store_set("inside.mkv", &member, 2)
                .into_iter()
                .map(|(name, bytes)| Post::plain(&name, bytes)),
        );
    }
    (posts, member)
}

// The plain file is decided onto a held output and the archive's first
// article is in, then output is filled to its hard limit while its writer
// holds what it was given: nothing more fits there. The plain file's next
// ordinal, and what fills output.
async fn fill_output(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    output: &Arc<Volume>,
) -> (u32, Reservation) {
    let mut next = decide_onto_output(pipeline, job_id, posts, output).await;
    assert!(take(pipeline, job_id, posts, 1, 0).await);
    while take(pipeline, job_id, posts, 0, next).await {
        next += 1;
    }
    let (_, hard) = output.buffer_limits();
    let mut fill = Reservation::none(Arc::clone(output));
    fill.count(hard.saturating_sub(output.unwritten_bytes()));
    assert!(output.buffered_bytes.load(Ordering::SeqCst) > 0);
    assert!(!output.fits(1));
    (next, fill)
}

// What a job of `posts` delivers and how it ends: the member's bytes, the
// delivery and the status, and the budget waits output recorded. Under
// `pressure` output is held and full when the archive's second article and
// its second volume's first arrive: both park, in arrival order, and output
// records no wait for them.
async fn stored_archive_run(
    job_id: JobId,
    pressure: bool,
) -> (Vec<u8>, Option<Delivery>, Option<JobStatus>) {
    stored_run(job_id, pressure, archive_posts(false, 21)).await
}

// The same over a plain file and a stored set of `posts`, whose member is
// delivered as `inside.mkv`.
async fn stored_run(
    job_id: JobId,
    pressure: bool,
    (posts, member): (Vec<Post>, Vec<u8>),
) -> (Vec<u8>, Option<Delivery>, Option<JobStatus>) {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let mut rest: Vec<(u32, u32)> = Vec::new();
    if pressure {
        let hold = pipeline.volume_writers.hold(&output);
        let (next, fill) = fill_output(&mut pipeline, job_id, &posts, &output).await;
        // The set's object passes its bytes on to the member on output.
        assert!(Arc::ptr_eq(
            &pipeline.landing_or_for_test(file(job_id, 1)).unwrap(),
            &output
        ));
        assert_eq!(
            offer(&mut pipeline, job_id, &posts, 1, 1).await,
            Some(false)
        );
        assert_eq!(
            offer(&mut pipeline, job_id, &posts, 2, 0).await,
            Some(false)
        );
        assert_eq!(pipeline.parked_download_results_for_test(), 2);
        assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);
        no_budget_waits(&output);
        no_loop_waits(&output);
        drop(fill);
        hold.open();
        assert_eq!(hand_on_all_parked(&mut pipeline).await, 2);
        assert_eq!(pipeline.parked_results_tried_for_test(), 2);
        rest.extend((2..articles(&posts, 1)).map(|ordinal| (1, ordinal)));
        rest.extend((1..articles(&posts, 2)).map(|ordinal| (2, ordinal)));
        rest.extend((next..articles(&posts, 0)).map(|ordinal| (0, ordinal)));
    } else {
        for index in 0..posts.len() as u32 {
            rest.extend((0..articles(&posts, index)).map(|ordinal| (index, ordinal)));
        }
    }
    for (index, ordinal) in rest {
        take_through(&mut pipeline, job_id, &posts, index, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    let delivered = read(&staging_dir.join("inside.mkv"));
    assert_eq!(delivered, member);
    no_budget_waits(&working);
    no_budget_waits(&output);
    within_limit(&working);
    nothing_waits_counted(&pipeline, job_id);
    let ended = (
        delivered,
        pipeline.graph_delivery(job_id),
        pipeline.graph_terminal_status(job_id),
    );
    retire_pipeline_database(pipeline).await;
    ended
}

// A stored set's volume lands on working and its object writes its bytes
// to the member on output in the same step. With output full, its results
// park for output's room, in arrival order, and the loop never waits there;
// once output drains, the member and the job's end are what they are with
// no pressure.
#[tokio::test]
async fn a_stored_archives_volume_parks_for_a_full_output_and_ends_the_same() {
    let pressed = stored_archive_run(JobId(92_960), true).await;
    let free = stored_archive_run(JobId(92_961), false).await;
    assert!(free.2.is_some());
    assert_eq!(pressed, free);
}

// A compressed set's volume is only kept for its engine: nothing of a step
// is written to output, so none is paid there, and a full output parks none
// of its results.
#[tokio::test]
async fn a_compressed_archives_volume_does_not_park_on_a_full_output() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_962);
    let (posts, _) = archive_posts(true, 23);
    let (mut pipeline, _, _, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    let (_, fill) = fill_output(&mut pipeline, job_id, &posts, &output).await;
    assert!(pipeline.landing_or_for_test(file(job_id, 1)).is_none());
    for ordinal in 1..articles(&posts, 1) {
        assert_eq!(
            offer(&mut pipeline, job_id, &posts, 1, ordinal).await,
            Some(true)
        );
    }
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    no_budget_waits(&output);
    no_loop_waits(&output);
    drop(fill);
    hold.open();
    retire_pipeline_database(pipeline).await;
}

// After each step of a stored set, output counts only what its writer was
// given: nothing prepaid for the step is left counted, whether the step
// wrote the member's bytes there, more than its own, or nothing at all.
#[tokio::test]
async fn a_stored_archives_step_leaves_nothing_prepaid_on_output() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_963);
    let (posts, _) = archive_posts(false, 25);
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    let owned = |output: &Volume| {
        assert_eq!(
            output.unwritten_bytes(),
            output.buffered_bytes.load(Ordering::SeqCst)
        );
    };
    decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    owned(&output);
    // Steps paid on output that wrote nothing there, and ones that wrote
    // more than their article: what the member held before it was decided.
    let (mut nothing, mut more) = (false, false);
    for index in [1, 2] {
        for ordinal in 0..articles(&posts, index) {
            let before = output.buffered_bytes.load(Ordering::SeqCst);
            assert!(take(&mut pipeline, job_id, &posts, index, ordinal).await);
            owned(&output);
            let written = output.buffered_bytes.load(Ordering::SeqCst) - before;
            nothing |= written == 0;
            more |= written > body(&posts, index, ordinal).len() as u64;
        }
    }
    assert!(nothing && more);
    // The same article again: taken in by nothing, and written nowhere.
    let before = output.buffered_bytes.load(Ordering::SeqCst);
    assert!(take_counted_on(&mut pipeline, job_id, &posts, &working, 1, 1).await);
    owned(&output);
    assert_eq!(output.buffered_bytes.load(Ordering::SeqCst), before);
    nothing_waits_counted(&pipeline, job_id);
    hold.open();
    retire_pipeline_database(pipeline).await;
}

// A job retired with a stored set's result parked for output's room lets
// it go: both volumes count only what their writers were given, and
// neither counts it as arrived.
#[tokio::test]
async fn a_retired_job_lets_a_result_parked_for_output_go_on_both_volumes() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_964);
    let (posts, _) = archive_posts(false, 27);
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    let (_, fill) = fill_output(&mut pipeline, job_id, &posts, &output).await;
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 1).await,
        Some(false)
    );
    assert_eq!(pipeline.parked_download_results_for_test(), 1);
    assert!(working.arrived_bytes.load(Ordering::SeqCst) > 0);
    drop(fill);
    pipeline.retire_graph_job(job_id);
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    assert!(!pipeline.volume_arrived_by_job.contains_key(&job_id));
    for volume in [&working, &output] {
        assert_eq!(volume.arrived_bytes.load(Ordering::SeqCst), 0);
        assert_eq!(
            volume.unwritten_bytes(),
            volume.buffered_bytes.load(Ordering::SeqCst)
        );
    }
    for hold in &holds {
        hold.open();
    }
    retire_pipeline_database(pipeline).await;
}

// The loop never waited for room on the volume, at an article's write to
// its leaf or at a release of held bytes into one, nor anywhere else.
fn no_leaf_budget_waits(volume: &Volume) {
    let by_site = &volume.writer.loop_waits_by_site[LoopWait::Budget as usize];
    for site in [LoopSite::LeafWrite, LoopSite::LeafRelease] {
        assert_eq!(
            by_site[site as usize].load(Ordering::Relaxed),
            0,
            "{site:?}"
        );
    }
    no_budget_waits(volume);
}

// A plain file, then a second plain file of `articles` whose bytes are what
// the test follows.
fn two_plain_posts(articles: usize, seed: u8) -> (Vec<Post>, Vec<u8>) {
    let dawn = payload(articles * ARTICLE + 7, seed);
    let posts = vec![
        Post::plain("lantern.bin", payload(24 * ARTICLE, seed + 1)),
        Post::plain("dawn.bin", dawn.clone()),
    ];
    (posts, dawn)
}

// The job's mail is heard while it still owes work its threads answer: a
// release of held bytes that found no room in its step is paid off the
// loop, and its bytes go down only once that mail is heard. A barrier
// taken before then stands for less than the job will write, and the
// payer's credit is counted on the volume but not yet buffered.
async fn settle(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline.graph_jobs[&job_id].work_pending() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
}

// The output volume counts only what its writer was given.
fn counts_only_buffered(output: &Volume) {
    assert_eq!(
        output.unwritten_bytes(),
        output.buffered_bytes.load(Ordering::SeqCst)
    );
}

// What a job of two plain files delivers and how it ends. The second
// file's later articles arrive before its first and are held; its first
// decides it onto output and releases them there in its step. Under
// `pressure` output is held and full when that first article arrives: it
// parks, and output records no wait for it or for its release.
async fn held_plain_run(
    job_id: JobId,
    pressure: bool,
) -> (Vec<u8>, Vec<u8>, Option<Delivery>, Option<JobStatus>) {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (posts, dawn) = two_plain_posts(6, 31);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    let mut next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    counts_only_buffered(&output);
    for ordinal in [2, 1] {
        assert!(take(&mut pipeline, job_id, &posts, 1, ordinal).await);
        counts_only_buffered(&output);
    }
    // The first article is paid on output with what the file holds.
    let (or, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    assert!(Arc::ptr_eq(&or, &output));
    assert_eq!(held, 2 * ARTICLE as u64);
    if pressure {
        while take(&mut pipeline, job_id, &posts, 0, next).await {
            next += 1;
        }
        // Room for the article, not for what it releases with it.
        let (_, hard) = output.buffer_limits();
        let mut fill = Reservation::none(Arc::clone(&output));
        fill.count(hard - ARTICLE as u64 - output.unwritten_bytes());
        assert!(output.fits(ARTICLE as u64));
        assert!(!output.fits(ARTICLE as u64 + held));
        assert_eq!(
            offer(&mut pipeline, job_id, &posts, 1, 0).await,
            Some(false)
        );
        assert_eq!(pipeline.parked_download_results_for_test(), 1);
        no_leaf_budget_waits(&output);
        drop(fill);
        hold.open();
        assert_eq!(hand_on_all_parked(&mut pipeline).await, 1);
    } else {
        hold.open();
        take_through(&mut pipeline, job_id, &posts, 1, 0).await;
    }
    settle(&mut pipeline, job_id).await;
    counts_only_buffered(&output);
    let mut rest: Vec<(u32, u32)> = (3..articles(&posts, 1)).map(|o| (1, o)).collect();
    rest.extend((next..articles(&posts, 0)).map(|o| (0, o)));
    for (index, ordinal) in rest {
        take_through(&mut pipeline, job_id, &posts, index, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    let delivered = read(&staging_dir.join("dawn.bin"));
    assert_eq!(delivered, dawn);
    let lantern = read(&staging_dir.join("lantern.bin"));
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    within_limit(&working);
    nothing_waits_counted(&pipeline, job_id);
    let ended = (
        delivered,
        lantern,
        pipeline.graph_delivery(job_id),
        pipeline.graph_terminal_status(job_id),
    );
    retire_pipeline_database(pipeline).await;
    ended
}

// Case (a): a plain file decided at a step that releases what it held. With
// output full the step parks, output never waits at the write or the
// release, and the job ends as it does with no pressure.
#[tokio::test]
async fn a_plain_file_decided_with_held_bytes_parks_for_a_full_output_and_ends_the_same() {
    let pressed = held_plain_run(JobId(92_965), true).await;
    let free = held_plain_run(JobId(92_966), false).await;
    assert!(free.3.is_some());
    assert_eq!(pressed, free);
}

// What a job of a plain file and a stored set delivers and how it ends. The
// set's second volume arrives before the first volume's last article, and
// its data is held; that last article decides the member onto output and
// releases it there in its step. Under `pressure` output is held, with room
// for the article but not for what it releases, when that article arrives:
// it parks, and output records no wait for it or for its release.
async fn held_store_run(
    job_id: JobId,
    pressure: bool,
) -> (Vec<u8>, Option<Delivery>, Option<JobStatus>) {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (posts, member) = archive_posts(false, 33);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    let mut next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let last = articles(&posts, 1) - 1;
    let mut early: Vec<(u32, u32)> = (0..last).map(|o| (1, o)).collect();
    early.extend((0..articles(&posts, 2)).map(|o| (2, o)));
    for (index, ordinal) in early {
        assert!(take(&mut pipeline, job_id, &posts, index, ordinal).await);
        counts_only_buffered(&output);
    }
    let (or, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    assert!(Arc::ptr_eq(&or, &output));
    let article = body(&posts, 1, last).len() as u64;
    // The second volume's data, held until the member is decided.
    assert!(held > article, "{held}");
    if pressure {
        while take(&mut pipeline, job_id, &posts, 0, next).await {
            next += 1;
        }
        let (_, hard) = output.buffer_limits();
        let mut fill = Reservation::none(Arc::clone(&output));
        fill.count(hard - article - output.unwritten_bytes());
        assert!(output.fits(article));
        assert!(!output.fits(article + held), "{held}");
        assert_eq!(
            offer(&mut pipeline, job_id, &posts, 1, last).await,
            Some(false)
        );
        assert_eq!(pipeline.parked_download_results_for_test(), 1);
        no_leaf_budget_waits(&output);
        drop(fill);
        hold.open();
        assert_eq!(hand_on_all_parked(&mut pipeline).await, 1);
    } else {
        hold.open();
        take_through(&mut pipeline, job_id, &posts, 1, last).await;
    }
    settle(&mut pipeline, job_id).await;
    counts_only_buffered(&output);
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    let delivered = read(&staging_dir.join("inside.mkv"));
    assert_eq!(delivered, member);
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    within_limit(&working);
    nothing_waits_counted(&pipeline, job_id);
    let ended = (
        delivered,
        pipeline.graph_delivery(job_id),
        pipeline.graph_terminal_status(job_id),
    );
    retire_pipeline_database(pipeline).await;
    ended
}

// Case (b): a stored set's member decided at the first volume's last
// article, with the second volume's data held. With output short of what
// the step releases, it parks, output never waits at the write or the
// release, and the job ends as it does with no pressure.
#[tokio::test]
async fn a_stored_member_decided_with_a_volume_held_parks_for_a_full_output_and_ends_the_same() {
    let pressed = held_store_run(JobId(92_967), true).await;
    let free = held_store_run(JobId(92_968), false).await;
    assert!(free.2.is_some());
    assert_eq!(pressed, free);
}

// A plain file decided at a step that releases more than output's limit:
// the step parks while output's writer holds anything, a look at the parked
// results then hands on none, and once output has written what it held the
// step goes on over the limit, as nothing else is buffered there. Output
// never waits, and the file is whole.
#[tokio::test]
async fn a_release_above_outputs_limit_parks_until_output_holds_nothing_then_goes_on() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_969);
    let (posts, dawn) = two_plain_posts(24, 35);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    // Held while output holds nothing: each fits there.
    for ordinal in (1..articles(&posts, 1)).rev() {
        take_through(&mut pipeline, job_id, &posts, 1, ordinal).await;
    }
    let (_, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    let (_, hard) = output.buffer_limits();
    assert!(held > hard, "{held} {hard}");
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    assert!(output.buffered_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 0).await,
        Some(false)
    );
    assert_eq!(pipeline.hand_on_parked_for_test().await, 0);
    assert_eq!(pipeline.parked_download_results_for_test(), 1);
    no_leaf_budget_waits(&output);
    hold.open();
    assert_eq!(hand_on_all_parked(&mut pipeline).await, 1);
    assert!(output.peak_bytes() > hard);
    settle(&mut pipeline, job_id).await;
    counts_only_buffered(&output);
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    assert_eq!(read(&staging_dir.join("dawn.bin")), dawn);
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// A stored set's object lent out, as a run takes it: the node answers as
// the object did when it left, so the set's volume is still paid on output,
// and with output full its result parks. The job retired lets it go.
#[tokio::test]
async fn a_lent_stored_archives_volume_still_parks_for_a_full_output() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_970);
    let (posts, _) = archive_posts(false, 37);
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    let (_, fill) = fill_output(&mut pipeline, job_id, &posts, &output).await;
    let graph = pipeline.graph_jobs.get_mut(&job_id).unwrap();
    let (child, _) = graph
        .tree()
        .child_of(weaver_graph::NodeId::ROOT, MemberId(1))
        .unwrap();
    assert!(graph.tree().passes_on(child));
    let lent = graph.lend_object_for_test(child).expect("lent");
    assert!(graph.tree().lent(child));
    assert!(graph.tree().passes_on(child));
    assert!(Arc::ptr_eq(
        &pipeline.landing_or_for_test(file(job_id, 1)).unwrap(),
        &output
    ));
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 1).await,
        Some(false)
    );
    assert_eq!(pipeline.parked_download_results_for_test(), 1);
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);
    no_leaf_budget_waits(&output);
    drop(fill);
    pipeline.retire_graph_job(job_id);
    drop(lent);
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    for hold in &holds {
        hold.open();
    }
    retire_pipeline_database(pipeline).await;
}

// A plain file, then a stored set of two volumes whose one member is a
// one-volume stored set, `inner.rar`, of `inside.mkv`.
fn nested_store_posts(seed: u8) -> (Vec<Post>, Vec<u8>) {
    use crate::pipeline::tests::direct_store::single_member_store_set;
    let member = payload(6 * ARTICLE + 7, seed);
    let (_, inner) = single_member_store_set("inside.mkv", &member, 1)
        .pop()
        .unwrap();
    let mut posts = vec![Post::plain("lantern.bin", payload(24 * ARTICLE, seed + 1))];
    posts.extend(
        single_member_store_set("inner.rar", &inner, 2)
            .into_iter()
            .map(|(name, bytes)| Post::plain(&name, bytes)),
    );
    (posts, member)
}

// A stored set inside a stored set: the outer set's volume is paid on
// output for what the inner set's object writes there in the same step, and
// what it holds. With output full its results park, output never waits,
// and the job ends as it does with no pressure.
#[tokio::test]
async fn a_stored_archive_one_level_down_parks_for_a_full_output_and_ends_the_same() {
    let pressed = stored_run(JobId(92_971), true, nested_store_posts(39)).await;
    let free = stored_run(JobId(92_972), false, nested_store_posts(39)).await;
    assert!(free.2.is_some());
    assert_eq!(pressed, free);
}

// Two stored sets, the second holding its second volume's data: a step of
// the first is paid on output for what the first holds only, never for
// what the second holds.
#[tokio::test]
async fn a_stored_archives_step_pays_nothing_for_what_another_set_holds() {
    use crate::pipeline::tests::direct_store::single_member_store_set;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_973);
    let (mut posts, _) = archive_posts(false, 41);
    posts.extend(
        single_member_store_set("other.mkv", &payload(6 * ARTICLE + 7, 43), 2)
            .into_iter()
            .map(|(name, bytes)| {
                Post::plain(&name.replace("silver.horizon", "golden.dawn"), bytes)
            }),
    );
    let (mut pipeline, _, _, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    assert!(take(&mut pipeline, job_id, &posts, 1, 0).await);
    let first = |pipeline: &Pipeline| {
        let (or, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
        assert!(Arc::ptr_eq(&or, &output));
        held
    };
    let before = first(&pipeline);
    // The second set's first volume short of its last article, and all of
    // its second volume: held until its member is decided.
    let last = articles(&posts, 3) - 1;
    let mut second: Vec<(u32, u32)> = (0..last).map(|o| (3, o)).collect();
    second.extend((0..articles(&posts, 4)).map(|o| (4, o)));
    for (index, ordinal) in second {
        take_through(&mut pipeline, job_id, &posts, index, ordinal).await;
    }
    let (_, theirs) = pipeline.landing_or_held_for_test(file(job_id, 3)).unwrap();
    assert!(theirs > ARTICLE as u64, "{theirs}");
    assert_eq!(first(&pipeline), before);
    retire_pipeline_database(pipeline).await;
}

// A step that releases more than output's limit parks with output's writer
// holding bytes and more counted there besides. Its wake is taken while the
// writer still holds bytes: the look finds it still short and leaves it
// parked. Its wake is asked again then, so once the writer has written what
// it holds, with nothing else counted, the step is woken and goes on.
#[tokio::test]
async fn a_front_a_look_leaves_parked_is_woken_once_output_holds_nothing() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_974);
    let (posts, dawn) = two_plain_posts(24, 41);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let hold = pipeline.volume_writers.hold(&output);
    for ordinal in (1..articles(&posts, 1)).rev() {
        take_through(&mut pipeline, job_id, &posts, 1, ordinal).await;
    }
    let (_, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    let (_, hard) = output.buffer_limits();
    assert!(held > hard, "{held} {hard}");
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let buffered = output.buffered_bytes.load(Ordering::SeqCst);
    assert!(buffered > 0);
    // Counted on output besides what its writer holds, as another job's
    // lease is: the wake is asked at that much.
    let mut besides = Reservation::none(Arc::clone(&output));
    besides.count(buffered + ARTICLE as u64);
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 0).await,
        Some(false)
    );
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);
    // The other bytes go, and the wake is taken while the writer still
    // holds what it was given: the look leaves the step parked.
    // A lease given back unfetched takes it, as a lane's returned article
    // does.
    drop(besides);
    let segment = segment_id(job_id, 0, next);
    assert!(pipeline.lease_for_test(segment, 1));
    pipeline.arrive_for_test(segment, 1, 0);
    pipeline.let_go_result_for_test(job_id, 0);
    assert!(output.buffered_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(pipeline.hand_on_parked_for_test().await, 0);
    assert_eq!(
        pipeline.parked_results_tried_for_test(),
        0,
        "the look took none"
    );
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);
    // The writer writes what it holds, and output counts nothing.
    hold.open();
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    assert!(
        pipeline.volume_rooms.try_recv().is_ok(),
        "the step left parked is woken once output holds nothing"
    );
    pipeline.volume_room_taken_for_test();
    assert_eq!(pipeline.hand_on_parked_for_test().await, 1);
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("dawn.bin")), dawn);
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// A step that releases more than output's limit, with output's writer
// holding nothing but much else counted there, as other jobs' leases are:
// it waits until its own article fits beside them, so output passes its
// limit by the step's held bytes at most, never by those and all it counted
// besides.
#[tokio::test]
async fn a_release_above_outputs_limit_passes_it_by_its_held_bytes_at_most() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_975);
    let (posts, dawn) = two_plain_posts(24, 43);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    for ordinal in (1..articles(&posts, 1)).rev() {
        take_through(&mut pipeline, job_id, &posts, 1, ordinal).await;
    }
    let (_, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    let (_, hard) = output.buffer_limits();
    assert!(held > hard, "{held} {hard}");
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    // Counted there besides, with nothing for the writer: no room for the
    // article beside it.
    let mut besides = Reservation::none(Arc::clone(&output));
    besides.count(hard - ARTICLE as u64 / 2);
    assert_eq!(output.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 0).await,
        Some(false),
        "the step went on over all that output counted"
    );
    // The other bytes go, and a lease given back takes the wake.
    drop(besides);
    let segment = segment_id(job_id, 0, next);
    assert!(pipeline.lease_for_test(segment, 1));
    pipeline.arrive_for_test(segment, 1, 0);
    pipeline.let_go_result_for_test(job_id, 0);
    assert_eq!(pipeline.hand_on_parked_for_test().await, 1);
    let article = body(&posts, 1, 0).len() as u64;
    assert!(
        output.peak_bytes() <= hard + held + article,
        "{} past {hard} by more than {held}",
        output.peak_bytes()
    );
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("dawn.bin")), dawn);
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// A step that releases more than output's limit, with output's writer
// holding nothing and all output counts carried by a queuing thread for
// output it holds in memory, at or above the line the step's own article
// fits below: only held bytes are counted, which nothing but more of that
// output frees, so the step goes on beside them and output passes its limit
// by held bytes and the article at most.
#[tokio::test]
async fn a_release_beside_only_carried_bytes_on_output_goes_on() {
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_976);
    let (posts, dawn) = two_plain_posts(24, 45);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    for ordinal in (1..articles(&posts, 1)).rev() {
        take_through(&mut pipeline, job_id, &posts, 1, ordinal).await;
    }
    let (_, held) = pipeline.landing_or_held_for_test(file(job_id, 1)).unwrap();
    let (_, hard) = output.buffer_limits();
    assert!(held > hard, "{held} {hard}");
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    // Carried on output by the job's queuing thread, above the line the
    // step's own article fits below.
    let producer = Arc::clone(disk.under(&staging_dir));
    assert!(Arc::ptr_eq(producer.volume(), &output));
    let carried = hard - ARTICLE as u64 / 2;
    let mut credit = Credit::try_reserve([&producer], carried, Stand::UntilStopsEnd).unwrap();
    assert!(credit.covers(carried));
    credit.carry();
    assert_eq!(output.carried_bytes(), carried);
    assert_eq!(output.unwritten_bytes(), carried);
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 0).await,
        Some(true),
        "the step parked beside held bytes only"
    );
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 0);
    let article = body(&posts, 1, 0).len() as u64;
    assert!(
        output.peak_bytes() <= hard + held + article,
        "{} past {hard} by more than {held}",
        output.peak_bytes()
    );
    drop(credit);
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(output.carried_bytes(), 0);
    assert_eq!(read(&staging_dir.join("dawn.bin")), dawn);
    no_budget_waits(&working);
    no_leaf_budget_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// What a job of a plain file and a stored set of two volumes delivers and
// how it ends. Under `pressure`, once the plain file and the set's first
// volume are in and working's writer holds nothing, all working counts is
// what the job's queuing thread carries there for output it holds in
// memory, above working's lease limit, as a member held until a later outer
// volume is: the bytes that would place it are still to be downloaded, so
// the second volume's articles are all leased beside it, arrive, and the
// held bytes are placed after.
async fn carried_run(
    job_id: JobId,
    pressure: bool,
) -> (Vec<u8>, Option<Delivery>, Option<JobStatus>) {
    use crate::pipeline::graph::disk::VolumeLeases;
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (posts, member) = archive_posts(false, 47);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    for index in 0..2 {
        for ordinal in 0..articles(&posts, index) {
            take_through(&mut pipeline, job_id, &posts, index, ordinal).await;
        }
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    let mut first = 0;
    if pressure {
        assert_eq!(working.unwritten_bytes(), 0);
        // Held above the lease limit: carried bytes use none of it.
        let carried = working.lease_limit() + 4 * ARTICLE as u64;
        let mut credit =
            Credit::try_reserve([disk.working()], carried, Stand::UntilStopsEnd).unwrap();
        assert!(credit.covers(carried));
        credit.carry();
        assert_eq!(working.carried_bytes(), carried);
        assert_eq!(working.unwritten_bytes(), carried);
        assert_eq!(working.buffered_bytes.load(Ordering::SeqCst), 0);
        // Every article of the file is leased together, as lanes lease
        // them, beside the held bytes.
        let count = articles(&posts, 2);
        assert!(count > 1);
        for ordinal in 0..count {
            assert!(
                pipeline.lease_for_test(segment_id(job_id, 2, ordinal), ARTICLE as u32),
                "a volume counting held bytes above its lease limit refused lease {ordinal}"
            );
            assert!(!pipeline.volume_holds_budget(job_id));
        }
        for ordinal in 0..count {
            let body = body(&posts, 2, ordinal);
            pipeline.arrive_for_test(
                segment_id(job_id, 2, ordinal),
                ARTICLE as u32,
                body.len() as u64,
            );
            let decoded = decoded_article(job_id, &posts, 2, ordinal, body, true);
            if !pipeline.hand_on_for_test(decoded).await {
                hand_on_all_parked(&mut pipeline).await;
            }
        }
        // The held bytes are placed.
        drop(credit);
        first = count;
    }
    for ordinal in first..articles(&posts, 2) {
        take_through(&mut pipeline, job_id, &posts, 2, ordinal).await;
    }
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    assert_eq!(working.carried_bytes(), 0);
    let delivered = read(&staging_dir.join("inside.mkv"));
    assert_eq!(delivered, member);
    no_budget_waits(&working);
    no_budget_waits(&output);
    nothing_waits_counted(&pipeline, job_id);
    let ended = (
        delivered,
        pipeline.graph_delivery(job_id),
        pipeline.graph_terminal_status(job_id),
    );
    retire_pipeline_database(pipeline).await;
    ended
}

// A volume whose whole count is held run output, above its lease limit,
// leases beside it as an empty one does: every article of a file is leased
// together, and the job ends as it does with no pressure.
#[tokio::test]
async fn a_lease_beside_only_held_bytes_at_the_lease_limit_is_granted_and_ends_the_same() {
    let pressed = carried_run(JobId(92_977), true).await;
    let free = carried_run(JobId(92_978), false).await;
    assert!(free.2.is_some());
    assert_eq!(pressed, free);
}

// Output carries held run output, its leases fill its lease budget beside
// it, and its writer holds a byte: a step whose bytes fit below the limit
// beside what output counts besides the carried bytes takes its room and
// does not park, as the leases were granted; one byte more parks.
#[tokio::test]
async fn a_step_beside_carried_bytes_and_full_leases_takes_its_room() {
    use crate::pipeline::graph::disk::VolumeLeases;
    use weaver_server_disk::operations::disk::writer::{Credit, Stand};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_986);
    let (posts, _) = two_plain_posts(24, 47);
    let (pipeline, staging_dir, _working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let submitter = Arc::clone(disk.under(&staging_dir));
    let (_, hard) = output.buffer_limits();
    let mut carried = Credit::try_reserve([&submitter], 2 * ARTICLE as u64, Stand::UntilStop)
        .expect("granted alone on an empty volume");
    carried.carry();
    assert_eq!(output.carried_bytes(), 2 * ARTICLE as u64);
    // A byte the writer holds, and leases up to the lease limit.
    let mut held = Reservation::none(Arc::clone(&output));
    held.count(1);
    output.buffered_bytes.fetch_add(1, Ordering::SeqCst);
    let mut leases = Reservation::none(Arc::clone(&output));
    leases.count(output.lease_limit() - 1);
    assert_eq!(output.uncarried_bytes(), output.lease_limit());
    let room = hard - output.uncarried_bytes();
    assert!(room > 0);
    assert!(
        pipeline.takes_landing_room_for_test(job_id, &output, room, room),
        "the step fits beside what is counted besides the carried bytes"
    );
    assert!(
        !pipeline.takes_landing_room_for_test(job_id, &output, room + 1, room + 1),
        "a byte more parks"
    );
    output.buffered_bytes.fetch_sub(1, Ordering::SeqCst);
    drop((held, leases, carried));
    assert_eq!(output.unwritten_bytes(), 0);
    retire_pipeline_database(pipeline).await;
}

// Output counts past its limit only bytes another job's articles arrived
// with, which wait to be handed on: nothing its writer writes frees them,
// only handing them on does, which may wait on this job. The job's next
// article counted on working lands on output and is handed on beside them,
// as beside held bytes only; it does not park for a wake nothing brings.
#[tokio::test]
async fn a_front_beside_only_anothers_arrived_bytes_is_handed_on() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_980);
    let other = JobId(92_981);
    let posts = [Post::plain("lantern.bin", payload(24 * ARTICLE, 19))];
    let (mut pipeline, _, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    let (_, hard) = output.buffer_limits();
    let away = hard + 4 * ARTICLE as u64;
    pipeline.arrive_on_for_test(other, &output, away);
    assert!(
        take_counted_on(&mut pipeline, job_id, &posts, &working, 0, next).await,
        "parked for room only handing on makes"
    );
    assert_eq!(pipeline.parked_download_results_for_test(), 0);
    pipeline.let_go_result_for_test(other, away);
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    assert_eq!(output.arrived_bytes.load(Ordering::SeqCst), 0);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

// A step that releases more than output's limit parks on output, which
// counts another job's lease beside nothing for its writer: no room for the
// step's own article beside it. That lease's article arrives: output then
// counts only bytes that wait to be handed on, beside which the step is
// taken (`room_for`), though no writer wakes for it. The arrival alone has
// the parked front looked at again.
#[tokio::test]
async fn bytes_arriving_on_a_volume_a_front_waits_on_have_it_looked_at_again() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_982);
    let other = JobId(92_983);
    let (posts, dawn) = two_plain_posts(24, 47);
    let (mut pipeline, staging_dir, working, output) =
        two_volume_job(&temp_dir, job_id, &posts, 16 * ARTICLE as u64).await;
    for ordinal in (1..articles(&posts, 1)).rev() {
        take_through(&mut pipeline, job_id, &posts, 1, ordinal).await;
    }
    let (_, hard) = output.buffer_limits();
    let next = decide_onto_output(&mut pipeline, job_id, &posts, &output).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert_eq!(output.unwritten_bytes(), 0);
    let away = hard - ARTICLE as u64 / 2;
    let mut lease = Reservation::none(Arc::clone(&output));
    lease.count(away);
    assert_eq!(output.buffered_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(
        offer(&mut pipeline, job_id, &posts, 1, 0).await,
        Some(false)
    );
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 1);
    assert_eq!(pipeline.hand_on_parked_for_test().await, 0);
    assert!(
        !pipeline.arrive_leased_for_test(other, lease, away),
        "a writer's wake came"
    );
    assert_eq!(
        pipeline.hand_on_parked_for_test().await,
        1,
        "the front was not looked at once output counted only arrived bytes"
    );
    assert_eq!(pipeline.parked_fronts_on_for_test(&output), 0);
    pipeline.let_go_result_for_test(other, away);
    for ordinal in next..articles(&posts, 0) {
        take_through(&mut pipeline, job_id, &posts, 0, ordinal).await;
    }
    settle(&mut pipeline, job_id).await;
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("dawn.bin")), dawn);
    assert_eq!(output.unwritten_bytes(), 0);
    assert_eq!(output.arrived_bytes.load(Ordering::SeqCst), 0);
    no_budget_waits(&working);
    nothing_waits_counted(&pipeline, job_id);
    retire_pipeline_database(pipeline).await;
}

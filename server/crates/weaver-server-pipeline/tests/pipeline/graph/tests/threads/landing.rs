// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each article's bytes count on the volume they land on, from its lease to
// its write: a full volume stops what is leased for it, never the loop, and
// never counts past its limit.

use super::*;
use weaver_server_disk::operations::disk::volume::LoopWait;

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

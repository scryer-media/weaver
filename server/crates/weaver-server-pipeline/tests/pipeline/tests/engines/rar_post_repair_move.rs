// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_base::e2e_failpoint::DIRECT_STORE_POST_REPAIR_VERIFY;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_pipeline::JobStatus;
use weaver_server_pipeline::pipeline::tests::{
    engines::{
        harness::{ARTICLE, JOB, Post, Scenario, admit, payload},
        rar_actor::{drive_to_move, moving, next_move, repaired},
        rar_fixtures::store::opening_lost_recovery_refused,
        recovery_actor::status as job_status_for_assert,
    },
    insert_active_job,
};

const OTHER: JobId = JobId(93_002);

// A graph job held at the post-repair hook stands at its move while another
// graph job is delivered beside it: the hold is the held job's alone.
#[test]
fn a_graph_job_held_after_its_repair_holds_no_other_job() {
    // The hook is read from the environment once per process and holds far
    // past any run. The row ends on the other job's delivery, never on the
    // hold, and the held thread is left behind when the runtime stops.
    // SAFETY: nothing else in this test's process reads the environment yet.
    unsafe {
        std::env::set_var(
            "WEAVER_E2E_DELAY",
            format!("{DIRECT_STORE_POST_REPAIR_VERIFY}=3600000"),
        );
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let held = opening_lost_recovery_refused();
        let other = Scenario::new(vec![Post::plain("ember.bin", payload(3 * ARTICLE + 5, 23))]);
        let (_temp_dir, mut pipeline, _) = admit(&held).await;
        insert_active_job(&mut pipeline, OTHER, other.spec()).await;

        drive_to_move(&mut pipeline, JOB, &held).await;
        assert!(repaired(&pipeline, JOB), "the held job repaired");
        drive_to_move(&mut pipeline, OTHER, &other).await;
        let done = next_move(&mut pipeline).await;
        assert_eq!(done.job(), OTHER, "the held job's move is still held");
        done.acknowledge(&mut pipeline).await;
        assert_eq!(
            job_status_for_assert(&pipeline, OTHER),
            Some(JobStatus::Complete)
        );
        assert!(moving(&pipeline, JOB));
        assert_eq!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Moving)
        );
    });
    runtime.shutdown_background();
}

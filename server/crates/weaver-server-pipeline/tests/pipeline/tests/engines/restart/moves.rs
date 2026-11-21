// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_composition::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::{
    harness::{ARTICLE, Scenario, payload},
    restart_fixtures::{Restarted, with_par3},
    restart_move_fixture::{Stop, run_stopping, stopped_row, waiting},
};
// The delivered names, without the job's folder.
fn delivered(restarted: &Restarted) -> Vec<&str> {
    restarted
        .files
        .keys()
        .map(|name| {
            std::path::Path::new(name)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        })
        .collect()
}

// One file posted under a hex name, named by the PAR3 set beside it.
fn renamed_once() -> Scenario {
    let files = [("lantern.bin", payload(5 * ARTICLE, 91))];
    Scenario::new(with_par3(&files, &["3f9a0c71e25b84d6"], "e81b6d3f0a4c9725"))
}

// One file posted under a hex name that ends in a piece number, named by
// the PAR3 set beside it: it is held as a piece of a split set until the set
// names it, and is then handed back as a file of its own and renamed.
fn renamed_from_a_piece() -> Scenario {
    let files = [("lantern.bin", payload(5 * ARTICLE, 94))];
    Scenario::new(with_par3(
        &files,
        &["3f9a0c71e25b84d6.201"],
        "e81b6d3f0a4c9725",
    ))
}

// Two files the PAR3 set names over each other: the one posted under a hex
// name is `beacon.bin`, and the one posted as `BEACON.BIN`, the same name
// to a file system that folds case, is `lantern.bin`. The second moves off
// the name before the first moves in.
fn renamed_in_a_chain() -> Scenario {
    let files = [
        ("beacon.bin", payload(2 * ARTICLE + 7, 93)),
        ("lantern.bin", payload(5 * ARTICLE, 92)),
    ];
    Scenario::new(with_par3(
        &files,
        &["a07d5e19c4b26f83", "BEACON.BIN"],
        "e81b6d3f0a4c9725",
    ))
}

// Down after the step that renamed the member, before its checkpoint
// committed: the file still sits at the name the database gives it.
#[tokio::test]
async fn a_rename_whose_checkpoint_never_committed_leaves_the_file_where_the_database_says() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Uncommitted).await;
    assert_eq!(waiting(&rows), []);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the checkpoint that records the name committed, before the
// move ran: the restore opens the file where it sat and moves it later.
#[tokio::test]
async fn a_rename_committed_but_not_moved_is_opened_where_the_file_sat() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Committed).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the checkpoint that records the name committed, and back to a
// directory the restore cannot remove at the name the file wants: the
// restore runs the move as it
// comes back, the move fails, and the job fails with it rather than wait
// on a move that never runs.
#[tokio::test]
async fn a_move_that_fails_as_the_job_comes_back_fails_the_job() {
    let (restarted, rows) = run_stopping(&renamed_once(), Stop::Blocked("lantern.bin")).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(
        matches!(restarted.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        restarted.status
    );
}

// Down after the move ran, before the checkpoint that says so committed:
// the file is not where it sat and is at the name the database wants, so
// the restore adopts it there.
#[tokio::test]
async fn a_file_moved_before_its_checkpoint_said_so_is_adopted_at_its_name() {
    let (rows, restarted) = stopped_row(renamed_once(), Stop::Moved("lantern.bin")).await;
    assert_eq!(waiting(&rows), [("3f9a0c71e25b84d6", "lantern.bin")]);
    assert!(delivered(&restarted).contains(&"lantern.bin"));
}

// Down after the first file left `beacon.bin` and before the second moved
// in: each restored file opens where it is, and neither is taken for the
// other.
#[tokio::test]
async fn a_chain_of_renames_down_after_the_first_move_opens_each_file_where_it_is() {
    let (rows, restarted) = stopped_row(renamed_in_a_chain(), Stop::Moved("lantern.bin")).await;
    // Both renames are recorded, and only the first file has moved.
    let mut rows = waiting(&rows);
    rows.sort();
    assert_eq!(
        rows,
        [
            ("BEACON.BIN", "lantern.bin"),
            ("a07d5e19c4b26f83", "beacon.bin")
        ]
    );
    let mut names = delivered(&restarted);
    names.sort();
    assert!(
        names.starts_with(&["beacon.bin", "lantern.bin"]),
        "{names:?}"
    );
}

// Down after the checkpoint that records the name a piece is handed back
// under committed, before any move ran: the split store kept none of the
// piece's bytes in a file of its own, and the step that renamed the piece
// handed it back, so its bytes went into the member's own file at the name
// the set gives. Nothing waits to move, and the restore opens it there.
#[tokio::test]
async fn a_piece_handed_back_and_renamed_but_not_moved_is_opened_where_it_sat() {
    let (rows, restarted) = stopped_row(renamed_from_a_piece(), Stop::Committed).await;
    // The piece is a file of the job's own folder now, bound to nothing.
    let piece = rows.member(0, 0).expect("the piece's row");
    assert_eq!(piece, ("lantern.bin", None, true));
    assert_eq!(waiting(&rows), []);
    assert_eq!(delivered(&restarted), ["lantern.bin"]);
}

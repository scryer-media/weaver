// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A posted file takes the name its articles' yEnc header gives it
// (`=ybegin ... name=`), once, from the first article taken in, before any
// of its bytes are written, as `weaver_nzb::header_name` decides:
// not an obfuscated-looking name, not over a posted PAR2 name, and never
// over the name a PAR2 set gives the file by its content. The name is the
// tree's, as a PAR2 set's rename is, so a split set the posted names
// founded is founded again under the header names, and the name survives a
// restart.

use std::collections::BTreeMap;

use crate::jobs::model::JobStatus;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_set, payload, run};
use super::restart::restart_row;

const JOB: &str = "lantern beacon";
const PIECE: usize = 6 * ARTICLE;
const TRUE_NAME: &str = "Show.Name.S01E01.1080p.WEB.h264-GRP.mkv";
// The header name of the ruling's counter-example: 32 lowercase hex.
const HEX_NAME: &str = "b082fa0beaa644d3aa01045d5b8d0b36.mkv";
// A file every scenario below also posts, whose header renames it: the
// rows that pin a header name kept also show a header name taken.
const CONTROL_POSTED: &str = "a1b2c3d4e5f6.bin";
const CONTROL_HEADER: &str = "Ember.Control.File.bin";

// The names the subjects give, read as the NZB intake reads them.
fn posted_names(subjects: &[&str]) -> Vec<String> {
    let names: Vec<String> = subjects
        .iter()
        .map(|subject| weaver_nzb::extract_filename(subject).unwrap().0)
        .collect();
    weaver_model::files::unique_download_filenames(names.iter().map(String::as_str))
}

fn joined() -> Vec<u8> {
    payload(3 * PIECE - 17, 113)
}

fn control() -> Post {
    Post::plain(CONTROL_POSTED, payload(4 * ARTICLE + 9, 117))
}

// The ruling's example: pieces posted as `Show Name [71ECB6A7].mkv.00N"`,
// whose headers give the release name.
fn example() -> (Vec<Post>, Vec<String>) {
    let subjects = [
        r#"Show Name [71ECB6A7].mkv.001" yEnc (1/50)"#,
        r#"Show Name [71ECB6A7].mkv.002" yEnc (1/50)"#,
        r#"Show Name [71ECB6A7].mkv.003" yEnc (1/50)"#,
    ];
    let posted = posted_names(&subjects);
    assert_eq!(
        posted,
        [
            "[71ECB6A7].mkv.001",
            "[71ECB6A7].mkv.002",
            "[71ECB6A7].mkv.003"
        ]
    );
    let posts = joined()
        .chunks(PIECE)
        .zip(&posted)
        .map(|(piece, name)| Post::plain(name, piece.to_vec()))
        .collect();
    let headers = (1..=3).map(|n| format!("{TRUE_NAME}.{n:03}")).collect();
    (posts, headers)
}

fn headed(posts: Vec<Post>, headers: &[String]) -> Scenario {
    headers
        .iter()
        .zip(0u32..)
        .fold(Scenario::new(posts), |scenario, (name, file)| {
            scenario.header(file, name)
        })
}

fn delivered(outcome: &Outcome) -> BTreeMap<String, &Vec<u8>> {
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    outcome
        .files
        .iter()
        .map(|(path, bytes)| {
            (
                path.strip_prefix(&format!("{JOB}/")).unwrap().to_string(),
                bytes,
            )
        })
        .collect()
}

fn assert_joined_under_header_names(outcome: &Outcome) {
    let files = delivered(outcome);
    assert_eq!(
        files.keys().collect::<Vec<_>>(),
        vec![TRUE_NAME],
        "delivered names"
    );
    assert!(*files[TRUE_NAME] == joined(), "joined bytes");
}

#[tokio::test]
async fn pieces_take_their_header_names_and_join_under_them() {
    let (posts, headers) = example();
    let outcome = run(&headed(posts, &headers)).await;
    assert_joined_under_header_names(&outcome);
}

// The split set the posted names founded before any byte came is founded
// again under the header names whichever piece is named first.
#[tokio::test]
async fn pieces_named_last_first_still_join_under_their_header_names() {
    let (posts, headers) = example();
    let outcome = run(&headed(posts, &headers).backwards().reversed(&[0, 1, 2])).await;
    assert_joined_under_header_names(&outcome);
}

// Only some pieces renamed: the posted set and the header-named set stand
// apart: the named pieces join, the third is delivered as it came.
#[tokio::test]
async fn a_piece_whose_header_is_obfuscated_stays_in_the_set_its_posted_name_founded() {
    let (posts, mut headers) = example();
    headers[2] = HEX_NAME.to_string();
    let outcome = run(&headed(posts, &headers)).await;
    // The joined file is whole; the piece left alone is a split part whose
    // set has no first piece, so the job is Partial.
    assert_eq!(
        outcome.status,
        Some(JobStatus::Partial),
        "{:?}",
        outcome.failure
    );
    let body = joined();
    let at = |name: &str| &outcome.files[&format!("{JOB}/{name}")];
    assert_eq!(
        outcome.files.keys().collect::<Vec<_>>(),
        vec![
            &format!("{JOB}/{TRUE_NAME}"),
            &format!("{JOB}/[71ECB6A7].mkv.003")
        ],
        "delivered names"
    );
    assert!(*at(TRUE_NAME) == body[..2 * PIECE], "the two named pieces");
    assert!(
        *at("[71ECB6A7].mkv.003") == body[2 * PIECE..],
        "the piece left alone"
    );
}

// The ruling's counter-example: an obfuscated header name is ignored and
// the subject's name kept, while the control takes its header's.
#[tokio::test]
async fn an_obfuscated_header_name_is_ignored() {
    let posted = posted_names(&[r#"[1/2] - "Show.Name.mkv" yEnc (1/50)"#]);
    let body = payload(5 * ARTICLE + 3, 119);
    let posts = vec![Post::plain(&posted[0], body.clone()), control()];
    let outcome = run(&Scenario::new(posts)
        .header(0, HEX_NAME)
        .header(1, CONTROL_HEADER))
    .await;
    let files = delivered(&outcome);
    assert_eq!(
        files.keys().collect::<Vec<_>>(),
        vec![CONTROL_HEADER, "Show.Name.mkv"],
        "delivered names"
    );
    assert!(*files["Show.Name.mkv"] == body);
    assert!(!outcome.graph().names.contains(HEX_NAME));
}

// Where a PAR2 set describes the file, the name the set gives it stands,
// whether the set is read before the file's first article or after it, and
// whether the set knows it by its posted name or by its content.
#[tokio::test]
async fn the_name_a_par2_set_gives_wins_over_the_header_name() {
    let body = payload(5 * ARTICLE + 11, 121);
    for posted in ["a1b2c3d4e5", "lantern.bin"] {
        for par2_first in [false, true] {
            let data = vec![Post::plain(posted, body.clone()), control()];
            let set = par2_set("lantern", &[("lantern.bin", &body)], 2);
            let (posts, data_at) = if par2_first {
                (set.into_iter().chain(data).collect::<Vec<_>>(), 2)
            } else {
                (data.into_iter().chain(set).collect::<Vec<_>>(), 0)
            };
            let scenario = Scenario::new(posts)
                .header(data_at, "Clear.Header.Name.bin")
                .header(data_at + 1, CONTROL_HEADER)
                .damaged(data_at, 1);
            let outcome = run(&scenario).await;
            let files = delivered(&outcome);
            assert_eq!(
                files.keys().collect::<Vec<_>>(),
                vec![CONTROL_HEADER, "lantern.bin"],
                "posted {posted}, set first {par2_first}"
            );
            assert!(*files["lantern.bin"] == body, "repaired under its name");
        }
    }
}

// A posted PAR2 name is kept whatever the header says.
#[tokio::test]
async fn a_posted_par2_name_is_kept() {
    let body = payload(5 * ARTICLE + 7, 123);
    let mut posts = vec![Post::plain("lantern.bin", body.clone()), control()];
    posts.extend(par2_set("lantern", &[("lantern.bin", &body)], 2));
    let outcome = run(&Scenario::new(posts)
        .header(1, CONTROL_HEADER)
        .header(2, "Other.Clear.Name.par2")
        .header(3, "Other.Clear.Name.vol00+01.par2")
        .damaged(0, 2))
    .await;
    let files = delivered(&outcome);
    assert_eq!(
        files.keys().collect::<Vec<_>>(),
        vec![CONTROL_HEADER, "lantern.bin"]
    );
    let names = &outcome.graph().names;
    assert!(names.contains("lantern.par2"), "{names:?}");
    assert!(
        !names.iter().any(|name| name.starts_with("Other.")),
        "{names:?}"
    );
}

// The first article taken in decides: a header that names the file
// otherwise in a later article renames nothing, its bytes being in.
#[tokio::test]
async fn a_header_name_after_the_first_article_renames_nothing() {
    let body = payload(5 * ARTICLE + 5, 125);
    let posts = vec![Post::plain(CONTROL_POSTED, body.clone())];
    let scenario =
        Scenario::new(posts)
            .header(0, CONTROL_HEADER)
            .header_from(0, 1, "Later.Header.Name.bin");
    let outcome = run(&scenario).await;
    let files = delivered(&outcome);
    assert_eq!(files.keys().collect::<Vec<_>>(), vec![CONTROL_HEADER]);
    assert!(*files[CONTROL_HEADER] == body);
    assert!(!outcome.graph().names.contains("Later.Header.Name.bin"));
}

// A uuencoded file is named by its `begin` line as a yEnc one is by its
// header.
#[tokio::test]
async fn a_uuencoded_file_takes_its_begin_name() {
    let posts = vec![control()];
    let outcome = run(&Scenario::new(posts).uuencoded().header(0, CONTROL_HEADER)).await;
    let files = delivered(&outcome);
    assert_eq!(files.keys().collect::<Vec<_>>(), vec![CONTROL_HEADER]);
}

// Restarted after every article, the job finishes as the run that never
// restarted does, under the header names.
#[tokio::test]
async fn header_names_survive_a_restart_after_every_article() {
    let (posts, headers) = example();
    let whole = restart_row(headed(posts, &headers)).await;
    assert_joined_under_header_names(&whole);
}

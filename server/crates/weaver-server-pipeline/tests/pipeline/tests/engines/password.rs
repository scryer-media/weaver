// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Password trials over RAR: a wrong candidate offered before the right one,
// one member, every byte verified. Under a candidate the set cannot prove,
// a checksum failure on verified bytes is the candidate's, not damage, and
// the next candidate opens the member. Every candidate wrong fails the job
// for the password. Each row runs under one engine.

use super::archive_scenario_fixtures::password::{
    MEMBER, SECRET, WRONG, assert_body, assert_whole, body, wrong_then_right,
};
use super::harness::{ARTICLE, Outcome, Scenario, par2_set, run, run_holding};
use super::rar::{posts, with_par2};
use super::restart::{Down, run_restarting_at};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::protected_sealed_rar4_store_set;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};

const FIXTURE_SECRET: &str = "testpass123";

const FIXTURE_MEMBER: &str = "compressible.txt";

fn fixture(name: &str) -> Vec<(String, Vec<u8>)> {
    let path = format!(
        "{}/../weaver-server-core/tests/fixtures/extraction_profiles/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    vec![(name.to_string(), std::fs::read(path).unwrap())]
}

fn assert_fixture_member(outcome: &Outcome, archive: &str) {
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let expected = &oracle["archives"][archive][FIXTURE_MEMBER];
    assert_whole(
        outcome,
        FIXTURE_MEMBER,
        expected["sha256"].as_str().unwrap(),
        expected["size"].as_u64().unwrap() as usize,
    );
}

fn rar4_stored_p() -> Vec<(String, Vec<u8>)> {
    encrypted_rar4_store_set(MEMBER, &body(), 2, SECRET, Some([7; 8]))
}

fn rar4_stored_hp() -> Vec<(String, Vec<u8>)> {
    protected_sealed_rar4_store_set(MEMBER, &body(), 2, 1, SECRET)
}

fn rar5_stored_p() -> Vec<(String, Vec<u8>)> {
    encrypted_store_set(MEMBER, &body(), 2, SECRET, Some(SECRET), false)
}

fn rar5_stored_hp() -> Vec<(String, Vec<u8>)> {
    header_encrypted_store_set(MEMBER, &body(), 2, SECRET, HeaderCheck::For(SECRET))
}

// 1. RAR4 stored, `-p`: no password check, so the wrong key is only seen
// as the member's checksum failing.
#[tokio::test]
async fn pw_rar4_stored_p_wrong_then_right_delivers() {
    let graph = run(&wrong_then_right(posts(rar4_stored_p()), SECRET)).await;
    assert_body(&graph, &body());
}

// 3. RAR4 compressed, `-p`.
#[tokio::test]
async fn pw_rar4_compressed_p_wrong_then_right_delivers() {
    let scenario = wrong_then_right(posts(fixture("rar4_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario).await, "rar4_enc_lz.rar");
}

// 4. RAR5 stored `-p`, stored `-hp`, compressed `-p`.
#[tokio::test]
async fn pw_rar5_stored_p_wrong_then_right_delivers() {
    let graph = run(&wrong_then_right(posts(rar5_stored_p()), SECRET)).await;
    assert_body(&graph, &body());
}

#[tokio::test]
async fn pw_rar5_stored_hp_wrong_then_right_delivers() {
    let graph = run(&wrong_then_right(posts(rar5_stored_hp()), SECRET)).await;
    assert_body(&graph, &body());
}

// The history row's summary of that job: the set needed a password, the one
// that opened it came from the settings list, and neither password tried is
// in the stored text.
#[tokio::test]
async fn pw_summary_names_the_settings_list_and_holds_no_password() {
    let graph = run(&wrong_then_right(posts(rar5_stored_p()), SECRET)).await;
    assert_body(&graph, &body());
    let text = graph.download_summary.clone().expect("a stored summary");
    assert!(!text.contains(SECRET) && !text.contains(WRONG), "{text}");
    // Nor in the events the run sent, which the timeline stores, or the
    // history row's metadata, which keeps the password only sealed.
    let elsewhere = format!("{:?} {:?}", graph.events, graph.history_metadata);
    assert!(!graph.events.is_empty());
    assert!(
        !elsewhere.contains(SECRET) && !elsewhere.contains(WRONG),
        "{elsewhere}"
    );
    let summary = super::download_summary::summary(&graph);
    let [node] = &summary.nodes[..] else {
        panic!("{summary:?}");
    };
    assert!(node.encrypted && node.password_needed, "{node:?}");
    assert_eq!(
        node.password_source.as_deref(),
        Some("settings"),
        "{node:?}"
    );
}

#[tokio::test]
async fn pw_rar5_compressed_p_wrong_then_right_delivers() {
    let scenario = wrong_then_right(posts(fixture("rar5_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario).await, "rar5_enc_lz.rar");
}

// 5. Every candidate wrong, RAR4 stored `-p`: the job fails for the
// password with nothing delivered.
fn assert_fails_for_the_password(outcome: &Outcome) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.failure_kind,
        Some(HistoryFailureKind::Password),
        "{:?}",
        outcome.failure
    );
    assert!(outcome.files.is_empty(), "{:?}", outcome.files.keys());
    // A password withheld is the extraction's failure, never a deletion
    // for health.
    assert!(!outcome.graph().health);
}

fn every_candidate_wrong() -> Scenario {
    Scenario::new(posts(rar4_stored_p()))
        .global_password(WRONG)
        .global_password("quill-ember-also-wrong")
}

#[tokio::test]
async fn pw_rar4_stored_p_every_candidate_wrong_fails_for_the_password() {
    assert_fails_for_the_password(&run(&every_candidate_wrong()).await);
}

// 5b. A header-encrypted RAR5 set given only wrong passwords: the job fails
// for the password, and its reason says a password was given and none of
// them opens the set, never that none was given. The same set given no
// password at all says none was given.
#[tokio::test]
async fn pw_rar5_stored_hp_every_candidate_wrong_says_no_password_given_opens_it() {
    let volumes = rar5_stored_hp();
    let names: Vec<String> = volumes.iter().map(|(name, _)| name.clone()).collect();
    let given = run(&Scenario::new(posts(volumes.clone()))
        .global_password(WRONG)
        .global_password("quill-ember-also-wrong"))
    .await;
    assert_fails_for_the_password(&given);
    assert_eq!(
        given.failure,
        Some(format!(
            "WEAVER_PASSWORD_REQUIRED: {} needs a password and no password given opens it",
            names.join(", ")
        ))
    );
    let none = run(&Scenario::new(posts(volumes))).await;
    assert_fails_for_the_password(&none);
    assert_eq!(
        none.failure,
        Some(format!(
            "WEAVER_PASSWORD_REQUIRED: {} needs a password and none was given",
            names.join(", ")
        ))
    );
}

// 6. RAR4 stored `-p` beside PAR2, one article of the member's cipher
// damaged: the bytes are not verified when the wrong candidate's checksum
// fails, so the failure is damage first; the set repairs it and the right
// candidate opens the member.
fn damaged_beside_par2() -> Scenario {
    wrong_then_right(with_par2(rar4_stored_p()), SECRET).damaged(0, 2)
}

#[tokio::test]
async fn pw_rar4_stored_p_damaged_beside_par2_repairs_then_the_right_candidate_delivers() {
    assert_body(&run(&damaged_beside_par2()).await, &body());
}

// 7. RAR4 stored `-p` in four volumes of one article each, the job
// submitted with a wrong password and shut down after the second volume:
// the bytes written before the restart are no longer in the restored leaf's
// fold, and the member's stated checksum is checked from the file. The job
// fails as the uninterrupted one does, publishing nothing.
#[tokio::test]
async fn pw_rar4_stored_p_wrong_password_restarted_after_the_second_volume_fails() {
    let volumes = encrypted_rar4_store_set(MEMBER, &body(), 4, SECRET, Some([7; 8]));
    let scenario = Scenario::new(posts(volumes))
        .framed(4 * ARTICLE)
        .password(WRONG);
    let restarted =
        run_restarting_at(&scenario, &mut |_| {}, Down::Shutdown, &|nth| nth == 2).await;
    assert!(restarted.restarts > 0, "the run restarted");
    assert!(
        matches!(restarted.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        restarted.status
    );
    assert!(restarted.files.is_empty(), "{:?}", restarted.files.keys());
}

// 2. RAR4 stored, `-hp`.
#[tokio::test]
async fn pw_rar4_stored_hp_wrong_then_right_delivers() {
    let graph = run(&wrong_then_right(posts(rar4_stored_hp()), SECRET)).await;
    assert_body(&graph, &body());
}

// 8. RAR4 and RAR5 compressed `-p` under the engine, the right candidate
// first and a wrong one after, a byte of the enciphered data wrong as
// posted and the PAR2 volume held until nothing moves: the run reads the
// damage under the right candidate, and the wrong one fails as well. The
// repair lands behind what the run read, so the job tries its candidates
// again from the first, which opens the member.
#[tokio::test]
async fn pw_compressed_p_right_first_damaged_then_repaired_delivers() {
    for name in ["rar4_enc_lz.rar", "rar5_enc_lz.rar"] {
        let written = fixture(name);
        let mut posted = posts(written.clone());
        let at = posted[0].bytes.len() / 2;
        posted[0].bytes[at] ^= 0x5a;
        posted.extend(par2_set(
            "moonlit",
            &[(written[0].0.as_str(), written[0].1.as_slice())],
            4,
        ));
        let scenario = Scenario::new(posted)
            .global_password(FIXTURE_SECRET)
            .global_password(WRONG);
        let held = run_holding(&scenario, 2).await;
        assert_eq!(held.status, Some(JobStatus::Complete), "{name}");
        assert!(
            held.files
                .keys()
                .any(|file| file.ends_with(&format!("/{FIXTURE_MEMBER}"))),
            "{name}: {:?}",
            held.files.keys()
        );
    }
}

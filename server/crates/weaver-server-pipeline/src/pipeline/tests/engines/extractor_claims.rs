// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Damage only an extractor sees, and damage the articles show, in zip and 7z
// containers, with and without a recovery set beside them; and an
// extractor's claim of damage heard before the recovery set beside it is in.
// A set that can rebuild the bytes ends the job Complete, every member as
// written. Bytes a set describes as they are are not rebuilt.

use std::collections::BTreeMap;

use sevenz_turbo::encoder_options::Lzma2Options;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, GraphView, JOB, Outcome, Post, Scenario, admit, collect, observe, par2_set, par3_set,
    run,
};
use super::rar_rev::{lz_four, rar3_rev};
use super::refused_then_repaired::{drive_to_finish, serve_posts};
use super::{sevenz, sfv, zip};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::{hear_graph_mail, job_status_for_assert, retire_pipeline_database};

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// The members, every byte as written, and nothing else.
fn assert_members(
    status: &Option<JobStatus>,
    files: &BTreeMap<String, Vec<u8>>,
    expected: &[(&str, Vec<u8>)],
    case: &str,
) {
    assert_eq!(
        *status,
        Some(JobStatus::Complete),
        "{case}: {:?}",
        files.keys()
    );
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>(),
        "{case}: delivered names",
    );
    for (name, bytes) in expected {
        assert!(files[&delivered(name)] == *bytes, "{case}: {name} bytes");
    }
}

fn said(case: &str, outcome: &Outcome) {
    eprintln!(
        "CASE {case}: status {:?} kind {:?} failure {:?} files {:?} outcomes {:?} reads {} refetched {:?}",
        outcome.status,
        outcome.failure_kind,
        outcome.failure,
        outcome.files.keys().collect::<Vec<_>>(),
        outcome.graph().outcomes,
        outcome.reads,
        outcome.refetched()
    );
}

// A container, and where in it a byte of the first member's data sits, one
// article past the article its data starts in: inside the packed stream,
// clear of every header.
struct Container {
    name: &'static str,
    bytes: Vec<u8>,
    spoiled_at: usize,
    members: Vec<(&'static str, Vec<u8>)>,
}

fn deflate_zip() -> Container {
    let bytes = zip::deflated();
    // ember.mkv's local header: 30 bytes and its 9-byte name.
    Container {
        name: "lantern.zip",
        spoiled_at: 30 + 9 + ARTICLE + 3,
        bytes,
        members: zip::members(),
    }
}

fn stored_zip() -> Container {
    let bytes = zip::archive(zip::stored());
    Container {
        name: "lantern.zip",
        spoiled_at: 30 + 9 + ARTICLE + 3,
        bytes,
        members: zip::members(),
    }
}

fn lzma2_7z() -> Container {
    let bytes = sevenz::container(Lzma2Options::from_level(1).into(), None);
    // The packed streams start after the 32-byte signature header.
    assert!(32 + ARTICLE + 3 < sevenz::packed_end(&bytes));
    Container {
        name: "lantern.7z",
        spoiled_at: 32 + ARTICLE + 3,
        bytes,
        members: sevenz::members(),
    }
}

fn stored_7z() -> Container {
    let bytes = sevenz::container(EncoderConfiguration::new(EncoderMethod::COPY), None);
    Container {
        name: "lantern.7z",
        spoiled_at: 32 + ARTICLE + 3,
        bytes,
        members: sevenz::members(),
    }
}

// What a case posts, and what reaches the extractor.
#[derive(Clone, Copy, Debug)]
enum Damage {
    // One article of the first member's data never arrives.
    Lost,
    // One article of it arrives with its yEnc checksum failing.
    Known,
    // One byte of it was wrong before the post was cut into articles: every
    // article checks, and only the extractor finds the damage.
    Unseen,
}

#[derive(Clone, Copy, Debug)]
enum Recovery {
    None,
    Par2,
}

// The archive posted whole, then the set beside it, made over the archive
// as it was written.
fn single(container: &Container, damage: Damage, recovery: Recovery) -> Scenario {
    let mut posted = container.bytes.clone();
    if let Damage::Unseen = damage {
        posted[container.spoiled_at] ^= 0x5a;
    }
    let mut posts = vec![Post::plain(container.name, posted)];
    if let Recovery::Par2 = recovery {
        posts.extend(par2_set(
            "lantern",
            &[(container.name, &container.bytes)],
            4,
        ));
    }
    let article = (container.spoiled_at / ARTICLE) as u32;
    let scenario = Scenario::new(posts);
    match damage {
        Damage::Lost => scenario.lost(0, article),
        Damage::Known => scenario.damaged(0, article),
        Damage::Unseen => scenario,
    }
}

// The container split into three `.7z.001` onward, the middle volume's
// second article the damaged one, the set beside made over the volumes as
// written.
fn split_7z(damage: Damage, recovery: Recovery) -> Scenario {
    let archive = sevenz::container(Lzma2Options::from_level(1).into(), None);
    let parts = sevenz::split(&archive, 3);
    // The middle volume lies inside the packed streams.
    let middle = parts[0].bytes.len();
    assert!(middle + parts[1].bytes.len() < sevenz::packed_end(&archive));
    let mut posts: Vec<Post> = parts
        .iter()
        .map(|post| Post::plain(&post.name, post.bytes.clone()))
        .collect();
    if let Damage::Unseen = damage {
        posts[1].bytes[ARTICLE + 3] ^= 0x5a;
    }
    if let Recovery::Par2 = recovery {
        let files: Vec<(&str, &[u8])> = parts
            .iter()
            .map(|post| (post.name.as_str(), post.bytes.as_slice()))
            .collect();
        posts.extend(par2_set("lantern", &files, 4));
    }
    let scenario = Scenario::new(posts);
    match damage {
        Damage::Lost => scenario.lost(1, 1),
        Damage::Known => scenario.damaged(1, 1),
        Damage::Unseen => scenario,
    }
}

// Item 1: beside a PAR2 set that can rebuild the damaged article, every
// container ends Complete with its members as written, whether the article
// never came, came failing its checksum, or came checking with wrong bytes.
#[tokio::test]
async fn a_zip_or_7z_beside_par2_completes_whatever_damaged_its_article() {
    let containers = [
        ("deflate zip", deflate_zip()),
        ("stored zip", stored_zip()),
        ("lzma2 7z", lzma2_7z()),
        ("stored 7z", stored_7z()),
    ];
    for (label, container) in &containers {
        for damage in [Damage::Lost, Damage::Known, Damage::Unseen] {
            let case = format!("{label} {damage:?} par2");
            let outcome = run(&single(container, damage, Recovery::Par2)).await;
            said(&case, &outcome);
            assert_members(&outcome.status, &outcome.files, &container.members, &case);
            assert!(outcome.refetched().is_empty(), "{case}");
        }
    }
    for damage in [Damage::Lost, Damage::Known, Damage::Unseen] {
        let case = format!("split lzma2 7z {damage:?} par2");
        let outcome = run(&split_7z(damage, Recovery::Par2)).await;
        said(&case, &outcome);
        assert_members(&outcome.status, &outcome.files, &sevenz::members(), &case);
        assert!(outcome.refetched().is_empty(), "{case}");
    }
}

// Item 1, with no recovery: what each post ends as. A member the extractor
// closed whole before the damage is delivered, and the job is Partial; with
// none, it fails.
#[tokio::test]
async fn a_zip_or_7z_with_no_recovery_ends_as_its_whole_members_say() {
    let containers = [
        ("deflate zip", deflate_zip()),
        ("stored zip", stored_zip()),
        ("lzma2 7z", lzma2_7z()),
        ("stored 7z", stored_7z()),
    ];
    for (label, container) in &containers {
        for damage in [Damage::Lost, Damage::Known, Damage::Unseen] {
            let case = format!("{label} {damage:?} none");
            let outcome = run(&single(container, damage, Recovery::None)).await;
            said(&case, &outcome);
            assert_ne!(outcome.status, Some(JobStatus::Complete), "{case}");
            assert!(outcome.refetched().is_empty(), "{case}");
        }
    }
    for damage in [Damage::Lost, Damage::Known, Damage::Unseen] {
        let case = format!("split lzma2 7z {damage:?} none");
        let outcome = run(&split_7z(damage, Recovery::None)).await;
        said(&case, &outcome);
        assert_ne!(outcome.status, Some(JobStatus::Complete), "{case}");
    }
}

// Item 2: the archive's articles served first, every one checking, and the
// job's runs heard until none is out, so the extractor has said what it
// found before any recovery set is in; then the set's articles, and the job
// driven to its end. What the job ended as, what it delivered, and the
// set's outcome and the slices its repair read back, as the job reported
// them once the set had ruled.
struct Heard {
    status: Option<JobStatus>,
    files: BTreeMap<String, Vec<u8>>,
    view: GraphView,
    reads: u64,
}

async fn until_no_run(pipeline: &mut Pipeline) {
    while pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|job| job.runs().live())
    {
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
    }
}

async fn archive_first(scenario: &Scenario, archive: u32) -> Heard {
    let (_temp, mut pipeline, complete_dir) = admit(scenario).await;
    serve_posts(&mut pipeline, scenario, 0..archive).await;
    until_no_run(&mut pipeline).await;
    serve_posts(
        &mut pipeline,
        scenario,
        archive..scenario.posts.len() as u32,
    )
    .await;
    hear_graph_mail(&mut pipeline, JOB).await;
    let mut view = GraphView::default();
    let mut reads = 0;
    observe(&pipeline, &mut view, &mut reads);
    drive_to_finish(&mut pipeline, scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    Heard {
        status,
        files,
        view,
        reads,
    }
}

fn heard(case: &str, heard: &Heard) {
    eprintln!(
        "HEARD {case}: status {:?} files {:?} outcomes {:?} reads {}",
        heard.status,
        heard.files.keys().collect::<Vec<_>>(),
        heard.view.outcomes,
        heard.reads
    );
}

// Which recovery set a case posts beside the container.
#[derive(Clone, Copy, Debug)]
enum Set {
    Par2,
    Par3,
    Sfv,
}

fn beside(set: Set, name: &str, bytes: &[u8]) -> Vec<Post> {
    match set {
        Set::Par2 => par2_set("lantern", &[(name, bytes)], 4),
        Set::Par3 => par3_set("lantern", &[(name, bytes)], 4),
        Set::Sfv => vec![sfv::listing("lantern.sfv", &[(name, sfv::crc(bytes))])],
    }
}

// Item 2, case (a): the container was spoiled after the set was made. The
// extractor claims damage before the set is in; the set then rules the
// spoiled bytes and, where it can, rebuilds them, and the job completes. An
// SFV listing finds the mismatch and rebuilds nothing.
#[tokio::test]
async fn an_extractor_claim_heard_before_its_set_is_in_is_repaired_by_the_set() {
    for (label, container) in [("deflate zip", deflate_zip()), ("lzma2 7z", lzma2_7z())] {
        for set in [Set::Par2, Set::Par3, Set::Sfv] {
            let case = format!("{label} spoiled {set:?}");
            let mut posted = container.bytes.clone();
            posted[container.spoiled_at] ^= 0x5a;
            let mut posts = vec![Post::plain(container.name, posted)];
            posts.extend(beside(set, container.name, &container.bytes));
            let outcome = archive_first(&Scenario::new(posts), 1).await;
            heard(&case, &outcome);
            match set {
                Set::Sfv => assert_ne!(outcome.status, Some(JobStatus::Complete), "{case}"),
                Set::Par2 | Set::Par3 => {
                    assert_members(&outcome.status, &outcome.files, &container.members, &case)
                }
            }
        }
    }
}

// Item 2, case (b): the container was broken before the set was made, so
// the set describes the bytes as posted. The extractor claims damage before
// the set is in; the set then rules the bytes clean. Nothing is rebuilt,
// and the job fails, as it does with no set at all.
#[tokio::test]
async fn an_extractor_claim_on_bytes_its_set_vouches_for_rebuilds_nothing_and_fails() {
    for (label, container) in [("deflate zip", deflate_zip()), ("lzma2 7z", lzma2_7z())] {
        for set in [Set::Par2, Set::Par3, Set::Sfv] {
            let case = format!("{label} broken {set:?}");
            let mut broken = container.bytes.clone();
            broken[container.spoiled_at] ^= 0x5a;
            let mut posts = vec![Post::plain(container.name, broken.clone())];
            posts.extend(beside(set, container.name, &broken));
            let outcome = archive_first(&Scenario::new(posts), 1).await;
            heard(&case, &outcome);
            assert!(
                matches!(
                    outcome.status,
                    Some(JobStatus::Failed { .. } | JobStatus::Partial)
                ),
                "{case}: {:?}",
                outcome.status
            );
            // The set may read the doubted bytes back to rule on them
            // (`reads`); it rebuilds none of them.
            assert!(
                !outcome
                    .view
                    .outcomes
                    .iter()
                    .any(|outcome| matches!(outcome, Some(RepairOutcome::Repaired { .. }))),
                "{case}: {:?}",
                outcome.view.outcomes
            );
        }
    }
}

// The compressed four-volume RAR4 set, a byte of its second volume's packed
// data flipped (`flip` before the set is made, else after), beside a PAR2
// set or the RAR3 recovery volume, the volumes served first.
fn lz_four_beside(set: &str, flip_first: bool) -> Scenario {
    const SPOILED: usize = 4_000;
    let original = lz_four();
    let mut posted = original.clone();
    posted[1].1[SPOILED] ^= 0x5a;
    let described = if flip_first { &posted } else { &original };
    let bytes: Vec<&[u8]> = described
        .iter()
        .map(|(_, bytes)| bytes.as_slice())
        .collect();
    let mut posts: Vec<Post> = posted
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    match set {
        "par2" => {
            let files: Vec<(&str, &[u8])> = described
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
                .collect();
            posts.extend(par2_set("silver.horizon", &files, 4));
        }
        _ => posts.push(Post::plain("silver.horizon.part1.rev", rar3_rev(&bytes))),
    }
    let mut scenario = Scenario::new(posts);
    scenario.article = 4096;
    scenario
}

// Item 2 on the RAR engine: the set's volumes spoiled after the set was
// made are rebuilt and the member delivered; broken before, nothing is
// rebuilt and the job fails.
#[tokio::test]
async fn a_rar_engine_claim_heard_before_its_set_is_in() {
    for set in ["par2", "rev"] {
        let case = format!("lz_four spoiled {set}");
        let outcome = archive_first(&lz_four_beside(set, false), 4).await;
        heard(&case, &outcome);
        assert_eq!(outcome.status, Some(JobStatus::Complete), "{case}");
        let case = format!("lz_four broken {set}");
        let outcome = archive_first(&lz_four_beside(set, true), 4).await;
        heard(&case, &outcome);
        assert!(
            matches!(
                outcome.status,
                Some(JobStatus::Failed { .. } | JobStatus::Partial)
            ),
            "{case}: {:?}",
            outcome.status
        );
        assert!(
            !outcome
                .view
                .outcomes
                .iter()
                .any(|outcome| matches!(outcome, Some(RepairOutcome::Repaired { .. }))),
            "{case}: {:?}",
            outcome.view.outcomes
        );
    }
}

// Item 5: a RAR4 file header whose CRC16 is wrong, its data good. Where a
// volume's file header starts: past the signature and the main header.
fn file_header(volume: &[u8]) -> usize {
    let main = 7;
    main + u16::from_le_bytes([volume[main + 5], volume[main + 6]]) as usize
}

fn header_spoiled(mut volume: Vec<u8>) -> Vec<u8> {
    let at = file_header(&volume);
    assert_eq!(volume[at + 2], 0x74, "a file header");
    volume[at] ^= 0xff;
    volume
}

// One RAR4 set as `rar4_sets` writes it.
type Rar4Set = (&'static str, Vec<(String, Vec<u8>)>, Vec<u8>, &'static str);

// A stored two-volume RAR4 set of one member, and the compressed four-volume
// set, as written.
fn rar4_sets() -> Vec<Rar4Set> {
    use crate::pipeline::tests::{single_member_rar4_old_numbering_set, with_rar4_header_crcs};
    let body = super::harness::payload(9 * ARTICLE, 71);
    let stored: Vec<(String, Vec<u8>)> =
        single_member_rar4_old_numbering_set("silver.horizon", "beacon.bin", &body, 2)
            .into_iter()
            .map(|(name, bytes)| (name, with_rar4_header_crcs(bytes)))
            .collect();
    vec![
        ("stored", stored, body, "beacon.bin"),
        ("lz_four", lz_four(), Vec::new(), "compressible.txt"),
    ]
}

fn report(case: &str, outcome: &Outcome) {
    said(case, outcome);
    let events: Vec<String> = outcome
        .events
        .iter()
        .map(|event| format!("{event:?}"))
        .filter(|text| !text.starts_with("Segment") && !text.contains("Progress"))
        .collect();
    eprintln!("EVENTS {case}: {events:?}");
}

#[tokio::test]
async fn a_rar4_file_header_with_a_wrong_crc_and_good_data() {
    for (label, volumes, body, member) in rar4_sets() {
        // The first volume's file header CRC wrong, as posted and as any set
        // describes it: no recovery.
        let mut posted = volumes.clone();
        posted[0].1 = header_spoiled(posted[0].1.clone());
        let posts: Vec<Post> = posted
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        let mut scenario = Scenario::new(posts);
        scenario.article = 4096;
        let case = format!("{label} header crc, no recovery");
        let outcome = run(&scenario).await;
        report(&case, &outcome);
        if !body.is_empty() {
            assert_eq!(outcome.files.get(&delivered(member)), Some(&body), "{case}");
        }
        assert_eq!(outcome.status, Some(JobStatus::Complete), "{case}");

        // The header CRC wrong and a byte of the volume's data wrong, after
        // a PAR2 set was made over the set as written.
        let mut posted = volumes.clone();
        posted[0].1 = header_spoiled(posted[0].1.clone());
        let data = file_header(&posted[0].1) + 200;
        posted[0].1[data] ^= 0x5a;
        let mut posts: Vec<Post> = posted
            .iter()
            .map(|(name, bytes)| Post::plain(name, bytes.clone()))
            .collect();
        let files: Vec<(&str, &[u8])> = volumes
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        posts.extend(par2_set("silver.horizon", &files, 4));
        let mut scenario = Scenario::new(posts);
        scenario.article = 4096;
        let case = format!("{label} header crc and data, par2");
        let outcome = run(&scenario).await;
        report(&case, &outcome);
        assert_eq!(outcome.status, Some(JobStatus::Complete), "{case}");
        if !body.is_empty() {
            assert_eq!(outcome.files.get(&delivered(member)), Some(&body), "{case}");
        }
    }
}

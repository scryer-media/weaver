// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// 7z containers: stored whole, split and posted header
// first, compressed, enciphered with and without the job's password, and
// damaged under a PAR2 set.

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set,
    par2_volumes_sliced, payload, run,
};
use super::sevenz_fixtures::store::{
    SECRET, assert_members, packed_end, sealed_header, stored,
};
use super::sevenz_fixtures::{container, delivered, members, split};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};
use sevenz_turbo::encoder_options::{AesEncoderOptions, Lzma2Options};
use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod, Password};
use std::io::Cursor;
use weaver_graph::recovery::RepairOutcome;

#[tokio::test]
async fn a_stored_7z_delivers_its_members() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", stored(None))]);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn a_stored_7z_split_into_three_volumes_delivers_its_members() {
    let scenario = Scenario::new(split(&stored(None), 3));
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The same split numbered from zero, `.7z.000` onward, is one set and
// delivers the same members.
#[tokio::test]
async fn a_stored_7z_split_numbered_from_zero_delivers_its_members() {
    let posts = split(&stored(None), 3)
        .into_iter()
        .zip(0..)
        .map(|(post, number)| Post::plain(&format!("lantern.7z.{number:03}"), post.bytes))
        .collect();
    let graph = run(&Scenario::new(posts)).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// `lantern.7z.001` and `.002`: the numbered names found the joiner, which
// writes both pieces into one file, and the 7z store founds beneath that
// file and reads it as the one file it is. The members come out of it whole.
#[tokio::test]
async fn two_numbered_pieces_join_into_the_one_file_the_7z_store_reads() {
    let graph = run(&Scenario::new(split(&stored(None), 2))).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), vec!["sevenz_store", "split"]);
}

// A PAR2 set over the pieces and one article of the second piece damaged:
// the set repairs that piece, the pieces join into the file, and the 7z
// store beneath delivers every member whole.
#[tokio::test]
async fn a_piece_repaired_by_par2_joins_and_delivers_every_member() {
    let parts = split(&stored(None), 3);
    let files: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let recovery = par2_set("lantern", &files, 4);
    let mut posts = parts;
    posts.extend(recovery);
    let graph = run(&Scenario::new(posts).damaged(1, 0)).await;
    assert_members(&graph);
    assert!(
        matches!(
            graph.graph().outcomes.as_slice(),
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
    assert_eq!(kinds(&graph), vec!["par2", "sevenz_store", "split"]);
}

// A split set's pieces under hash names that number nothing, the second
// posted first, and the PAR2 set that names them last. Only the first
// piece's signature says what it is; the others have no signature, so they
// wait unowned until the PAR2 set names them, and the store founded on the
// first claims each by the name it is given. Nothing is fetched twice.
fn hashed(volumes: &[Post]) -> Vec<Post> {
    let mut posts: Vec<Post> = volumes
        .iter()
        .zip(0u32..)
        .map(|(post, index)| {
            Post::plain(&format!("{:032x}", 0x5eed_0000 + index), post.bytes.clone())
        })
        .collect();
    posts.swap(0, 1);
    posts
}

fn hashed_with_par2_last() -> Vec<Post> {
    let volumes = split(&stored(None), 3);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let mut posts = hashed(&volumes);
    posts.extend(par2_set("lantern", &files, 2));
    posts
}

// The members, and the PAR2 set's ruling that every piece was whole under
// the names it gave.
fn assert_named_by_par2(graph: &Outcome) {
    let view = graph.graph();
    assert!(view.delivery.withheld.is_empty(), "{:?}", view.delivery);
    assert_eq!(view.outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A stored split set, a hundred articles from its first volume's head on
// spoiled before posting, and the PAR2 set over the volumes as they were.
// Every article arrives clean. No volume's front signs anything, so the
// set's numbered names found a joiner that writes the pieces into one
// member, its run open for a volume the set may yet rebuild, and the
// archive is read from that member as far as the spoiled bytes. The set
// rewrites the volumes under the joiner's run, so the joined member starts
// over: it has no file of its own until it is decided again, is written
// anew from its rewritten volumes, and the archive is read again whole.
#[tokio::test]
async fn a_split_7z_whose_volumes_the_set_rewrote_under_its_joiner_delivers() {
    const FRAME: usize = 768;
    const VOLUMES: usize = 4;
    const PER_VOLUME: usize = 32;
    const SPOILED: usize = 100;
    let member = payload(VOLUMES * PER_VOLUME * FRAME - 7 * FRAME, 71);
    let archive = build_7z_shaped(
        &[Entry::file("lantern.mkv", member.clone())],
        sevenz_turbo::EncoderMethod::COPY,
        None,
        false,
    );
    let volumes: Vec<Post> = split(&archive, VOLUMES);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let mut posts: Vec<Post> = volumes
        .iter()
        .map(|post| Post::plain(&post.name, post.bytes.clone()))
        .collect();
    let mut left = SPOILED;
    for post in &mut posts {
        let end = (left * FRAME).min(post.bytes.len());
        post.bytes[..end].fill(0);
        left -= end.div_ceil(FRAME);
    }
    assert_eq!(left, 0);
    posts.extend(par2_volumes_sliced(
        "lantern",
        &files,
        FRAME as u64,
        SPOILED as u32 + 4,
        1,
    ));
    let graph = run(&Scenario::new(posts).framed(FRAME)).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.mkv")]
    );
    assert!(
        graph.files[&delivered("lantern.mkv")] == member,
        "member bytes"
    );
    assert_fetched_once(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired {
            slices: SPOILED as u64
        })]
    );
}

#[tokio::test]
async fn a_split_7z_under_hash_names_named_by_par2_last_delivers_its_members() {
    let graph = run(&Scenario::new(hashed_with_par2_last())).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
    assert_named_by_par2(&graph);
}

// The same set one level down: its pieces and its PAR2 set are the members
// of one stored RAR, and the names the PAR2 set gives are claimed in that
// namespace as they are at the top. The PAR2 files are spent there as they
// are at the top, so only the 7z's members are delivered.
#[tokio::test]
async fn a_split_7z_under_hash_names_inside_a_stored_rar_named_by_par2_last_delivers() {
    let inner = hashed_with_par2_last();
    let named: Vec<(&str, Vec<u8>)> = inner
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.clone()))
        .collect();
    let outer = multi_member_store_set(&named, 1).remove(0).1;
    let scenario = Scenario::new(vec![Post::plain("lantern.rar", outer)]);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    let mut expected: Vec<String> = members().iter().map(|(name, _)| delivered(name)).collect();
    expected.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), expected);
    for (name, bytes) in members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name} bytes");
    }
    assert_named_by_par2(&graph);
}

// A coded split set whose middle volume was never posted, PAR2 beside: the
// engine takes the volumes on either side, the repair rebuilds the middle
// one, and the run reads through it to the members.
#[tokio::test]
async fn a_coded_split_7z_missing_its_middle_volume_is_rebuilt_by_par2_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let volumes = split(&archive, 3);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let slices = volumes[1].bytes.len().div_ceil(ARTICLE) as u32;
    let mut posts = par2_set("lantern", &files, slices + 1);
    posts.extend(
        volumes
            .iter()
            .filter(|post| post.name != volumes[1].name)
            .map(|post| Post::plain(&post.name, post.bytes.clone())),
    );
    let graph = run(&Scenario::new(posts)).await;
    assert_members(&graph);
}

// The header volume is file 0, so it is answered before any of the body:
// the body arrives after its map and still reaches the members.
#[tokio::test]
async fn a_split_7z_posted_header_volume_first_delivers_its_members() {
    let mut volumes = split(&stored(None), 3);
    volumes.rotate_right(1);
    assert_eq!(volumes[0].name, "lantern.7z.003");
    let scenario = Scenario::new(volumes);
    let graph = run(&scenario).await;
    // `.003` has no signature and is a leaf until `.001` binds the store,
    // which is then offered it as it would have been had it come second.
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_lzma2_7z_is_extracted() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

#[tokio::test]
async fn an_aes_stored_7z_with_the_job_password_delivers_its_members() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))]).password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// An archive that needs no password leaves none in the history.
#[tokio::test]
async fn an_archive_with_no_password_keeps_none_in_the_history() {
    let scenario =
        Scenario::new(vec![Post::plain("lantern.7z", stored(None))]).password("filename-key");
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(graph.validated_password, None);
}

// No password for an AES-stored 7z: the store gives the container back
// withheld, since nothing opened it, so nothing is whole and the job fails
// naming the password it needs, with no refetch.
#[tokio::test]
async fn an_aes_stored_7z_without_a_password_fails_for_it() {
    let archive = stored(Some(SECRET));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;

    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(
        graph.failure_kind,
        Some(HistoryFailureKind::Password),
        "failure kind",
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert_eq!(
        graph.failure.as_deref(),
        Some("WEAVER_PASSWORD_REQUIRED: lantern.7z needs a password and none was given")
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

#[tokio::test]
async fn a_damaged_stored_7z_under_par2_is_repaired_and_delivered() {
    let archive = stored(None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is member bytes: the signature header is the first
    // article's, and the map is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The same under AES: the damaged article is cipher bytes, and the repair
// reads the volume back through the store's keys and chain.
#[tokio::test]
async fn a_damaged_aes_stored_7z_under_par2_is_repaired_and_delivered() {
    let archive = stored(Some(SECRET));
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    let scenario = Scenario::new(posts).damaged(0, 1).password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
}

// The coded stream damaged beside a PAR2 set: the engine waits at the
// damaged article for its repair, then decodes the members once, and the
// job ends with nothing parked.
#[tokio::test]
async fn a_damaged_lzma2_7z_under_par2_is_repaired_in_the_stream_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 2));
    // The second article is coded bytes: the signature header is the first
    // article's, and the end header is at the tail.
    let scenario = Scenario::new(posts).damaged(0, 1);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_fetched_once(&graph);
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    // An 11-slice archive (661 bytes): the count follows its size. The
    // damaged article vouched for nothing, so only the rebuild reads the ten
    // good slices, once.
    assert_eq!(graph.reads, 10, "the slices the repair read back");
}

// How many candidates each node was offered.
fn offered(graph: &Outcome) -> Vec<usize> {
    graph.graph().offered.values().copied().collect()
}

fn aes_stored() -> Scenario {
    Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))])
}

// The password reaches the job only as a candidate. The store proves it by
// the smallest member's checksum and asks once.
#[tokio::test]
async fn an_aes_stored_7z_opened_by_a_password_candidate_delivers() {
    let graph = run(&aes_stored().global_password(SECRET)).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "one try for the one candidate");
    assert_fetched_once(&graph);
}

// A wrong candidate and then the right one: the proof refutes the first, the
// store asks again, and each candidate is offered once.
#[tokio::test]
async fn an_aes_stored_7z_tries_a_wrong_candidate_once_then_the_right_one() {
    let scenario = aes_stored()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_fetched_once(&graph);
}

// Every candidate wrong for a coded AES 7z: each run under a wrong one
// writes until the checksum refutes it, and none of what a refused run
// wrote stays. The job fails for the password, and its working directory
// never holds more than the posted archive.
#[tokio::test]
async fn a_coded_aes_7z_no_candidate_opens_keeps_no_more_than_was_posted() {
    let archive = container(Lzma2Options::from_level(1).into(), Some(SECRET));
    let posted = archive.len() as u64;
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)])
        .global_password("cinder-wrong-guess")
        .global_password("cinder-wrong-again");
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert!(
        graph.graph().scratch <= posted,
        "the working directory held {} bytes, {posted} were posted",
        graph.graph().scratch
    );
}

fn sealed() -> Scenario {
    Scenario::new(vec![Post::plain(
        "lantern.7z",
        sealed_header(EncoderConfiguration::new(EncoderMethod::COPY)),
    )])
}

// The job's own password is its first candidate: the store, which cannot
// parse the header without one, asks and is offered it once.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_and_the_job_password_delivers() {
    let graph = run(&sealed().password(SECRET)).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(offered(&graph), [1], "the job's password, once");
    assert_fetched_once(&graph);
}

// The header refutes the wrong candidate by failing to parse, and opens on
// the right one.
#[tokio::test]
async fn an_aes_7z_with_encrypted_headers_opened_by_the_second_candidate_delivers() {
    let scenario = sealed()
        .global_password("cinder-wrong-guess")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(kinds(&graph), ["sevenz_store"]);
    assert_eq!(
        offered(&graph),
        [2],
        "one try for each of the two candidates"
    );
    assert_fetched_once(&graph);
}

fn lzma2() -> Scenario {
    Scenario::new(vec![Post::plain(
        "lantern.7z",
        container(Lzma2Options::from_level(1).into(), None),
    )])
}

// The LZMA2 container cut into articles of every size from the whole file
// down to a byte (4 KiB is the whole 401-byte file again): one outcome and
// one set of files, and nothing fetched twice.
#[tokio::test]
async fn an_lzma2_7z_delivers_alike_in_every_frame_size() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let graph = run(&lzma2().framed(size)).await;
        assert_members(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// The three parts served last article first, as a crowd of workers may:
// the end header that maps the members is in hand first, the signature
// header that finds it last.
#[tokio::test]
async fn a_stored_7z_served_last_article_first_delivers_its_members() {
    let scenario = Scenario::new(split(&stored(None), 3)).backwards();
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The same frames served last article first: the folder's packed stream is
// in hand before the headers that say the container needs the engine.
#[tokio::test]
async fn an_lzma2_7z_served_last_article_first_delivers_alike_in_every_frame_size() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let graph = run(&lzma2().framed(size).backwards()).await;
        assert_members(&graph);
        assert!(
            kinds(&graph).contains(&"sevenz_engine"),
            "{:?}",
            kinds(&graph)
        );
        assert_fetched_once(&graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// In 16-byte articles, the last article wholly inside the packed streams:
// quill.mkv's, the second folder, touching no header.
fn last_packed_article(archive: &[u8]) -> u32 {
    (packed_end(archive) / 16 - 1) as u32
}

// quill.mkv's stream damaged with no PAR2. The engine closes ember.mkv whole,
// then fails quill.mkv's CRC: graph delivers ember.mkv as written, leaves
// quill.mkv out, puts the container as posted in its place, and ends the job
// Partial.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_member_and_no_par2_delivers_the_members_it_closed() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let damaged = last_packed_article(&archive);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive.clone())])
        .framed(16)
        .damaged(0, damaged);
    let graph = run(&scenario).await;

    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "graph: {:?}",
        graph.failure
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [delivered("ember.mkv"), delivered("lantern.7z")]
    );
    assert!(
        graph.files[&delivered("ember.mkv")] == members()[0].1,
        "ember.mkv closed before the failure, delivered whole"
    );
    assert_eq!(
        graph.files[&delivered("lantern.7z")].len(),
        archive.len(),
        "the container as posted"
    );
    assert_eq!(
        graph.graph().claims.get("lantern.7z"),
        Some(&Claim::Damaged)
    );
}

// A single 7z whose head arrived wrong under a good article checksum, with
// no recovery: no store founds on it, so it reaches the end whole as
// posted. A whole archive is not a file the user opens, and nothing else
// came out of it, so the job fails and publishes nothing.
#[tokio::test]
async fn a_single_7z_with_a_damaged_head_and_no_recovery_fails() {
    let (name, ember) = members().swap_remove(0);
    let mut archive = build_7z_shaped(
        &[Entry::file(name, ember.clone())],
        EncoderMethod::COPY,
        None,
        false,
    );
    for byte in &mut archive[..ARTICLE] {
        *byte ^= 0x5a;
    }
    let graph = run(&Scenario::new(vec![Post::plain("lantern.7z", archive)])).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "graph: {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(
        graph.files.get(&delivered(name)) != Some(&ember),
        "ember.mkv never comes out of a container no store founds on"
    );
}

// The last article of a container, counting from zero.
fn last_article(archive: &[u8]) -> u32 {
    (archive.len().div_ceil(ARTICLE) - 1) as u32
}

fn beside_par2(archive: &[u8], blocks: u32) -> Scenario {
    let mut posts = vec![Post::plain("lantern.7z", archive.to_vec())];
    posts.extend(par2_set("lantern", &[("lantern.7z", archive)], blocks));
    Scenario::new(posts)
}

// The end header cut short and posted whole: every volume closes and the
// store never maps. Graph refuses it (`Why::Unmapped`) and gives the
// container back withheld, as it stands, in place of its members; the
// object failed and nothing is whole, so the job fails for unpacking, worded
// by the refusal, never Complete with its members missing.
#[tokio::test]
async fn a_7z_whose_end_header_is_cut_short_is_refused_and_fails() {
    let mut archive = stored(None);
    archive.truncate(archive.len() - 8);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)]);
    let graph = run(&scenario).await;
    assert_eq!(
        graph.failure.as_deref(),
        Some("lantern.7z never placed every member it named")
    );
    assert!(graph.files.is_empty(), "graph: {:?}", graph.files.keys());
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// The end header damaged beside PAR2: the repair reads the container back
// from what the namespace holds for it, rewrites the header, the store maps
// from the repaired bytes and the members are decoded.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_end_header_beside_par2_is_repaired_and_decoded() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, last_article(&archive));
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// More of the end header damaged than PAR2 can rebuild: the store never
// maps and the repair is impossible, so graph fails the job for the repair
// rather than ending Complete with nothing.
#[tokio::test]
async fn an_lzma2_7z_with_its_end_header_damaged_past_par2_is_refused() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let last = last_article(&archive);
    let scenario = beside_par2(&archive, 1)
        .damaged(0, last)
        .damaged(0, last - 1);
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "graph: {:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Repair));
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// The first article damaged beside PAR2: the front cannot be sniffed, so
// the container is a leaf until the repair rewrites its front; then it is
// identified again by the one offer every unowned member gets, the store
// binds, hands the coded folders to the engine, and the members are
// decoded.
#[tokio::test]
async fn an_lzma2_7z_with_a_damaged_first_article_beside_par2_binds_its_store_after_the_repair() {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let scenario = beside_par2(&archive, 2).damaged(0, 0);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert!(
        kinds(&graph).contains(&"sevenz_engine"),
        "{:?}",
        kinds(&graph)
    );
    assert!(
        graph.refetched().is_empty(),
        "graph: {:?}",
        graph.refetched()
    );
}

// A split 7z whose last volume, which holds the end header, is cut short:
// the store never maps, and every volume it holds is refused with the set.
// No volume of a container is a delivery of its own, so nothing is whole
// and the job fails for unpacking.
#[tokio::test]
async fn a_split_7z_whose_last_volume_is_cut_short_delivers_no_volume_whole() {
    let mut volumes = split(&stored(None), 3);
    let last = volumes.last_mut().unwrap();
    last.bytes.truncate(last.bytes.len() - 8);
    let graph = run(&Scenario::new(volumes)).await;
    let view = graph.graph();
    assert!(view.delivery.whole.is_empty(), "{:?}", view.delivery);
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Unpack));
}

// An AES-stored 7z whose one file is larger than any a password is proved
// against, offered a wrong candidate before the right one: the graph's store
// opens the wrong one on trial, its checksum fails on verified bytes, the
// folder's bytes come again and open under the right one, and the file is
// delivered whole with nothing fetched twice.
#[tokio::test]
async fn a_large_aes_stored_7z_offered_a_wrong_password_first_delivers() {
    let file = payload(4 * 1024 * 1024 + 1, 61);
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![
        AesEncoderOptions::new(Password::new(SECRET))
            .with_num_cycles_power(4)
            .into(),
        EncoderConfiguration::new(EncoderMethod::COPY),
    ]);
    let mut entry = ArchiveEntry::new_file("lantern.bin");
    entry.size = file.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(file.clone())))
        .unwrap();
    let archive = writer.finish().unwrap().into_inner();
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", archive)])
        .framed(256 << 10)
        .global_password("not-the-secret")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("lantern.bin")]
    );
    assert!(graph.files[&delivered("lantern.bin")] == file, "bytes");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// A 7z coded by `method` holding an empty directory beside a member.
fn with_empty_directory(method: EncoderConfiguration) -> Vec<u8> {
    let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
    writer.set_content_methods(vec![method]);
    writer
        .push_archive_entry::<Cursor<Vec<u8>>>(ArchiveEntry::new_directory("hollow"), None)
        .unwrap();
    let (name, bytes) = members().remove(0);
    let mut entry = ArchiveEntry::new_file(name);
    entry.size = bytes.len() as u64;
    writer
        .push_archive_entry(entry, Some(Cursor::new(bytes)))
        .unwrap();
    writer.finish().unwrap().into_inner()
}

// The empty directory is delivered as a directory, stored through the store
// and LZMA2 through the engine.
#[tokio::test]
async fn a_7z_holding_an_empty_directory_delivers_it() {
    let methods: [EncoderConfiguration; 2] = [
        EncoderConfiguration::new(EncoderMethod::COPY),
        Lzma2Options::from_level(1).into(),
    ];
    for method in methods {
        let scenario = Scenario::new(vec![Post::plain(
            "lantern.7z",
            with_empty_directory(method),
        )]);
        let graph = run(&scenario).await;
        assert!(graph.completed(), "{:?}", graph.failure);
        assert_eq!(
            graph.empty.iter().collect::<Vec<_>>(),
            [&delivered("hollow/")],
            "empty directories"
        );
        assert_fetched_once(&graph);
    }
}

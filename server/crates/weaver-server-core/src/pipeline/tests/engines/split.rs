// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A plain file posted in numbered pieces, `lantern.bin.001` onward, whose
// first piece opens no signature: the pieces are one file, joined in order,
// and that file is what is delivered, never the pieces.

use weaver_graph::recovery::RepairOutcome;

use crate::jobs::model::JobStatus;

use super::harness::{
    ARTICLE, Outcome, Post, Scenario, assert_fetched_once, kinds, par2_set, par3_set, payload, run,
};

mod unnamed;

const NAME: &str = "lantern.bin";
const PIECE: usize = 6 * ARTICLE;

fn joined() -> Vec<u8> {
    payload(3 * PIECE - 17, 61)
}

fn pieces(bytes: &[u8]) -> Vec<Post> {
    bytes
        .chunks(PIECE)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("{NAME}.{:03}", index + 1), piece.to_vec()))
        .collect()
}

// The joined file alone, every byte as written.
fn assert_joined(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![format!("lantern beacon/{NAME}")],
        "delivered names",
    );
    assert!(
        outcome.files[&format!("lantern beacon/{NAME}")] == joined(),
        "joined bytes",
    );
}

#[tokio::test]
async fn a_clean_plain_split_is_delivered_joined() {
    let graph = run(&Scenario::new(pieces(&joined()))).await;
    assert_joined(&graph);
    assert!(graph.graph().outcomes.is_empty());
}

// The usual shape: the set describes the pieces as posted. It repairs the
// middle piece in place, and the joined file is what is delivered.
#[tokio::test]
async fn a_plain_split_under_par2_over_its_pieces_is_repaired_and_delivered_joined() {
    let original = joined();
    let posted = pieces(&original);
    let described: Vec<(String, Vec<u8>)> = posted
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let described: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = posted;
    posts.extend(par2_set("lantern", &described, 2));
    let graph = run(&Scenario::new(posts).damaged(1, 2)).await;
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}
// Three slices of the middle piece zeroed as posted: every article arrives
// whole, and only the set's hashes over the joined file say they are wrong.
#[tokio::test]
async fn a_plain_split_under_par2_over_the_joined_file_is_repaired_and_delivered_joined() {
    let original = joined();
    let mut posts = pieces(&original);
    posts[1].bytes[ARTICLE..4 * ARTICLE].fill(0);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 4));
    let graph = run(&Scenario::new(posts)).await;
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 3 })]
    );
}

// Nothing damaged: the set over the joined file rules it clean where it is
// joined, and rebuilds nothing.
#[tokio::test]
async fn a_clean_plain_split_under_par2_over_the_joined_file_is_ruled_clean() {
    let original = joined();
    let mut posts = pieces(&original);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 4));
    let graph = run(&Scenario::new(posts)).await;
    assert_joined(&graph);
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
}

// More slices zeroed than the set has blocks: the set rules where the file
// is joined that it cannot rebuild it, the job fails as a recovery failure,
// and the pieces are never delivered in its place.
#[tokio::test]
async fn a_plain_split_whose_par2_over_the_joined_file_is_short_fails_without_its_pieces() {
    let original = joined();
    let mut posts = pieces(&original);
    posts[1].bytes[ARTICLE..4 * ARTICLE].fill(0);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 2));
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        matches!(
            &graph.status,
            Some(JobStatus::Failed { error }) if error.starts_with("PAR2 recovery failed")
        ),
        "{:?}",
        graph.status
    );
    assert!(
        graph
            .files
            .keys()
            .all(|name| !name.starts_with(&format!("lantern beacon/{NAME}."))),
        "pieces delivered: {:?}",
        graph.files.keys().collect::<Vec<_>>()
    );
    assert!(graph.files.is_empty(), "graph: {:?}", graph.files.keys());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Impossible {
            damaged: 3,
            reachable: 2
        })]
    );
}

// Five pieces, `lantern.bin.001` to `.005`, of a file one article short of
// five whole pieces.
fn five() -> Vec<u8> {
    payload(5 * PIECE - ARTICLE, 67)
}

fn assert_five(outcome: &Outcome, bytes: &[u8]) {
    assert!(
        outcome.completed(),
        "{:?} {:?}",
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![format!("lantern beacon/{NAME}")],
        "delivered names",
    );
    assert!(
        outcome.files[&format!("lantern beacon/{NAME}")] == bytes,
        "joined bytes"
    );
}

// Every piece lands where it lies in the one file as it arrives: the job's
// working directory never holds the pieces and a joined copy beside them.
#[tokio::test]
async fn a_clean_split_is_placed_once_and_never_joined() {
    let original = five();
    let graph = run(&Scenario::new(pieces(&original))).await;
    assert_five(&graph, &original);
    assert_fetched_once(&graph);
    assert!(graph.graph().outcomes.is_empty());
    assert_never_joined(&graph, original.len(), 0);
}

// The working directory never held the pieces and a joined copy beside
// them: at most the file once, with `beside` bytes of other posts.
fn assert_never_joined(outcome: &Outcome, file: usize, beside: usize) {
    assert!(
        outcome.graph().scratch < (file * 3 / 2 + beside) as u64,
        "the working directory held {} bytes for a file of {file} and {beside} beside it",
        outcome.graph().scratch,
    );
}

// The last piece's articles come first, before any piece declared where it
// ends: they wait, and go to their place once every piece before them has.
#[tokio::test]
async fn a_later_piece_arriving_first_is_held_then_placed() {
    let original = five();
    let graph = run(&Scenario::new(pieces(&original)).backwards()).await;
    assert_five(&graph, &original);
    assert_fetched_once(&graph);
    assert_never_joined(&graph, original.len(), 0);
}

// The second piece declares a size one article short of what it holds. The
// pieces after it were placed by that size; at its close they move once,
// and the file is every piece in order. Nothing is fetched again.
#[tokio::test]
async fn a_piece_that_declared_the_wrong_size_shifts_the_tail_once() {
    let original = five();
    let graph = run(&Scenario::new(pieces(&original))
        .declares(1, (PIECE - ARTICLE) as u64)
        .together(&[0, 1, 2, 3, 4]))
    .await;
    assert_five(&graph, &original);
    assert_fetched_once(&graph);
    assert_never_joined(&graph, original.len(), 0);
}

// The second piece declares a size one article past what it holds: the
// member is said too long, the pieces after it were placed too far, and at
// its close they move back once and the member is said at its length. The
// file is every piece in order, nothing past its end.
#[tokio::test]
async fn a_piece_that_declared_too_long_pulls_the_tail_back_once() {
    let original = five();
    let graph = run(&Scenario::new(pieces(&original))
        .declares(1, (PIECE + ARTICLE) as u64)
        .together(&[0, 1, 2, 3, 4]))
    .await;
    assert_five(&graph, &original);
    assert_fetched_once(&graph);
    assert_never_joined(&graph, original.len(), 0);
}

// The same, the last articles first: the piece closes short before the
// pieces ahead of it declared, and its held bytes, placed after, keep it at
// what it reached.
#[tokio::test]
async fn a_piece_that_declared_too_long_pulls_the_tail_back_in_reverse() {
    let original = five();
    let graph = run(&Scenario::new(pieces(&original))
        .declares(1, (PIECE + ARTICLE) as u64)
        .backwards())
    .await;
    assert_five(&graph, &original);
    assert_fetched_once(&graph);
}

// A RAR in numbered pieces whose first piece declares a size one article
// short: the RAR store beneath the split is told the joined file's length
// again when its bytes move, and what the RAR holds is delivered whole.
#[tokio::test]
async fn a_rar_in_pieces_with_a_wrong_declared_size_still_unpacks() {
    let beacon = payload(9 * ARTICLE + 5, 71);
    let archive = crate::pipeline::tests::direct_store::multi_member_store_set(
        &[("beacon.bin", beacon.clone())],
        1,
    )
    .remove(0)
    .1;
    let posts: Vec<Post> = archive
        .chunks(PIECE)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("lantern.rar.{:03}", index + 1), piece.to_vec()))
        .collect();
    let all: Vec<u32> = (0..posts.len() as u32).collect();
    let graph = run(&Scenario::new(posts)
        .declares(0, (PIECE - ARTICLE) as u64)
        .together(&all))
    .await;
    assert!(
        graph.completed(),
        "{:?} {:?} kinds {:?}",
        graph.status,
        graph.failure,
        kinds(&graph)
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/beacon.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/beacon.bin"] == beacon);
    assert_fetched_once(&graph);
}

// The same RAR in numbered pieces, the first declaring a size one article
// short, its articles arriving last first: what lays the archive out before
// the first piece's length moves is laid again once it does, and the job
// ends as it does in order, with the same file, nothing fetched twice.
#[tokio::test]
async fn a_rar_in_pieces_with_a_wrong_declared_size_ends_alike_in_either_order() {
    let beacon = payload(9 * ARTICLE + 5, 71);
    let archive = crate::pipeline::tests::direct_store::multi_member_store_set(
        &[("beacon.bin", beacon.clone())],
        1,
    )
    .remove(0)
    .1;
    let all: Vec<u32> = (0..archive.len().div_ceil(PIECE) as u32).collect();
    let scenario = || {
        let posts = archive
            .chunks(PIECE)
            .enumerate()
            .map(|(index, piece)| {
                Post::plain(&format!("lantern.rar.{:03}", index + 1), piece.to_vec())
            })
            .collect();
        Scenario::new(posts).declares(0, (PIECE - ARTICLE) as u64)
    };
    let forward = run(&scenario().together(&all)).await;
    let backward = run(&scenario().backwards()).await;
    for graph in [&forward, &backward] {
        assert!(
            graph.completed(),
            "{:?} {:?} kinds {:?}",
            graph.status,
            graph.failure,
            kinds(graph)
        );
        assert_fetched_once(graph);
    }
    assert_eq!(
        backward.files.keys().collect::<Vec<_>>(),
        forward.files.keys().collect::<Vec<_>>(),
    );
    assert!(backward.files == forward.files);
    assert!(forward.files["lantern beacon/beacon.bin"] == beacon);
}

// `archive` cut into numbered pieces named `name.001` onward, the first
// declaring `declared` bytes rather than what it holds, every piece fetched
// together: each member of the archive is delivered whole and nothing is
// fetched again.
async fn assert_in_pieces_with_a_wrong_declared_size_unpacks(
    name: &str,
    archive: &[u8],
    declared: usize,
    members: &[(&str, Vec<u8>)],
) {
    assert_in_pieces_arriving_so_unpacks(name, archive, declared, members, |scenario, all| {
        scenario.together(all)
    })
    .await;
}

// The same, the pieces' articles arriving as `arrive` lays them out over
// every piece's index.
async fn assert_in_pieces_arriving_so_unpacks(
    name: &str,
    archive: &[u8],
    declared: usize,
    members: &[(&str, Vec<u8>)],
    arrive: fn(Scenario, &[u32]) -> Scenario,
) {
    let posts: Vec<Post> = archive
        .chunks(PIECE)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("{name}.{:03}", index + 1), piece.to_vec()))
        .collect();
    assert!(posts.len() > 1, "the archive spans more than one piece");
    let all: Vec<u32> = (0..posts.len() as u32).collect();
    let graph = run(&arrive(
        Scenario::new(posts).declares(0, declared as u64),
        &all,
    ))
    .await;
    assert!(
        graph.completed(),
        "{:?} {:?} kinds {:?}",
        graph.status,
        graph.failure,
        kinds(&graph)
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(member, _)| format!("lantern beacon/{member}"))
        .collect();
    expected.sort();
    assert_eq!(graph.files.keys().cloned().collect::<Vec<_>>(), expected);
    for (member, bytes) in members {
        assert!(
            graph.files[&format!("lantern beacon/{member}")] == *bytes,
            "{member} bytes"
        );
    }
    assert_fetched_once(&graph);
}

// A tar in numbered pieces whose first piece declares a size one article
// short: every later piece moves once, and the tar beneath the split reads
// each moved run as it comes, before the next one goes.
#[tokio::test]
async fn a_tar_in_pieces_with_a_wrong_declared_size_still_unpacks() {
    let members = super::streams::members();
    let archive = super::streams::tar(&members);
    assert_in_pieces_with_a_wrong_declared_size_unpacks(
        "lantern.tar",
        &archive,
        PIECE - ARTICLE,
        &members,
    )
    .await;
}

// A tar that stops where its next entry would start, without the end
// blocks, in numbered pieces whose first piece declares a size one article
// long: the joined file is said too long, then said again at its length at
// the close, and the tar takes that length, so it ends where the file does.
#[tokio::test]
async fn a_tar_without_end_blocks_in_pieces_declared_too_long_still_unpacks() {
    let members = super::streams::members();
    let mut archive = super::streams::tar(&members);
    archive.truncate(archive.len() - 1024);
    assert_in_pieces_with_a_wrong_declared_size_unpacks(
        "lantern.tar",
        &archive,
        PIECE + ARTICLE,
        &members,
    )
    .await;
}

// The same tar, the first piece declaring a size one article short, its
// articles arriving last first: a later piece's front is judged whole
// whichever comes first, and the tar unpacks as it does in order.
#[tokio::test]
async fn a_tar_in_pieces_with_a_wrong_declared_size_arriving_last_first_still_unpacks() {
    let members = super::streams::members();
    let archive = super::streams::tar(&members);
    assert_in_pieces_arriving_so_unpacks(
        "lantern.tar",
        &archive,
        PIECE - ARTICLE,
        &members,
        |scenario, _| scenario.backwards(),
    )
    .await;
}

// The same tar without its end blocks, the first piece declaring a size one
// article long, its articles arriving last first: it unpacks as it does in
// order.
#[tokio::test]
async fn a_tar_without_end_blocks_in_pieces_declared_too_long_arriving_last_first_still_unpacks() {
    let members = super::streams::members();
    let mut archive = super::streams::tar(&members);
    archive.truncate(archive.len() - 1024);
    assert_in_pieces_arriving_so_unpacks(
        "lantern.tar",
        &archive,
        PIECE + ARTICLE,
        &members,
        |scenario, _| scenario.backwards(),
    )
    .await;
}

// A zip in numbered pieces whose first piece declares a size one article
// short: the zip beneath the split is told the joined file's length again
// when its bytes move, and looks again.
#[tokio::test]
async fn a_zip_in_pieces_with_a_wrong_declared_size_still_unpacks() {
    let members = super::zip::members();
    let archive = super::zip::archive(super::zip::stored());
    assert_in_pieces_with_a_wrong_declared_size_unpacks(
        "lantern.zip",
        &archive,
        PIECE - ARTICLE,
        &members,
    )
    .await;
}

// Every article of the third piece lost: the set over the pieces rebuilds
// it, and its bytes land where it lies in the one file.
#[tokio::test]
async fn a_missing_piece_is_repaired_through_its_placement() {
    let original = five();
    let posted = pieces(&original);
    let described: Vec<(String, Vec<u8>)> = posted
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let described: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut posts = posted;
    let recovery = par2_set("lantern", &described, 6);
    let beside = recovery.iter().map(|post| post.bytes.len()).sum();
    posts.extend(recovery);
    let scenario = (0..6).fold(Scenario::new(posts), |scenario, ordinal| {
        scenario.lost(2, ordinal)
    });
    let graph = run(&scenario).await;
    assert_five(&graph, &original);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 6 })]
    );
    assert_never_joined(&graph, original.len(), beside);
}

// The same piece lost with nothing to rebuild it: no file came out whole,
// and a piece is no usable file, so the job fails.
#[tokio::test]
async fn a_missing_piece_without_recovery_fails_the_job() {
    let original = five();
    let scenario = (0..6).fold(Scenario::new(pieces(&original)), |scenario, ordinal| {
        scenario.lost(2, ordinal)
    });
    let graph = run(&scenario).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(
        graph.files.is_empty(),
        "delivered: {:?}",
        graph.files.keys()
    );
}

// `.001`, `.002` and `.004`: no `.003` was posted, so nothing says where
// `.004` lies. The pieces are kept as posted, none is a file, and the job
// fails.
#[tokio::test]
async fn a_gap_in_numbering_is_not_a_split() {
    let mut posts = pieces(&five());
    posts.truncate(4);
    posts.remove(2);
    let originals: std::collections::BTreeMap<_, _> = posts
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(
        graph.files.is_empty(),
        "delivered: {:?}",
        graph.files.keys()
    );
    for (name, bytes) in originals {
        let kept = graph
            .retained_files
            .iter()
            .find(|(path, _)| std::path::Path::new(path).file_name().unwrap() == name.as_str())
            .map(|(_, bytes)| bytes);
        assert_eq!(kept, Some(&bytes), "piece {name}");
    }
}

// The same gapped set beside one whole plain file: the set fails only
// itself and its pieces go back as posted, the plain file is delivered, and
// the job ends Partial.
#[tokio::test]
async fn a_gapped_split_beside_a_whole_file_ends_partial() {
    let mut posts = pieces(&five());
    posts.truncate(4);
    posts.remove(2);
    let mut expected: std::collections::BTreeMap<String, Vec<u8>> = posts
        .iter()
        .map(|post| (format!("lantern beacon/{}", post.name), post.bytes.clone()))
        .collect();
    let plain = payload(3 * ARTICLE + 9, 73);
    posts.push(Post::plain("ember.mkv", plain.clone()));
    expected.insert("lantern beacon/ember.mkv".to_string(), plain);
    let graph = run(&Scenario::new(posts)).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(
        graph.files.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "delivered names",
    );
    for (name, bytes) in &expected {
        assert!(graph.files[name] == *bytes, "{name} as posted");
    }
}

// A stored RAR posted in numbered pieces, `lantern.rar.001` onward, the
// first opening the RAR's signature: the name outranks it. The pieces join
// into `lantern.rar`, that file founds a RAR store beneath the split, and
// what the RAR holds is delivered whole.
#[tokio::test]
async fn a_rar_posted_in_numbered_pieces_joins_then_unpacks() {
    let beacon = payload(9 * ARTICLE + 5, 71);
    let archive = crate::pipeline::tests::direct_store::multi_member_store_set(
        &[("beacon.bin", beacon.clone())],
        1,
    )
    .remove(0)
    .1;
    assert!(archive.len() > PIECE, "the archive spans two pieces");
    let posts: Vec<Post> = archive
        .chunks(PIECE)
        .enumerate()
        .map(|(index, piece)| Post::plain(&format!("lantern.rar.{:03}", index + 1), piece.to_vec()))
        .collect();
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        graph.completed(),
        "{:?} {:?} kinds {:?}",
        graph.status,
        graph.failure,
        kinds(&graph)
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec!["lantern beacon/beacon.bin".to_string()],
    );
    assert!(graph.files["lantern beacon/beacon.bin"] == beacon);
    assert_eq!(kinds(&graph), vec!["rar_store", "split"]);
}

// A RAR's own volume names, `.part01.rar` onward, are no numbered pieces:
// they found the RAR store, and no split.
#[tokio::test]
async fn rar_volume_names_found_the_rar_store_not_a_split() {
    let beacon = payload(9 * ARTICLE + 5, 71);
    let posts: Vec<Post> = crate::pipeline::tests::direct_store::multi_member_store_set(
        &[("beacon.bin", beacon.clone())],
        2,
    )
    .into_iter()
    .map(|(name, bytes)| Post::plain(&name, bytes))
    .collect();
    let graph = run(&Scenario::new(posts)).await;
    assert!(
        graph.completed(),
        "{:?} {:?} kinds {:?}",
        graph.status,
        graph.failure,
        kinds(&graph)
    );
    assert!(graph.files["lantern beacon/beacon.bin"] == beacon);
    assert_eq!(kinds(&graph), vec!["rar_store"]);
}

// A tar split into `lantern.tar.001` and `.002` exactly where its second
// member's header starts, so the second piece opens as a tar of its own.
// Nothing straddles the cut: the first piece holds the first member whole,
// the second the rest and the end blocks. Each member is delivered once,
// whole, whichever reading the pieces take: in articles too short to hold a
// tar header's verdict the second piece's front signs nothing and the
// pieces join; in articles that hold it the second piece refutes the set
// and each piece is read as its own tar.
#[tokio::test]
async fn a_tar_split_on_a_member_header_delivers_every_member_once() {
    let members = super::streams::members();
    let archive = super::streams::tar(&members);
    let cut = 512 + members[0].1.len().next_multiple_of(512);
    assert_eq!(
        &archive[cut + 257..cut + 262],
        b"ustar",
        "the cut opens a header"
    );
    let mut expected: Vec<String> = members
        .iter()
        .map(|(name, _)| format!("lantern beacon/{name}"))
        .collect();
    expected.sort();
    for article in [ARTICLE, 4096] {
        let posts = vec![
            Post::plain("lantern.tar.001", archive[..cut].to_vec()),
            Post::plain("lantern.tar.002", archive[cut..].to_vec()),
        ];
        let graph = run(&Scenario::new(posts).framed(article)).await;
        assert!(
            graph.completed(),
            "articles of {article}: {:?} {:?} kinds {:?} files {:?}",
            graph.status,
            graph.failure,
            kinds(&graph),
            graph.files.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            graph.files.keys().cloned().collect::<Vec<_>>(),
            expected,
            "articles of {article}: delivered names",
        );
        for (name, bytes) in &members {
            assert!(
                graph.files[&format!("lantern beacon/{name}")] == *bytes,
                "articles of {article}: {name} whole"
            );
        }
        assert_fetched_once(&graph);
    }
}

// One piece's article lost: the set posted over the joined file repairs the
// joined file where the join writes it, and that file is what is handed
// over, whole, with none of the pieces beside it.
#[tokio::test]
async fn a_plain_split_with_a_lost_article_under_par2_over_the_joined_file_is_repaired() {
    let original = joined();
    let mut posts = pieces(&original);
    posts.extend(par2_set("lantern", &[(NAME, &original)], 2));
    let graph = run(&Scenario::new(posts).lost(1, 2)).await;
    assert_joined(&graph);
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
}

// The first piece posted one article longer than its declared size, beside
// the set `recovery` builds over the pieces as cut.
fn posted_long(recovery: impl FnOnce(&[(&str, &[u8])]) -> Vec<Post>) -> Scenario {
    posted_off(|piece| piece.extend_from_slice(&[0x5a; ARTICLE]), recovery)
}

// The pieces under a recovery set over them as they should be, the first
// posted changed by `change` but still declaring its true size.
fn posted_off(
    change: impl FnOnce(&mut Vec<u8>),
    recovery: impl FnOnce(&[(&str, &[u8])]) -> Vec<Post>,
) -> Scenario {
    let posted = pieces(&joined());
    let described: Vec<(String, Vec<u8>)> = posted
        .iter()
        .map(|post| (post.name.clone(), post.bytes.clone()))
        .collect();
    let described: Vec<(&str, &[u8])> = described
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let recovery = recovery(&described);
    let mut posts = posted;
    change(&mut posts[0].bytes);
    posts.extend(recovery);
    Scenario::new(posts).declares(0, PIECE as u64)
}

// The set over the pieces proves the first piece the declared size: the
// stray bytes are cut and the join comes out whole.
#[tokio::test]
async fn a_piece_posted_long_is_cut_to_the_length_its_recovery_set_proves() {
    let graph = run(&posted_long(|described| par2_set("lantern", described, 2))).await;
    assert_joined(&graph);
}

#[tokio::test]
async fn a_piece_posted_long_is_cut_to_the_length_its_recovery_set_proves_in_reverse() {
    let scenario = posted_long(|described| par2_set("lantern", described, 2)).backwards();
    assert_joined(&run(&scenario).await);
}

// A PAR3 set over the pieces proves it alike.
#[tokio::test]
async fn a_piece_posted_long_is_cut_to_the_length_its_par3_set_proves() {
    let graph = run(&posted_long(|described| par3_set("lantern", described, 2))).await;
    assert_joined(&graph);
}

#[tokio::test]
async fn a_piece_posted_long_is_cut_to_the_length_its_par3_set_proves_in_reverse() {
    let scenario = posted_long(|described| par3_set("lantern", described, 2)).backwards();
    assert_joined(&run(&scenario).await);
}

// The first piece posted an article short of the size it declares: the
// tail it lacks is damage, and the set over the pieces rebuilds it.
fn posted_short() -> Scenario {
    posted_off(
        |piece| piece.truncate(PIECE - ARTICLE),
        |described| par2_set("lantern", described, 2),
    )
}

#[tokio::test]
async fn a_piece_posted_short_is_rebuilt_to_the_length_its_recovery_set_proves() {
    let graph = run(&posted_short()).await;
    assert_joined(&graph);
    assert!(
        matches!(
            graph.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
}

#[tokio::test]
async fn a_piece_posted_short_is_rebuilt_to_the_length_its_recovery_set_proves_in_reverse() {
    let graph = run(&posted_short().backwards()).await;
    assert_joined(&graph);
    assert!(
        matches!(
            graph.graph().outcomes[..],
            [Some(RepairOutcome::Repaired { .. })]
        ),
        "{:?}",
        graph.graph().outcomes
    );
}

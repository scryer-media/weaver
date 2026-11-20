// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 set describes a file no posting carries by its name: the file's
// bytes are posted under another name, and that posting's first article is
// another post's, so neither its front nor its name binds it. The intact
// slices of that posting are the described file's, and the recovery on hand
// rebuilds the rest, as par2cmdline does when it scans the posting.

use super::harness::{Post, Scenario, par2_volumes_sliced, payload, run};
use super::rar::delivered;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::lines::Lines;
use crate::pipeline::tests::direct_store::single_member_rar4_store_set;

const MEMBER: &str = "imageset.jpg";
const FRAME: usize = 32 * 1024;
// About fourteen and a half slices to an article, as the field set's
// 53,024-byte slices under 768,000-byte articles.
const SLICE: u64 = 2_260;
const VOLUMES: usize = 5;
const STEM_DESCRIBED: &str = "Some_Image_Set_GRP";
const STEM_POSTED: &str = "Some.Image.Set.GRP";

// Five old-style volumes of eight articles each, the last shorter.
fn body() -> Vec<u8> {
    payload(VOLUMES * 8 * FRAME - 3 * FRAME + 77, 41)
}

fn truth(body: &[u8]) -> Vec<(String, Vec<u8>)> {
    single_member_rar4_store_set(MEMBER, body, VOLUMES)
        .into_iter()
        .enumerate()
        .map(|(index, (_, bytes))| {
            let name = match index {
                0 => format!("{STEM_DESCRIBED}.rar"),
                n => format!("{STEM_DESCRIBED}.r{:02}", n - 1),
            };
            (name, bytes)
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Layout {
    // One set over every volume, as the field post's.
    OneSet,
    // One set per volume, every set's packets in the same files: each
    // `vol-NN.par2` holds every set's packets at that place.
    SetPerVolume,
}

fn recovery(truth: &[(String, Vec<u8>)], layout: Layout, blocks: u32, files: u32) -> Vec<Post> {
    let sets: Vec<Vec<Post>> = match layout {
        Layout::OneSet => {
            let described: Vec<(&str, &[u8])> = truth
                .iter()
                .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
                .collect();
            vec![par2_volumes_sliced(
                STEM_POSTED,
                &described,
                SLICE,
                blocks,
                files,
            )]
        }
        Layout::SetPerVolume => truth
            .iter()
            .map(|(name, bytes)| {
                par2_volumes_sliced(
                    name,
                    &[(name.as_str(), bytes.as_slice())],
                    SLICE,
                    blocks,
                    files,
                )
            })
            .collect(),
    };
    (0..sets[0].len())
        .map(|at| {
            let bytes: Vec<u8> = sets.iter().flat_map(|set| set[at].bytes.clone()).collect();
            Post::plain(&format!("{STEM_POSTED}.vol-{:02}.par2", at + 1), bytes)
        })
        .collect()
}

struct Shape {
    posts: Vec<Post>,
    // The misnamed first volume's post.
    first: u32,
    // The PAR2 posts.
    par2: Vec<u32>,
}

// `.r00` to `.r03` under the described names, then the first volume under
// the posted name, as the NZB lists them; the PAR2 posts after the data, as
// the NZB lists them too, or before it.
fn shape(body: &[u8], layout: Layout, par2_first: bool, blocks: u32, files: u32) -> Shape {
    let truth = truth(body);
    let mut volumes: Vec<Post> = truth[1..]
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    volumes.push(Post::plain(
        &format!("{STEM_POSTED}.part01.rar"),
        truth[0].1.clone(),
    ));
    let par2 = recovery(&truth, layout, blocks, files);
    let (data, count) = (volumes.len() as u32, par2.len() as u32);
    match par2_first {
        false => {
            let mut posts = volumes;
            posts.extend(par2);
            Shape {
                posts,
                first: data - 1,
                par2: (data..data + count).collect(),
            }
        }
        true => {
            let mut posts = par2;
            posts.extend(volumes);
            Shape {
                posts,
                first: count + data - 1,
                par2: (0..count).collect(),
            }
        }
    }
}

// The field post's other articles in its last three PAR2 volumes: one in
// `vol-07`, three in `vol-08`, four in `vol-09`, as far as each has them.
fn par2_foreign(shape: &Shape) -> Vec<(u32, u32)> {
    let wanted: [&[u32]; 3] = [&[0], &[0, 1, 2], &[0, 1, 4, 5]];
    shape.par2[shape.par2.len() - 3..]
        .iter()
        .zip(wanted)
        .flat_map(|(&file, ordinals)| {
            let articles = shape.posts[file as usize].bytes.len().div_ceil(FRAME) as u32;
            ordinals
                .iter()
                .filter(move |ordinal| **ordinal < articles)
                .map(move |ordinal| (file, *ordinal))
        })
        .collect()
}

// The misnamed volume's first article is another post's; with `in_par2`,
// so are the field's articles in the PAR2 volumes; `together` fetches every
// post side by side, as many connections do.
fn scenario(shape: Shape, in_par2: bool, together: bool) -> Scenario {
    let count = shape.posts.len() as u32;
    let foreign = match in_par2 {
        true => par2_foreign(&shape),
        false => Vec::new(),
    };
    let mut scenario = Scenario::new(shape.posts)
        .framed(FRAME)
        .foreign(shape.first, 0);
    for (file, ordinal) in foreign {
        scenario = scenario.foreign(file, ordinal);
    }
    if together {
        let all: Vec<u32> = (0..count).collect();
        scenario = scenario.together(&all);
    }
    scenario
}

// The slices of one article's worth of damage, as each failing ruling
// below can cover: the misnamed volume's lost first article, rounded up.
const ARTICLE_SLICES: u64 = (FRAME as u64).div_ceil(SLICE) + 1;

// The job completes and the archive's member is delivered byte for byte.
// Every ruling's damage stays within what its lost articles can cover:
// `lost` articles of `FRAME` bytes each.
async fn assert_repaired(scenario: Scenario, body: &[u8], lost: u64) {
    let (lines, guard) = Lines::capture();
    let graph = run(&scenario).await;
    drop(guard);
    let ruled = lines.of("recovery set ruled");
    for line in &ruled {
        let damaged: u64 = line
            .split(' ')
            .find_map(|field| field.strip_prefix("damaged_slices="))
            .and_then(|count| count.parse().ok())
            .unwrap_or_else(|| panic!("a ruling names its damage: {line}"));
        assert!(
            damaged <= lost * ARTICLE_SLICES,
            "damage past what {lost} lost article(s) can cover: {line}"
        );
    }
    assert_eq!(
        graph.status,
        Some(JobStatus::Complete),
        "failure: {:?}\nruled: {ruled:#?}",
        graph.failure
    );
    assert!(
        graph.files.get(&delivered(MEMBER)).map(Vec::as_slice) == Some(body),
        "{MEMBER} delivered whole; ruled: {ruled:#?}"
    );
}

// PAR2 posted first, one set over every volume. Before the held-first read
// the set ruled every slice of the misnamed volume damaged, as the field
// post's 472 of 472: the repair read the unbound posting's still-held bytes
// from its file, where they are zeros.
#[tokio::test]
async fn a_misnamed_volume_with_a_foreign_front_is_repaired_when_par2_comes_first() {
    let body = body();
    let shape = shape(&body, Layout::OneSet, true, 20 * VOLUMES as u32, 4);
    assert_repaired(scenario(shape, false, false), &body, 1).await;
}

// The same, one set per volume.
#[tokio::test]
async fn a_misnamed_volume_with_a_foreign_front_is_repaired_by_its_own_set_when_par2_comes_first() {
    let body = body();
    let shape = shape(&body, Layout::SetPerVolume, true, 20, 4);
    assert_repaired(scenario(shape, false, false), &body, 1).await;
}

// One set per volume, every post fetched side by side.
#[tokio::test]
async fn a_misnamed_volume_with_a_foreign_front_is_repaired_by_its_own_set_fetched_together() {
    let body = body();
    let shape = shape(&body, Layout::SetPerVolume, false, 20, 4);
    assert_repaired(scenario(shape, false, true), &body, 1).await;
}

// The field's order: data first, PAR2 after in nine files, 47 blocks, the
// foreign articles in the last three PAR2 volumes too, fetched side by side.
#[tokio::test]
async fn a_misnamed_volume_in_the_field_order_with_foreign_par2_articles_is_repaired() {
    let body = body();
    let shape = shape(&body, Layout::OneSet, false, 47, 8);
    assert_repaired(scenario(shape, true, true), &body, 1).await;
}

// One article of a PAR2 volume is lost: the volume's other recovery packets
// still count. One volume holds all 40 blocks over several articles; its
// last article goes, with the blocks in it. A data article is lost too,
// which needs about fifteen blocks. The set repairs only if the blocks
// outside the lost article are read.
#[tokio::test]
async fn a_lost_article_of_a_par2_volume_drops_only_the_packets_it_held() {
    let body = body();
    let truth = truth(&body);
    let files: Vec<(&str, &[u8])> = truth
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let par2 = par2_volumes_sliced(STEM_DESCRIBED, &files, SLICE, 40, 1);
    let mut posts: Vec<Post> = truth
        .iter()
        .map(|(name, bytes)| Post::plain(name, bytes.clone()))
        .collect();
    let volume = (posts.len() + par2.len()) as u32 - 1;
    posts.extend(par2);
    let articles = posts[volume as usize].bytes.len().div_ceil(FRAME) as u32;
    assert!(
        articles >= 3,
        "the recovery volume spans {articles} articles"
    );
    let scenario = Scenario::new(posts)
        .framed(FRAME)
        .lost(1, 3)
        .lost(volume, articles - 1);
    assert_repaired(scenario, &body, 1).await;
}

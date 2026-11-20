// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A PAR2 set every file of which is posted under a name that says nothing
// and loses its first article: no index and no volume opens on its own
// packets, so no front founds the set. The packets past the holes do: a
// packet whose hash passes is the set's, wherever it lies, and the set it
// founds takes the other volumes by their packets past their holes, and
// repairs the data file's lost article with their blocks.

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes_sliced, payload, run};

const REAL_ARTICLE: usize = 716_800;
const REAL_BLOCK: u64 = 768_000;

#[derive(Clone, Copy, Debug)]
enum Size {
    Small,
    Real,
}

impl Size {
    fn article(self) -> usize {
        match self {
            Self::Small => ARTICLE,
            Self::Real => REAL_ARTICLE,
        }
    }

    fn block(self) -> u64 {
        match self {
            Self::Small => ARTICLE as u64,
            Self::Real => REAL_BLOCK,
        }
    }
}

// The data file, then every file of its set under a name that says
// nothing, the index too.
fn posts(size: Size, seed: u8) -> Vec<Post> {
    // Enough data that the lost fronts keep the job above the health floor.
    let articles = 24;
    let data = payload(articles * size.article(), seed);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    let set = par2_volumes_sliced(
        "lantern",
        &[("lantern.bin", data.as_slice())],
        size.block(),
        4,
        1,
    );
    for (at, post) in set.into_iter().enumerate() {
        // At the real article size the index is one article, which a lost
        // front takes whole: it is left out, as one never posted.
        if at == 0 && matches!(size, Size::Real) {
            continue;
        }
        posts.push(Post {
            bytes: post.bytes,
            ..Post::plain(&format!("q8z{at}w.bin"), Vec::new())
        });
    }
    posts
}

// Every set file loses its first article, and the data file its third.
fn frontless(posts: Vec<Post>, size: Size, backwards: bool) -> Scenario {
    let files = posts.len() as u32;
    let mut scenario = Scenario::new(posts).framed(size.article()).lost(0, 2);
    for file in 1..files {
        scenario = scenario.lost(file, 0);
    }
    if backwards {
        scenario.backwards()
    } else {
        scenario
    }
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings,
        graph.files.keys().collect::<Vec<_>>()
    )
}

#[tokio::test]
async fn a_par2_set_with_no_front_anywhere_repairs_from_past_its_holes() {
    for size in [Size::Small, Size::Real] {
        for backwards in [false, true] {
            let graph = run(&frontless(posts(size, 92), size, backwards)).await;
            assert!(
                graph.completed(),
                "{size:?} backwards {backwards}: {}",
                summary(&graph)
            );
            assert!(
                !graph.files.keys().any(|name| name.contains("q8z")),
                "{size:?} backwards {backwards}: a set file delivered: {}",
                summary(&graph)
            );
        }
    }
}

// Files that are no PAR2 at all, under names that say nothing, each losing
// its first article: the bytes past their holes found no set.
#[tokio::test]
async fn frontless_files_with_no_par2_found_no_set() {
    for size in [Size::Small, Size::Real] {
        let posts: Vec<Post> = (0..3u8)
            .map(|at| {
                Post::plain(
                    &format!("q8z{at}w.bin"),
                    payload(4 * size.article(), 30 + at),
                )
            })
            .collect();
        let mut scenario = Scenario::new(posts).framed(size.article());
        for file in 0..3 {
            scenario = scenario.lost(file, 0);
        }
        let graph = run(&scenario).await;
        assert!(
            !graph.graph().kinds.contains("par2"),
            "{size:?}: {:?} {}",
            graph.graph().kinds,
            summary(&graph)
        );
    }
}

// The set of another data file, every file of it frontless and nameless,
// beside a data file it does not describe: its packets found that set, and
// none of its blocks repairs this file, whose lost article stays lost.
#[tokio::test]
async fn a_frontless_set_of_another_file_repairs_nothing() {
    let mixed = |size| {
        let mut ours = posts(size, 92);
        ours.truncate(1);
        ours.extend(posts(size, 17).into_iter().skip(1));
        ours
    };
    for size in [Size::Small, Size::Real] {
        for backwards in [false, true] {
            let graph = run(&frontless(mixed(size), size, backwards)).await;
            assert!(
                !graph.completed(),
                "{size:?} backwards {backwards}: {}",
                summary(&graph)
            );
        }
    }
}

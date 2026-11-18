// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A recovery volume posted under a name that says nothing of it, whose
// first article is lost or was never listed: its front never comes, so
// neither its name nor its front says what it is. Its clean bytes past the
// hole do: the set it belongs to finds its own packets there, counts its
// blocks, and repairs the data file's lost article with them.

use super::harness::{
    ARTICLE, Outcome, Post, Scenario, par2_volumes, par2_volumes_sliced, par3_set, par3_set_sized,
    payload, run,
};
use super::restart::restart_row;

#[derive(Clone, Copy, Debug)]
enum Kind {
    Par2,
    Par3,
}

// How the post is cut: the harness's small articles and blocks, or a real
// post's, an article of 700 KiB and recovery blocks of 750 KiB, so the
// clean bytes past the hole start inside a recovery packet and the next
// packet starts far past the first FIRST_BYTES of them.
#[derive(Clone, Copy, Debug)]
enum Size {
    Small,
    Real,
}

const REAL_ARTICLE: usize = 716_800;
const REAL_BLOCK: u64 = 768_000;

impl Size {
    fn article(self) -> usize {
        match self {
            Self::Small => ARTICLE,
            Self::Real => REAL_ARTICLE,
        }
    }
}

// The data file, then the set's index under its own name, then its
// volumes, each renamed to a name that says nothing. At real size a lost
// first article takes the first block of every volume with it, so the
// PAR3 set, whose creator doubles its volumes (one block, two, four), has
// seven blocks: what is left past the holes still covers the loss.
fn renamed(kind: Kind, size: Size) -> Vec<Post> {
    let data = match size {
        Size::Small => payload(8 * ARTICLE, 92),
        Size::Real => payload(4 * REAL_ARTICLE, 92),
    };
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    let files = [("lantern.bin", data.as_slice())];
    let set = match (kind, size) {
        (Kind::Par2, Size::Small) => par2_volumes("lantern", &files, 4, 1),
        (Kind::Par2, Size::Real) => par2_volumes_sliced("lantern", &files, REAL_BLOCK, 4, 1),
        (Kind::Par3, Size::Small) => par3_set("lantern", &files, 4),
        (Kind::Par3, Size::Real) => par3_set_sized("lantern", &files, REAL_BLOCK, 7),
    };
    let mut volumes = 0;
    for post in set {
        if post.name.contains(".vol") {
            let name = format!("k3x9q{volumes}.bin");
            volumes += 1;
            posts.push(Post {
                bytes: post.bytes,
                ..Post::plain(&name, Vec::new())
            });
        } else {
            posts.push(post);
        }
    }
    assert!(volumes > 0, "{kind:?}: the set has a volume");
    posts
}

// Every renamed volume loses its first article, or never lists it, and
// the data file loses its third. A volume of one block no longer than an
// article and a block keeps its first article: past that hole it holds
// only the cut end of its one recovery packet, which nothing can verify
// and which no repair could use.
fn frontless(kind: Kind, size: Size, unlisted: bool, backwards: bool) -> Scenario {
    let posts = renamed(kind, size);
    let block = match size {
        Size::Small => ARTICLE,
        Size::Real => REAL_BLOCK as usize,
    };
    let volumes: Vec<u32> = posts
        .iter()
        .enumerate()
        .filter(|(_, post)| post.name.starts_with("k3x9q"))
        .filter(|(_, post)| post.bytes.len() > size.article() + block)
        .map(|(at, _)| at as u32)
        .collect();
    let mut scenario = Scenario::new(posts).framed(size.article()).lost(0, 2);
    for volume in volumes {
        scenario = if unlisted {
            scenario.unlisted(volume, 0)
        } else {
            scenario.lost(volume, 0)
        };
    }
    if backwards {
        scenario.backwards()
    } else {
        scenario
    }
}

fn summary(graph: &Outcome) -> String {
    format!(
        "{:?} {:?} {:?}",
        graph.status,
        graph.failure,
        graph.graph().rulings
    )
}

async fn repairs(kind: Kind, size: Size) {
    for unlisted in [false, true] {
        for backwards in [false, true] {
            let graph = run(&frontless(kind, size, unlisted, backwards)).await;
            assert!(
                graph.completed(),
                "{kind:?} {size:?} unlisted {unlisted} backwards {backwards}: {}",
                summary(&graph)
            );
            assert!(
                !graph.files.keys().any(|name| name.starts_with("k3x9q")),
                "{kind:?} {size:?} unlisted {unlisted} backwards {backwards}: a volume delivered as a file: {:?}",
                graph.files.keys().collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn a_renamed_par2_volume_with_no_front_repairs_from_past_its_hole() {
    repairs(Kind::Par2, Size::Small).await;
}

#[tokio::test]
async fn a_renamed_par3_volume_with_no_front_repairs_from_past_its_hole() {
    repairs(Kind::Par3, Size::Small).await;
}

#[tokio::test]
async fn a_renamed_par2_volume_with_no_front_repairs_across_a_restart() {
    restart_row(frontless(Kind::Par2, Size::Small, false, false)).await;
}

#[tokio::test]
async fn a_renamed_par3_volume_with_no_front_repairs_across_a_restart() {
    restart_row(frontless(Kind::Par3, Size::Small, false, false)).await;
}

// Real articles and blocks: the clean bytes past the lost first article
// begin inside a recovery packet, and the first packet start past them is
// most of a block away, far past the first FIRST_BYTES.
#[tokio::test]
async fn a_real_size_par2_volume_with_no_front_repairs_from_past_its_hole() {
    repairs(Kind::Par2, Size::Real).await;
}

#[tokio::test]
async fn a_real_size_par3_volume_with_no_front_repairs_from_past_its_hole() {
    repairs(Kind::Par3, Size::Real).await;
}

#[tokio::test]
async fn a_real_size_par2_volume_with_no_front_repairs_across_a_restart() {
    for backwards in [false, true] {
        restart_row(frontless(Kind::Par2, Size::Real, false, backwards)).await;
    }
}

#[tokio::test]
async fn a_real_size_par3_volume_with_no_front_repairs_across_a_restart() {
    for backwards in [false, true] {
        restart_row(frontless(Kind::Par3, Size::Real, false, backwards)).await;
    }
}

// A volume of another set, renamed and with no front, beside the set: its
// packets carry the other set's id, so the set does not take it, and the
// data file's lost article stays lost.
#[tokio::test]
async fn a_frontless_volume_of_another_set_is_not_taken() {
    for size in [Size::Small, Size::Real] {
        let (slice, blocks) = match size {
            Size::Small => (ARTICLE as u64, 4),
            Size::Real => (REAL_BLOCK, 3),
        };
        let data = payload(4 * size.article(), 92);
        let other = payload(4 * size.article(), 17);
        let posts = || {
            let set = |bytes: &[u8]| {
                par2_volumes_sliced("lantern", &[("lantern.bin", bytes)], slice, blocks, 1)
            };
            let mut ours = set(&data);
            let mut theirs = set(&other);
            let index = ours.remove(0);
            vec![
                Post::plain("lantern.bin", data.clone()),
                index,
                Post {
                    bytes: theirs.remove(1).bytes,
                    ..Post::plain("k3x9q0.bin", Vec::new())
                },
            ]
        };
        for backwards in [false, true] {
            let mut scenario = Scenario::new(posts())
                .framed(size.article())
                .lost(0, 2)
                .lost(2, 0);
            if backwards {
                scenario = scenario.backwards();
            }
            let graph = run(&scenario).await;
            assert!(
                !graph.completed(),
                "{size:?} backwards {backwards}: {}",
                summary(&graph)
            );
        }
    }
}

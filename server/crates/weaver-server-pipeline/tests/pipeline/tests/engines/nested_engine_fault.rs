// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An engine bound over a member of a nested namespace whose run fails on a
// fault in the format, which the root's recovery set cannot touch, or on
// bytes lost above it that nothing can bring back: the member is refused
// and delivered as it stands, and the job ends.

use std::io::{Cursor, Write};

use ::zip::ZipWriter;
use ::zip::write::SimpleFileOptions;

use super::harness::{ARTICLE, Outcome, Post, Scenario, par2_volumes_sliced, payload, try_run};
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::direct_store::multi_member_store_set;

// A zip of mixed deflated and stored entries whose `broken` stored entry
// carries a CRC its data does not match, the same in its local and central
// headers.
fn inner_zip(broken: Option<usize>) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let mut damaged = None;
    for index in 0..40 {
        let bytes = payload(3 * ARTICLE + index * 7, 40 + index as u8);
        let options = if index % 3 == 0 {
            SimpleFileOptions::default().compression_method(::zip::CompressionMethod::Deflated)
        } else {
            super::zip::stored()
        };
        writer
            .start_file(format!("assets/entry{index:02}.bin"), options)
            .unwrap();
        writer.write_all(&bytes).unwrap();
        if Some(index) == broken {
            damaged = Some(bytes);
        }
    }
    let mut bytes = writer.finish().unwrap().into_inner();
    let Some(member) = damaged else {
        return bytes;
    };
    let at = bytes
        .windows(32)
        .position(|window| window == &member[..32])
        .expect("the entry's data");
    for byte in &mut bytes[at + member.len() / 2..][..8] {
        *byte ^= 0x5a;
    }
    bytes
}

fn release(broken: Option<usize>) -> Vec<(&'static str, Vec<u8>)> {
    vec![
        ("lantern.nfo", b"lantern release notes\r\n".to_vec()),
        ("file_id.diz", b"lantern disk 1 of 1\r\n".to_vec()),
        ("inner.zip", inner_zip(broken)),
        ("lantern.url", b"[InternetShortcut]\r\nURL=x\r\n".to_vec()),
        ("lantern2.url", b"[InternetShortcut]\r\nURL=y\r\n".to_vec()),
    ]
}

fn cat(parts: Vec<Vec<Post>>) -> Vec<Post> {
    parts.into_iter().flatten().collect()
}

// Case A: the root PAR2 set covers a stored single-volume RAR (posted as
// `.part01.rar`, named `.rar` in the PAR2) and an nfo. The RAR holds a
// compressed zip one of whose stored entries fails its CRC under headers
// that agree. The zip engine refuses the zip; nothing the PAR2 set holds can
// mend it, so the zip is delivered as it stands and the job ends, whatever
// order the set and the payload arrive in, with or without the set's index.
#[tokio::test]
async fn a_zip_engine_refusing_a_nested_zip_beside_root_par2_ends_the_job() {
    let rar = multi_member_store_set(&release(Some(14)), 1).remove(0).1;
    let nfo = b"lantern outer notes\r\n".to_vec();
    let mut stalled = Vec::new();
    for slice in [ARTICLE as u64, 4096] {
        for indexed in [true, false] {
            for par2_first in [true, false] {
                for backwards in [false, true] {
                    let mut par2 = par2_volumes_sliced(
                        "lantern",
                        &[("lantern.rar", &rar), ("lantern.nfo", &nfo)],
                        slice,
                        3,
                        2,
                    );
                    if !indexed {
                        par2.remove(0);
                    }
                    let payload = vec![
                        Post::plain("lantern.nfo", nfo.clone()),
                        Post::plain("lantern.part01.rar", rar.clone()),
                    ];
                    let posts = if par2_first {
                        cat(vec![par2, payload])
                    } else {
                        cat(vec![payload, par2])
                    };
                    let mut scenario = Scenario::new(posts);
                    if backwards {
                        scenario = scenario.backwards();
                    }
                    let case = format!(
                        "slice={slice} indexed={indexed} par2_first={par2_first} backwards={backwards}"
                    );
                    match try_run(&scenario).await {
                        Ok(outcome) if outcome.status.is_some() => {}
                        Ok(outcome) => stalled.push(format!("{case}: {:?}", outcome.status)),
                        Err(stall) => {
                            stalled.push(format!("{case}: {:?} {}", stall.status, stall.state))
                        }
                    }
                }
            }
        }
    }
    assert!(stalled.is_empty(), "{}", stalled.join("\n"));
}

// An article of a stored RAR decodes one byte short, inside the compressed
// zip the RAR holds, as a scene post's did: the RAR's member `inner.zip`
// has a byte that never arrives, and the zip engine's run reads up to it.
// The RAR store closes that member only once the job is quiet, and the run
// waiting on it kept the job from ever being quiet. With no recovery set,
// or one that cannot rebuild the byte, the run hears the byte is lost for
// good and the job ends; with one that can, it is repaired.
async fn short_article_in_nested_zip(par2: Option<u32>) -> Outcome {
    let rar = multi_member_store_set(&release(None), 1).remove(0).1;
    let mut posts = vec![Post::plain("lantern.part01.rar", rar.clone())];
    if let Some(blocks) = par2 {
        posts.extend(par2_volumes_sliced(
            "lantern",
            &[("lantern.rar", &rar)],
            4096,
            blocks,
            1,
        ));
    }
    let article = 1024;
    let middle = (rar.len() / article / 2) as u32;
    let scenario = Scenario::new(posts).framed(article).short(0, middle, 1);
    try_run(&scenario)
        .await
        .unwrap_or_else(|stall| panic!("par2={par2:?}: {:?} {}", stall.status, stall.state))
}

#[tokio::test]
async fn a_short_article_inside_a_nested_zip_leaves_the_job_partial() {
    let outcome = short_article_in_nested_zip(None).await;
    // Nothing can rebuild the lost byte: the zip is kept as it came, and
    // the furniture delivered whole beside it leaves the job delivered in
    // part.
    assert_eq!(outcome.status, Some(JobStatus::Partial));
    assert_eq!(outcome.graph().delivery.damaged, ["inner.zip"]);
}

#[tokio::test]
async fn a_short_article_inside_a_nested_zip_is_repaired_by_one_block() {
    let outcome = short_article_in_nested_zip(Some(1)).await;
    assert_eq!(outcome.status, Some(JobStatus::Complete));
    assert!(!outcome.files.contains_key("lantern beacon/inner.zip"));
}

#[tokio::test]
async fn a_short_article_inside_a_nested_zip_is_repaired() {
    let outcome = short_article_in_nested_zip(Some(8)).await;
    assert_eq!(outcome.status, Some(JobStatus::Complete));
    assert!(!outcome.files.contains_key("lantern beacon/inner.zip"));
}

// The scene post's shape: a stored RAR, posted as `.part01.rar` and named
// `.rar` in the root PAR2 set, which binds it by content, holds a
// compressed zip, and one RAR article decodes a byte short inside the zip.
// The set has blocks enough. Whether the set comes before the payload or
// after it ends, with or without its index, its recovery volume still
// downloading as the RAR closes, or interleaved with it, the byte is
// rebuilt and every entry of the zip is delivered whole: the run over the
// zip never hears the byte lost while the set can still rule.
#[tokio::test]
async fn a_short_article_inside_a_nested_zip_is_repaired_in_every_order() {
    let rar = multi_member_store_set(&release(None), 1).remove(0).1;
    let article = 1024;
    let middle = (rar.len() / article / 2) as u32;
    let mut wrong = Vec::new();
    for par2_first in [false, true] {
        for indexed in [true, false] {
            for schedule in ["plain", "backwards", "together"] {
                let mut par2 = par2_volumes_sliced("lantern", &[("lantern.rar", &rar)], 4096, 8, 1);
                if !indexed {
                    par2.remove(0);
                }
                let sets = par2.len() as u32;
                let posted = vec![Post::plain("lantern.part01.rar", rar.clone())];
                let (posts, at) = if par2_first {
                    (cat(vec![par2, posted]), sets)
                } else {
                    (cat(vec![posted, par2]), 0)
                };
                let mut scenario = Scenario::new(posts).framed(article).short(at, middle, 1);
                scenario = match schedule {
                    "backwards" => scenario.backwards(),
                    "together" => scenario.together(&(0..=sets).collect::<Vec<_>>()),
                    _ => scenario,
                };
                let case = format!("par2_first={par2_first} indexed={indexed} {schedule}");
                let outcome = match try_run(&scenario).await {
                    Ok(outcome) => outcome,
                    Err(stall) => {
                        wrong.push(format!(
                            "{case}: stalled {:?} {}",
                            stall.status, stall.state
                        ));
                        continue;
                    }
                };
                let whole = (0..40).all(|index| {
                    outcome
                        .files
                        .get(&format!("lantern beacon/assets/entry{index:02}.bin"))
                        .is_some_and(|bytes| {
                            *bytes == payload(3 * ARTICLE + index * 7, 40 + index as u8)
                        })
                });
                if outcome.status != Some(JobStatus::Complete) || !whole {
                    wrong.push(format!("{case}: {:?} whole={whole}", outcome.status));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

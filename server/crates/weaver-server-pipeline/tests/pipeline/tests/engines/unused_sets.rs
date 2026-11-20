// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A posted recovery set that ends having bound no file says so once, as a
// job event naming the set and the reason.

use super::harness::{ARTICLE, JOB, Post, Scenario, par2_set, par2_volumes, payload, try_run};
use weaver_server_base::events::model::{PipelineEvent, recovery_set_unused_message};

// The messages of every unused-set event the run sent for the job.
fn unused(events: &[PipelineEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            PipelineEvent::RecoverySetUnused {
                job_id,
                set,
                reason,
            } if *job_id == JOB => Some(recovery_set_unused_message(set, reason)),
            _ => None,
        })
        .collect()
}

// The index damaged and each volume lost past its first article: the set is
// known from the recovery blocks at the volumes' heads, but no packet says
// what it describes, so it bound nothing. The set is called what the first
// file its object holds is, as every other line about the set calls it.
#[tokio::test]
async fn a_set_whose_index_never_arrived_says_so() {
    let data = payload(6 * ARTICLE, 47);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_volumes("lantern", &[("lantern.bin", &data)], 4, 2));
    let articles: Vec<u32> = posts
        .iter()
        .map(|post| post.bytes.len().div_ceil(ARTICLE) as u32)
        .collect();
    let mut scenario = Scenario::new(posts);
    for ordinal in 0..articles[1] {
        scenario = scenario.damaged(1, ordinal);
    }
    for (file, count) in articles.iter().enumerate().skip(2) {
        for ordinal in 1..*count {
            scenario = scenario.lost(file as u32, ordinal);
        }
    }
    let graph = try_run(&scenario).await.expect("the job settles");
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        unused(&graph.events),
        vec![
            "recovery set lantern.vol0+2.par2 was not used: its index never arrived whole"
                .to_owned()
        ]
    );
}

// A set that bound its file sends no such event.
#[tokio::test]
async fn a_bound_set_sends_no_unused_event() {
    let data = payload(4 * ARTICLE, 53);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let graph = try_run(&Scenario::new(posts))
        .await
        .expect("the job settles");
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(unused(&graph.events), Vec::<String>::new());
}

// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod rar {
    use std::collections::BTreeMap;

    use super::super::harness::{JOB, Scenario, admit, collect};
    use super::super::recovery_actor::{drive_to_finish, serve_queued};
    use crate::jobs::model::JobStatus;
    use crate::pipeline::Pipeline;
    use crate::pipeline::tests::{
        job_status_for_assert, pump_pipeline_runtime_queues, retire_pipeline_database,
    };
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

    use super::super::rar_fixtures::posts;
    use super::super::rar_fixtures::store::{MEMBER, SECRET, body};
    use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;
    use crate::pipeline::tests::direct_store::single_member_store_set;

    // Articles of 60 bytes over a one-volume set: the one in the middle,
    // `sent_back`, holds `MEMBER`'s bytes only.
    const FRAME: usize = 60;

    // One volume of `MEMBER`, enciphered under `password` when there is one,
    // with `spoiled` the copy of the article at `sent_back` a server holds that
    // is not the posted bytes, though it passes its own check. One set stands
    // behind every copy: each enciphering draws its own salt.
    fn sent_back_scenario(
        posted: &[(String, Vec<u8>)],
        password: Option<&str>,
        sent_back: u32,
        spoiled: bool,
    ) -> Scenario {
        let mut volumes = posted.to_vec();
        let at = sent_back as usize * FRAME;
        if spoiled {
            volumes[0].1[at..at + FRAME]
                .iter_mut()
                .for_each(|byte| *byte ^= 0x5a);
        }
        let scenario = Scenario::new(posts(volumes)).framed(FRAME);
        match password {
            Some(password) => scenario.password(password),
            None => scenario,
        }
    }

    // Whether `MEMBER` of the job's store is closed.
    fn member_closed(pipeline: &Pipeline) -> bool {
        let tree = pipeline.graph_jobs[&JOB].tree();
        tree.child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))
            .and_then(|(store, _)| tree.namespace(store))
            .is_some_and(|namespace| {
                namespace
                    .members()
                    .any(|member| member.name == MEMBER && member.closed)
            })
    }

    // The articles come in order, `first`'s copy of each: every one with
    // `closed`, so `MEMBER` closes, or else up to and with the one in the
    // middle. Then that one is sent back for another copy, as the decoder sends
    // back an article its neighbours refute, and `again`'s copy comes; then the
    // rest. What the job delivers once it ends, and how it ends.
    pub async fn sent_back_once(
        sealing: Sealing,
        first_spoiled: bool,
        closed: bool,
    ) -> (bool, Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
        let body = body();
        let (posted, password) = match sealing {
            Sealing::Stored => (single_member_store_set(MEMBER, &body, 1), None),
            Sealing::Enciphered { keyed } => (
                encrypted_store_set(MEMBER, &body, 1, SECRET, Some(SECRET), keyed),
                Some(SECRET),
            ),
        };
        let articles = posted[0].1.len().div_ceil(FRAME) as u32;
        let sent_back = articles / 2;
        let first = sent_back_scenario(&posted, password, sent_back, first_spoiled);
        let again = sent_back_scenario(&posted, password, sent_back, !first_spoiled);
        let held = if closed { articles } else { sent_back + 1 };
        let (_temp, mut pipeline, complete_dir) = admit(&first).await;
        for ordinal in 0..held {
            serve_queued(&mut pipeline, &first, (0, ordinal)).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        let was_closed = member_closed(&pipeline);
        pipeline.graph_article_sent_back(SegmentId {
            file_id: NzbFileId {
                job_id: JOB,
                file_index: 0,
            },
            segment_number: sent_back,
        });
        serve_queued(&mut pipeline, &again, (0, sent_back)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        for ordinal in held..articles {
            serve_queued(&mut pipeline, &again, (0, ordinal)).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        drive_to_finish(&mut pipeline, &again).await;
        let status = job_status_for_assert(&pipeline, JOB);
        let mut files = BTreeMap::new();
        collect(&complete_dir, &complete_dir, &mut files);
        retire_pipeline_database(pipeline).await;
        (was_closed, status, files)
    }

    // How the set's one member is posted: stored as it is, or enciphered, its
    // checksum either the member's CRC32 or one keyed by the password.
    #[derive(Clone, Copy)]
    pub enum Sealing {
        Stored,
        Enciphered { keyed: bool },
    }
}

pub mod sevenz {
    use std::collections::BTreeMap;

    use super::super::harness::{JOB, Post, Scenario, admit, collect};
    use super::super::recovery_actor::{drive_to_finish, serve_queued};
    use crate::jobs::model::JobStatus;
    use crate::pipeline::Pipeline;
    use crate::pipeline::tests::{
        job_status_for_assert, pump_pipeline_runtime_queues, retire_pipeline_database,
    };
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

    use super::super::sevenz_fixtures::members;
    use super::super::sevenz_fixtures::store::{FRAME, stored};

    const SENT_BACK: u32 = 2;

    // The stored container `posted`, enciphered under `password` when there is
    // one, and with `spoiled` the copy of `SENT_BACK` a server holds that is not
    // the posted bytes, though it passes its own check. One container stands
    // behind every copy: each enciphering draws its own salt and vector.
    fn sent_back_scenario(posted: &[u8], password: Option<&str>, spoiled: bool) -> Scenario {
        let mut bytes = posted.to_vec();
        let at = SENT_BACK as usize * FRAME;
        if spoiled {
            bytes[at..at + FRAME]
                .iter_mut()
                .for_each(|byte| *byte ^= 0x5a);
        }
        let scenario = Scenario::new(vec![Post::plain("lantern.7z", bytes)]).framed(FRAME);
        match password {
            Some(password) => scenario.password(password),
            None => scenario,
        }
    }

    // Whether `ember.mkv` of the job's store is closed, its checksum stated.
    fn ember_closed(pipeline: &Pipeline) -> bool {
        let tree = pipeline.graph_jobs[&JOB].tree();
        let (store, _) = tree
            .child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))
            .expect("the store is bound");
        tree.namespace(store)
            .unwrap()
            .members()
            .any(|member| member.name == "ember.mkv" && member.closed && member.crc32.is_some())
    }

    // The articles come in order, `first`'s copy of each: every one with
    // `closed`, so `ember.mkv` closes, or else all but the one that holds its
    // last bytes. Then `SENT_BACK` is sent back for another copy, as the decoder
    // sends back an article its neighbours refute, and `again`'s copy comes;
    // then the rest. The store says the member's bytes from there again, so its
    // own checksum fold no longer covers them all. What the job delivers once
    // it ends, and how it ends.
    pub async fn sent_back_once(
        password: Option<&str>,
        first_spoiled: bool,
        closed: bool,
    ) -> (bool, Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
        let posted = stored(password);
        let first = sent_back_scenario(&posted, password, first_spoiled);
        let again = sent_back_scenario(&posted, password, !first_spoiled);
        let articles = first.posts[0].bytes.len().div_ceil(FRAME) as u32;
        // `ember.mkv` ends 32 + its length into the container.
        let last = (32 + members()[0].1.len() - 1) as u32 / FRAME as u32;
        let held = if closed { articles } else { last };
        let (_temp, mut pipeline, complete_dir) = admit(&first).await;
        for ordinal in 0..held {
            serve_queued(&mut pipeline, &first, (0, ordinal)).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        let was_closed = ember_closed(&pipeline);
        pipeline.graph_article_sent_back(SegmentId {
            file_id: NzbFileId {
                job_id: JOB,
                file_index: 0,
            },
            segment_number: SENT_BACK,
        });
        serve_queued(&mut pipeline, &again, (0, SENT_BACK)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
        for ordinal in held..articles {
            serve_queued(&mut pipeline, &again, (0, ordinal)).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
        }
        drive_to_finish(&mut pipeline, &again).await;
        let status = job_status_for_assert(&pipeline, JOB);
        let mut files = BTreeMap::new();
        collect(&complete_dir, &complete_dir, &mut files);
        retire_pipeline_database(pipeline).await;
        (was_closed, status, files)
    }
}

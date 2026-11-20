// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::queries::HistoryQueries;
use crate::torrents::queries::TorrentsQueries;

// A torrent that cannot go on reaches the finished jobs as failed, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_torrent_reaches_the_finished_jobs() {
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let mut changes = state.subscribe_job_changes();
    changes.borrow_and_update();
    rig.service.fail(job_id, "no room".into()).await;
    loop {
        let jobs = state.list_jobs();
        if jobs.iter().any(|job| {
            job.job_id == job_id
                && job.status
                    == JobStatus::Failed {
                        error: "no room".into(),
                    }
        }) {
            assert_eq!(jobs.len(), 1);
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(rig.service.torrent(job_id).await.is_none());
}

// The attributes a torrent is added with stay with its job, from the queue
// to history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_keeps_the_attributes_it_was_added_with_into_history() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let metadata = vec![
        ("rss.feed_id".to_string(), "4".to_string()),
        ("tier".to_string(), "gold".to_string()),
    ];
    let job_id = rig
        .service
        .add_opened(AddRequest {
            metadata: metadata.clone(),
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().metadata,
        metadata
    );
    let infos = rig.service.job_infos().await;
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].metadata, metadata);

    rig.service.remove(job_id, false, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    let kept: Vec<(String, String)> =
        serde_json::from_str(history.metadata.as_deref().unwrap()).unwrap();
    assert_eq!(kept, metadata);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_private_torrent_with_an_unmet_goal_stays_unless_the_removal_confirms() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let private = metainfo("p", &[("p/a.bin", &[7; 2000])], 1024, true);
    let job_id = rig
        .service
        .add_opened(AddRequest {
            policy_override: Some(SeedingPolicy {
                seed_time_counting: Default::default(),
                ratio_goal: Leaf::Set(1.0),
                seed_time_goal_minutes: Leaf::Inherit,
                inactive_goal_minutes: Leaf::Inherit,
                mode: Leaf::Set(SeedingGoalMode::Any),
                action: Leaf::Set(SeedingAction::Stop),
            }),
            ..request(AddSource::Metainfo(private))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.service.remove(job_id, false, false).await,
        Err(ActionError::UnmetGoal("ratio 0.00 of 1.00".into()))
    );
    rig.service.remove(job_id, false, true).await.unwrap();
    assert_eq!(rig.db.get_torrent(job_id).unwrap(), None);
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "cancelled");
    assert_eq!(history.protocol, Protocol::Torrent);

    // A public torrent with no goal leaves when asked.
    let public = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("q", 3))))
        .await
        .unwrap();
    rig.service.remove(public, true, false).await.unwrap();
    assert_eq!(
        rig.service.remove(public, false, false).await,
        Err(ActionError::NotFound(public))
    );
}

use super::*;
use std::os::unix::fs::MetadataExt;

/// One file the probe found, with the two numbers that answer everything:
/// `dev` says which filesystem it is on, `ino` says whether a later file is
/// the *same* file (a rename) or a new one (a copy).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Found {
    path: PathBuf,
    dev: u64,
    ino: u64,
    len: u64,
}

fn roots() -> Option<(PathBuf, PathBuf)> {
    let intermediate = std::env::var_os("WEAVER_XDEV_INTERMEDIATE")?;
    let complete = std::env::var_os("WEAVER_XDEV_COMPLETE")?;
    Some((PathBuf::from(intermediate), PathBuf::from(complete)))
}

fn dev_of(path: &Path) -> u64 {
    std::fs::metadata(path)
        .unwrap_or_else(|error| panic!("stat {}: {error}", path.display()))
        .dev()
}

/// Every regular file under `root`, deepest-first order irrelevant.
fn walk(root: &Path) -> Vec<Found> {
    let mut out = Vec::new();
    let mut queue = vec![root.to_path_buf()];
    while let Some(dir) = queue.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                queue.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Ok(metadata) = std::fs::metadata(&path) else {
                continue;
            };
            out.push(Found {
                dev: metadata.dev(),
                ino: metadata.ino(),
                len: metadata.len(),
                path,
            });
        }
    }
    out.sort_by(|left, right| left.path.cmp(&right.path));
    out
}

fn matching(root: &Path, predicate: impl Fn(&str) -> bool) -> Vec<Found> {
    walk(root)
        .into_iter()
        .filter(|found| {
            found
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(&predicate)
        })
        .collect()
}

fn report(stage: &str, found: &[Found]) {
    if found.is_empty() {
        println!("XDEV {stage} <none>");
    }
    for entry in found {
        println!(
            "XDEV {stage} dev={} ino={} len={} path={}",
            entry.dev,
            entry.ino,
            entry.len,
            entry.path.display()
        );
    }
}

fn fresh(root: &Path, tag: &str) -> PathBuf {
    let dir = root.join(tag);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The headline: where the payload is born, and whether the publish is a
/// rename or a byte copy.
#[tokio::test]
async fn direct_store_payload_and_publish_across_two_filesystems() {
    let Some((intermediate_root, complete_root)) = roots() else {
        println!("XDEV skipped: set WEAVER_XDEV_INTERMEDIATE and WEAVER_XDEV_COMPLETE");
        return;
    };
    let expected = std::env::var("WEAVER_XDEV_EXPECT").unwrap_or_else(|_| "rename".to_string());

    let intermediate_dir = fresh(&intermediate_root, "payload-intermediate");
    let complete_dir = fresh(&complete_root, "payload-complete");
    let data_dir = fresh(&intermediate_root, "payload-data");
    let intermediate_dev = dev_of(&intermediate_dir);
    let complete_dev = dev_of(&complete_dir);
    println!("XDEV roots intermediate_dev={intermediate_dev} complete_dev={complete_dev}");
    assert_ne!(
        intermediate_dev, complete_dev,
        "the harness must mount the two roots on different filesystems, or this probe \
         proves nothing"
    );

    let member_name = "Silver.Horizon.S09E01.mkv";
    let payload: Vec<u8> = (0..24_000u32).map(|index| (index % 251) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
        data_dir,
        intermediate_dir.clone(),
        complete_dir.clone(),
        intermediate_dir.join("weaver.db"),
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        0,
        None,
    )
    .await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    // Small enough that held payload pages out to the scratch file, so the
    // working-data half of the split is observable too.
    pipeline.direct_store.set_holds_budget(64);

    let job_id = JobId(49001);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    println!("XDEV working_dir path={}", working_dir.display());

    // Payload before the header on volume 0, so something is held and paged.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;

    // (1) Where is the payload while the job is still downloading?
    let mid_intermediate = matching(&intermediate_dir, |name| name.ends_with(".direct.partial"));
    let mid_complete = matching(&complete_dir, |name| name.ends_with(".direct.partial"));
    report("partial-in-intermediate", &mid_intermediate);
    report("partial-in-complete", &mid_complete);

    // (2) Working data must be on the intermediate filesystem either way.
    let scratch = matching(&intermediate_dir, |name| name.starts_with(".weaver-holds."));
    let envelopes = matching(&intermediate_dir, |name| name.ends_with(".envelope"));
    report("holds-scratch", &scratch);
    report("envelopes", &envelopes);
    assert!(
        !scratch.is_empty(),
        "non-vacuity: the holds budget should have forced a scratch file"
    );
    for entry in scratch.iter().chain(envelopes.iter()) {
        assert_eq!(
            entry.dev,
            intermediate_dev,
            "working data must stay on the intermediate filesystem: {}",
            entry.path.display()
        );
    }
    assert!(
        matching(&complete_dir, |name| name.starts_with(".weaver-holds.")
            || name.ends_with(".envelope"))
        .is_empty(),
        "no working data may be written to the complete filesystem"
    );

    let partials: Vec<Found> = mid_intermediate
        .iter()
        .chain(mid_complete.iter())
        .cloned()
        .collect();
    assert!(
        !partials.is_empty(),
        "non-vacuity: nothing routed into a member partial"
    );
    let born_dev = partials[0].dev;
    println!(
        "XDEV verdict payload-born-on={}",
        if born_dev == complete_dev {
            "complete"
        } else {
            "intermediate"
        }
    );

    // Finish the download; the set finalizes and commits its member.
    for (file_index, segment_number) in in_order_arrivals(volumes.len()) {
        if (file_index, segment_number) == (0, 0) || (file_index, segment_number) == (0, 1) {
            continue;
        }
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    drain_rar_refreshes(&mut pipeline).await;
    let shape = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    assert!(
        shape.contains("Finalized"),
        "the set must finalize, got {shape}"
    );

    // (3) The committed member, immediately before completion publishes it.
    let committed: Vec<Found> = walk(&intermediate_dir)
        .into_iter()
        .chain(walk(&complete_dir))
        .filter(|found| {
            found
                .path
                .file_name()
                .is_some_and(|name| name == member_name)
        })
        .collect();
    report("member-after-commit", &committed);
    assert_eq!(
        committed.len(),
        1,
        "the member must exist in exactly one place before the move"
    );
    let before_move = committed[0].clone();
    assert_eq!(
        before_move.len,
        payload.len() as u64,
        "and hold the whole member"
    );

    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;

    // (4) The published member.
    let published: Vec<Found> = walk(&complete_dir)
        .into_iter()
        .filter(|found| {
            found
                .path
                .file_name()
                .is_some_and(|name| name == member_name)
        })
        .collect();
    report("member-after-publish", &published);
    assert_eq!(
        published.len(),
        1,
        "the job must publish exactly one member"
    );
    let after_move = published[0].clone();
    assert_eq!(
        std::fs::read(&after_move.path).unwrap(),
        payload,
        "and it must be byte-correct"
    );
    assert_eq!(
        after_move.dev, complete_dev,
        "the published member is on the complete filesystem by definition"
    );

    // The verdict, and it needs no instrumentation: a rename keeps the
    // inode, a copy cannot.
    let verdict = if after_move.dev == before_move.dev && after_move.ino == before_move.ino {
        "rename"
    } else {
        "copy"
    };
    println!(
        "XDEV verdict publish={verdict} before=(dev={},ino={}) after=(dev={},ino={})",
        before_move.dev, before_move.ino, after_move.dev, after_move.ino
    );

    // (5) Nothing of the payload may be left on the intermediate filesystem.
    let leftovers = walk(&intermediate_dir)
        .into_iter()
        .filter(|found| {
            found
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == member_name || name.ends_with(".direct.partial"))
        })
        .collect::<Vec<_>>();
    report("payload-left-in-intermediate", &leftovers);
    assert!(
        leftovers.is_empty(),
        "no payload may survive on the intermediate filesystem"
    );

    assert_eq!(
        verdict, expected,
        "publish verdict; set WEAVER_XDEV_EXPECT to the behaviour this tree guarantees"
    );
}

/// The failure path: a cancelled job leaves nothing behind on the complete
/// filesystem.
#[tokio::test]
async fn a_cancelled_job_cleans_what_it_wrote_on_the_complete_filesystem() {
    let Some((intermediate_root, complete_root)) = roots() else {
        println!("XDEV skipped: set WEAVER_XDEV_INTERMEDIATE and WEAVER_XDEV_COMPLETE");
        return;
    };

    let intermediate_dir = fresh(&intermediate_root, "cancel-intermediate");
    let complete_dir = fresh(&complete_root, "cancel-complete");
    let data_dir = fresh(&intermediate_root, "cancel-data");
    assert_ne!(dev_of(&intermediate_dir), dev_of(&complete_dir));

    let member_name = "Silver.Horizon.S09E02.mkv";
    let payload: Vec<u8> = (0..24_000u32).map(|index| (index % 241) as u8).collect();
    let volumes = single_member_store_set(member_name, &payload, 3);

    let (mut pipeline, _, _) = new_direct_pipeline_at_roots(
        data_dir,
        intermediate_dir.clone(),
        complete_dir.clone(),
        intermediate_dir.join("weaver.db"),
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        0,
        None,
    )
    .await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);

    let job_id = JobId(49002);
    let spec = direct_store_job_spec("Silver Horizon", &volumes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    // Mid-store: routed, not finished.
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    let staging = complete_dir
        .join(".weaver-staging")
        .join(job_id.0.to_string());
    println!(
        "XDEV cancel staging_exists_before={} path={}",
        staging.exists(),
        staging.display()
    );
    report("cancel-before-complete", &walk(&complete_dir));
    report("cancel-before-intermediate", &walk(&intermediate_dir));

    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJob {
            job_id,
            origin: crate::jobs::handle::CancellationOrigin::User,
            reply: reply_tx,
        })
        .await;
    reply_rx.await.unwrap().unwrap();

    // The cleanup is spawned; give it a bounded window to land.
    for _ in 0..200 {
        if !staging.exists() && !working_dir.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    report("cancel-after-complete", &walk(&complete_dir));
    report("cancel-after-intermediate", &walk(&intermediate_dir));
    println!(
        "XDEV cancel staging_exists_after={} working_dir_exists_after={}",
        staging.exists(),
        working_dir.exists()
    );
    assert!(
        !staging.exists(),
        "a cancelled job must not leave its staging directory on the complete filesystem"
    );
    assert!(
        walk(&complete_dir).is_empty(),
        "and must leave no bytes there at all: {:?}",
        walk(&complete_dir)
    );
    assert!(
        !working_dir.exists(),
        "nor its working directory on the intermediate filesystem"
    );
}

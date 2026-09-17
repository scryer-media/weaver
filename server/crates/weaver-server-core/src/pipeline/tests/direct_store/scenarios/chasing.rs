//! Chasing the compressed member of the existing RARLAB mixed-member fixture.

use super::*;
use crate::pipeline::direct_unpack::settings::{DirectUnpackGate, DirectUnpackSettings};
use crate::pipeline::direct_unpack::wiring::DirectUnpackRuntime;

#[tokio::test]
async fn mixed_chase_extracts_only_compressed_members_before_the_job_finishes() {
    run_chase(DirectStoreGate::Enabled, false).await;
}

#[tokio::test]
async fn conventional_chase_extracts_the_whole_set_before_the_job_finishes() {
    run_chase(DirectStoreGate::Disabled, false).await;
}

#[tokio::test]
async fn mixed_chase_invalidated_by_repair_uses_conventional_fallback() {
    run_chase(DirectStoreGate::Enabled, true).await;
}

#[tokio::test]
async fn solid_members_demote_direct_store_before_mixed_chase_can_skip_them() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let volumes = vec![("solid.rar".into(), rar5_fixture_bytes("rar5_solid.rar"))];
    let par2_bytes = par2_index_over_volumes(&volumes);
    let (spec, _) = par2_bearing_job_spec("Solid admission", &volumes, &par2_bytes);
    let job_id = JobId(41953);
    insert_active_job(&mut pipeline, job_id, spec).await;
    for segment in 0..2 {
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .expect("candidate set was admitted")
            .is_demoted()
    );
}

#[tokio::test]
async fn dropping_the_pipeline_wakes_a_partial_rar_chase() {
    drop_partial_chase(false).await;
}

#[tokio::test]
async fn dropping_an_unpolled_consumer_wakes_a_partial_rar_chase() {
    drop_partial_chase(true).await;
}

#[tokio::test]
async fn rar_chase_rejects_unacceptable_extension_before_writing_payload() {
    for gate in [DirectStoreGate::Disabled, DirectStoreGate::Enabled] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.direct_store.set_gate(gate);
        pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
            gate: DirectUnpackGate::Enabled,
        });
        pipeline
            .db
            .save_post_processing_settings(&crate::post_processing::model::PostProcessingSettings {
                unacceptable_extensions: vec!["bin".into()],
                ..Default::default()
            })
            .unwrap();
        let volumes = vec![(
            "mixed.rar".into(),
            rar5_fixture_bytes("rar5_multifile_lz.rar"),
        )];
        let job_id = JobId(41952);
        let par2_bytes = par2_index_over_volumes(&volumes);
        let (spec, _) = par2_bearing_job_spec("Blocked chase", &volumes, &par2_bytes);
        insert_active_job(&mut pipeline, job_id, spec).await;
        for segment in 0..2 {
            submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
        }
        if gate == DirectStoreGate::Enabled {
            pipeline.update_mixed_rar_chase(job_id, 0);
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                pipeline.reap_direct_unpack().await;
                if pipeline.direct_unpack.outcome(job_id, "mixed").is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let outcome = pipeline.direct_unpack.outcome(job_id, "mixed").unwrap();
        assert!(
            outcome
                .result
                .as_ref()
                .err()
                .expect("chase must reject the blocked member")
                .contains("unacceptable extension 'bin'")
        );
        assert!(!outcome.staging_dir.join("zeros_64k.bin").exists());
    }
}

#[tokio::test]
async fn rar_chase_settle_ignores_partial_topology_but_fences_reordered_parts() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    // Only the first article is published. The worker remains behind coverage;
    // the test exercises roster identity, not the contents of the second part.
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let volumes = vec![
        ("fixture.part1.rar".into(), bytes.clone()),
        ("fixture.part2.rar".into(), bytes),
    ];
    let job_id = JobId(41954);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Roster", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "fixture")
        .unwrap();
    assert_eq!(coverage.part_count(), 2);
    let armed = pipeline.direct_unpack.counters().armed;
    let topology = crate::jobs::assembly::ArchiveTopology {
        archive_type: crate::jobs::assembly::ArchiveType::Rar,
        volume_map: std::collections::HashMap::from([("fixture.part1.rar".into(), 0)]),
        complete_volumes: Default::default(),
        expected_volume_count: Some(2),
        members: Vec::new(),
        unresolved_spans: Vec::new(),
    };
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .set_archive_topology("fixture".into(), topology);
    pipeline.settle_direct_unpack_after_download(job_id);
    let survived = pipeline
        .direct_unpack
        .armed_coverage(job_id, "fixture")
        .is_some_and(|current| std::sync::Arc::ptr_eq(&current, &coverage));
    assert_eq!(pipeline.direct_unpack.counters().armed, armed);

    // A true rebind changes ordered identities even when the count is equal.
    pipeline
        .rar_sets
        .entry((job_id, "fixture".into()))
        .or_default()
        .volume_files = std::collections::BTreeMap::from([
        (0, "fixture.part2.rar".into()),
        (1, "fixture.part1.rar".into()),
    ]);
    pipeline.settle_direct_unpack_after_download(job_id);
    let fenced = coverage.abort_reason().is_some();
    pipeline.direct_unpack_shutdown("test teardown").await;
    assert!(
        survived,
        "an incomplete topology must preserve the original chase"
    );
    assert!(
        fenced,
        "same-count reordering must invalidate the original mapping"
    );
}

/// Land one article's bytes at their offset, as the decode path does: a
/// positioned write into the volume, never a truncating rewrite of it. The
/// chase worker is already reading the first article's bytes, and a truncate
/// under it hands it an empty header.
fn write_article_in_place(path: &Path, offset: usize, article: &[u8]) {
    use std::io::{Seek, Write};
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(offset as u64)).unwrap();
    file.write_all(article).unwrap();
    file.sync_data().unwrap();
}

/// Extraction can take a chase before the chase hears that its last part
/// finished: the commit that completes a part can start extraction before it
/// publishes the part's floor. The handoff has to tell the chase, or the
/// worker parks on bytes already on disk until the consumption deadline.
#[tokio::test]
async fn handing_a_chase_to_extraction_publishes_parts_that_finished_unheard() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let volumes = vec![("mixed.rar".to_string(), bytes.clone())];
    let job_id = JobId(41952);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Unheard completion", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .unwrap();
    assert!(!coverage.part_is_complete(0));

    // The last article reaches the disk and the assembly, and its notice
    // never reaches the chase.
    let (start, end) = article_extent(bytes.len(), 1, 2);
    write_article_in_place(&working_dir.join("mixed.rar"), start, &bytes[start..end]);
    let file = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .file_mut(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    file.commit_segment(1, (end - start) as u32).unwrap();
    assert!(file.is_complete());
    assert!(!coverage.part_is_complete(0));

    let disposition = pipeline.take_direct_unpack_disposition(job_id, "mixed");
    let crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(pending) = disposition
    else {
        panic!("the armed chase must be handed over while it runs");
    };
    assert!(coverage.part_is_complete(0));
    let joined = tokio::time::timeout(std::time::Duration::from_secs(20), pending.handle)
        .await
        .expect("the chase must finish once the handoff publishes the finished part");
    let outcome = joined.unwrap().unwrap();
    assert_eq!(
        outcome.extracted.len(),
        unrar_rs::RarArchive::open(std::io::Cursor::new(bytes))
            .unwrap()
            .len()
    );
}

async fn drop_partial_chase(transfer_to_consumer: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let volumes = vec![(
        "mixed.rar".to_string(),
        rar5_fixture_bytes("rar5_multifile_lz.rar"),
    )];
    let job_id = JobId(41951);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Partial chase", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .unwrap();
    assert!(!coverage.part_is_complete(0));
    if transfer_to_consumer {
        let disposition = pipeline.take_direct_unpack_disposition(job_id, "mixed");
        assert!(matches!(
            disposition,
            crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(_)
        ));
        drop(disposition);
    }
    drop(pipeline);
    assert_eq!(
        coverage.abort_reason().as_deref(),
        Some(if transfer_to_consumer {
            "direct-unpack consumer dropped"
        } else {
            "direct-unpack owner dropped"
        })
    );
}

async fn run_chase(gate: DirectStoreGate, invalidate_for_repair: bool) {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(gate);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let mut archive = unrar_rs::RarArchive::open(std::io::Cursor::new(bytes.clone())).unwrap();
    let mut expected = std::collections::BTreeMap::new();
    for index in 0..archive.len() {
        let name = archive.member_info(index).unwrap().name;
        let mut output = Vec::new();
        archive
            .by_index(index)
            .unwrap()
            .copy_to(&mut output)
            .unwrap();
        expected.insert(name, output);
    }
    let volumes = vec![("mixed.rar".to_string(), bytes)];
    let par2_bytes = par2_index_over_volumes(&volumes);
    let job_id = JobId(41950);
    let (spec, index_file_index) = par2_bearing_job_spec("Mixed chase", &volumes, &par2_bytes);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    for segment in 0..2 {
        submit_volume_article(&mut pipeline, job_id, &volumes, 0, segment).await;
    }
    let file = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .assembly
        .file(NzbFileId {
            job_id,
            file_index: 0,
        })
        .unwrap();
    let set_name = pipeline
        .classified_archive_set_name_for_file(job_id, file)
        .unwrap();
    if gate == DirectStoreGate::Enabled {
        assert!(!pipeline.direct_store.set(job_id, 0).unwrap().is_demoted());
        pipeline.update_mixed_rar_chase(job_id, 0);
    } else {
        pipeline.try_arm_rar_chase(job_id, &set_name);
    }
    for _ in 0..3000 {
        pipeline.reap_direct_unpack().await;
        if pipeline.direct_unpack.outcome(job_id, &set_name).is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let outcome = pipeline
        .direct_unpack
        .outcome(job_id, &set_name)
        .unwrap_or_else(|| panic!(
            "RAR chase must finish while the PAR2 index is queued: armed={} latch={:?} coverage={:?}",
            pipeline.direct_unpack.is_armed(job_id, &set_name),
            pipeline.direct_unpack.latched_reason(job_id, &set_name),
            pipeline.direct_unpack.armed_coverage(job_id, &set_name).map(|coverage| (coverage.part_progress(0), coverage.abort_reason())),
        ));
    let chased: std::collections::BTreeSet<_> = outcome
        .result
        .as_ref()
        .unwrap()
        .extracted
        .iter()
        .cloned()
        .collect();
    if gate == DirectStoreGate::Enabled {
        assert_eq!(
            chased,
            std::collections::BTreeSet::from(["zeros_64k.bin".to_string()])
        );
    } else {
        assert_eq!(chased, expected.keys().cloned().collect());
    }
    let chased_bytes: u64 = chased.iter().map(|name| expected[name].len() as u64).sum();
    assert_eq!(outcome.total_bytes, chased_bytes);
    assert_eq!(outcome.completed_bytes, chased_bytes);
    assert_eq!(
        std::fs::read(outcome.staging_dir.join("zeros_64k.bin")).unwrap(),
        expected["zeros_64k.bin"]
    );
    assert_eq!(
        outcome.staging_dir.join("hello.txt").exists(),
        gate == DirectStoreGate::Disabled
    );
    assert_eq!(
        outcome.staging_dir.join("second.txt").exists(),
        gate == DirectStoreGate::Disabled
    );
    assert_eq!(
        working_dir.join("mixed.rar").exists(),
        gate == DirectStoreGate::Disabled
    );

    if invalidate_for_repair {
        pipeline.prepare_direct_unpack_for_par3_repair(job_id);
        pipeline.release_direct_unpack_after_repair(job_id);
        assert!(
            pipeline
                .direct_unpack
                .outcome(job_id, &set_name)
                .unwrap()
                .tainted,
            "repair must make speculative output unusable"
        );
    } else if gate == DirectStoreGate::Enabled {
        // A completed, verified chase no longer needs a decoder. Reducing
        // the allowance now must not make the tolerated ticket reopen and
        // reject the archive's dictionary instead of installing its output.
        let mut limits = (*pipeline.extraction_limits).clone();
        limits.max_memory_bytes = 1;
        pipeline.extraction_limits = std::sync::Arc::new(limits);
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: index_file_index,
        },
        0,
        0,
        &par2_bytes,
        "silver.horizon.par2",
        None,
    )
    .await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    state.download_queue = crate::DownloadQueue::new();
    state.recovery_queue = crate::DownloadQueue::new();
    pipeline.check_job_completion(job_id).await;
    drain_rar_refreshes(&mut pipeline).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        drive_extractions_to_terminal(&mut pipeline, job_id, 64),
    )
    .await
    .unwrap_or_else(|_| panic!("{}", debug_job_state(&pipeline, job_id)));
    let output_root = complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Mixed chase"));
    for (name, bytes) in expected {
        assert_eq!(
            std::fs::read(output_root.join(&name)).unwrap(),
            bytes,
            "{name}"
        );
    }
    assert_eq!(
        pipeline.direct_unpack.counters().consumed,
        u64::from(!invalidate_for_repair)
    );
    assert_eq!(
        pipeline.direct_unpack.counters().discarded,
        u64::from(invalidate_for_repair)
    );
    assert!(
        !pipeline
            .direct_unpack_staging_dir(job_id, &set_name)
            .exists()
    );
}

/// A set demoted on its first volume's header must not take its siblings'
/// outstanding downloads with it.
///
/// The whole-set refetch fallback runs when nothing was ever routed, and it is
/// the only demotion arm that touches every volume of the set at once. The
/// volumes behind the one that demoted have been dispatched for nothing yet:
/// their articles are still queued, uncommitted and nobody else's, and the
/// refetch's own rule — requeue what the deleted routed storage owned, leave
/// everything else to its owner — must read a queued article as owned by the
/// queue rather than as work to drop. If it does not, the pass ends with an
/// empty queue and files nothing ever attempted, and the completion verdict
/// blames retries that never ran.
#[tokio::test]
async fn a_demotion_on_the_first_volume_leaves_the_rest_of_the_set_queued() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let bytes = rar5_fixture_bytes("rar5_solid.rar");
    let volumes = vec![
        ("archive.part1.rar".to_string(), bytes.clone()),
        ("archive.part2.rar".to_string(), bytes.clone()),
        ("archive.part3.rar".to_string(), bytes),
    ];
    let job_id = JobId(41971);
    insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Ineligible member demotion", &volumes),
    )
    .await;
    assert_eq!(
        peek_queued_segments(&mut pipeline, job_id),
        vec![(0, 0), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1)],
        "every article is queued before the pass dispatches anything"
    );

    // Only the first volume is dispatched. Its header names a member the
    // router cannot route, and nothing was routed before it, so the set takes
    // the whole-set refetch fallback rather than a reconstruction sweep.
    for segment in 0..2 {
        dispatch_and_submit(&mut pipeline, job_id, &volumes, 0, segment, 2).await;
    }
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .expect("candidate set was admitted")
            .is_demoted(),
        "the ineligible member must demote the set"
    );

    let queued = peek_queued_segments(&mut pipeline, job_id);
    for expected in [(1, 0), (1, 1), (2, 0), (2, 1)] {
        assert!(
            queued.contains(&expected),
            "volume article {expected:?} was never attempted and must still be queued, \
             queue holds {queued:?}"
        );
    }
}

/// A demotion's refetch must not drain a retry that is still in flight for
/// another article of the job.
///
/// The refetch requeues the articles the deleted routed storage owned. Those
/// are fresh work, not re-entering retries, so they must leave the pending-retry
/// counters alone. Draining the job-wide counter for them cancels the real
/// retry sleeping for a different article, the pass-end gate reads the job as
/// having nothing left, and the job fails before that retry ever fires.
#[tokio::test]
async fn a_demotion_refetch_keeps_another_articles_scheduled_retry_pending() {
    let volumes = demotion_fixture_volumes("Copper.Lantern.S02E03.mkv");
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41972);
    // Volume 2 never arrived: one of its articles was dispatched, failed, and
    // is sleeping on its retry delay, out of the queue and booked as pending.
    let retrying = SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 2,
        },
        segment_number: 0,
    };
    let mut retry_work = None;

    let (mut pipeline, _, _) =
        demote_mid_download(&temp_dir, job_id, &volumes, |pipeline, working_dir| {
            // No envelopes, so reconstruction has nothing to rebuild and the
            // routed volumes' committed articles go back on the wire.
            for volume_index in 0..2 {
                std::fs::remove_file(
                    working_dir.join(format!("silver.horizon.f0.vol{volume_index:05}.envelope")),
                )
                .unwrap();
            }
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            let (mut taken, kept): (Vec<_>, Vec<_>) = state
                .download_queue
                .drain_all()
                .into_iter()
                .partition(|work| work.segment_id == retrying);
            for work in kept {
                state.download_queue.push(work);
            }
            retry_work = taken.pop();
            pipeline.note_retry_scheduled(retrying);
        })
        .await;
    let retry_work = retry_work.expect("the retrying article was queued");

    let queued = peek_queued_segments(&mut pipeline, job_id);
    for expected in [(0, 0), (0, 1), (1, 0)] {
        assert!(
            queued.contains(&expected),
            "the refetch must requeue committed article {expected:?}, queue holds {queued:?}"
        );
    }
    assert!(
        !queued.contains(&(2, 0)),
        "the retrying article is not the refetch's to requeue, queue holds {queued:?}"
    );
    assert_eq!(
        pipeline.pending_retries_by_job.get(&job_id).copied(),
        Some(1),
        "the refetch's fresh work must not drain the job's pending retry"
    );
    assert_eq!(
        pipeline.pending_retries_by_segment.get(&retrying).copied(),
        Some(1),
        "the retrying article must still be booked as a pending retry"
    );

    // When the retry fires it drains its own booking and re-enters the queue.
    pipeline.requeue_retry_work(retry_work);
    assert!(!pipeline.pending_retries_by_job.contains_key(&job_id));
    assert!(!pipeline.pending_retries_by_segment.contains_key(&retrying));
    assert!(peek_queued_segments(&mut pipeline, job_id).contains(&(2, 0)));
}

/// The block size the recovery set below describes its volume on: two blocks
/// over the fixture, so damage in the first leaves a second the set could
/// still vouch for.
const DAMAGE_SLICE: u64 = 128;

/// A single-volume RAR chase armed on its first article, then completed by a
/// second article the chase never hears about, with a real recovery set whose
/// grid holds a Damaged verdict for the volume's first block.
///
/// Returns the pipeline, the working directory and the volume bytes. The
/// chase is still armed and still ungated: what happens next is the test.
async fn armed_chase_completed_with_damaged_block_zero(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
) -> (Pipeline, std::path::PathBuf, Vec<u8>) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Disabled);
    pipeline.direct_unpack = DirectUnpackRuntime::with_settings(DirectUnpackSettings {
        gate: DirectUnpackGate::Enabled,
    });
    let bytes = rar5_fixture_bytes("rar5_multifile_lz.rar");
    let volumes = vec![("mixed.rar".to_string(), bytes.clone())];
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Damaged first block", &volumes),
    )
    .await;
    submit_volume_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .expect("armed on the first article");
    assert!(!coverage.part_is_complete(0));

    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    install_test_par2_runtime(
        &mut pipeline,
        job_id,
        build_repairable_par2_set_for_files(&[("mixed.rar", &bytes)], DAMAGE_SLICE, 1),
        &[],
    );

    // The last article reaches the disk and the assembly, and its notice
    // never reaches the chase.
    let (start, end) = article_extent(bytes.len(), 1, 2);
    write_article_in_place(&working_dir.join("mixed.rar"), start, &bytes[start..end]);
    let file = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .assembly
        .file_mut(file_id)
        .unwrap();
    file.commit_segment(1, (end - start) as u32).unwrap();
    assert!(file.is_complete());

    // The grid's verdicts, cut on the recovery set's own blocks: the first
    // block's bytes do not match what the set describes, the rest do.
    let plan = pipeline.par2_checkpoint_plan(job_id);
    let mut offset = 0usize;
    while offset < bytes.len() {
        let end = (offset + DAMAGE_SLICE as usize).min(bytes.len());
        let chunk = &bytes[offset..end];
        let mut crc32 = par2_rs::checksum::crc32(chunk);
        if offset == 0 {
            crc32 = !crc32;
        }
        pipeline.note_block_crc_segments_for_plan(
            file_id,
            &plan,
            offset as u64,
            chunk.len() as u64,
            crc32,
            true,
            false,
            &[weaver_yenc::Segment {
                file_offset: offset as u64,
                len: chunk.len() as u64,
                crc32,
            }],
        );
        offset = end;
    }
    pipeline
        .block_crcs
        .note_file_len(file_id, bytes.len() as u64);
    assert_eq!(
        pipeline.in_stream_chase_evidence(file_id),
        Some((Some(0), 0)),
        "non-vacuity: the recovery data has to report damage at byte zero"
    );
    assert!(!coverage.is_gated(), "nothing has published the damage yet");
    assert!(pipeline.direct_unpack_gated_sets(job_id).is_empty());
    (pipeline, working_dir, bytes)
}

/// A RAR volume that completes after its chase armed publishes what the
/// recovery data says about it at the completion seam — while the set is still
/// armed, so the completion check can see the gate, force the authoritative
/// PAR2 pass, and let the repair resume the chase.
#[tokio::test]
async fn a_rar_part_completing_after_arming_gates_the_chase_where_finalize_can_see_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41956);
    let (mut pipeline, _, _) =
        armed_chase_completed_with_damaged_block_zero(&temp_dir, job_id).await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .unwrap();

    pipeline
        .refresh_archive_state_for_completed_file(job_id, file_id, false)
        .await;

    assert!(
        coverage.part_is_complete(0),
        "the seam publishes completion"
    );
    assert!(
        coverage.is_gated(),
        "the seam publishes the damage the recovery data reports"
    );
    assert!(
        pipeline.direct_unpack.is_armed(job_id, "mixed"),
        "and the set is still armed, so the gate can be seen"
    );
    assert_eq!(
        pipeline.direct_unpack_gated_sets(job_id),
        vec!["mixed".to_string()],
        "finalize reads the gate from armed sets"
    );

    pipeline.direct_unpack_shutdown("test teardown").await;
}

/// The handoff to extraction publishes completion, never a gate. A gate raised
/// as the set leaves `armed` has nobody to lift it: the completion check and
/// both release paths read gates from armed sets only, and the worker would
/// park on a vouched prefix of zero until the consumption deadline.
#[tokio::test]
async fn handing_a_chase_to_extraction_never_raises_a_gate() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(41957);
    let (mut pipeline, _, bytes) =
        armed_chase_completed_with_damaged_block_zero(&temp_dir, job_id).await;
    let coverage = pipeline
        .direct_unpack
        .armed_coverage(job_id, "mixed")
        .unwrap();

    let disposition = pipeline.take_direct_unpack_disposition(job_id, "mixed");
    let crate::pipeline::direct_unpack::wiring::ChaseDisposition::Pending(pending) = disposition
    else {
        panic!("the armed chase must be handed over while it runs");
    };
    assert!(
        coverage.part_is_complete(0),
        "the handoff publishes completion"
    );
    assert!(
        !coverage.is_gated(),
        "a gate raised at handoff can never be lifted; the handoff must not raise one"
    );
    let joined = tokio::time::timeout(std::time::Duration::from_secs(20), pending.handle)
        .await
        .expect("the chase must finish once the handoff publishes the finished part");
    let outcome = joined.unwrap().unwrap();
    assert_eq!(
        outcome.extracted.len(),
        unrar_rs::RarArchive::open(std::io::Cursor::new(bytes))
            .unwrap()
            .len()
    );
}

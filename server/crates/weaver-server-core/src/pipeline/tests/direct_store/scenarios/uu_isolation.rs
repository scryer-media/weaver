//! Encoding-specific admission must not strand unrelated downloads.

use super::*;
use crate::pipeline::tests::decode_and_files::submit_uu_segment_named;

/// A UU part with 45 decoded bytes per line, a begin line only in the
/// opening article and an end marker only in the closing article.
fn uu_article(filename: &str, bytes: &[u8], ordinal: u32, count: usize) -> Vec<u8> {
    fn sextet(value: u8) -> u8 {
        if value & 63 == 0 {
            b'`'
        } else {
            (value & 63) + 32
        }
    }
    let mut out = Vec::new();
    if ordinal == 0 {
        out.extend_from_slice(format!("begin 644 {filename}\r\n").as_bytes());
    }
    for line in bytes.chunks(45) {
        out.push(sextet(line.len() as u8));
        for triple in line.chunks(3) {
            let a = triple[0];
            let b = triple.get(1).copied().unwrap_or(0);
            let c = triple.get(2).copied().unwrap_or(0);
            out.extend_from_slice(&[
                sextet(a >> 2),
                sextet((a << 4) | (b >> 4)),
                sextet((b << 2) | (c >> 6)),
                sextet(c),
            ]);
        }
        out.extend_from_slice(b"\r\n");
    }
    if ordinal as usize + 1 == count {
        out.extend_from_slice(b"`\r\nend\r\n");
    }
    out
}

/// A real local provider: the test exercises leases, workers, wire decoding,
/// assembly, archive extraction and final publication, then checks the bytes.
async fn download_archives(uu: bool, mixed: bool, low_space: bool) {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let payload: Vec<u8> = (0..98_304).map(|i| (i % 251) as u8).collect();
    let mut volumes = encrypted_store_set(
        "first.bin",
        &payload,
        3,
        "test-password",
        Some("test-password"),
        true,
    );
    for (i, (name, _)) in volumes.iter_mut().enumerate() {
        *name = format!("first.part{:02}.rar", i + 1);
    }
    let first_count = volumes.len();
    if mixed {
        let mut second = encrypted_store_set(
            "second.bin",
            &payload,
            3,
            "test-password",
            Some("test-password"),
            true,
        );
        for (i, (name, _)) in second.iter_mut().enumerate() {
            *name = format!("second.part{:02}.rar", i + 1);
        }
        volumes.extend(second);
    }
    let parts = 16;
    let mut spec = direct_store_job_spec_with_articles("Encoding isolation", &volumes, parts);
    spec.password = Some("test-password".to_string());
    let mut articles = HashMap::new();
    for (file_index, (filename, bytes)) in volumes.iter().enumerate() {
        let file_is_uu = uu && file_index < first_count;
        for ordinal in 0..parts as u32 {
            let (start, end) = article_extent(bytes.len(), ordinal, parts);
            let encoded = if file_is_uu {
                uu_article(filename, &bytes[start..end], ordinal, parts)
            } else {
                encode_article_part(
                    filename,
                    &bytes[start..end],
                    ordinal + 1,
                    parts as u32,
                    start as u64 + 1,
                    bytes.len() as u64,
                )
                .to_vec()
            };
            spec.files[file_index].segments[ordinal as usize].bytes = encoded.len() as u32;
            let mut wire = Vec::new();
            for line in encoded.split_inclusive(|byte| *byte == b'\n') {
                if line.first() == Some(&b'.') {
                    wire.push(b'.');
                }
                wire.extend_from_slice(line);
            }
            wire.extend_from_slice(b".\r\n");
            articles.insert(format!("<direct-{file_index}-{ordinal}@example.com>"), wire);
        }
    }
    spec.total_bytes = spec
        .files
        .iter()
        .flat_map(|f| &f.segments)
        .map(|s| u64::from(s.bytes))
        .sum();
    let articles = Arc::new(articles);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let articles = Arc::clone(&articles);
            clients.spawn(async move {
                let (reader, mut writer) = socket.into_split();
                writer.write_all(b"200 local fixture provider\r\n").await?;
                let mut lines = BufReader::new(reader).lines();
                while let Some(line) = lines.next_line().await? {
                    if line == "CAPABILITIES" {
                        writer
                            .write_all(b"101 Capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                            .await?;
                    } else if line.starts_with("GROUP ") {
                        writer
                            .write_all(b"211 10000 1 10000 alt.binaries.test\r\n")
                            .await?;
                    } else if let Some(id) = line.strip_prefix("BODY ") {
                        if let Some(body) = articles.get(id) {
                            if id == "<direct-0-0@example.com>" {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                            writer
                                .write_all(format!("222 0 {id}\r\n").as_bytes())
                                .await?;
                            writer.write_all(body).await?;
                        } else {
                            writer.write_all(b"430 No such article\r\n").await?;
                        }
                    } else if let Some(id) = line.strip_prefix("STAT ") {
                        let status = if articles.contains_key(id) { 223 } else { 430 };
                        writer
                            .write_all(format!("{status} 0 {id}\r\n").as_bytes())
                            .await?;
                    } else if line == "QUIT" {
                        break;
                    } else {
                        writer.write_all(b"500 Unknown command\r\n").await?;
                    }
                }
                Ok::<(), std::io::Error>(())
            });
        }
    });
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        30,
    )
    .await;
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".to_string(),
            port,
            tls: false,
            connect_timeout: Duration::from_secs(2),
            command_timeout: Duration::from_secs(2),
            ..Default::default()
        },
        30,
    )));
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_scratch_ceiling(16);
    pipeline.uu_spool_available_bytes_for_test = Some(Some(if low_space {
        pipeline.uu_spool_min_free_bytes - 1
    } else {
        u64::MAX
    }));
    let shared = pipeline.shared_state.clone();
    let (commands, receiver) = mpsc::channel(64);
    pipeline.cmd_rx = receiver;
    let handle = SchedulerHandle::new(commands, pipeline.event_tx.clone(), shared.clone());
    let task = tokio::spawn(async move {
        pipeline.run().await;
        pipeline
    });
    let job_id = JobId(58010);
    handle
        .add_job(
            job_id,
            spec,
            temp_dir.path().join("fixture.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    let output_dir = complete_dir.join("Encoding isolation");
    let result = wait_until(Duration::from_secs(30), || {
        // The destination can exist while the move worker is still reporting
        // completion. Wait for the actor's terminal state before shutdown.
        shared
            .get_job(job_id)
            .is_some_and(|job| job.status == JobStatus::Complete)
    })
    .await;
    handle.shutdown().await.unwrap();
    let pipeline = task.await.unwrap();
    server.abort();
    let _ = server.await;
    assert!(
        result.is_ok(),
        "uu={uu} mixed={mixed} low_space={low_space}: status={:?}, queue={}",
        job_status_for_assert(&pipeline, job_id),
        shared
            .metrics()
            .download_queue_depth
            .load(Ordering::Relaxed)
    );
    assert_eq!(
        std::fs::read(output_dir.join("first.bin")).unwrap(),
        payload
    );
    if mixed {
        assert_eq!(
            std::fs::read(output_dir.join("second.bin")).unwrap(),
            payload
        );
    }
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(pipeline.uu_spooled_bytes, 0);
    assert_eq!(pipeline.uu_parked_segments, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn yenc_archive_completes_below_the_uu_disk_reserve() {
    download_archives(false, false, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uu_archive_downloads_decodes_and_extracts() {
    download_archives(true, false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_uu_and_yenc_archives_complete_with_uu_admission_capped() {
    download_archives(true, true, true).await;
}

#[tokio::test]
async fn capped_uu_keeps_yenc_batches_and_trials_while_restricting_its_own_tail() {
    for (trial, unknown_capacity) in [(false, false), (true, false), (false, true), (true, true)] {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 8,
                medium_count: 4,
                large_count: 2,
            },
            30,
        )
        .await;
        let job_id = JobId(58011);
        // Enough work for multi-article leases after the ordinary 30-lane fair share.
        let mut spec = segmented_job_spec("Mixed leases", "yenc.bin", &[100; 128]);
        let mut uu_spec = segmented_job_spec("UU", "uu.txt", &[100; 3]);
        spec.total_bytes += uu_spec.total_bytes;
        spec.files.append(&mut uu_spec.files);
        insert_active_job(&mut pipeline, job_id, spec).await;
        let uu_file = NzbFileId {
            job_id,
            file_index: 1,
        };
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: uu_file,
                segment_number: 0,
            },
        );
        submit_uu_segment_named(&mut pipeline, uu_file, 0, b"prefix", false, false, "uu.txt").await;
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut queued = state.download_queue.drain_all();
        // Put the forbidden UU tail before its cursor and the unrelated work.
        queued.sort_by_key(|work| {
            if work.segment_id.file_id == uu_file {
                2 - work.segment_id.segment_number
            } else {
                3
            }
        });
        for work in queued {
            state.download_queue.push(work);
        }
        if unknown_capacity {
            pipeline.write_backlog_budget_bytes = 1;
            pipeline.uu_spool_available_bytes_for_test = Some(None);
            assert!(!pipeline.admit_uu_spill(100));
        } else {
            pipeline.uu_spool_max_segments = 0;
        }
        pipeline.hot_dispatch_job = Some(job_id);
        assert!(
            pipeline.job_has_dispatchable_work_for_test(job_id),
            "a blocked UU head must not hide queued yEnc from hot scheduling"
        );
        let pressure = pipeline.refresh_download_pressure();
        let lease = if trial {
            pipeline.try_lease_ip_replacement_trial_batch_for_test(job_id, 0)
        } else {
            pipeline.try_lease_initial_download_batch_for_test(job_id, pressure)
        }
        .expect("UU admission must leave yEnc work leasable");
        assert!(
            lease.works.len() > 1,
            "UU must not clamp unrelated batches to one"
        );
        assert!(
            lease
                .works
                .iter()
                .any(|work| work.segment_id.file_id.file_index == 0)
        );
        assert!(
            lease
                .works
                .iter()
                .any(|work| work.segment_id.file_id == uu_file)
        );
        assert!(
            lease
                .works
                .iter()
                .all(|work| work.segment_id.file_id != uu_file
                    || work.segment_id.segment_number == 1)
        );
        if !trial {
            let compatibility = lease.compatibility.clone();
            for work in lease.works {
                pipeline
                    .jobs
                    .get_mut(&job_id)
                    .unwrap()
                    .download_queue
                    .push(work);
            }
            let refill = pipeline
                .try_lease_refill_download_batch_for_test(job_id, compatibility, pressure)
                .expect("UU admission must leave yEnc refills leasable");
            assert!(refill.works.len() > 1);
            assert!(
                refill
                    .works
                    .iter()
                    .all(|work| work.segment_id.file_id != uu_file
                        || work.segment_id.segment_number == 1)
            );
        }
    }
}

#[tokio::test]
async fn capped_uu_hot_job_leaves_idle_capacity_for_a_yenc_peer() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        2,
    )
    .await;
    let hot = JobId(58013);
    let peer = JobId(58014);
    insert_active_job(
        &mut pipeline,
        hot,
        segmented_job_spec("UU hot", "uu.txt", &[100; 3]),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        peer,
        segmented_job_spec("yEnc peer", "yenc.bin", &[100; 32]),
    )
    .await;
    let file_id = NzbFileId {
        job_id: hot,
        file_index: 0,
    };
    take_queued_segment(
        &mut pipeline,
        hot,
        SegmentId {
            file_id,
            segment_number: 1,
        },
    );
    submit_uu_segment_named(&mut pipeline, file_id, 1, b"parked", false, false, "uu.txt").await;
    pipeline.uu_spool_max_segments = 0;
    // The missing cursor is already in flight. Only the forbidden UU tail
    // remains queued; it cannot use the second connection.
    take_queued_segment(
        &mut pipeline,
        hot,
        SegmentId {
            file_id,
            segment_number: 0,
        },
    );
    pipeline.active_download_connections = 1;
    pipeline.active_download_connections_by_job.insert(hot, 1);
    pipeline.active_downloads = 1;
    pipeline.active_downloads_by_job.insert(hot, 1);
    pipeline.hot_dispatch_job = Some(hot);
    pipeline.hot_dispatch_started_at = Some(Instant::now() - Duration::from_secs(60));
    pipeline.hot_dispatch_underfill_since = Some(Instant::now() - Duration::from_secs(60));
    pipeline.dispatch_downloads();
    assert_eq!(
        pipeline.active_download_connections_by_job.get(&peer),
        Some(&1)
    );
    assert_eq!(pipeline.jobs[&hot].download_queue.len(), 1);
}

#[tokio::test]
async fn yenc_tails_dispatch_after_scratch_demotion_below_uu_reserve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
        &temp_dir,
        BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        },
        30,
    )
    .await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    pipeline.direct_store.set_holds_budget(64);
    pipeline.direct_store.set_holds_scratch_ceiling(16);
    let job_id = JobId(58012);
    let volumes = encrypted_store_set(
        "demotion.bin",
        &[73; 2400],
        3,
        "test-password",
        Some("test-password"),
        true,
    );
    let mut spec = direct_store_job_spec("Demotion", &volumes);
    spec.password = Some("test-password".to_string());
    insert_active_job(&mut pipeline, job_id, spec).await;
    for (file_index, segment_number) in [(0, 0), (0, 1), (1, 1), (1, 0), (2, 0)] {
        take_queued_segment(
            &mut pipeline,
            job_id,
            SegmentId {
                file_id: NzbFileId { job_id, file_index },
                segment_number,
            },
        );
        submit_volume_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
    }
    assert!(
        format!("{:?}", pipeline.direct_store.sets_for(job_id))
            .contains("Demoted(HoldsScratchCeiling)")
    );
    assert!(pipeline.direct_demotion_in_flight.is_empty());
    assert!(pipeline.uu_files.is_empty());
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Extracting);
    pipeline.uu_spool_available_bytes_for_test = Some(Some(pipeline.uu_spool_min_free_bytes - 1));
    assert!(!pipeline.jobs[&job_id].download_queue.is_empty());
    pipeline.dispatch_downloads();
    assert!(
        pipeline.active_downloads > 0,
        "demotion must not strand yEnc tails"
    );
}

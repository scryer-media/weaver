use super::*;

#[tokio::test]
async fn decoded_catalog_rejects_invalid_frames_without_advancing_facts() {
    use crate::pipeline::extraction::catalog::{InputProjection, MemberProjection, MemberRange};
    use crate::pipeline::extraction::catalog::{MemberCatalog, decoded::DecodedCatalog};
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 4096, 8);
    let mut catalog = DecodedCatalog::new(
        generation.clone(),
        output.budget.try_reserve_memory(4096).unwrap(),
    );
    output
        .send_async(7, 8, || Event::File {
            path: "inner.7z".into(),
            length: None,
        })
        .await
        .unwrap();
    let mut declaration = receiver.recv().await.unwrap();
    declaration.member.owner.generation += 1;
    assert!(catalog.accept(&declaration).is_err());
    assert!(catalog.members().is_empty());
    declaration.member.owner.generation -= 1;
    assert_eq!(catalog.accept(&declaration).unwrap(), 0);
    assert!(catalog.accept(&declaration).is_err());
    drop(declaration);
    assert_eq!(catalog.length(0), None);
    assert_eq!(catalog.revision(), 1);
    output
        .send_async(7, 3, || Event::Bytes {
            offset: 0,
            bytes: bytes::Bytes::from_static(b"abc"),
        })
        .await
        .unwrap();
    let bytes = receiver.recv().await.unwrap();
    assert_eq!(catalog.accept(&bytes).unwrap(), 0);
    assert!(catalog.accept(&bytes).is_err());
    drop(bytes);
    for (length, valid) in [(2, false), (3, true)] {
        output
            .send_async(7, 0, || Event::DataComplete { length })
            .await
            .unwrap();
        assert_eq!(
            catalog.accept(&receiver.recv().await.unwrap()).is_ok(),
            valid
        );
    }
    assert_eq!(catalog.length(0), Some(3));
    assert_eq!(catalog.revision(), 2);
    assert_eq!(
        catalog
            .project_member(
                0,
                MemberRange {
                    offset: 0,
                    len: 3,
                    write_offset: 9
                }
            )
            .unwrap(),
        InputProjection::NotReversible
    );
    assert!(
        catalog
            .project_member(
                0,
                MemberRange {
                    offset: 0,
                    len: 4,
                    write_offset: 0
                }
            )
            .is_err()
    );
    assert!(
        catalog.close().is_err(),
        "data completion is not integrity proof"
    );
    output
        .send_async(7, 0, || Event::Integrity(Integrity::Verified))
        .await
        .unwrap();
    catalog.accept(&receiver.recv().await.unwrap()).unwrap();
    catalog.close().unwrap();
    generation.revoke();
    assert!(
        catalog.close().is_err(),
        "successful members do not outlive worker revocation"
    );
}

#[tokio::test]
async fn decoded_catalog_retention_is_separately_bounded() {
    use crate::pipeline::extraction::catalog::{MemberCatalog, decoded::DecodedCatalog};
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 4096, 8);
    let mut catalog = DecodedCatalog::new(
        generation,
        output.budget.try_reserve_memory(512 + 8).unwrap(),
    );
    for occurrence in 0..2 {
        output
            .send_async(occurrence, 8, || Event::File {
                path: "inner.7z".into(),
                length: Some(0),
            })
            .await
            .unwrap();
        assert_eq!(
            catalog.accept(&receiver.recv().await.unwrap()).is_ok(),
            occurrence == 0
        );
    }
    assert_eq!(catalog.members().len(), 1);
    assert_eq!(catalog.revision(), 1);
}

#[tokio::test]
async fn tracked_physical_member_stops_before_next_write_after_revocation() {
    for close_receiver in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let root = ExtractionRoot::open(dir.path()).unwrap();
        let (output, generation, mut receiver) = output(dir.path(), 4096, 8);
        let (ready, opened) = tokio::sync::oneshot::channel();
        let (resume, released) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let files = TrackedFiles {
                output: &output,
                root: &root,
            };
            let mut member = files.file(1, Path::new("leaf"), Some(1)).unwrap();
            ready.send(()).unwrap();
            released.blocking_recv().unwrap();
            assert_eq!(
                member.write(&[1]).unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            assert!(member.flush().is_err());
            assert!(member.metadata(Metadata::default()).is_err());
            assert!(member.finish_data().is_err());
            assert!(files.file(2, Path::new("later"), Some(0)).is_err());
        });
        opened.await.unwrap();
        if close_receiver {
            receiver.close();
        } else {
            generation.revoke();
        }
        resume.send(()).unwrap();
        worker.await.unwrap();
        assert_eq!(std::fs::metadata(dir.path().join("leaf")).unwrap().len(), 0);
        assert!(!dir.path().join("later").exists());
    }
}

fn output(
    root: &Path,
    bytes: u64,
    slots: usize,
) -> (ChannelOutput, Generation, mpsc::Receiver<Frame>) {
    let budget = super::super::tests::budget(root);
    let memory = budget.try_reserve_memory(bytes).unwrap();
    let (sender, receiver) = mpsc::channel(slots);
    let (output, generation) = ChannelOutput::new(
        StreamOwner {
            root: 7,
            node: 2,
            generation: 3,
        },
        memory,
        sender,
        Handle::current(),
        budget,
    )
    .unwrap();
    (output, generation, receiver)
}

#[tokio::test]
async fn stream_orders_data_and_proof_and_keeps_generation_revocable() {
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 4096, 2);
    let worker = tokio::task::spawn_blocking(move || {
        let mut file = output.file(11, Path::new("leaf.bin"), Some(2048)).unwrap();
        file.write_all(&[53; 2048]).unwrap();
        file.metadata(Metadata::default()).unwrap();
        file.finish_data()
            .unwrap()
            .integrity(Integrity::Verified)
            .unwrap();
    });
    let mut events = Vec::new();
    let mut proof = None;
    while let Some(frame) = receiver.recv().await {
        assert_eq!(
            frame.member,
            MemberId {
                owner: generation.owner,
                occurrence: 11
            }
        );
        assert!(!frame.lease.is_revoked());
        match frame.event {
            Event::File { path, length } => {
                assert_eq!(path, Path::new("leaf.bin"));
                assert_eq!(length, Some(2048));
                events.push("file");
            }
            Event::Bytes { offset, bytes } => {
                assert_eq!(offset, 0);
                assert_eq!(bytes.as_ref(), &[53; 2048]);
                events.push("bytes");
            }
            Event::Metadata(metadata) => {
                assert!(metadata.modified.is_none());
                events.push("metadata");
            }
            Event::DataComplete { length } => {
                assert_eq!(length, 2048);
                events.push("data");
            }
            Event::Integrity(Integrity::Verified) => {
                events.push("proof");
                proof = Some(frame.lease);
            }
            _ => panic!("unexpected output event"),
        }
    }
    worker.await.unwrap();
    assert_eq!(events, ["file", "bytes", "metadata", "data", "proof"]);
    generation.revoke();
    assert!(
        proof.unwrap().is_revoked(),
        "member proof cannot detach from archive failure"
    );
}

#[tokio::test]
async fn dequeue_retains_byte_admission_and_revocation_wakes_writer() {
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 1024, 8);
    output
        .send_async(1, 1024 - FRAME_OVERHEAD, || Event::Bytes {
            offset: 0,
            bytes: bytes::Bytes::from(vec![9; 1024 - FRAME_OVERHEAD]),
        })
        .await
        .unwrap();
    let flight = receiver.recv().await.unwrap();
    assert_eq!(output.admission.capacity.available_permits(), 0);
    let waiting = output.send_async(1, 0, || Event::DataComplete { length: 768 });
    tokio::pin!(waiting);
    assert_pending(waiting.as_mut());
    generation.revoke();
    assert_eq!(waiting.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    assert!(flight.lease.is_revoked());
}

fn assert_pending<T>(future: std::pin::Pin<&mut impl std::future::Future<Output = T>>) {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(future.poll(&mut context).is_pending());
}

#[tokio::test]
async fn completed_catalog_retains_revocation_without_pinning_stream_admission() {
    use crate::pipeline::extraction::session::{ArchiveSessions, InputVersion};

    let dir = tempfile::tempdir().unwrap();
    let budget = super::super::tests::budget(dir.path());
    let mut sessions = ArchiveSessions::new(7, 0);
    let node = sessions.register(None, InputVersion(1)).unwrap();
    let lease = sessions.start(node).unwrap();
    let (sender, mut receiver) = mpsc::channel(1);
    let (output, generation) = ChannelOutput::new(
        lease.owner,
        budget.try_reserve_memory(1024).unwrap(),
        sender,
        Handle::current(),
        Arc::clone(&budget),
    )
    .unwrap();
    let admission = Arc::downgrade(&output.admission);
    let mut catalog = crate::pipeline::extraction::catalog::decoded::DecodedCatalog::new(
        generation.clone(),
        budget.try_reserve_memory(4096).unwrap(),
    );
    sessions.attach_channel(generation.clone()).unwrap();
    output
        .send_async(0, 0, || Event::Directory {
            path: "empty".into(),
        })
        .await
        .unwrap();
    let frame = receiver.recv().await.unwrap();
    catalog.accept(&frame).unwrap();
    drop(output);
    catalog.close().unwrap();
    sessions.close(lease.owner).unwrap();
    assert!(sessions.ancestry_verified(lease.owner));
    assert!(admission.upgrade().is_some());
    drop(frame);
    assert!(admission.upgrade().is_none());
    // Both the catalog and closed session still retain their control handle.
    sessions.revoke(lease.owner).unwrap();
    sessions.joined(lease.owner).unwrap();
    assert!(generation.is_revoked());
    assert!(catalog.close().is_err());
}

#[tokio::test]
async fn receiver_closure_and_job_stop_wake_byte_capacity_waits() {
    for close_receiver in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let (output, generation, mut receiver) = output(dir.path(), 1024, 8);
        output
            .send_async(1, 1024 - FRAME_OVERHEAD, || Event::Bytes {
                offset: 0,
                bytes: bytes::Bytes::from(vec![3; 1024 - FRAME_OVERHEAD]),
            })
            .await
            .unwrap();
        let held = receiver.recv().await.unwrap();
        let waiting = output.send_async(1, 0, || Event::DataComplete { length: 0 });
        tokio::pin!(waiting);
        assert_pending(waiting.as_mut());
        if close_receiver {
            receiver.close();
        } else {
            output.budget.cancel();
            assert!(held.lease.is_revoked());
            assert!(generation.is_revoked());
        }
        assert!(waiting.await.is_err());
        drop(held);
    }
}

#[tokio::test]
async fn queue_capacity_wait_is_cancelled_without_receiver_drain() {
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 4096, 1);
    output
        .send_async(1, 5, || Event::Directory {
            path: "first".into(),
        })
        .await
        .unwrap();
    let waiting = output.send_async(2, 6, || Event::Directory {
        path: "second".into(),
    });
    tokio::pin!(waiting);
    assert_pending(waiting.as_mut());
    generation.revoke();
    assert_eq!(waiting.await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
    let frame = receiver.recv().await.unwrap();
    assert!(frame.lease.is_revoked());
    assert!(matches!(frame.event, Event::Directory { path } if path == Path::new("first")));
}

#[tokio::test]
async fn abandoned_member_revokes_earlier_successes_without_queueing_abort() {
    let dir = tempfile::tempdir().unwrap();
    let (output, generation, mut receiver) = output(dir.path(), 4096, 1);
    let worker = tokio::task::spawn_blocking(move || {
        let output = output.file(1, Path::new("incomplete"), Some(1)).unwrap();
        drop(output);
    });
    // No drain before joining: abort cannot wait on the occupied queue.
    worker.await.unwrap();
    assert!(generation.is_revoked());
    assert!(receiver.recv().await.unwrap().lease.is_revoked());
}

#[tokio::test]
async fn worker_failure_after_successful_member_revokes_its_proof() {
    for panic_worker in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (output, generation, mut receiver) = output(dir.path(), 4096, 8);
        let worker = tokio::task::spawn_blocking(move || {
            output.run(|output| -> io::Result<()> {
                output
                    .file(1, Path::new("empty"), Some(0))?
                    .finish_data()?
                    .integrity(Integrity::Verified)?;
                if panic_worker {
                    panic!("decoder failure after verified member");
                }
                Err(io::Error::other("archive trailer rejected"))
            })
        });
        let result = worker.await;
        if panic_worker {
            assert!(result.unwrap_err().is_panic());
        } else {
            assert!(result.unwrap().is_err());
        }
        assert!(generation.is_revoked());
        let mut saw_proof = false;
        while let Some(frame) = receiver.recv().await {
            assert!(frame.lease.is_revoked());
            saw_proof |= matches!(frame.event, Event::Integrity(Integrity::Verified));
        }
        assert!(saw_proof);
    }
}

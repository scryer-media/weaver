// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use weaver_graph::{Coverage, MemberId, NodeId};

use super::{READ_BACK, Verification};
use crate::pipeline::graph::hardware::Shutdown;
use weaver_server_disk::operations::disk::StorageKind;
use weaver_server_disk::operations::disk::reader::{SOLID_READERS, SPINNING_READERS};
use weaver_server_disk::operations::disk::volume::Volume;

fn check(path: std::path::PathBuf, length: u64, volume: Option<Arc<Volume>>) -> Verification {
    Verification {
        node: NodeId(1),
        member: MemberId(0),
        path,
        revision: (0, 0),
        length,
        stated: 0,
        coverage: Coverage::default(),
        folded: None,
        settled: None,
        volume,
        stop: Arc::default(),
        barrier: None,
    }
}

// A member's check made in turns of its file's volume reads what the
// check made without them reads, read for read, and comes to the same
// CRC, or the same error. On solid state it holds one turn across its
// reads; on a spinning volume each read takes the one turn and gives it
// back. Every turn is back once it returns.
#[test]
fn a_check_in_its_volumes_turns_reads_what_a_check_without_them_reads() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("member.bin");
    let length = 2 * READ_BACK + READ_BACK / 2 + 7;
    let body: Vec<u8> = (0..length).map(|at| (at * 31 % 251) as u8).collect();
    std::fs::write(&path, &body).unwrap();
    let shutdown = Shutdown::default();
    let run = |volume: Option<Arc<Volume>>, path: std::path::PathBuf| {
        let mut reads = Vec::new();
        let mut free = Vec::new();
        let watched = volume.clone();
        let verified = check(path, length, volume).run_with(&shutdown, |len| {
            reads.push(len);
            if let Some(volume) = &watched {
                free.push(volume.read_turns().turns().available_permits());
            }
        });
        let actual = verified.actual.map_err(|error| {
            matches!(
                error,
                crate::pipeline::graph::leaf::LeafError::Io { ref source, .. }
                    if source.kind() == std::io::ErrorKind::NotFound
            )
        });
        (actual, reads, free)
    };
    let (old, old_reads, _) = run(None, path.clone());
    let crc = crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &body) as u32;
    assert_eq!(old, Ok(Some(crc)));
    assert_eq!(old_reads.len(), 3);
    let (old_missing, _, _) = run(None, dir.path().join("gone.bin"));
    assert_eq!(old_missing, Err(true));
    for (kind, during, all) in [
        (StorageKind::Solid, SOLID_READERS - 1, SOLID_READERS),
        (StorageKind::Unknown, SOLID_READERS - 1, SOLID_READERS),
        (StorageKind::Rotational, SPINNING_READERS, SPINNING_READERS),
    ] {
        let volume = Arc::new(Volume::new(None, "test".to_owned()));
        volume.reads(|| kind);
        let (new, new_reads, free) = run(Some(Arc::clone(&volume)), path.clone());
        assert_eq!(new, old, "{kind:?}");
        assert_eq!(new_reads, old_reads, "{kind:?}");
        assert_eq!(free, vec![during; old_reads.len()], "{kind:?}");
        assert_eq!(volume.read_turns().turns().available_permits(), all);
        let (missing, _, _) = run(Some(Arc::clone(&volume)), dir.path().join("gone.bin"));
        assert_eq!(missing, old_missing, "{kind:?}");
        assert_eq!(volume.read_turns().turns().available_permits(), all);
    }
}

// A check waiting for a turn that the volume's other readers hold ends
// when its job stops it, having read nothing, on solid state and on a
// spinning volume alike.
#[test]
fn a_check_waiting_for_a_turn_ends_when_it_is_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("member.bin");
    std::fs::write(&path, vec![7; 64]).unwrap();
    for kind in [StorageKind::Solid, StorageKind::Rotational] {
        let volume = Arc::new(Volume::new(None, "test".to_owned()));
        let reads = volume.reads(|| kind);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let every = runtime
            .block_on(Arc::clone(reads.turns()).acquire_many_owned(reads.readers() as u32))
            .unwrap();
        let check = check(path.clone(), 64, Some(Arc::clone(&volume)));
        let stop = Arc::clone(&check.stop);
        let waiting = std::thread::spawn(move || {
            let mut read = 0;
            let verified = check.run_with(&Shutdown::default(), |len| read += len);
            (verified.actual.ok(), read)
        });
        stop.await_waiters(1);
        stop.raise();
        assert_eq!(waiting.join().unwrap(), (Some(None), 0), "{kind:?}");
        drop(every);
        assert_eq!(
            reads.turns().available_permits(),
            reads.readers(),
            "{kind:?}"
        );
    }
}

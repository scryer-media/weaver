//! Coverage withdrawal and replacement through the real direct-volume adapter.
use super::*;
use crate::pipeline::tests::build_repairable_par2_set_for_files;
use par2_rs::{Par2RepairSession, Par2RepairSessionOptions, Par2RepairStatus};

fn read_span(access: &dyn FileAccess, file: &par2_rs::FileId, start: u64, len: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    while (bytes.len() as u64) < len {
        let part = access
            .read_file_range(file, start + bytes.len() as u64, len - bytes.len() as u64)
            .unwrap();
        if part.is_empty() {
            break;
        }
        bytes.extend_from_slice(&part);
    }
    bytes
}

fn options(
    dir: &Path,
    set: &par2_rs::Par2FileSet,
    volume: super::super::provider::VirtualVolume,
) -> Par2RepairSessionOptions {
    let (access, _, _) = encrypted_file_access(volume, set, dir);
    let mut options =
        Par2RepairSessionOptions::from_set(dir.to_path_buf(), set.clone(), Arc::new(access));
    options.memory_limit = Some(64 << 20);
    options.retained_state_limit = 32 << 20;
    options
}

fn signature(outcome: &par2_rs::Par2RepairOutcome) -> (Par2RepairStatus, u32, u32) {
    (
        outcome.status,
        outcome.available_blocks,
        outcome.missing_blocks,
    )
}

fn campaign(encrypted: bool, checksums: bool) {
    // Exhaust all 3! source-publication orders, followed by withdrawal and
    // re-publication. Each prefix/hole has an independent byte oracle.
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let root = tempfile::tempdir().unwrap();
        let fixture = provider_fixture(whole_volume_covered());
        let (posted, plain, crypt, plaintext_coverage) = encrypted_member_facts(557, 64);
        let expected = if encrypted {
            posted
        } else {
            fixture.conventional.clone()
        };
        let volume = if encrypted {
            let facts = crypt
                .cipher_facts(plain.len() as u64, &plaintext_coverage)
                .unwrap();
            let mut volume = cipher_volume(root.path(), &plain, facts, expected.len() as u64);
            // The member extent ends at plaintext length. The posted padding
            // is owned by the envelope, just as it is after real routing.
            std::fs::write(&volume.envelope, &expected).unwrap();
            volume
                .envelope_covered
                .insert(plain.len() as u64, (expected.len() - plain.len()) as u64);
            volume
        } else {
            fixture.volume.clone()
        };
        let mut set = build_repairable_par2_set_for_files(&[("source.rar", &expected)], 64, 12);
        if !checksums {
            set.slice_checksums.clear();
        }
        let mut session =
            Par2RepairSession::open(options(root.path(), &set, volume.clone())).unwrap();
        let index = crate::pipeline::tests::build_test_par2_index("source.rar", &expected, 64);
        for (step, state) in order.into_iter().chain([0, 2]).enumerate() {
            let mut next = volume.clone();
            let len = expected.len() as u64;
            let mut coverage = ByteRanges::new();
            match state {
                0 => {
                    coverage.insert(0, 73);
                }
                1 => {
                    coverage.insert(0, 73);
                    coverage.insert(101, len - 101);
                }
                2 => {
                    coverage.insert(0, len);
                }
                _ => unreachable!(),
            }
            next.covered = coverage;
            if !encrypted {
                next.envelope_covered = provider_envelope_covered(&next.covered);
            }
            let fresh_options = options(root.path(), &set, next.clone());
            let new_access = options(root.path(), &set, next).source_access.unwrap();
            let file_id = set.recovery_file_ids[0];
            for (start, count) in if state == 2 {
                vec![(0, len)]
            } else if state == 1 {
                vec![(0, 73), (101, len - 101)]
            } else {
                vec![(0, 73)]
            } {
                assert_eq!(
                    read_span(new_access.as_ref(), &file_id, start, count),
                    expected[start as usize..(start + count) as usize],
                    "encrypted={encrypted} order={order:?} step={step}"
                );
            }
            if !checksums {
                assert_eq!(
                    par2_rs::verify_full_hash(&set, &file_id, new_access.as_ref()).unwrap(),
                    state == 2,
                    "no-IFSC whole-file hash: encrypted={encrypted} order={order:?} step={step}"
                );
                continue;
            }
            let mut verifier = par2_rs::VerificationSession::new();
            verifier.add_par2_data(
                &par2_rs::packet::scan_packets(&index, 0)
                    .unwrap()
                    .into_iter()
                    .map(|(packet, _)| packet)
                    .collect::<Vec<_>>(),
            );
            for start in (0..len).step_by(64) {
                let count = (len - start).min(64);
                let bytes = read_span(new_access.as_ref(), &file_id, start, count);
                verifier.feed_data(&file_id, start, &bytes);
            }
            let generation = session.source_generation();
            assert_eq!(session.set_source_access(new_access), Some(generation + 1));
            assert!(
                session.assessment().is_err(),
                "a replaced handle must retire its cached assessment"
            );
            let mut fresh_session = Par2RepairSession::open(fresh_options).unwrap();
            for evidence in verifier.slice_evidence() {
                session.add_slice_evidence_for_file(evidence).unwrap();
                fresh_session.add_slice_evidence_for_file(evidence).unwrap();
            }
            let retained = session.analyze().unwrap();
            let fresh = fresh_session.analyze().unwrap();
            assert_eq!(
                signature(&retained),
                signature(&fresh),
                "encrypted={encrypted} checksums={checksums} order={order:?} step={step}"
            );
            if state == 2 {
                assert_eq!(retained.missing_blocks, 0);
            } else {
                assert!(
                    retained.missing_blocks > 0,
                    "withdrawn bytes must not remain verified"
                );
                if checksums {
                    let repaired = session.repair().unwrap();
                    assert_eq!(repaired.status, Par2RepairStatus::Repaired);
                    assert_eq!(
                        std::fs::read(root.path().join("source.rar")).unwrap(),
                        expected
                    );
                    // Remove installed output before the next view: the source
                    // remains the virtual handle, not its repaired disk copy.
                    std::fs::remove_file(root.path().join("source.rar")).unwrap();
                }
            }
        }
    }
}

#[test]
fn plain_direct_volume_schedules() {
    campaign(false, true);
}
#[test]
fn encrypted_direct_volume_schedules() {
    campaign(true, true);
}
#[test]
fn plain_no_ifsc_schedules() {
    campaign(false, false);
}
#[test]
fn encrypted_no_ifsc_schedules() {
    campaign(true, false);
}

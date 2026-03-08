pub mod error;
pub mod file_assembly;
pub mod job_assembly;
pub mod write_buffer;

pub use error::AssemblyError;
pub use file_assembly::{CommitResult, FileAssembly, IncrementalSliceState};
pub use job_assembly::{
    ArchiveMember, ArchiveTopology, ArchiveType, ExtractionReadiness, JobAssembly, Par2Metadata,
};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use weaver_core::checksum::{crc32, md5};
    use weaver_core::classify::FileRole;
    use weaver_core::id::{JobId, NzbFileId};

    use super::*;

    fn make_file_id(file_index: u32) -> NzbFileId {
        NzbFileId {
            job_id: JobId(1),
            file_index,
        }
    }

    // ========================================================================
    // FileAssembly tests
    // ========================================================================

    #[test]
    fn new_assembly() {
        let fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000, 500],
        );
        assert_eq!(fa.progress(), 0.0);
        assert_eq!(fa.missing_count(), 3);
        assert!(!fa.is_complete());
        assert_eq!(fa.total_segments(), 3);
        assert_eq!(fa.total_bytes(), 2500);
        assert_eq!(fa.filename(), "test.rar");
    }

    #[test]
    fn commit_single_segment() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000, 500],
        );

        let data = vec![0u8; 1000];
        let result = fa.commit_segment(0, &data).unwrap();
        assert!(!result.file_complete);
        assert!(!result.was_duplicate);
        assert_eq!(fa.missing_count(), 2);
        assert!((fa.progress() - 1.0 / 3.0).abs() < 1e-10);
    }

    #[test]
    fn commit_all_segments() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000, 500],
        );

        fa.commit_segment(0, &vec![0u8; 1000]).unwrap();
        fa.commit_segment(1, &vec![0u8; 1000]).unwrap();
        let result = fa.commit_segment(2, &vec![0u8; 500]).unwrap();

        assert!(result.file_complete);
        assert!(fa.is_complete());
        assert_eq!(fa.missing_count(), 0);
        assert!((fa.progress() - 1.0).abs() < 1e-10);
    }

    #[test]
    fn commit_out_of_order() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000, 500],
        );

        fa.commit_segment(2, &vec![0u8; 500]).unwrap();
        assert_eq!(fa.missing_count(), 2);
        assert!(!fa.is_complete());

        fa.commit_segment(0, &vec![0u8; 1000]).unwrap();
        assert_eq!(fa.missing_count(), 1);

        let result = fa.commit_segment(1, &vec![0u8; 1000]).unwrap();
        assert!(result.file_complete);
        assert!(fa.is_complete());
    }

    #[test]
    fn duplicate_segment() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000],
        );

        let result1 = fa.commit_segment(0, &vec![0u8; 1000]).unwrap();
        assert!(!result1.was_duplicate);

        let result2 = fa.commit_segment(0, &vec![0u8; 1000]).unwrap();
        assert!(result2.was_duplicate);

        // Should still only count once.
        assert_eq!(fa.missing_count(), 1);
    }

    #[test]
    fn segment_out_of_range() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000],
        );

        let err = fa.commit_segment(5, &vec![0u8; 100]).unwrap_err();
        match err {
            AssemblyError::SegmentOutOfRange {
                segment_number,
                total_segments,
            } => {
                assert_eq!(segment_number, 5);
                assert_eq!(total_segments, 2);
            }
            _ => panic!("expected SegmentOutOfRange"),
        }
    }

    #[test]
    fn segment_offset_calculation() {
        let fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 2000, 500, 1500],
        );

        assert_eq!(fa.segment_offset(0), 0);
        assert_eq!(fa.segment_offset(1), 1000);
        assert_eq!(fa.segment_offset(2), 3000);
        assert_eq!(fa.segment_offset(3), 3500);
    }

    #[test]
    fn progress_accuracy() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![100; 10],
        );

        assert_eq!(fa.progress(), 0.0);

        for i in 0..5 {
            fa.commit_segment(i, &vec![0u8; 100]).unwrap();
        }
        assert!((fa.progress() - 0.5).abs() < 1e-10);

        for i in 5..10 {
            fa.commit_segment(i, &vec![0u8; 100]).unwrap();
        }
        assert!((fa.progress() - 1.0).abs() < 1e-10);
    }

    // ========================================================================
    // Incremental verification tests
    // ========================================================================

    #[test]
    fn attach_par2_metadata() {
        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![500, 500],
        );

        assert!(fa.slice_verification_results().is_none());

        // 2 slices of 500 bytes each.
        let checksums = vec![
            (crc32(&[0u8; 500]), md5(&[0u8; 500])),
            (crc32(&[0u8; 500]), md5(&[0u8; 500])),
        ];
        fa.attach_par2_metadata(500, &checksums);

        let results = fa.slice_verification_results().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], (0, None));
        assert_eq!(results[1], (1, None));
    }

    #[test]
    fn incremental_verify_valid() {
        // File with 2 segments of 500 bytes each, 1 PAR2 slice of 1000 bytes.
        let seg0_data = vec![0xAAu8; 500];
        let seg1_data = vec![0xBBu8; 500];

        // Compute the expected checksum for the full 1000-byte slice.
        let mut full_data = seg0_data.clone();
        full_data.extend_from_slice(&seg1_data);
        let expected_crc = crc32(&full_data);
        let expected_md5 = md5(&full_data);

        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![500, 500],
        );

        fa.attach_par2_metadata(1000, &[(expected_crc, expected_md5)]);

        // Commit first segment -- slice not yet complete.
        let r1 = fa.commit_segment(0, &seg0_data).unwrap();
        assert!(r1.newly_verified_slices.is_empty());

        // Commit second segment -- slice should now be verified as valid.
        let r2 = fa.commit_segment(1, &seg1_data).unwrap();
        assert_eq!(r2.newly_verified_slices.len(), 1);
        assert_eq!(r2.newly_verified_slices[0], (0, true));

        assert_eq!(fa.verified_slice_count(), 1);
        assert_eq!(fa.damaged_slice_count(), 0);
    }

    #[test]
    fn incremental_verify_damaged() {
        // File with 1 segment of 500 bytes, 1 PAR2 slice.
        let data = vec![0xAAu8; 500];

        // Use wrong expected checksums.
        let wrong_crc = 0xDEADBEEF;
        let wrong_md5 = [0xFF; 16];

        let mut fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![500],
        );

        fa.attach_par2_metadata(500, &[(wrong_crc, wrong_md5)]);

        let result = fa.commit_segment(0, &data).unwrap();
        assert_eq!(result.newly_verified_slices.len(), 1);
        assert_eq!(result.newly_verified_slices[0], (0, false));

        assert_eq!(fa.damaged_slice_count(), 1);
        assert_eq!(fa.verified_slice_count(), 0);
    }

    #[test]
    fn verify_without_metadata() {
        let fa = FileAssembly::new(
            make_file_id(0),
            "test.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![500],
        );

        assert!(fa.slice_verification_results().is_none());
        assert_eq!(fa.damaged_slice_count(), 0);
        assert_eq!(fa.verified_slice_count(), 0);
    }

    // ========================================================================
    // JobAssembly tests
    // ========================================================================

    #[test]
    fn job_assembly_basic() {
        let mut job = JobAssembly::new(JobId(1));

        let fa1 = FileAssembly::new(
            make_file_id(0),
            "file1.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000],
        );
        let fa2 = FileAssembly::new(
            make_file_id(1),
            "file2.rar".into(),
            FileRole::RarVolume { volume_number: 1 },
            vec![1000],
        );

        job.add_file(fa1);
        job.add_file(fa2);

        assert_eq!(job.total_file_count(), 2);
        assert_eq!(job.complete_file_count(), 0);
        assert!(job.file(make_file_id(0)).is_some());
        assert!(job.file(make_file_id(1)).is_some());
        assert!(job.file(make_file_id(99)).is_none());
    }

    #[test]
    fn job_progress() {
        let mut job = JobAssembly::new(JobId(1));

        // File 1: 2000 bytes total (2 segments).
        let fa1 = FileAssembly::new(
            make_file_id(0),
            "file1.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000],
        );
        // File 2: 1000 bytes total (1 segment).
        let fa2 = FileAssembly::new(
            make_file_id(1),
            "file2.rar".into(),
            FileRole::RarVolume { volume_number: 1 },
            vec![1000],
        );

        job.add_file(fa1);
        job.add_file(fa2);

        assert_eq!(job.progress(), 0.0);

        // Complete file 2 (1000 bytes out of 3000 total).
        job.file_mut(make_file_id(1))
            .unwrap()
            .commit_segment(0, &vec![0u8; 1000])
            .unwrap();

        // Progress should be weighted: 1000/3000 = 1/3.
        let progress = job.progress();
        assert!(
            (progress - 1.0 / 3.0).abs() < 1e-10,
            "expected ~0.333, got {progress}"
        );

        assert_eq!(job.complete_file_count(), 1);
    }

    #[test]
    fn extraction_not_applicable() {
        let mut job = JobAssembly::new(JobId(1));

        let fa = FileAssembly::new(
            make_file_id(0),
            "readme.nfo".into(),
            FileRole::Standalone,
            vec![500],
        );
        job.add_file(fa);

        assert_eq!(job.extraction_readiness(), ExtractionReadiness::NotApplicable);
    }

    #[test]
    fn extraction_ready() {
        let mut job = JobAssembly::new(JobId(1));

        let fa = FileAssembly::new(
            make_file_id(0),
            "movie.part01.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000],
        );
        job.add_file(fa);

        let topo = ArchiveTopology {
            archive_type: ArchiveType::Rar,
            volume_map: [("movie.part01.rar".into(), 0)].into_iter().collect(),
            complete_volumes: [0].into_iter().collect(),
            expected_volume_count: Some(1),
            members: vec![ArchiveMember {
                name: "movie.mkv".into(),
                first_volume: 0,
                last_volume: 0,
                unpacked_size: 1_000_000,
            }],
        };
        job.set_archive_topology("test".into(), topo);

        assert_eq!(job.extraction_readiness(), ExtractionReadiness::Ready);
    }

    #[test]
    fn extraction_partial() {
        let mut job = JobAssembly::new(JobId(1));

        let fa1 = FileAssembly::new(
            make_file_id(0),
            "movie.part01.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000],
        );
        let fa2 = FileAssembly::new(
            make_file_id(1),
            "movie.part02.rar".into(),
            FileRole::RarVolume { volume_number: 1 },
            vec![1000],
        );
        job.add_file(fa1);
        job.add_file(fa2);

        let topo = ArchiveTopology {
            archive_type: ArchiveType::Rar,
            volume_map: [
                ("movie.part01.rar".into(), 0),
                ("movie.part02.rar".into(), 1),
            ]
            .into_iter()
            .collect(),
            complete_volumes: [0].into_iter().collect(), // Only volume 0 complete.
            expected_volume_count: Some(2),
            members: vec![
                ArchiveMember {
                    name: "small.txt".into(),
                    first_volume: 0,
                    last_volume: 0,
                    unpacked_size: 100,
                },
                ArchiveMember {
                    name: "movie.mkv".into(),
                    first_volume: 0,
                    last_volume: 1,
                    unpacked_size: 2000,
                },
            ],
        };
        job.set_archive_topology("test".into(), topo);

        match job.extraction_readiness() {
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                assert_eq!(extractable, vec!["small.txt"]);
                assert_eq!(waiting_on, vec!["movie.mkv"]);
            }
            other => panic!("expected Partial, got {other:?}"),
        }
    }

    #[test]
    fn extraction_blocked() {
        let mut job = JobAssembly::new(JobId(1));

        let fa = FileAssembly::new(
            make_file_id(0),
            "movie.part01.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000],
        );
        job.add_file(fa);

        let topo = ArchiveTopology {
            archive_type: ArchiveType::Rar,
            volume_map: [("movie.part01.rar".into(), 0)].into_iter().collect(),
            complete_volumes: HashSet::new(), // Nothing complete.
            expected_volume_count: Some(1),
            members: vec![ArchiveMember {
                name: "movie.mkv".into(),
                first_volume: 0,
                last_volume: 0,
                unpacked_size: 1_000_000,
            }],
        };
        job.set_archive_topology("test".into(), topo);

        match job.extraction_readiness() {
            ExtractionReadiness::Blocked { .. } => {
                // Expected: blocked because no volumes are complete.
            }
            other => panic!("expected Blocked, got {other:?}"),
        }
    }

    #[test]
    fn repair_confidence() {
        let mut job = JobAssembly::new(JobId(1));

        let data_good = vec![0xAAu8; 1000];
        let data_bad = vec![0xBBu8; 1000];

        let expected_crc = crc32(&data_good);
        let expected_md5 = md5(&data_good);

        let fa = FileAssembly::new(
            make_file_id(0),
            "movie.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000, 1000],
        );

        job.add_file(fa);

        // Use set_par2_metadata to attach checksums via the public API.
        let mut file_checksums = std::collections::HashMap::new();
        file_checksums.insert(
            "movie.rar".into(),
            vec![
                (expected_crc, expected_md5),
                (expected_crc, expected_md5), // second slice uses same expected but bad data
            ],
        );
        let metadata = Par2Metadata {
            slice_size: 1000,
            recovery_block_count: 5,
            file_checksums,
        };
        job.set_par2_metadata(metadata);

        // Commit segment 0 (good data, covers slice 0).
        job.file_mut(make_file_id(0))
            .unwrap()
            .commit_segment(0, &data_good)
            .unwrap();
        // Commit segment 1 (bad data, covers slice 1).
        job.file_mut(make_file_id(0))
            .unwrap()
            .commit_segment(1, &data_bad)
            .unwrap();

        let (damaged, total, recovery) = job.repair_confidence().unwrap();
        assert_eq!(damaged, 1);
        assert_eq!(total, 2); // 1 good + 1 damaged
        assert_eq!(recovery, 5);
    }

    #[test]
    fn repair_confidence_none_without_par2() {
        let job = JobAssembly::new(JobId(1));
        assert!(job.repair_confidence().is_none());
    }

    #[test]
    fn par2_metadata_attaches_to_files() {
        let mut job = JobAssembly::new(JobId(1));

        let fa1 = FileAssembly::new(
            make_file_id(0),
            "movie.rar".into(),
            FileRole::RarVolume { volume_number: 0 },
            vec![1000],
        );
        let fa2 = FileAssembly::new(
            make_file_id(1),
            "movie.r00".into(),
            FileRole::RarVolume { volume_number: 1 },
            vec![1000],
        );
        let fa3 = FileAssembly::new(
            make_file_id(2),
            "readme.nfo".into(),
            FileRole::Standalone,
            vec![200],
        );

        job.add_file(fa1);
        job.add_file(fa2);
        job.add_file(fa3);

        let dummy_crc = crc32(&[0u8; 1000]);
        let dummy_md5 = md5(&[0u8; 1000]);

        let mut file_checksums = std::collections::HashMap::new();
        file_checksums.insert("movie.rar".into(), vec![(dummy_crc, dummy_md5)]);
        file_checksums.insert("movie.r00".into(), vec![(dummy_crc, dummy_md5)]);
        // No entry for readme.nfo.

        let metadata = Par2Metadata {
            slice_size: 1000,
            recovery_block_count: 3,
            file_checksums,
        };
        job.set_par2_metadata(metadata);

        // movie.rar and movie.r00 should have slice states attached.
        assert!(job.file(make_file_id(0)).unwrap().slice_verification_results().is_some());
        assert!(job.file(make_file_id(1)).unwrap().slice_verification_results().is_some());
        // readme.nfo should not.
        assert!(job.file(make_file_id(2)).unwrap().slice_verification_results().is_none());
    }

    #[test]
    fn mark_volume_complete() {
        let mut job = JobAssembly::new(JobId(1));

        let topo = ArchiveTopology {
            archive_type: ArchiveType::Rar,
            volume_map: std::collections::HashMap::new(),
            complete_volumes: std::collections::HashSet::new(),
            expected_volume_count: Some(3),
            members: vec![ArchiveMember {
                name: "movie.mkv".into(),
                first_volume: 0,
                last_volume: 2,
                unpacked_size: 3000,
            }],
        };
        job.set_archive_topology("test".into(), topo);

        // Not ready yet.
        assert_ne!(job.extraction_readiness(), ExtractionReadiness::Ready);

        job.mark_volume_complete("test",0);
        job.mark_volume_complete("test",1);
        job.mark_volume_complete("test",2);

        assert_eq!(job.extraction_readiness(), ExtractionReadiness::Ready);
    }
}

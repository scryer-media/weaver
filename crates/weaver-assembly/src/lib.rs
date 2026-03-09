pub mod error;
pub mod file_assembly;
pub mod job_assembly;
pub mod write_buffer;

pub use error::AssemblyError;
pub use file_assembly::{CommitResult, FileAssembly};
pub use job_assembly::{
    ArchiveMember, ArchiveTopology, ArchiveType, ExtractionReadiness, JobAssembly,
};

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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

        let result = fa.commit_segment(0, 1000).unwrap();
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

        fa.commit_segment(0, 1000).unwrap();
        fa.commit_segment(1, 1000).unwrap();
        let result = fa.commit_segment(2, 500).unwrap();

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

        fa.commit_segment(2, 500).unwrap();
        assert_eq!(fa.missing_count(), 2);
        assert!(!fa.is_complete());

        fa.commit_segment(0, 1000).unwrap();
        assert_eq!(fa.missing_count(), 1);

        let result = fa.commit_segment(1, 1000).unwrap();
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

        let result1 = fa.commit_segment(0, 1000).unwrap();
        assert!(!result1.was_duplicate);

        let result2 = fa.commit_segment(0, 1000).unwrap();
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

        let err = fa.commit_segment(5, 100).unwrap_err();
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
            fa.commit_segment(i, 100).unwrap();
        }
        assert!((fa.progress() - 0.5).abs() < 1e-10);

        for i in 5..10 {
            fa.commit_segment(i, 100).unwrap();
        }
        assert!((fa.progress() - 1.0).abs() < 1e-10);
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
            .commit_segment(0, 1000)
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
    fn data_file_count_excludes_recovery() {
        let mut job = JobAssembly::new(JobId(1));

        job.add_file(FileAssembly::new(
            make_file_id(0), "movie.part01.rar".into(),
            FileRole::RarVolume { volume_number: 0 }, vec![1000],
        ));
        job.add_file(FileAssembly::new(
            make_file_id(1), "movie.par2".into(),
            FileRole::Par2 { is_index: true, recovery_block_count: 0 }, vec![500],
        ));
        job.add_file(FileAssembly::new(
            make_file_id(2), "movie.vol00+01.par2".into(),
            FileRole::Par2 { is_index: false, recovery_block_count: 1 }, vec![2000],
        ));

        assert_eq!(job.total_file_count(), 3);
        assert_eq!(job.data_file_count(), 2); // RAR + PAR2 index
        assert_eq!(job.complete_data_file_count(), 0);

        // Complete the RAR volume.
        job.file_mut(make_file_id(0)).unwrap().commit_segment(0, 1000).unwrap();
        assert_eq!(job.complete_data_file_count(), 1);
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
            complete_volumes: [0].into_iter().collect(),
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
            complete_volumes: HashSet::new(),
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
            ExtractionReadiness::Blocked { .. } => {}
            other => panic!("expected Blocked, got {other:?}"),
        }
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

        assert_ne!(job.extraction_readiness(), ExtractionReadiness::Ready);

        job.mark_volume_complete("test", 0);
        job.mark_volume_complete("test", 1);
        job.mark_volume_complete("test", 2);

        assert_eq!(job.extraction_readiness(), ExtractionReadiness::Ready);
    }
}

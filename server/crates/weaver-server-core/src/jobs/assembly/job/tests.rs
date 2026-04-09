use super::*;
use crate::jobs::ids::NzbFileId;

fn add_file(
    assembly: &mut JobAssembly,
    file_index: u32,
    filename: &str,
    role: FileRole,
    segment_sizes: &[u32],
    committed_segments: usize,
) {
    let file_id = NzbFileId {
        job_id: assembly.job_id(),
        file_index,
    };
    let mut file = FileAssembly::new(file_id, filename.to_string(), role, segment_sizes.to_vec());
    for (segment_number, size) in segment_sizes.iter().enumerate().take(committed_segments) {
        file.commit_segment(segment_number as u32, *size).unwrap();
    }
    assembly.add_file(file);
}

#[test]
fn optional_recovery_bytes_are_zero_when_not_present() {
    let mut assembly = JobAssembly::new(JobId(1));
    add_file(
        &mut assembly,
        0,
        "show.part01.rar",
        FileRole::RarVolume { volume_number: 0 },
        &[100],
        1,
    );

    assert_eq!(assembly.optional_recovery_bytes(), (0, 0));
}

#[test]
fn optional_recovery_bytes_track_unused_recovery_data() {
    let mut assembly = JobAssembly::new(JobId(2));
    add_file(
        &mut assembly,
        0,
        "show.part01.rar",
        FileRole::RarVolume { volume_number: 0 },
        &[100],
        1,
    );
    add_file(
        &mut assembly,
        1,
        "show.vol00+02.par2",
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 2,
        },
        &[20, 30],
        0,
    );

    assert_eq!(assembly.optional_recovery_bytes(), (50, 0));
}

#[test]
fn optional_recovery_bytes_track_partial_recovery_downloads() {
    let mut assembly = JobAssembly::new(JobId(3));
    add_file(
        &mut assembly,
        0,
        "show.part01.rar",
        FileRole::RarVolume { volume_number: 0 },
        &[100],
        1,
    );
    add_file(
        &mut assembly,
        1,
        "show.vol00+02.par2",
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 2,
        },
        &[20, 30],
        1,
    );

    assert_eq!(assembly.optional_recovery_bytes(), (50, 20));
}

#[test]
fn par2_only_jobs_do_not_mark_recovery_bytes_optional() {
    let mut assembly = JobAssembly::new(JobId(4));
    add_file(
        &mut assembly,
        0,
        "repair.par2",
        FileRole::Par2 {
            is_index: true,
            recovery_block_count: 0,
        },
        &[10],
        1,
    );
    add_file(
        &mut assembly,
        1,
        "repair.vol00+02.par2",
        FileRole::Par2 {
            is_index: false,
            recovery_block_count: 2,
        },
        &[20, 30],
        2,
    );

    assert_eq!(assembly.optional_recovery_bytes(), (0, 0));
}

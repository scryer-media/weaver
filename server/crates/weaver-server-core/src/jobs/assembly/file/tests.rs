use crate::jobs::ids::{JobId, NzbFileId};
use weaver_model::files::FileRole;

use super::*;

fn make_assembly(segment_sizes: Vec<u32>) -> FileAssembly {
    let file_id = NzbFileId {
        job_id: JobId(1),
        file_index: 0,
    };
    FileAssembly::new(
        file_id,
        "test.rar".into(),
        FileRole::RarVolume { volume_number: 0 },
        segment_sizes,
    )
}

#[test]
fn commit_in_order() {
    let mut asm = make_assembly(vec![500, 500]);
    let r0 = asm.commit_segment(0, 500).unwrap();
    assert!(!r0.file_complete);
    assert!(!r0.was_duplicate);
    let r1 = asm.commit_segment(1, 500).unwrap();
    assert!(r1.file_complete);
}

#[test]
fn commit_out_of_order() {
    let mut asm = make_assembly(vec![500, 500, 500]);
    let r2 = asm.commit_segment(2, 500).unwrap();
    assert!(!r2.file_complete);
    let r0 = asm.commit_segment(0, 500).unwrap();
    assert!(!r0.file_complete);
    let r1 = asm.commit_segment(1, 500).unwrap();
    assert!(r1.file_complete);
}

#[test]
fn duplicate_segment() {
    let mut asm = make_assembly(vec![500]);
    let r0 = asm.commit_segment(0, 500).unwrap();
    assert!(!r0.was_duplicate);
    let r1 = asm.commit_segment(0, 500).unwrap();
    assert!(r1.was_duplicate);
}

#[test]
fn segment_out_of_range() {
    let mut asm = make_assembly(vec![500]);
    assert!(asm.commit_segment(1, 500).is_err());
}

#[test]
fn file_assembly_role_and_set_name_follow_declared_role_only() {
    let file_id = NzbFileId {
        job_id: JobId(1),
        file_index: 0,
    };
    let asm = FileAssembly::new(file_id, "release.2024".into(), FileRole::Unknown, vec![1]);

    assert!(matches!(asm.role(), FileRole::Unknown));
    assert!(matches!(asm.effective_role(), FileRole::Unknown));
    assert_eq!(asm.archive_set_name(), None);
}

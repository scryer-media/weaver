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
    assert!(asm.has_duplicate_segments());
}

#[test]
fn contiguous_placements_prove_a_gap_free_decoded_tiling() {
    // Production shape: declared (encoded) sizes are LARGER than decoded.
    let mut asm = make_assembly(vec![516, 516]);
    asm.record_placement(0, 0, 500);
    asm.commit_segment(0, 500).unwrap();
    asm.record_placement(1, 500, 500);
    asm.commit_segment(1, 500).unwrap();
    assert!(asm.contiguous_placements_proven());

    asm.reset();
    assert!(!asm.contiguous_placements_proven());
}

#[test]
fn a_placement_gap_or_missing_record_disproves_contiguity() {
    // A gap between placements: the decoded tiling is not airtight.
    let mut asm = make_assembly(vec![516, 516]);
    asm.record_placement(0, 0, 500);
    asm.commit_segment(0, 500).unwrap();
    asm.record_placement(1, 512, 500);
    asm.commit_segment(1, 500).unwrap();
    assert!(!asm.contiguous_placements_proven());

    // Complete but with no placement observations at all (the shape
    // verification/repair completion leaves): nothing is proven.
    let mut unobserved = make_assembly(vec![516]);
    unobserved.commit_segment(0, 500).unwrap();
    assert!(!unobserved.contiguous_placements_proven());

    // Incomplete files prove nothing either.
    let mut incomplete = make_assembly(vec![516, 516]);
    incomplete.record_placement(0, 0, 500);
    incomplete.commit_segment(0, 500).unwrap();
    assert!(!incomplete.contiguous_placements_proven());
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

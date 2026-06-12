use super::*;

#[test]
fn copy_fallback_copies_file_then_removes_source() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source.bin");
    let dst = temp.path().join("dest").join("source.bin");
    std::fs::write(&src, b"copied payload").unwrap();

    move_path_with_copy_fallback(&src, &dst).unwrap();

    assert!(!src.exists());
    assert_eq!(std::fs::read(&dst).unwrap(), b"copied payload");
}

#[test]
fn copy_fallback_moves_nested_directory_contents() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("source-dir");
    let nested = src.join("nested");
    let dst = temp.path().join("dest-dir");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("payload.bin"), b"nested payload").unwrap();

    move_path_with_copy_fallback(&src, &dst).unwrap();

    assert!(!src.exists());
    assert_eq!(
        std::fs::read(dst.join("nested").join("payload.bin")).unwrap(),
        b"nested payload"
    );
}

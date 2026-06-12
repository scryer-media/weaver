use super::*;

#[test]
fn large_copy_helper_preserves_contents() {
    let temp = tempfile::tempdir().unwrap();
    let src = temp.path().join("src.bin");
    let dst = temp.path().join("nested").join("dst.bin");
    let bytes = (0..4096)
        .map(|value| (value % 251) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&src, &bytes).unwrap();

    let copied = copy_large_file(&src, &dst).unwrap();

    assert_eq!(copied, bytes.len() as u64);
    assert_eq!(std::fs::read(&dst).unwrap(), bytes);
    assert!(src.exists(), "copy helper must not remove the source");
}

#[test]
fn cache_advice_path_helpers_swallow_open_errors() {
    let missing = std::path::Path::new("/definitely/missing/weaver/cache-advice.bin");

    advise_path_sequential(missing);
    drop_path_cache(missing);
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[test]
fn cache_advice_noops_on_unsupported_platforms() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    let file = File::open(temp.path()).unwrap();

    assert!(raw_cache_advice(&file, 0, 0, CacheAdvice::Sequential).is_ok());
    assert!(raw_cache_advice(&file, 0, 0, CacheAdvice::DontNeed).is_ok());
}

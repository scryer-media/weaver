//! Private repair candidates. Original virtual source coverage never changes.
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::pipeline::direct_store::provider::{VirtualVolume, VirtualVolumeReader};

#[cfg(test)]
pub(crate) fn materialize_candidates(
    directory: &Path,
    volumes: Vec<(String, VirtualVolume)>,
    maximum_bytes: u64,
    sparse: super::super::sparse::SparseMarking,
) -> io::Result<Vec<PathBuf>> {
    materialize_candidates_checked(directory, volumes, maximum_bytes, sparse, || Ok(()))
}

pub(crate) fn materialize_candidates_checked(
    directory: &Path,
    volumes: Vec<(String, VirtualVolume)>,
    maximum_bytes: u64,
    sparse: super::super::sparse::SparseMarking,
    mut check: impl FnMut() -> io::Result<()>,
) -> io::Result<Vec<PathBuf>> {
    let mut bytes = 0u64;
    let mut names = std::collections::BTreeSet::new();
    for (name, volume) in &volumes {
        crate::pipeline::extraction::validate_sanitized_rar_member_path(name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        if !names.insert(name.to_lowercase()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "repair input path collision",
            ));
        }
        bytes = bytes
            .checked_add(volume.len)
            .filter(|bytes| *bytes <= maximum_bytes)
            .ok_or_else(|| {
                io::Error::other("repair input materialization exceeds disk admission")
            })?;
    }
    for name in &names {
        if name
            .match_indices('/')
            .any(|(index, _)| names.contains(&name[..index]))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "repair input file/directory collision",
            ));
        }
    }
    check()?;
    std::fs::create_dir(directory)?;
    let mut paths = Vec::new();
    let mut buffer = vec![0; 256 * 1024];
    for (name, mut volume) in volumes {
        volume.retain_placed_only();
        let ranges = volume.readable_ranges();
        let path = directory.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut output = super::super::sparse::create_sparse(&path, &sparse)
            .map_err(|error| io::Error::other(format!("repair sparse input: {error:?}")))?;
        output.set_len(volume.len)?;
        let mut input = VirtualVolumeReader::<true>::new(volume, Arc::default());
        for (start, end) in ranges {
            input.seek(SeekFrom::Start(start))?;
            output.seek(SeekFrom::Start(start))?;
            let mut remaining = end - start;
            while remaining > 0 {
                check()?;
                let wanted = usize::try_from(remaining)
                    .unwrap_or(buffer.len())
                    .min(buffer.len());
                input.read_exact(&mut buffer[..wanted])?;
                output.write_all(&buffer[..wanted])?;
                remaining -= wanted as u64;
            }
        }
        output.sync_all()?;
        paths.push(path);
    }
    #[cfg(unix)]
    std::fs::File::open(directory)?.sync_all()?;
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::direct_store::ByteRanges;

    #[test]
    fn sparse_candidates_copy_only_placed_bytes_and_preserve_originals() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("original");
        std::fs::write(&source, b"abcdefgh").unwrap();
        let mut covered = ByteRanges::new();
        covered.insert(0, 3);
        covered.insert(5, 3);
        let image = VirtualVolume {
            backing_leases: None,
            volume_index: 0,
            envelope: source.clone(),
            extents: Vec::new(),
            partials: Arc::default(),
            nested: Default::default(),
            covered: covered.clone(),
            envelope_covered: covered,
            held: Arc::default(),
            len: 8,
            ciphers: Arc::default(),
        };
        let paths = materialize_candidates(
            &directory.path().join("candidate"),
            vec![("volume.rar".into(), image.clone())],
            8,
            Default::default(),
        )
        .unwrap();
        assert_eq!(std::fs::read(&paths[0]).unwrap(), b"abc\0\0fgh");
        std::fs::write(&paths[0], b"repaired").unwrap();
        assert_eq!(std::fs::read(&source).unwrap(), b"abcdefgh");
        assert_eq!(image.readable_ranges(), vec![(0, 3), (5, 8)]);
        assert!(
            materialize_candidates(
                &directory.path().join("collision"),
                vec![("a".into(), image.clone()), ("a/b".into(), image.clone())],
                16,
                Default::default()
            )
            .is_err()
        );
        assert!(!directory.path().join("collision").exists());
        assert!(
            materialize_candidates_checked(
                &directory.path().join("cancelled"),
                vec![("volume.rar".into(), image.clone())],
                8,
                Default::default(),
                || Err(io::Error::other("cancelled"))
            )
            .is_err()
        );
        assert!(!directory.path().join("cancelled").exists());
        assert!(
            materialize_candidates(
                &directory.path().join("unsafe"),
                vec![("../outside".into(), image)],
                8,
                Default::default()
            )
            .is_err()
        );
        assert!(!directory.path().join("unsafe").exists());
    }
}

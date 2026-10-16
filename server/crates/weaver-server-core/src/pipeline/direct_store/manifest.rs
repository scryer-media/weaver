//! Filesystem backing for a root's checkpoint. SQL selects the authoritative
//! image; filesystem timestamps and directory order never select a checkpoint.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::jobs::ids::JobId;

pub(crate) mod graph;

const MAGIC: &[u8; 4] = b"WDM1";
const REFERENCE_BYTES: usize = 76;
pub(crate) const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const DIRECTORY: &str = ".weaver-checkpoints";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Reference {
    owner: [u8; 32],
    length: u64,
    digest: [u8; 32],
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl Reference {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(REFERENCE_BYTES);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.owner);
        bytes.extend_from_slice(&self.length.to_le_bytes());
        bytes.extend_from_slice(&self.digest);
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != REFERENCE_BYTES || !is_reference(bytes) {
            return Err(invalid("invalid direct checkpoint reference"));
        }
        let Some((owner, length, digest)) = Self::fields(bytes) else {
            return Err(invalid("invalid direct checkpoint reference"));
        };
        if length == 0 || length > MAX_MANIFEST_BYTES as u64 {
            return Err(invalid("direct checkpoint exceeds its size limit"));
        }
        Ok(Self {
            owner,
            length,
            digest,
        })
    }

    fn fields(bytes: &[u8]) -> Option<([u8; 32], u64, [u8; 32])> {
        let (owner, rest) = bytes
            .strip_prefix(MAGIC.as_slice())?
            .split_first_chunk::<32>()?;
        let (length, digest) = rest.split_first_chunk::<8>()?;
        Some((*owner, u64::from_le_bytes(*length), digest.try_into().ok()?))
    }
}

pub(crate) fn is_reference(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

#[derive(Debug, Clone)]
pub(crate) struct ManifestStore {
    working_dir: PathBuf,
    directory: PathBuf,
    owner: [u8; 32],
}

impl ManifestStore {
    pub(crate) fn for_job(working_dir: &Path, job_id: JobId) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"weaver-job-archive-checkpoint-v1\0");
        hash.update(&job_id.0.to_le_bytes());
        Self {
            working_dir: working_dir.to_path_buf(),
            directory: working_dir.join(DIRECTORY),
            owner: *hash.finalize().as_bytes(),
        }
    }

    pub(crate) fn new(working_dir: &Path, job_id: JobId, set_name: &str) -> Self {
        let mut hash = blake3::Hasher::new();
        hash.update(b"weaver-direct-checkpoint-v1\0");
        hash.update(&job_id.0.to_le_bytes());
        hash.update(set_name.as_bytes());
        Self {
            working_dir: working_dir.to_path_buf(),
            directory: working_dir.join(DIRECTORY),
            owner: *hash.finalize().as_bytes(),
        }
    }

    fn prefix(&self) -> String {
        format!(".weaver-checkpoint-{}-", hex::encode(self.owner))
    }

    fn path(&self, reference: &Reference) -> PathBuf {
        self.directory.join(format!(
            "{}{}.manifest",
            self.prefix(),
            hex::encode(reference.digest)
        ))
    }

    fn reference(&self, bytes: &[u8]) -> io::Result<Reference> {
        let reference = Reference::decode(bytes)?;
        if reference.owner != self.owner {
            return Err(invalid("direct checkpoint belongs to another root"));
        }
        Ok(reference)
    }

    pub(crate) fn read(&self, bytes: &[u8]) -> io::Result<Vec<u8>> {
        let reference = self.reference(bytes)?;
        if !fs::symlink_metadata(&self.directory)?.file_type().is_dir() {
            return Err(invalid("checkpoint directory is not an owned directory"));
        }
        let path = self.path(&reference);
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(invalid("direct checkpoint is not a regular file"));
        }
        let file = File::open(path)?;
        if file.metadata()?.len() != reference.length {
            return Err(invalid("direct checkpoint length mismatch"));
        }
        let mut body = Vec::with_capacity(reference.length as usize);
        file.take(reference.length + 1).read_to_end(&mut body)?;
        if body.len() as u64 != reference.length
            || blake3::hash(&body).as_bytes() != &reference.digest
        {
            return Err(invalid("direct checkpoint digest mismatch"));
        }
        Ok(body)
    }

    /// Called only by the root's single checkpoint owner, after payload sync.
    /// `current` must come from SQL, including after an ambiguous commit error.
    /// At most the SQL-selected generation and this candidate remain on disk.
    pub(crate) fn prepare(&self, body: &[u8], current: Option<&[u8]>) -> io::Result<Vec<u8>> {
        if body.is_empty() || body.len() > MAX_MANIFEST_BYTES {
            return Err(invalid("direct checkpoint exceeds its size limit"));
        }
        let reference = Reference {
            owner: self.owner,
            length: body.len() as u64,
            digest: *blake3::hash(body).as_bytes(),
        };
        let encoded = reference.encode();
        match fs::create_dir(&self.directory) {
            Ok(()) => sync_directory(&self.working_dir)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if !fs::symlink_metadata(&self.directory)?.file_type().is_dir() {
            return Err(invalid("checkpoint directory is not an owned directory"));
        }
        self.reclaim(current)?;
        let path = self.path(&reference);
        if current == Some(encoded.as_slice()) {
            self.read(&encoded)?;
            return Ok(encoded);
        }
        // The content-addressed candidate is not authoritative until SQL names
        // it. A crash during this write leaves an orphan, never a partial commit.
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.write_all(body)?;
        file.sync_all()?;
        sync_directory(&self.directory)?;
        Ok(encoded)
    }

    /// Keep only the SQL-selected generation. Never follow names from the
    /// manifest, and never traverse directories or remove unrelated artifacts.
    pub(crate) fn reclaim(&self, current: Option<&[u8]>) -> io::Result<()> {
        let keep = current
            .filter(|bytes| is_reference(bytes))
            .map(|bytes| self.reference(bytes).map(|reference| self.path(&reference)))
            .transpose()?;
        let prefix = self.prefix();
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if !fs::symlink_metadata(&self.directory)?.file_type().is_dir() {
            return Err(invalid("checkpoint directory is not an owned directory"));
        }
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Some(digest) = name
                .strip_prefix(&prefix)
                .and_then(|n| n.strip_suffix(".manifest"))
            else {
                continue;
            };
            if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            if keep.as_ref() == Some(&entry.path()) {
                continue;
            }
            if !entry.file_type()?.is_file() {
                return Err(invalid("unexpected direct checkpoint artifact type"));
            }
            fs::remove_file(entry.path())?;
        }
        Ok(())
    }
}

/// Reclaim orphan candidates only after loading the complete authoritative
/// inventory. A failed or unsupported reference keeps its root's files until
/// the restore owner has durably invalidated that row.
pub(crate) fn reclaim_unreferenced(
    working_dir: &Path,
    job_id: JobId,
    rows: &std::collections::HashMap<String, Vec<u8>>,
) -> io::Result<()> {
    let directory = working_dir.join(DIRECTORY);
    let metadata = match fs::symlink_metadata(&directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_dir() {
        return Err(invalid("checkpoint directory is not an owned directory"));
    }
    let owners: Vec<_> = rows
        .iter()
        .map(|(name, blob)| {
            let store = ManifestStore::new(working_dir, job_id, name);
            let selected = store
                .reference(blob)
                .ok()
                .map(|reference| store.path(&reference));
            (store.prefix(), selected)
        })
        .collect();
    let job_prefix = ManifestStore::for_job(working_dir, job_id).prefix();
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // A legacy inventory cannot decide the authority of a job checkpoint.
        // Only the job owner may reclaim its candidates after reading SQL.
        if name.starts_with(&job_prefix) {
            continue;
        }
        let Some(body) = name
            .strip_prefix(".weaver-checkpoint-")
            .and_then(|name| name.strip_suffix(".manifest"))
        else {
            continue;
        };
        let Some((owner, digest)) = body.split_once('-') else {
            continue;
        };
        if owner.len() != 64
            || digest.len() != 64
            || !owner
                .bytes()
                .chain(digest.bytes())
                .all(|byte| byte.is_ascii_hexdigit())
        {
            continue;
        }
        if owners.iter().any(|(prefix, selected)| {
            name.starts_with(prefix) && selected.as_ref().is_none_or(|path| *path == entry.path())
        }) {
            continue;
        }
        if !entry.file_type()?.is_file() {
            return Err(invalid("unexpected direct checkpoint artifact type"));
        }
        fs::remove_file(entry.path())?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    // Windows does not support FlushFileBuffers on directories. The immutable
    // candidate itself is flushed before the database reference is committed.
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_manifest_identity_cannot_alias_an_archive_set() {
        let temp = tempfile::tempdir().unwrap();
        let job = ManifestStore::for_job(temp.path(), JobId(1));
        let archive = ManifestStore::new(temp.path(), JobId(1), "");
        let other = ManifestStore::for_job(temp.path(), JobId(2));
        let reference = job.prepare(b"job graph", None).unwrap();
        assert_eq!(job.read(&reference).unwrap(), b"job graph");
        assert!(archive.read(&reference).is_err());
        assert!(other.read(&reference).is_err());
        archive.reclaim(None).unwrap();
        other.reclaim(None).unwrap();
        reclaim_unreferenced(temp.path(), JobId(1), &Default::default()).unwrap();
        assert_eq!(job.read(&reference).unwrap(), b"job graph");
    }

    #[test]
    fn root_reference_is_bounded_and_selected_only_by_database() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path(), JobId(1), "archive");
        let old = store.prepare(b"old checkpoint", None).unwrap();
        let candidate = store.prepare(b"new checkpoint", Some(&old)).unwrap();
        assert_eq!(old.len(), REFERENCE_BYTES);
        assert_eq!(candidate.len(), REFERENCE_BYTES);
        assert_eq!(store.read(&old).unwrap(), b"old checkpoint");
        assert_eq!(store.read(&candidate).unwrap(), b"new checkpoint");
        // Recovery before SQL commit retains old, regardless of newer files.
        store.reclaim(Some(&old)).unwrap();
        assert!(store.read(&candidate).is_err());
        assert_eq!(store.prepare(b"old checkpoint", Some(&old)).unwrap(), old);
        assert_eq!(fs::read_dir(&store.directory).unwrap().count(), 1);
    }

    #[test]
    fn reference_refuses_wrong_root_tampering_and_oversized_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path(), JobId(1), "archive");
        let reference = store.prepare(b"checkpoint", None).unwrap();
        let other = ManifestStore::new(dir.path(), JobId(2), "archive");
        assert!(other.read(&reference).is_err());
        let path = store.path(&store.reference(&reference).unwrap());
        fs::write(path, b"CHECKPOINT").unwrap();
        assert!(store.read(&reference).is_err());
        let mut forged = reference;
        forged[36..44].copy_from_slice(&(MAX_MANIFEST_BYTES as u64 + 1).to_le_bytes());
        assert!(store.read(&forged).is_err());
    }

    #[test]
    fn failed_candidate_is_reclaimed_without_growing_history() {
        let dir = tempfile::tempdir().unwrap();
        let store = ManifestStore::new(dir.path(), JobId(1), "archive");
        let mut current = store.prepare(b"initial", None).unwrap();
        for generation in 0u64..100 {
            let candidate = store
                .prepare(&generation.to_le_bytes(), Some(&current))
                .unwrap();
            assert!(fs::read_dir(&store.directory).unwrap().count() <= 2);
            current = candidate;
            store.reclaim(Some(&current)).unwrap();
            assert_eq!(fs::read_dir(&store.directory).unwrap().count(), 1);
        }
    }
}

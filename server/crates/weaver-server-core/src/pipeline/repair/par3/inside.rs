//! Embedded carrier discovery and explicit staged container self-repair.

use super::*;
use par3_rs::inside::{ContainerLimits, SelfRepairPlan};
use par3_rs::session_repair::{InstalledFile, SessionRepairReport};
use std::path::{Component, Path};

#[derive(Default)]
// Entries are bounded by the admitted jobs' file counts, not engine payloads.
pub(in crate::pipeline) struct Probes(std::collections::HashSet<NzbFileId>);

impl Probes {
    pub(in crate::pipeline) fn contains(&self, file: NzbFileId) -> bool {
        self.0.contains(&file)
    }
    pub(in crate::pipeline) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub(in crate::pipeline) fn insert(&mut self, file: NzbFileId) {
        self.0.insert(file);
    }
    pub(in crate::pipeline) fn remove(&mut self, file: NzbFileId) {
        self.0.remove(&file);
    }
    pub(super) fn remove_job(&mut self, job: JobId) {
        self.0.retain(|file| file.job_id != job);
    }
}

/// Container framing supplies a scan hint, never authentication. Ordinary
/// archives require only bounded framing reads; admitted packets are hashed by
/// the retained scanner using committed source ranges.
pub(super) fn probe(path: PathBuf) -> EngineResult<Option<u64>> {
    let options = execution_options();
    let _reservation = assessment::ViewReservation::acquire(66 << 10)?;
    use std::io::{Read, Seek, SeekFrom};
    // This framing hint establishes no source evidence. In particular, avoid
    // the strong whole-file Windows generation used for admitted publications.
    let _handle = options.handles.acquire()?;
    let mut file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let len = file.metadata()?.len();
    if len < 32 {
        return Ok(None);
    }
    let mut read = |offset, buffer: &mut [u8]| -> EngineResult<()> {
        options.cancel.check()?;
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buffer)?;
        Ok(())
    };
    let mut head = [0; 32];
    read(0, &mut head)?;
    let seven = head.starts_with(&crate::pipeline::direct_unpack::start_header::MAGIC);
    let mut boundary = crate::pipeline::direct_unpack::start_header::StartHeader::parse(&head)
        .and_then(|header| header.total_len())
        .ok();
    if boundary == Some(len) {
        return Ok(None);
    }
    let mut tail = [0; 65_557];
    let size = len.min(tail.len() as u64) as usize;
    let start = len - size as u64;
    read(start, &mut tail[..size])?;
    if !seven {
        for at in (0..=size.saturating_sub(22)).rev() {
            let footer = &tail[at..size];
            if !footer.starts_with(b"PK\x05\x06") || footer.len() < 22 {
                continue;
            }
            let comment = u16::from_le_bytes([footer[20], footer[21]]) as u64;
            if 22 + comment != footer.len() as u64 {
                continue;
            }
            let mut end =
                u64::from(le32(&footer[16..])).checked_add(u64::from(le32(&footer[12..])));
            // A maximum-length comment puts the locator just before the
            // tail window. Read it by absolute offset instead of missing ZIP64.
            let mut locator = [0; 20];
            if let Some(offset) = (start + at as u64).checked_sub(20) {
                read(offset, &mut locator)?;
            }
            if locator.starts_with(b"PK\x06\x07") {
                let record = le64(&locator[8..]);
                if record.checked_add(12).is_some_and(|end| end <= len) {
                    let mut header = [0; 12];
                    read(record, &mut header)?;
                    end = if header.starts_with(b"PK\x06\x06") {
                        record
                            .checked_add(12)
                            .and_then(|n| n.checked_add(le64(&header[4..])))
                            .and_then(|n| n.checked_add(20))
                    } else {
                        None
                    };
                } else {
                    end = None;
                }
            }
            boundary = end.and_then(|n| n.checked_add(22 + comment));
            break;
        }
    }
    if boundary == Some(len) {
        return Ok(None);
    }
    if let Some(boundary) = boundary.filter(|&at| at.checked_add(8).is_some_and(|end| end <= len)) {
        let mut magic = [0; 8];
        read(boundary, &mut magic)?;
        if magic == *par3_rs::MAGIC {
            return Ok(Some(boundary));
        }
    }
    // Damaged original framing may still leave authenticated metadata near the
    // end. The scanner, not this signature hint, decides whether a set exists.
    Ok(tail[..size]
        .windows(par3_rs::MAGIC.len())
        .position(|bytes| bytes == par3_rs::MAGIC)
        .map(|at| start + at as u64))
}

fn le32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[..4].try_into().expect("four bytes"))
}
fn le64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[..8].try_into().expect("eight bytes"))
}

pub(super) fn repair(
    session: &mut par3_rs::Par3RepairSession,
    matrix: par3_rs::Fingerprint,
    output: &Path,
    options: &ExecutionOptions,
) -> EngineResult<SessionRepairReport> {
    let layout = session
        .layout()?
        .ok_or(EngineError::InvalidState("incomplete embedded layout"))?;
    if layout.files().len() != 1 {
        return Err(EngineError::Unsupported("embedded single-file layout"));
    }
    let file = &layout.files()[0];
    // Only an existing, directly bound archive in the job directory can be
    // replaced. Nested destinations need the ordinary engine's path traversal
    // guard and are not implicitly authorized by embedded metadata.
    if Path::new(&file.path).components().count() != 1
        || !matches!(
            Path::new(&file.path).components().next(),
            Some(Component::Normal(_))
        )
    {
        return Err(EngineError::Unsupported("embedded destination path"));
    }
    let destination = output.join(&file.path);
    if !std::fs::symlink_metadata(&destination)?
        .file_type()
        .is_file()
    {
        return Err(EngineError::Unsupported(
            "embedded destination is not a regular file",
        ));
    }
    let plan = SelfRepairPlan::replacement(session, matrix, &[], ContainerLimits::default())?;
    // The job's private scratch tree is excluded from final delivery and
    // retired with the working directory even after an interrupted repair.
    let scratch_root = output.join(".weaver-chunks");
    match std::fs::create_dir(&scratch_root) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if !std::fs::symlink_metadata(&scratch_root)?
                .file_type()
                .is_dir()
            {
                return Err(EngineError::Unsupported(
                    "embedded scratch is not a directory",
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    let scratch = tempfile::Builder::new()
        .prefix("par3-inside-")
        .tempdir_in(&scratch_root)?;
    let staged = scratch.path().join("archive");
    let report = plan.execute(session, &staged, scratch.path())?;
    crate::e2e_failpoint::maybe_trip("par3.inside.staged");
    options.cancel.check()?;
    std::fs::rename(&staged, &destination)?;
    tracing::warn!(path = %destination.display(), restoration = ?report.restoration,
        "Embedded PAR3 carrier replaced; available authenticated packets preserved, original byte-for-byte restoration unavailable");
    Ok(SessionRepairReport {
        installed: vec![InstalledFile {
            path: destination,
            backup: None,
        }],
        reconstructed_blocks: report.reconstructed_blocks,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn ordinary_zip_payload_and_comment_signatures_do_not_admit_a_carrier() {
        let dir = tempfile::tempdir().unwrap();
        for large_file in [false, true] {
            let path = dir
                .path()
                .join(if large_file { "zip64.zip" } else { "plain.zip" });
            let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
            zip.start_file(
                "member",
                zip::write::SimpleFileOptions::default()
                    .compression_method(zip::CompressionMethod::Stored)
                    .large_file(large_file),
            )
            .unwrap();
            zip.write_all(par3_rs::MAGIC).unwrap();
            zip.set_raw_comment(par3_rs::MAGIC.to_vec().into_boxed_slice())
                .unwrap();
            zip.finish().unwrap();
            assert_eq!(probe(path).unwrap(), None);
        }
    }

    #[test]
    fn maximum_zip64_comment_keeps_the_locator_outside_the_probe_window() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("comment.zip");
        let mut zip = zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        zip.start_file("member", zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(b"ordinary archive").unwrap();
        let mut comment = vec![b'x'; u16::MAX as usize];
        comment[..par3_rs::MAGIC.len()].copy_from_slice(par3_rs::MAGIC);
        zip.set_raw_comment(comment.into_boxed_slice()).unwrap();
        zip.set_raw_zip64_extensible_data_sector(Box::new([]));
        zip.finish().unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let footer = bytes.len() - 65_557;
        assert_eq!(&bytes[footer - 20..footer - 16], b"PK\x06\x07");
        assert_eq!(probe(path).unwrap(), None);
    }

    #[test]
    fn framing_probe_handles_empty_missing_and_short_archives() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("archive.zip");
        assert_eq!(probe(path.clone()).unwrap(), None);
        for size in [0, 1, 7, 31, 32, 100] {
            std::fs::write(&path, vec![0; size]).unwrap();
            assert_eq!(probe(path.clone()).unwrap(), None);
        }
    }
}

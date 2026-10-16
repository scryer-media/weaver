use super::*;
use crate::pipeline::extraction::output::{ArchiveOutput, Integrity, Metadata, OutputMember};

struct ProgressWriter<W> {
    inner: W,
    attempt: Arc<PhaseAttemptCounters>,
}
impl<W: Write> Write for ProgressWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.attempt.record_completed(written as u64);
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Pipeline {
    pub(crate) fn extract_rar_member_to_sink<O: ArchiveOutput>(
        archive: &mut unrar_rs::RarArchive,
        volume_paths: &std::collections::BTreeMap<u32, PathBuf>,
        idx: usize,
        options: &unrar_rs::ExtractOptions,
        output: &O,
        budget: &Arc<JobExtractionBudget>,
        counters: Arc<PhaseCounters>,
    ) -> Result<(String, u64, u64), ExtractionError> {
        let info = archive
            .member_info(idx)
            .ok_or_else(|| ExtractionError::terminal("RAR member missing"))?;
        let relative = validate_sanitized_rar_member_path(&info.name)
            .map_err(|e| budget.reject_unsafe_path(e))?;
        let length = info.unpacked_size.unwrap_or(0);
        budget.check_member_metadata(&info.name, length)?;
        if info.is_symlink || info.is_hardlink || info.is_file_copy {
            return Err(budget
                .reject_unsupported_entry(format!(
                    "RAR member '{}' is a link or file-copy entry",
                    info.name
                ))
                .into());
        }
        if info.is_directory {
            output
                .directory(idx, &relative)
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            output
                .directory_metadata(idx, &relative, Metadata::rar(&info))
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            return Ok((info.name, 0, 0));
        }
        let attempt = Arc::new(PhaseAttemptCounters::new(counters));
        attempt.reserve_total(length);
        let result = (|| {
            let inner = output
                .file(idx, &relative, info.unpacked_size)
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            let mut writer = ProgressWriter {
                inner,
                attempt: Arc::clone(&attempt),
            };
            let written = if archive.is_solid() {
                member::rar_entry(archive, idx, options)
                    .and_then(|entry| entry.copy_to(&mut writer))
            } else {
                let mut paths = std::collections::HashMap::new();
                for volume in info.volumes.first_volume..=info.volumes.last_volume {
                    let path = volume_paths.get(&(volume as u32)).ok_or_else(|| {
                        ExtractionError::terminal(format!(
                            "missing local RAR volume {volume} for member {}",
                            info.name
                        ))
                    })?;
                    paths.insert(volume, path.clone());
                }
                let provider = super::readahead::ReadaheadVolumeProvider::new(paths);
                let provider =
                    member::BudgetedRarVolumeProvider::new(&provider, Arc::clone(budget));
                member::rar_entry_via(archive, idx, &provider, options)
                    .and_then(|entry| entry.copy_to(&mut writer))
            }
            .map_err(|e| ExtractionError::terminal(e.to_string()))?;
            writer
                .flush()
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            writer
                .inner
                .metadata(Metadata::rar(&info))
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            writer
                .inner
                .finish_data()
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?
                .integrity(if info.crc32.is_some() || info.hash.is_some() {
                    Integrity::Verified
                } else {
                    Integrity::NoChecksum
                })
                .map_err(|e| ExtractionError::from_io(e.to_string(), &e))?;
            Ok((info.name, written, length))
        })();
        if result.is_ok() {
            attempt.commit();
        } else {
            attempt.rollback();
        }
        result
    }
}

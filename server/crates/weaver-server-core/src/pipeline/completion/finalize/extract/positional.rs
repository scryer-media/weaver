use super::*;
use crate::pipeline::direct_unpack::coverage::RunEpoch;
use crate::pipeline::direct_unpack::sevenz_runs::{
    MemberSpec, PositionalManifest, RunJournal, RunSource, decode_pass,
};
use crate::pipeline::direct_unpack::{GatedSplitReader, SetCoverage};
use sevenz_turbo::{PositionalLzma2Folder, ReadAt};
use std::io::{self, Read, Seek, SeekFrom};

pub(in crate::pipeline) struct PositionalHandoff {
    pub(in crate::pipeline) manifest: Arc<PositionalManifest>,
    pub(in crate::pipeline) epoch: RunEpoch,
}

pub(in crate::pipeline) type PositionalSlot = Arc<std::sync::Mutex<Option<PositionalHandoff>>>;

/// What a positional chase does with a member that is itself an archive.
#[derive(Clone, Copy)]
pub(in crate::pipeline) enum ChildArchives {
    /// Written in place like any other member; the nested engine opens them
    /// from disk once the set is installed.
    Written,
    /// Owed to the set's stream, so the positional pass declines the archive
    /// and the streaming decoder routes them.
    Streamed,
}

struct ReplaySelection<'a> {
    coverage: &'a SetCoverage,
    selected: bool,
}
impl Drop for ReplaySelection<'_> {
    fn drop(&mut self) {
        if !self.selected {
            self.coverage.disable_run_replay();
        }
    }
}

// Capture the parser's logical reads above buffering. Encoded-header PackInfo
// may point outside the conventional archive tail; only observed source ranges
// establish which bytes supplied the metadata. Read-ahead payload is not captured.
struct HeaderReads<R> {
    inner: R,
    position: u64,
    logical_bytes: u64,
    limit: u64,
    ranges: Vec<(u64, u64)>,
}

impl<R> HeaderReads<R> {
    fn new(inner: R, limit: u64) -> io::Result<Self> {
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(1).map_err(io::Error::other)?;
        ranges.push((0, 32)); // Always include the complete signature/start header.
        Ok(Self {
            inner,
            position: 0,
            logical_bytes: 0,
            limit,
            ranges,
        })
    }

    fn record(&mut self, start: u64, mut end: u64) -> io::Result<()> {
        let first = self.ranges.partition_point(|&(_, old_end)| old_end < start);
        let mut last = first;
        let mut start = start;
        while let Some(&(old_start, old_end)) = self.ranges.get(last) {
            if old_start > end {
                break;
            }
            start = start.min(old_start);
            end = end.max(old_end);
            last += 1;
        }
        if first == last {
            if self.ranges.len() >= 1024 {
                return Err(io::Error::other("too many positional header source ranges"));
            }
            self.ranges.try_reserve_exact(1).map_err(io::Error::other)?;
            self.ranges.insert(first, (start, end));
        } else {
            self.ranges[first] = (start, end);
            self.ranges.drain(first + 1..last);
        }
        Ok(())
    }
}

impl<R: Read> Read for HeaderReads<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        let remaining = self.limit.saturating_sub(self.logical_bytes);
        if remaining == 0 {
            return Err(io::Error::other(
                "positional header logical reads exceed memory policy",
            ));
        }
        let wanted = bytes
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let count = self.inner.read(&mut bytes[..wanted])?;
        let end = self
            .position
            .checked_add(count as u64)
            .ok_or(io::ErrorKind::InvalidData)?;
        self.logical_bytes += count as u64;
        if count != 0 {
            self.record(self.position, end)?;
        }
        self.position = end;
        Ok(count)
    }
}

impl<R: Seek> Seek for HeaderReads<R> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.position = self.inner.seek(position)?;
        Ok(self.position)
    }
}

struct HeaderIdentity {
    ranges: Vec<(u64, u64)>,
    total: u64,
    digest: [u8; 32],
}

impl HeaderIdentity {
    fn read(
        source: &dyn ReadAt,
        ranges: Vec<(u64, u64)>,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<Self, String> {
        let total = source.size().map_err(|error| error.to_string())?;
        let digest = Self::fingerprint(source, &ranges, total, budget)?;
        Ok(Self {
            ranges,
            total,
            digest,
        })
    }

    fn matches(
        &self,
        source: &dyn ReadAt,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<bool, String> {
        if source.size().map_err(|error| error.to_string())? != self.total {
            return Ok(false);
        }
        Ok(Self::fingerprint(source, &self.ranges, self.total, budget)? == self.digest)
    }

    fn fingerprint(
        source: &dyn ReadAt,
        ranges: &[(u64, u64)],
        total: u64,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<[u8; 32], String> {
        let mut length = 0u64;
        for &(start, end) in ranges {
            if start > end || end > total {
                return Err("invalid positional header source range".to_string());
            }
            length = length
                .checked_add(end - start)
                .ok_or("header source size overflow")?;
        }
        if length > budget.max_memory_bytes() {
            return Err("positional header image exceeds memory policy".to_string());
        }
        let scratch_bytes = (64 << 10).min(
            budget
                .max_memory_bytes()
                .saturating_sub(budget.memory_reserved_bytes()),
        ) as usize;
        if scratch_bytes == 0 {
            return Err("positional header check exceeds remaining memory policy".to_string());
        }
        let _scratch = budget.reserve_more_memory_wait(scratch_bytes as u64)?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(scratch_bytes)
            .map_err(|error| error.to_string())?;
        buffer.resize(scratch_bytes, 0);
        let mut digest = lzma_turbo::crypto::awslc::Sha256::new();
        for &(mut offset, end) in ranges {
            digest.update(&offset.to_le_bytes());
            digest.update(&end.to_le_bytes());
            while offset < end {
                let wanted = buffer
                    .len()
                    .min(usize::try_from(end - offset).unwrap_or(usize::MAX));
                let read = source
                    .read_at(&mut buffer[..wanted], offset)
                    .map_err(|error| error.to_string())?;
                if read == 0 {
                    return Err("truncated positional header image".to_string());
                }
                digest.update(&buffer[..read]);
                offset += read as u64;
            }
        }
        Ok(digest.finalize())
    }
}

#[allow(clippy::too_many_arguments)]
pub(in crate::pipeline) fn try_extract(
    context: &SevenZipExtractionContext,
    paths: &[PathBuf],
    coverage: &Arc<SetCoverage>,
    pool: &Arc<rayon::ThreadPool>,
    destination: &Path,
    slot: &PositionalSlot,
    children: ChildArchives,
) -> Result<Option<FullSetExtractionOutcome>, String> {
    try_extract_inner(
        context,
        paths,
        coverage,
        pool,
        destination,
        slot,
        children,
        |_| {},
    )
}

#[allow(clippy::too_many_arguments)]
fn try_extract_inner(
    context: &SevenZipExtractionContext,
    paths: &[PathBuf],
    coverage: &Arc<SetCoverage>,
    pool: &Arc<rayon::ThreadPool>,
    destination: &Path,
    slot: &PositionalSlot,
    children: ChildArchives,
    on_created: impl FnOnce(&Arc<PositionalManifest>),
) -> Result<Option<FullSetExtractionOutcome>, String> {
    let end_header_bytes = match context.decode_memory {
        SevenZipDecodeMemory::ReservedPerPass {
            end_header_bytes, ..
        } => end_header_bytes,
        _ => return Ok(None),
    };
    let mut selection = ReplaySelection {
        coverage,
        selected: false,
    };
    let mut epoch = coverage
        .begin_run_probe()
        .map_err(|error| error.to_string())?;
    let read_header = |password: &sevenz_turbo::Password| {
        // Payload memory is admitted per capped run below. The ordinary
        // listing's full-dictionary estimate would reject otherwise bounded
        // runs before their size is known. Header parsing/KDF limits stay the
        // same; unsupported payload graphs return to the ordinary path.
        // The parse failure keeps its class until the password retry below
        // has read it; the other failures are reported as they read.
        let permit = context
            .budget
            .reserve_memory_wait(chase_header_pass_memory_bytes(
                end_header_bytes,
                context.budget.max_memory_bytes(),
            ))
            .map_err(|error| ExtractionError::terminal(String::from(error)))?;
        let reader = GatedSplitReader::open(paths, Arc::clone(coverage))
            .map_err(|error| ExtractionError::terminal(error.to_string()))?;
        let reader = BudgetedReader::new(
            std::io::BufReader::with_capacity(64 << 10, reader),
            Arc::clone(&context.budget),
        );
        let mut reader = HeaderReads::new(reader, context.budget.max_memory_bytes())
            .map_err(|error| ExtractionError::terminal(error.to_string()))?;
        let archive = sevenz_turbo::Archive::read_with_limits(
            &mut reader,
            password,
            &sevenz_archive_limits(&context.budget),
        )
        .map_err(|error| sevenz_extraction_error(&error))?;
        Ok::<_, ExtractionError>((archive, permit, reader.ranges))
    };
    let ((archive, _header_memory, header_ranges), encrypted_headers) = loop {
        let parsed = match read_header(&sevenz_turbo::Password::empty()) {
            Ok(archive) => Ok((archive, false)),
            Err(error)
                if error.is(ExtractionErrorClass::PasswordRequired)
                    && !context.password.is_empty() =>
            {
                read_header(&context.password).map(|archive| (archive, true))
            }
            Err(error) => Err(error),
        };
        if epoch.check().is_err() {
            epoch = coverage.run_epoch().map_err(|error| error.to_string())?;
            continue;
        }
        break parsed.map_err(String::from)?;
    };
    if matches!(children, ChildArchives::Streamed)
        && archive.files.iter().any(|entry| {
            !entry.is_directory()
                && crate::pipeline::extraction::output::routes_to_child_engine(Path::new(
                    entry.name(),
                ))
        })
    {
        return Ok(None);
    }
    let archive = Arc::new(archive);
    // New positional adapters retain file maps/checksum boundaries and a
    // per-volume handle cache alongside the parsed archive. Include both
    // caches briefly present while a repaired epoch replaces its source.
    let adapter_bytes = (archive.files.len() as u64)
        .checked_mul(128)
        .and_then(|bytes| bytes.checked_add((archive.blocks.len() as u64).checked_mul(256)?))
        .and_then(|bytes| bytes.checked_add((paths.len() as u64).checked_mul(128)?))
        .ok_or("positional adapter memory overflow")?;
    let adapter_bytes = paths.iter().try_fold(adapter_bytes, |bytes, path| {
        (path.as_os_str().as_encoded_bytes().len() as u64)
            .checked_mul(2)
            .and_then(|path| bytes.checked_add(path))
            .ok_or("positional source memory overflow")
    })?;
    if adapter_bytes
        > context
            .budget
            .max_memory_bytes()
            .saturating_sub(context.budget.memory_reserved_bytes())
    {
        return Err("positional adapters exceed remaining memory policy".to_string());
    }
    let _adapter_memory = context.budget.reserve_more_memory_wait(adapter_bytes)?;
    let paths: Arc<[PathBuf]> = Arc::from(paths.to_vec());
    let mut source = Arc::new(
        RunSource::new(Arc::clone(&paths), Arc::clone(coverage), epoch.clone())
            .map_err(|error| error.to_string())?,
    );
    let mut folders = Vec::new();
    folders
        .try_reserve_exact(archive.blocks.len())
        .map_err(|error| error.to_string())?;
    for index in 0..archive.blocks.len() {
        let Some(folder) = PositionalLzma2Folder::new(
            Arc::clone(&archive),
            source.clone(),
            index,
            sevenz_archive_limits(&context.budget),
        )
        .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        if folder.entries().any(|(_, entry)| !entry.has_crc) {
            return Ok(None);
        }
        folders.push(folder);
    }
    if folders.is_empty() {
        return Ok(None);
    }
    coverage
        .select_run_replay(&epoch)
        .map_err(|error| error.to_string())?;
    let root = Arc::new(ExtractionRoot::open(destination).map_err(|error| error.to_string())?);
    let mut specs = Vec::new();
    specs
        .try_reserve_exact(archive.files.len())
        .map_err(|error| error.to_string())?;
    let mut extracted = Vec::new();
    extracted
        .try_reserve_exact(archive.files.len())
        .map_err(|error| error.to_string())?;
    let mut known_total = 0u64;
    for entry in &archive.files {
        let name = root
            .validate_relative_path(entry.name())
            .map_err(|error| context.budget.reject_unsafe_path(error))?;
        context
            .budget
            .check_member_metadata(entry.name(), entry.size())?;
        if entry.is_anti_item() {
            specs.push(None);
            continue;
        }
        if !entry.is_directory() {
            known_total = known_total
                .checked_add(entry.size())
                .ok_or("declared positional output overflow")?;
            extracted.push(entry.name().to_string());
        }
        specs.push(Some(MemberSpec {
            name,
            len: entry.size(),
            directory: entry.is_directory(),
            times: sevenz_entry_times(entry).map(|times| times.file_times()),
            directory_times: sevenz_entry_times(entry)
                .map(|times| (times.modified, times.accessed)),
        }));
    }
    context
        .budget
        .check_archive_metadata(specs.iter().flatten().count() as u64, Some(known_total))?;
    let identity = HeaderIdentity::read(&*source, header_ranges, &context.budget)?;
    let mut owner = lzma_turbo::crypto::awslc::Sha256::new();
    owner.update(context.set_name.as_bytes());
    owner.update(&identity.digest);
    let owner = owner
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let manifest = PositionalManifest::create(
        root,
        destination.to_path_buf(),
        Arc::clone(&context.budget),
        &owner,
        specs,
    )?;
    on_created(&manifest);
    selection.selected = true;
    let mut journal = RunJournal::default();
    loop {
        let decoded = decode_pass(
            &folders,
            &*source,
            coverage,
            &epoch,
            &manifest,
            &context.budget,
            pool,
            &mut journal,
        );
        if epoch.check().is_ok() {
            decoded?;
            break;
        }
        // decode_pass has joined every old worker before any output is reused.
        epoch = coverage.run_epoch().map_err(|error| error.to_string())?;
        source = Arc::new(
            RunSource::new(Arc::clone(&paths), Arc::clone(coverage), epoch.clone())
                .map_err(|error| error.to_string())?,
        );
        if !identity.matches(&*source, &context.budget)? {
            return Err("repair changed the positional archive header".to_string());
        }
        folders.clear();
        for index in 0..archive.blocks.len() {
            folders.push(
                PositionalLzma2Folder::new(
                    Arc::clone(&archive),
                    source.clone(),
                    index,
                    sevenz_archive_limits(&context.budget),
                )
                .map_err(|error| error.to_string())?
                .ok_or("positional folder graph changed")?,
            );
        }
    }
    epoch
        .commit(|| {
            *slot.lock().unwrap_or_else(|p| p.into_inner()) = Some(PositionalHandoff {
                manifest,
                epoch: epoch.clone(),
            });
            context
                .phase_counters
                .total_bytes
                .store(known_total, Ordering::Relaxed);
            context
                .phase_counters
                .completed_bytes
                .store(known_total, Ordering::Relaxed);
            Ok(())
        })
        .map_err(|error| error.to_string())?;
    let selected_password = encrypted_headers
        .then(|| {
            let units: Vec<_> = context
                .password
                .as_slice()
                .chunks_exact(2)
                .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
                .collect();
            String::from_utf16(&units).ok()
        })
        .flatten();
    Ok(Some(FullSetExtractionOutcome {
        extracted,
        failed: Vec::new(),
        selected_password,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::metrics::PipelineMetrics;
    use crate::pipeline::extraction::ExtractionLimits;
    use sevenz_turbo::{
        ArchiveEntry, ArchiveWriter, SourceReader,
        encoder_options::{AesEncoderOptions, Lzma2Options},
    };
    use std::io::Cursor;

    fn fixture(copy: bool) -> (Vec<u8>, Vec<Vec<u8>>) {
        fixture_layout(copy, true)
    }

    fn fixture_layout(copy: bool, solid: bool) -> (Vec<u8>, Vec<Vec<u8>>) {
        let part_len = if solid { 1_048_593 } else { 262_161 };
        let parts: Vec<Vec<u8>> = (0..3)
            .map(|part| {
                (0..part_len + part * 14)
                    .map(|i| ((i * 29) ^ (i >> 7) ^ part) as u8)
                    .collect()
            })
            .collect();
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        let mut options = Lzma2Options::from_level_mt(1, 2, 1 << 20);
        options.set_dictionary_size(1 << 16);
        writer.set_content_methods(if copy {
            vec![sevenz_turbo::EncoderMethod::COPY.into()]
        } else {
            vec![options.into()]
        });
        writer.set_encrypt_header(false);
        let entries: Vec<_> = parts
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let mut entry = ArchiveEntry::new_file(&format!("folder/member-{index}"));
                entry.size = bytes.len() as u64;
                entry
            })
            .collect();
        if solid {
            writer
                .push_archive_entries(
                    entries,
                    parts
                        .iter()
                        .map(|bytes| SourceReader::new(bytes.as_slice()))
                        .collect(),
                )
                .unwrap();
        } else {
            for (index, (entry, bytes)) in entries.into_iter().zip(&parts).enumerate() {
                writer
                    .push_archive_entry(entry, Some(Cursor::new(bytes)))
                    .unwrap();
                writer
                    .push_archive_entry(
                        ArchiveEntry::new_file(&format!("gap-{index}")),
                        Some(Cursor::new(Vec::<u8>::new())),
                    )
                    .unwrap();
            }
        }
        writer
            .push_archive_entry(
                ArchiveEntry::new_file("empty"),
                Some(Cursor::new(Vec::<u8>::new())),
            )
            .unwrap();
        (writer.finish().unwrap().into_inner(), parts)
    }

    fn test_context(
        root: &Path,
        archive_len: u64,
        end_header_bytes: u64,
    ) -> SevenZipExtractionContext {
        let private = root.join("private");
        let root = Arc::new(ExtractionRoot::create_empty(&private).unwrap());
        let limits = Arc::new(ExtractionLimits {
            max_job_bytes: 32 << 20,
            max_member_bytes: 16 << 20,
            max_entries: 64,
            max_ratio: 10000,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 256 << 20,
        });
        let budget = JobExtractionBudget::new(
            limits,
            private.clone(),
            archive_len,
            0,
            0,
            PipelineMetrics::new(),
        )
        .unwrap();
        let (event_tx, _) = tokio::sync::broadcast::channel(1);
        SevenZipExtractionContext {
            job_id: JobId(41),
            set_name: "positional-fixture".to_string(),
            root,
            budget,
            password: sevenz_turbo::Password::empty(),
            event_tx,
            phase_counters: Arc::new(PhaseCounters::default()),
            decode_memory: SevenZipDecodeMemory::ReservedPerPass {
                end_header_bytes,
                widening: ChaseWidening::default(),
            },
            decode_threads: 2,
        }
    }

    fn source(root: &Path, bytes: &[u8]) -> (Vec<PathBuf>, Arc<SetCoverage>, u64) {
        let path = root.join("source.7z");
        std::fs::write(&path, bytes).unwrap();
        let coverage = Arc::new(SetCoverage::new(1));
        coverage.set_total_len(bytes.len() as u64);
        coverage.note_part_len(0, bytes.len() as u64);
        coverage.mark_part_complete(0);
        let end_header = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        (vec![path], coverage, end_header)
    }

    #[test]
    fn positional_chase_keeps_outputs_provisional_until_verified_handoff() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        let outcome = try_extract(
            &context,
            &paths,
            &coverage,
            &pool,
            &destination,
            &slot,
            ChildArchives::Written,
        )
        .unwrap()
        .unwrap();
        assert_eq!(outcome.extracted.len(), 4);
        assert!(!destination.join("folder/member-0").exists());
        let handoff = slot.lock().unwrap().take().unwrap();
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
        assert_eq!(
            std::fs::metadata(destination.join("empty")).unwrap().len(),
            0
        );
        assert_eq!(
            context
                .phase_counters
                .completed_bytes
                .load(Ordering::Relaxed),
            parts.iter().map(|part| part.len() as u64).sum::<u64>()
        );
    }

    // A header encrypted with the content lists only once the job's password
    // opens it, so the chase reads it again with that password.
    #[test]
    fn positional_chase_reads_an_encrypted_header_with_the_jobs_password() {
        let temp = tempfile::tempdir().unwrap();
        let member: Vec<u8> = (0..262_161usize)
            .map(|i| ((i * 29) ^ (i >> 7)) as u8)
            .collect();
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        let mut options = Lzma2Options::from_level_mt(1, 2, 1 << 20);
        options.set_dictionary_size(1 << 16);
        writer.set_content_methods(vec![
            AesEncoderOptions::new(sevenz_turbo::Password::new("fixture password")).into(),
            options.into(),
        ]);
        writer.set_encrypt_header(true);
        let mut entry = ArchiveEntry::new_file("member");
        entry.size = member.len() as u64;
        writer
            .push_archive_entry(entry, Some(Cursor::new(&member)))
            .unwrap();
        let bytes = writer.finish().unwrap().into_inner();
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let mut context = test_context(temp.path(), bytes.len() as u64, end_header);
        context.password = sevenz_turbo::Password::new("fixture password");
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        // AES content is no positional run, so the set goes on to the
        // streaming chase instead of failing as if it had no password.
        let outcome = try_extract(
            &context,
            &paths,
            &coverage,
            &pool,
            &destination,
            &slot,
            ChildArchives::Written,
        );
        assert!(matches!(outcome, Ok(None)), "{:?}", outcome.err());
        assert!(slot.lock().unwrap().is_none());
    }

    #[test]
    fn positional_chase_refuses_stale_publication_and_preserves_copy_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, _) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        assert!(
            try_extract(
                &context,
                &paths,
                &coverage,
                &pool,
                &destination,
                &slot,
                ChildArchives::Written
            )
            .unwrap()
            .is_some()
        );
        let handoff = slot.lock().unwrap().take().unwrap();
        coverage.pause_for_repair();
        assert!(
            handoff
                .manifest
                .publish(&handoff.epoch, &destination)
                .is_err()
        );
        assert!(!destination.join("folder/member-0").exists());
        drop(handoff);
        assert!(
            std::fs::read_dir(destination.join(".weaver-chunks"))
                .unwrap()
                .next()
                .is_none()
        );

        let temp = tempfile::tempdir().unwrap();
        let (bytes, _) = fixture(true);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let destination = temp.path().join("destination");
        assert!(
            try_extract(
                &context,
                &paths,
                &coverage,
                &pool,
                &destination,
                &slot,
                ChildArchives::Written
            )
            .unwrap()
            .is_none()
        );
        assert!(!coverage.has_run_replay());
        assert!(!destination.exists());
    }

    #[test]
    fn independent_folders_overlap_on_the_caller_pool() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture_layout(false, false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let visited = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
        let observed = Arc::clone(&visited);
        let outcome = try_extract_inner(
            &context,
            &paths,
            &coverage,
            &pool,
            &destination,
            &slot,
            ChildArchives::Written,
            |manifest| {
                manifest.observe_writes(Arc::new(move |file, _| {
                    // The first two independently writable folders must both reach
                    // the sink before either is allowed to finish. Empty members
                    // between them must not shift the archive file mapping.
                    let first = observed.lock().unwrap().insert(file);
                    if first && (file == 0 || file == 2) {
                        barrier.wait();
                    }
                }))
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(outcome.extracted.len(), 7);
        assert!(visited.lock().unwrap().contains(&2));
        let handoff = slot.lock().unwrap().take().unwrap();
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
            assert_eq!(
                std::fs::metadata(destination.join(format!("gap-{index}")))
                    .unwrap()
                    .len(),
                0
            );
        }
    }

    #[test]
    fn restart_redecodes_without_trusting_or_publishing_crash_partials() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let workspace = destination.join(".weaver-chunks");
        std::fs::create_dir_all(&workspace).unwrap();
        let old = workspace.join("positional-stale-generation.partial");
        std::fs::write(&old, b"unverified previous process output").unwrap();
        let slot = Arc::new(std::sync::Mutex::new(None));
        assert!(
            try_extract(
                &context,
                &paths,
                &coverage,
                &pool,
                &destination,
                &slot,
                ChildArchives::Written
            )
            .unwrap()
            .is_some()
        );
        let handoff = slot.lock().unwrap().take().unwrap();
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        drop(handoff);
        assert_eq!(
            std::fs::read(&old).unwrap(),
            b"unverified previous process output"
        );
        assert_eq!(
            std::fs::read_dir(&workspace).unwrap().count(),
            1,
            "only the current generation's owned aliases are removed"
        );
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
    }

    #[test]
    fn repair_during_a_sink_write_redecodes_before_any_member_is_published() {
        repair_during_sink_write(false);
    }

    #[test]
    fn repair_that_changes_the_header_refuses_all_provisional_output() {
        repair_during_sink_write(true);
    }

    fn repair_during_sink_write(change_header: bool) {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        let (entered, ready) = std::sync::mpsc::sync_channel(1);
        let (resume, released) = std::sync::mpsc::sync_channel(1);
        let released = std::sync::Mutex::new(released);
        let writes = Arc::new(AtomicU32::new(0));
        let worker_writes = Arc::clone(&writes);
        let worker_coverage = Arc::clone(&coverage);
        let worker_destination = destination.clone();
        let worker_slot = Arc::clone(&slot);
        let worker_paths = paths.clone();
        let worker = std::thread::spawn(move || {
            try_extract_inner(
                &context,
                &worker_paths,
                &worker_coverage,
                &pool,
                &worker_destination,
                &worker_slot,
                ChildArchives::Written,
                |manifest| {
                    manifest.observe_writes(Arc::new(move |_, _| {
                        if worker_writes.fetch_add(1, Ordering::AcqRel) == 0 {
                            entered.send(()).unwrap();
                            released.lock().unwrap().recv().unwrap();
                        }
                    }));
                },
            )
        });
        ready.recv().unwrap();
        let old = coverage.run_epoch().unwrap();
        coverage.pause_for_repair();
        assert!(old.check().is_err());
        assert!(slot.lock().unwrap().is_none());
        assert!(!destination.join("folder/member-0").exists());
        let replacement = temp.path().join("repaired.7z");
        let mut repaired = bytes.clone();
        if change_header {
            *repaired.last_mut().unwrap() ^= 1;
        }
        std::fs::write(&replacement, &repaired).unwrap();
        std::fs::rename(replacement, &paths[0]).unwrap();
        resume.send(()).unwrap();
        coverage.release_after_repair(0, bytes.len() as u64);
        coverage.resume_after_repair();
        let result = worker.join().unwrap();
        if change_header {
            assert!(
                result
                    .err()
                    .expect("changed header must refuse output")
                    .contains("repair changed the positional archive header")
            );
            assert!(slot.lock().unwrap().is_none());
            assert!(!destination.join("folder/member-0").exists());
            assert!(
                std::fs::read_dir(destination.join(".weaver-chunks"))
                    .unwrap()
                    .next()
                    .is_none()
            );
            return;
        }
        assert!(result.unwrap().is_some());
        assert!(writes.load(Ordering::Acquire) > 1);
        let handoff = slot.lock().unwrap().take().unwrap();
        assert!(handoff.epoch.generation > old.generation);
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
    }

    #[tokio::test]
    async fn pending_install_keeps_positional_budget_active_until_publication() {
        use crate::pipeline::direct_unpack::wiring::{
            ChaseDisposition, PendingChase, PendingChaseCancellation,
        };
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        let budget = Arc::clone(&context.budget);
        let counters = Arc::clone(&context.phase_counters);
        let staging_dir = temp.path().join("private");
        let worker_coverage = Arc::clone(&coverage);
        let worker_slot = Arc::clone(&slot);
        let worker_destination = destination.clone();
        let handle = tokio::task::spawn_blocking(move || {
            try_extract(
                &context,
                &paths,
                &worker_coverage,
                &pool,
                &worker_destination,
                &worker_slot,
                ChildArchives::Written,
            )?
            .ok_or_else(|| "fixture must select positional decoding".to_string())
        });
        let pending = PendingChase {
            cancel_on_drop: PendingChaseCancellation::new(
                Arc::clone(&coverage),
                Arc::clone(&budget),
            ),
            positional: slot,
            budget: Arc::clone(&budget),
            handle,
            staging_dir: staging_dir.clone(),
            counters,
            coverage,
            set_name: "positional-fixture".to_string(),
        };
        let phase = Arc::new(PhaseCounters::default());
        let outcome = install_direct_unpack(
            ChaseDisposition::Pending(pending),
            destination.clone(),
            Arc::clone(&phase),
            JobId(41),
            "positional-fixture",
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(outcome.extracted.len(), 4);
        assert!(!staging_dir.exists());
        assert!(budget.check_active_io().is_ok());
        assert_eq!(
            phase.completed_bytes.load(Ordering::Relaxed),
            parts.iter().map(|p| p.len() as u64).sum::<u64>()
        );
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
    }

    #[test]
    fn repair_reuses_only_completed_runs_inside_the_new_vouched_prefix() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let archive = Arc::new(
            sevenz_turbo::Archive::read(&mut Cursor::new(&bytes), &sevenz_turbo::Password::empty())
                .unwrap(),
        );
        let epoch = coverage.enable_run_replay().unwrap();
        let paths: Arc<[PathBuf]> = Arc::from(paths);
        let source = Arc::new(
            RunSource::new(Arc::clone(&paths), Arc::clone(&coverage), epoch.clone()).unwrap(),
        );
        let folder = PositionalLzma2Folder::new(
            Arc::clone(&archive),
            source.clone(),
            0,
            sevenz_archive_limits(&context.budget),
        )
        .unwrap()
        .unwrap();
        let range = folder.packed_range();
        let mut scanner = folder.scanner();
        let mut position = range.offset as usize;
        let first = loop {
            let used = scanner
                .feed(&bytes[position..range.end() as usize])
                .unwrap();
            position += used;
            if let Some(run) = scanner.next_run() {
                break run;
            }
            assert_ne!(used, 0);
        };
        assert!(first.output_len() <= parts[0].len() as u64);
        let destination = temp.path().join("destination");
        let manifest = PositionalManifest::create(
            Arc::new(ExtractionRoot::open(&destination).unwrap()),
            destination.clone(),
            Arc::clone(&context.budget),
            &"a".repeat(64),
            archive
                .files
                .iter()
                .map(|entry| {
                    Some(MemberSpec {
                        name: PathBuf::from(entry.name()),
                        len: entry.size(),
                        directory: entry.is_directory(),
                        times: None,
                        directory_times: None,
                    })
                })
                .collect(),
        )
        .unwrap();
        let writes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&writes);
        manifest.observe_writes(Arc::new(move |file, offset| {
            observed.lock().unwrap().push((file, offset))
        }));
        let mut journal = RunJournal::default();
        decode_pass(
            &[folder],
            &*source,
            &coverage,
            &epoch,
            &manifest,
            &context.budget,
            &pool,
            &mut journal,
        )
        .unwrap();
        assert!(writes.lock().unwrap().contains(&(0, 0)));
        writes.lock().unwrap().clear();
        coverage.pause_for_repair();
        assert!(coverage.set_run_vouches(&[first.packed_range().end()]));
        coverage.release_after_repair(0, bytes.len() as u64);
        coverage.resume_after_repair();
        let epoch = coverage.run_epoch().unwrap();
        let source = Arc::new(RunSource::new(paths, Arc::clone(&coverage), epoch.clone()).unwrap());
        let folder = PositionalLzma2Folder::new(
            archive,
            source.clone(),
            0,
            sevenz_archive_limits(&context.budget),
        )
        .unwrap()
        .unwrap();
        decode_pass(
            &[folder],
            &*source,
            &coverage,
            &epoch,
            &manifest,
            &context.budget,
            &pool,
            &mut journal,
        )
        .unwrap();
        let writes = writes.lock().unwrap();
        assert!(!writes.is_empty(), "unvouched runs must be decoded again");
        assert!(
            writes
                .iter()
                .all(|&(file, offset)| file != 0 || offset >= first.output_len()),
            "the vouched completed first run must not be rewritten"
        );
        drop(writes);
        drop(journal);
        assert!(
            context.budget.memory_reserved_bytes() > 0,
            "manifest remains charged until handoff ends"
        );
        manifest.publish(&epoch, &destination).unwrap();
        drop(manifest);
        assert_eq!(context.budget.memory_reserved_bytes(), 0);
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
    }

    #[test]
    fn short_run_with_an_oversized_dictionary_uses_its_bounded_admission() {
        let temp = tempfile::tempdir().unwrap();
        let data = vec![b'x'; 100_003];
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        writer.set_encrypt_header(false);
        let mut options = Lzma2Options::from_level(1);
        options.set_dictionary_size(1 << 16);
        writer.set_content_methods(vec![options.into()]);
        writer
            .push_archive_entry(ArchiveEntry::new_file("a"), Some(Cursor::new(&data)))
            .unwrap();
        let mut bytes = writer.finish().unwrap().into_inner();
        let header = 32 + u64::from_le_bytes(bytes[12..20].try_into().unwrap()) as usize;
        assert_eq!(bytes[header], 1, "small fixture has a plain header");
        let coder = bytes[header..]
            .windows(4)
            .position(|part| part == [0x21, 0x21, 1, 8])
            .expect("one LZMA2 coder with a 64 KiB dictionary");
        bytes[header + coder + 3] = 40; // A valid 4 GiB advertised dictionary.
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &bytes[header..]) as u32;
        bytes[28..32].copy_from_slice(&crc.to_le_bytes());
        let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, &bytes[12..32]) as u32;
        bytes[8..12].copy_from_slice(&crc.to_le_bytes());
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let archive =
            sevenz_turbo::Archive::read(&mut Cursor::new(&bytes), &sevenz_turbo::Password::empty())
                .unwrap();
        assert!(
            check_sevenz_decoder_memory(&archive, &sevenz_archive_limits(&context.budget)).is_err()
        );
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        assert!(
            try_extract(
                &context,
                &paths,
                &coverage,
                &pool,
                &destination,
                &slot,
                ChildArchives::Written
            )
            .unwrap()
            .is_some()
        );
        let handoff = slot.lock().unwrap().take().unwrap();
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        assert_eq!(std::fs::read(destination.join("a")).unwrap(), data);
    }

    #[test]
    fn partial_publication_failure_rolls_back_owned_links_and_remains_retryable() {
        let temp = tempfile::tempdir().unwrap();
        let (bytes, parts) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        try_extract(
            &context,
            &paths,
            &coverage,
            &pool,
            &destination,
            &slot,
            ChildArchives::Written,
        )
        .unwrap()
        .unwrap();
        let handoff = slot.lock().unwrap().take().unwrap();
        std::fs::create_dir(destination.join("folder")).unwrap();
        let collision = destination.join("folder/member-1");
        std::fs::write(&collision, b"pre-existing output").unwrap();
        assert!(
            handoff
                .manifest
                .publish(&handoff.epoch, &destination)
                .is_err()
        );
        assert!(
            !destination.join("folder/member-0").exists(),
            "first link must be rolled back"
        );
        assert_eq!(std::fs::read(&collision).unwrap(), b"pre-existing output");
        assert_eq!(
            std::fs::read_dir(destination.join(".weaver-chunks"))
                .unwrap()
                .count(),
            4
        );
        std::fs::remove_file(collision).unwrap(); // This test created the collision.
        handoff
            .manifest
            .publish(&handoff.epoch, &destination)
            .unwrap();
        for (index, bytes) in parts.iter().enumerate() {
            assert_eq!(
                std::fs::read(destination.join(format!("folder/member-{index}"))).unwrap(),
                *bytes
            );
        }
    }

    #[test]
    fn scanner_refuses_its_own_retained_budget_without_waiting() {
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path(), 32, 0);
        let destination = temp.path().join("destination");
        let manifest = PositionalManifest::create(
            Arc::new(ExtractionRoot::open(&destination).unwrap()),
            destination,
            Arc::clone(&context.budget),
            &"a".repeat(64),
            Vec::new(),
        )
        .unwrap();
        let held = context
            .budget
            .reserve_more_memory_wait(
                context.budget.max_memory_bytes()
                    - context.budget.memory_reserved_bytes()
                    - (32 << 10),
            )
            .unwrap();
        let coverage = SetCoverage::new(1);
        let epoch = coverage.enable_run_replay().unwrap();
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let source = sevenz_turbo::SerialReadAt::new(Cursor::new(Vec::<u8>::new()));
        let error = decode_pass(
            &[],
            &source,
            &coverage,
            &epoch,
            &manifest,
            &context.budget,
            &pool,
            &mut RunJournal::default(),
        )
        .unwrap_err();
        assert!(error.contains("retained metadata exceed memory policy"));
        drop(held);
        drop(manifest);
        assert_eq!(context.budget.memory_reserved_bytes(), 0);
    }

    #[test]
    fn header_ranges_merge_overlap_and_adjacency_and_bound_logical_reads() {
        let mut reader = HeaderReads::new(Cursor::new(vec![0; 64]), 16).unwrap();
        reader.record(40, 50).unwrap();
        reader.record(32, 40).unwrap();
        reader.record(20, 60).unwrap();
        reader.record(100, 120).unwrap();
        reader.record(90, 100).unwrap();
        assert_eq!(reader.ranges, [(0, 60), (90, 120)]);
        let mut bytes = [0; 32];
        assert_eq!(reader.read(&mut bytes).unwrap(), 16);
        assert!(reader.read(&mut bytes).is_err());
        for index in 0..1022 {
            reader.record(1000 + 2 * index, 1001 + 2 * index).unwrap();
        }
        assert!(reader.record(10_000, 10_001).is_err());
    }

    #[test]
    fn encoded_header_in_the_prefix_gap_is_part_of_repair_identity() {
        fn uint(out: &mut Vec<u8>, value: usize) {
            assert!(value < 16384);
            if value < 128 {
                out.push(value as u8);
            } else {
                out.extend_from_slice(&[0x80 | (value >> 8) as u8, value as u8]);
            }
        }
        fn crc(bytes: &[u8]) -> u32 {
            crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, bytes) as u32
        }
        let mut writer = ArchiveWriter::new(Cursor::new(Vec::new())).unwrap();
        writer.set_encrypt_header(false);
        let data = vec![b'x'; 100_003];
        writer
            .push_archive_entry(ArchiveEntry::new_file("a"), Some(Cursor::new(&data)))
            .unwrap();
        let original = writer.finish().unwrap().into_inner();
        let header_at = 32 + u64::from_le_bytes(original[12..20].try_into().unwrap()) as usize;
        let mut plain = original[header_at..].to_vec();
        assert_eq!(&plain[..4], &[1, 4, 6, 0]);
        // Put the main payload 512 bytes after the signature. The encoded
        // header's COPY stream lives inside that prefix gap, before payload.
        plain.splice(3..4, [0x82, 0]);
        assert!(plain.len() < 512);
        let mut descriptor = vec![0x17, 6, 0, 1, 9];
        uint(&mut descriptor, plain.len());
        descriptor.extend_from_slice(&[0, 7, 0x0b, 1, 0, 1, 1, 0, 0x0c]);
        uint(&mut descriptor, plain.len());
        descriptor.extend_from_slice(&[0x0a, 1]);
        descriptor.extend_from_slice(&crc(&plain).to_le_bytes());
        descriptor.extend_from_slice(&[0, 0]);
        let mut bytes = original[..32].to_vec();
        bytes.extend_from_slice(&plain);
        bytes.resize(32 + 512, 0);
        bytes.extend_from_slice(&original[32..header_at]);
        let next_offset = bytes.len() as u64 - 32;
        bytes.extend_from_slice(&descriptor);
        bytes[12..20].copy_from_slice(&next_offset.to_le_bytes());
        bytes[20..28].copy_from_slice(&(descriptor.len() as u64).to_le_bytes());
        bytes[28..32].copy_from_slice(&crc(&descriptor).to_le_bytes());
        let start_crc = crc(&bytes[12..32]);
        bytes[8..12].copy_from_slice(&start_crc.to_le_bytes());
        let mut reader = HeaderReads::new(Cursor::new(&bytes), 1 << 20).unwrap();
        let archive =
            sevenz_turbo::Archive::read(&mut reader, &sevenz_turbo::Password::empty()).unwrap();
        assert_eq!(archive.pack_pos(), 512);
        assert!(
            reader
                .ranges
                .iter()
                .any(|&(start, end)| start <= 32 && end >= 32 + plain.len() as u64)
        );
        let temp = tempfile::tempdir().unwrap();
        let context = test_context(temp.path(), bytes.len() as u64, descriptor.len() as u64);
        let source = sevenz_turbo::SerialReadAt::new(Cursor::new(bytes.clone()));
        let identity = HeaderIdentity::read(&source, reader.ranges, &context.budget).unwrap();
        assert!(identity.matches(&source, &context.budget).unwrap());
        // Neither signature nor conventional tail changes. Only the packed
        // encoded-header bytes change, and they must still invalidate replay.
        bytes[32] ^= 1;
        let changed = sevenz_turbo::SerialReadAt::new(Cursor::new(bytes));
        assert!(!identity.matches(&changed, &context.budget).unwrap());
    }

    #[tokio::test]
    async fn failed_set_publication_emits_no_install_completion_or_partial_members() {
        use crate::pipeline::direct_unpack::wiring::{ChaseDisposition, ChaseOutcome};
        let temp = tempfile::tempdir().unwrap();
        let (bytes, _) = fixture(false);
        let (paths, coverage, end_header) = source(temp.path(), &bytes);
        let context = test_context(temp.path(), bytes.len() as u64, end_header);
        let pool = Arc::new(
            rayon::ThreadPoolBuilder::new()
                .num_threads(2)
                .build()
                .unwrap(),
        );
        let destination = temp.path().join("destination");
        let slot = Arc::new(std::sync::Mutex::new(None));
        let outcome = try_extract(
            &context,
            &paths,
            &coverage,
            &pool,
            &destination,
            &slot,
            ChildArchives::Written,
        )
        .unwrap()
        .unwrap();
        std::fs::create_dir(destination.join("folder")).unwrap();
        let collision = destination.join("folder/member-1");
        std::fs::write(&collision, b"existing output").unwrap();
        let chase = ChaseOutcome {
            positional: slot.lock().unwrap().take(),
            result: Ok(outcome),
            elapsed: std::time::Duration::ZERO,
            staging_dir: temp.path().join("private"),
            total_bytes: context.phase_counters.total_bytes.load(Ordering::Relaxed),
            completed_bytes: context
                .phase_counters
                .completed_bytes
                .load(Ordering::Relaxed),
            tainted: false,
            damage_reported: false,
        };
        let phase = Arc::new(PhaseCounters::default());
        assert!(
            install_direct_unpack(
                ChaseDisposition::Ready(Box::new(chase)),
                destination.clone(),
                Arc::clone(&phase),
                JobId(41),
                "positional-fixture",
                None,
                None
            )
            .await
            .is_none()
        );
        assert_eq!(phase.completed_bytes.load(Ordering::Relaxed), 0);
        assert!(!destination.join("folder/member-0").exists());
        assert_eq!(std::fs::read(&collision).unwrap(), b"existing output");
        assert!(
            std::fs::read_dir(destination.join(".weaver-chunks"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}

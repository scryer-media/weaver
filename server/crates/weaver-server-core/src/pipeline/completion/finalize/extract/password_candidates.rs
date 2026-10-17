use super::*;

use crate::pipeline::extraction::error::sevenz_password_refuted;

// What is left when every candidate was refuted. A 7z AES key is never
// checked on its own, so a wrong key fails exactly as damage behind the right
// one would. A refusal that surfaced as a data error keeps that class: a
// repair may yet change it, as it would have with a single password. It
// still says the candidates were tried, so the verdict names the passwords
// beside the damage instead of damage alone. Any other refusal is the
// password's.
fn candidates_exhausted(refusal: Option<ExtractionError>) -> ExtractionError {
    match refusal {
        Some(error) if error.is(ExtractionErrorClass::Recoverable) => {
            error.refuting_password().map_message(|message| {
                format!(
                    "no supplied password could decrypt the 7z archive, or its data is damaged: \
                     {message}"
                )
            })
        }
        _ => {
            ExtractionError::password_required("no supplied password could decrypt the 7z archive")
        }
    }
}

// Keeps the refusal that says the most: a data error over any other, and the
// first of its class, so the verdict's class does not depend on candidate
// order.
fn keep_refusal(kept: &mut Option<ExtractionError>, error: ExtractionError) {
    if kept
        .as_ref()
        .is_none_or(|kept| !kept.is(ExtractionErrorClass::Recoverable))
    {
        *kept = Some(error);
    }
}

#[cfg(test)]
pub(super) fn extract_7z_with_password_candidates<R, F>(
    context: SevenZipExtractionContext,
    passwords: &[crate::jobs::ArchivePasswordCandidate],
    open_reader: F,
) -> Result<FullSetExtractionOutcome, ExtractionError>
where
    R: std::io::Read + std::io::Seek + Send + 'static,
    F: FnMut() -> Result<R, ExtractionError>,
{
    with_password_candidate(context, passwords, open_reader, extract_7z_stream)
}

pub(super) fn extract_7z_with_password_candidates_to<R, F, O: ArchiveOutput>(
    context: SevenZipExtractionContext,
    passwords: &[crate::jobs::ArchivePasswordCandidate],
    open_reader: F,
    output: &O,
) -> Result<FullSetExtractionOutcome, ExtractionError>
where
    R: std::io::Read + std::io::Seek + Send + 'static,
    F: FnMut() -> Result<R, ExtractionError>,
{
    with_password_candidate(context, passwords, open_reader, |context, reader| {
        extract_7z_to(context.stream_context(), output, reader)
    })
}

// What a 7z archive's header says about which candidate is its key.
pub(in crate::pipeline) enum SevenZipKey<'a> {
    // No coder is AES, so the archive opens without a password.
    Unneeded,
    // The header is encrypted, and it lists only under its key, so the
    // candidate that listed it is proven and whatever its decode meets is
    // the data's. The listing is kept so the caller need not read it again.
    Proven {
        candidate: &'a crate::jobs::ArchivePasswordCandidate,
        listing: Box<(sevenz_turbo::Archive, MemoryPermit)>,
    },
    // The header is clear and lists under any key, so only the data can
    // refute a candidate. Each distinct candidate, in order.
    Unproven(Vec<&'a crate::jobs::ArchivePasswordCandidate>),
}

// Lists the archive's header under each candidate until one proves itself.
// No candidate listing an encrypted header leaves the candidates exhausted.
pub(in crate::pipeline) fn sevenz_key<'a, R, F>(
    context: &SevenZipExtractionContext,
    passwords: &'a [crate::jobs::ArchivePasswordCandidate],
    open_reader: &mut F,
) -> Result<SevenZipKey<'a>, ExtractionError>
where
    R: std::io::Read + std::io::Seek,
    F: FnMut() -> Result<R, ExtractionError>,
{
    let mut seen = HashSet::new();
    let distinct = passwords
        .iter()
        .filter(|candidate| seen.insert(candidate.value()));
    match list(context, &sevenz_turbo::Password::empty(), open_reader) {
        Ok((archive, _)) => {
            let encrypted = archive
                .blocks
                .iter()
                .flat_map(|block| block.coders.iter())
                .any(|coder| {
                    coder.encoder_method_id() == sevenz_turbo::EncoderMethod::ID_AES256_SHA256
                });
            return Ok(if encrypted {
                SevenZipKey::Unproven(distinct.collect())
            } else {
                SevenZipKey::Unneeded
            });
        }
        Err(error) if error.is(ExtractionErrorClass::PasswordRequired) => {}
        Err(error) => return Err(error),
    }
    let mut refusal = None;
    for candidate in distinct {
        context
            .budget
            .check_active_io()
            .map_err(|error| ExtractionError::io(&error))?;
        match list(
            context,
            &sevenz_turbo::Password::new(candidate.value()),
            open_reader,
        ) {
            Ok(listing) => {
                return Ok(SevenZipKey::Proven {
                    candidate,
                    listing: Box::new(listing),
                });
            }
            Err(error) if error.refuted_password() => keep_refusal(&mut refusal, error),
            Err(error) => return Err(error),
        }
    }
    // A stopped budget wrapped in the last listing's refusal is still a stop.
    context
        .budget
        .check_active_io()
        .map_err(|error| ExtractionError::io(&error))?;
    Err(candidates_exhausted(refusal))
}

fn end_header_bytes(context: &SevenZipExtractionContext) -> u64 {
    match context.decode_memory {
        SevenZipDecodeMemory::ReservedForFixedThreads {
            end_header_bytes, ..
        }
        | SevenZipDecodeMemory::ReservedPerPass {
            end_header_bytes, ..
        } => end_header_bytes,
    }
}

fn list<R, F>(
    context: &SevenZipExtractionContext,
    password: &sevenz_turbo::Password,
    open_reader: &mut F,
) -> Result<(sevenz_turbo::Archive, MemoryPermit), ExtractionError>
where
    R: std::io::Read + std::io::Seek,
    F: FnMut() -> Result<R, ExtractionError>,
{
    let end_header_bytes = end_header_bytes(context);
    read_sevenz_archive_for_listing(
        &context.decode_memory,
        end_header_bytes,
        &context.budget,
        password,
        &|granted| header_pass_archive_limits(&context.budget, granted, end_header_bytes),
        open_reader,
    )
}

// A reader that remembers the first failure its source returned. The 7z
// reader reports every failure inside an AES block as the key's, so without
// this a source that stopped reading mid-block would be judged a wrong key.
struct WatchedSource<R> {
    inner: R,
    fault: Arc<std::sync::Mutex<Option<ExtractionError>>>,
}

impl<R> WatchedSource<R> {
    fn new(inner: R) -> (Self, Arc<std::sync::Mutex<Option<ExtractionError>>>) {
        let fault = Arc::new(std::sync::Mutex::new(None));
        let watched = Self {
            inner,
            fault: Arc::clone(&fault),
        };
        (watched, fault)
    }

    fn note<T>(&self, result: std::io::Result<T>) -> std::io::Result<T> {
        if let Err(error) = &result {
            let mut fault = self
                .fault
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            fault.get_or_insert_with(|| ExtractionError::io(error));
        }
        result
    }
}

impl<R: std::io::Read> std::io::Read for WatchedSource<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let result = self.inner.read(buffer);
        self.note(result)
    }
}

impl<R: std::io::Seek> std::io::Seek for WatchedSource<R> {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        let result = self.inner.seek(position);
        self.note(result)
    }
}

fn with_password_candidate<R, F>(
    mut context: SevenZipExtractionContext,
    passwords: &[crate::jobs::ArchivePasswordCandidate],
    mut open_reader: F,
    extract: impl FnOnce(
        &SevenZipExtractionContext,
        F,
    ) -> Result<FullSetExtractionOutcome, ExtractionError>,
) -> Result<FullSetExtractionOutcome, ExtractionError>
where
    R: std::io::Read + std::io::Seek + Send + 'static,
    F: FnMut() -> Result<R, ExtractionError>,
{
    let budget = Arc::clone(&context.budget);
    let end_header_bytes = end_header_bytes(&context);
    let (candidates, header_encrypted) = match sevenz_key(&context, passwords, &mut open_reader)? {
        SevenZipKey::Unneeded => {
            context.password = sevenz_turbo::Password::empty();
            return extract(&context, open_reader);
        }
        SevenZipKey::Proven { candidate, listing } => (vec![(candidate, Some(listing))], true),
        SevenZipKey::Unproven(candidates) => (
            candidates
                .into_iter()
                .map(|candidate| (candidate, None))
                .collect(),
            false,
        ),
    };
    let mut refusal = None;
    for (candidate, listing) in candidates {
        budget
            .check_active_io()
            .map_err(|error| ExtractionError::io(&error))?;
        let password = sevenz_turbo::Password::new(candidate.value());
        let listed = match listing {
            Some(listing) => Ok(*listing),
            None => list(&context, &password, &mut open_reader),
        };
        let (archive, header_permit) = match listed {
            Ok(archive) => archive,
            Err(error) if error.refuted_password() => {
                keep_refusal(&mut refusal, error);
                continue;
            }
            Err(error) => return Err(error),
        };
        let entries = archive.files.len() as u64;
        for entry in &archive.files {
            budget.check_member_metadata(entry.name(), entry.size())?;
            context
                .root
                .validate_relative_path(entry.name())
                .map_err(|error| budget.reject_unsafe_path(error))?;
        }
        let memory = fixed_decode_memory_bytes(
            context.job_id,
            &context.set_name,
            &archive,
            end_header_bytes,
            context.decode_threads,
            budget.max_memory_bytes(),
        );
        drop(header_permit);
        let permit = memory.reserve(&budget)?;
        let mut limits = sevenz_archive_limits(&budget);
        limits.memory_limit_bytes = permit.bytes();
        let mut total = 0u64;
        let (source, source_fault) = WatchedSource::new(open_reader()?);
        let probe = decode_7z_streaming(
            context.job_id,
            &context.set_name,
            BudgetedReader::new(source, Arc::clone(&budget)),
            None,
            password.clone(),
            limits,
            SevenZipDecodeThreads::Fixed(context.decode_threads),
            None,
            |entry, _, reader| {
                let mut member = 0u64;
                let mut buffer = [0u8; 8192];
                loop {
                    budget.check_active_io()?;
                    let count = reader.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    member = member.saturating_add(count as u64);
                    total = total.saturating_add(count as u64);
                    budget
                        .check_member_metadata(entry.name(), member)
                        .map_err(std::io::Error::other)?;
                    budget
                        .check_archive_metadata(entries, Some(total))
                        .map_err(std::io::Error::other)?;
                }
                Ok(true)
            },
        );
        drop(permit);
        // Budget/cancellation failures keep their own classification even if
        // a decoder wraps the underlying read error as a password failure.
        budget
            .check_active_io()
            .map_err(|error| ExtractionError::io(&error))?;
        // A source that failed under the key was never the key's refusal.
        let source_fault = source_fault
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match probe {
            Ok(_) => {
                context.password = password;
                let mut outcome = extract(&context, open_reader)?;
                outcome.selected_password = Some(candidate.value().to_string());
                return Ok(outcome);
            }
            Err(_) if source_fault.is_some() => {
                return Err(source_fault.expect("checked"));
            }
            Err(error) if !header_encrypted && sevenz_password_refuted(&error) => {
                keep_refusal(&mut refusal, sevenz_extraction_error(&error));
                continue;
            }
            Err(error) => return Err(sevenz_extraction_error(&error)),
        }
    }
    // A stopped budget wrapped in the last listing's refusal is still a stop.
    budget
        .check_active_io()
        .map_err(|error| ExtractionError::io(&error))?;
    Err(candidates_exhausted(refusal))
}

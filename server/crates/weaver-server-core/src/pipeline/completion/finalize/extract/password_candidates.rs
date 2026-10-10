use super::*;

fn sevenz_password_error(error: &sevenz_turbo::Error) -> bool {
    match error {
        sevenz_turbo::Error::PasswordRequired => true,
        // Integrity and decoder failures cannot distinguish a wrong password
        // from damaged encrypted bytes. Never advance candidates for them.
        sevenz_turbo::Error::BlockDecode {
            kind: sevenz_turbo::BlockErrorKind::Password,
            message,
            ..
        } => message == "PasswordRequired",
        _ => false,
    }
}

pub(super) fn extract_7z_with_password_candidates<R, F>(
    mut context: SevenZipExtractionContext,
    passwords: &[crate::jobs::ArchivePasswordCandidate],
    mut open_reader: F,
) -> Result<FullSetExtractionOutcome, String>
where
    R: std::io::Read + std::io::Seek + Send,
    F: FnMut() -> Result<R, String>,
{
    let budget = Arc::clone(&context.budget);
    let end_header_bytes = match context.decode_memory {
        SevenZipDecodeMemory::ReservedForFixedThreads {
            end_header_bytes, ..
        }
        | SevenZipDecodeMemory::ReservedPerPass { end_header_bytes } => end_header_bytes,
    };
    let list = |password: &sevenz_turbo::Password, open: &mut F| {
        read_sevenz_archive_for_listing(
            &context.decode_memory,
            end_header_bytes,
            &budget,
            password,
            &|granted| header_pass_archive_limits(&budget, granted, end_header_bytes),
            open,
        )
    };
    let plaintext = list(&sevenz_turbo::Password::empty(), &mut open_reader);
    match plaintext {
        Ok((archive, _permit)) => {
            let encrypted = archive
                .blocks
                .iter()
                .flat_map(|block| block.coders.iter())
                .any(|coder| {
                    coder.encoder_method_id() == sevenz_turbo::EncoderMethod::ID_AES256_SHA256
                });
            if !encrypted {
                drop(_permit);
                context.password = sevenz_turbo::Password::empty();
                return extract_7z_stream(&context, open_reader);
            }
        }
        Err(error) if error.starts_with("WEAVER_PASSWORD_REQUIRED:") => {}
        Err(error) => return Err(error),
    }
    let mut seen = HashSet::new();
    for candidate in passwords
        .iter()
        .filter(|candidate| seen.insert(candidate.value()))
    {
        budget
            .check_active_io()
            .map_err(|error| error.to_string())?;
        let password = sevenz_turbo::Password::new(candidate.value());
        let listed = list(&password, &mut open_reader);
        let (archive, header_permit) = match listed {
            Ok(archive) => archive,
            Err(error) if error.starts_with("WEAVER_PASSWORD_REQUIRED:") => continue,
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
        let probe = decode_7z_streaming(
            context.job_id,
            &context.set_name,
            BudgetedReader::new(open_reader()?, Arc::clone(&budget)),
            &context.output_dir,
            password.clone(),
            limits,
            SevenZipDecodeThreads::Fixed(context.decode_threads),
            |entry, reader, _| {
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
            .map_err(|error| error.to_string())?;
        match probe {
            Ok(_) => {
                context.password = password;
                let mut outcome = extract_7z_stream(&context, open_reader)?;
                outcome.selected_password = Some(candidate.value().to_string());
                return Ok(outcome);
            }
            Err(error) if sevenz_password_error(&error) => continue,
            Err(error) => return Err(sevenz_extraction_error(&error)),
        }
    }
    Err("WEAVER_PASSWORD_REQUIRED: no supplied password could decrypt the 7z archive".into())
}

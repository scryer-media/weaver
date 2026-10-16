use super::*;
use crate::pipeline::extraction::{
    apply_server_rar_limits_with_memory_limit, rar_entry_via, rar_member_decoder_memory_bytes,
};
use std::num::NonZeroUsize;
use unrar_rs::VolumeProvider;

// Password retries finish before the selected generation can emit a member.
pub(super) fn select_password(
    input: &crate::pipeline::extraction::input::LiveArchiveInput,
    candidates: &[crate::jobs::ArchivePasswordCandidate],
    budget: &Arc<crate::pipeline::extraction::JobExtractionBudget>,
) -> Result<Option<String>, ExtractionError> {
    if candidates.len() < 2 {
        return Ok(candidates
            .first()
            .map(|candidate| candidate.value().to_owned()));
    }
    let selection = Pipeline::try_rar_password_candidates(
        "live RAR password selection",
        candidates,
        |password| {
            use crate::pipeline::RarPasswordAttemptError as E;
            input.wait_volume(0).map_err(|error| {
                E::Fatal(ExtractionError::from_io("live RAR password input", &error))
            })?;
            let first = input.get_volume(0).map_err(|error| {
                E::Fatal(ExtractionError::terminal(format!(
                    "live RAR password input: {error}"
                )))
            })?;
            let mut archive = unrar_rs::RarArchive::open_prefix(first, password, NonZeroUsize::MIN)
                .map_err(E::from)?;
            let ceiling =
                apply_server_rar_limits_with_memory_limit(&mut archive, budget.max_memory_bytes());
            let mut volume = 0;
            while input.wait_volume(volume).map_err(|error| {
                E::Fatal(ExtractionError::from_io("live RAR password roster", &error))
            })? {
                let mut requested = NonZeroUsize::MIN;
                loop {
                    let reader = input.get_volume(volume).map_err(|error| {
                        E::Fatal(ExtractionError::terminal(format!(
                            "live RAR password input: {error}"
                        )))
                    })?;
                    let count = archive
                        .extend_volume_prefix(volume, reader, requested)
                        .map_err(E::from)?;
                    if let Some((index, member)) = archive
                        .metadata()
                        .members
                        .iter()
                        .enumerate()
                        .find(|(_, member)| !member.is_directory && member.is_encrypted)
                        .map(|(index, member)| (index, member.clone()))
                    {
                        let needed = rar_member_decoder_memory_bytes(&member);
                        if needed > ceiling {
                            return Err(E::from(unrar_rs::RarError::DictionaryTooLarge {
                                size: needed,
                                max: ceiling,
                            }));
                        }
                        let _memory = budget
                            .reserve_memory_wait(needed)
                            .map_err(|error| E::Fatal(error.into()))?;
                        let options = unrar_rs::ExtractOptions {
                            verify: true,
                            password: password.map(str::to_owned),
                            restore_owners: false,
                        };
                        rar_entry_via(&mut archive, index, input, &options)
                            .and_then(|entry| entry.copy_to(&mut std::io::sink()))
                            .map_err(E::from)?;
                        return Ok(());
                    }
                    if count < requested.get() {
                        break;
                    }
                    requested = requested.checked_add(1).ok_or_else(|| {
                        E::Fatal(ExtractionError::terminal(
                            "RAR password header count overflow",
                        ))
                    })?;
                }
                volume = volume.checked_add(1).ok_or_else(|| {
                    E::Fatal(ExtractionError::terminal(
                        "RAR password volume count overflow",
                    ))
                })?;
            }
            Ok(())
        },
    )?;
    Ok(selection.selected_password)
}

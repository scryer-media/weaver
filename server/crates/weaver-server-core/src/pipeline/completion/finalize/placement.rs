use std::io;
use std::path::{Component, Path};

use crate::runtime::fs::{paths_equivalent_for_placement, rename_no_overwrite};

/// A verified placement is a permutation, not necessarily independent renames
/// or pairs. Vacate every source before installing any destination so a cycle
/// of any length can settle. Success means the *whole* mapping was installed;
/// the caller may only then rebind file identities and archive topology.
pub(super) fn apply_complete_plan(dir: &Path, plan: &par2_rs::PlacementPlan) -> io::Result<usize> {
    apply_complete_plan_with_move(dir, plan, rename_no_overwrite)
}

fn apply_complete_plan_with_move(
    dir: &Path,
    plan: &par2_rs::PlacementPlan,
    mut move_file: impl FnMut(&Path, &Path) -> io::Result<()>,
) -> io::Result<usize> {
    let mut entries: Vec<&par2_rs::PlacementEntry> = Vec::new();
    for entry in plan
        .swaps
        .iter()
        .flat_map(|(left, right)| [left, right])
        .chain(plan.renames.iter())
    {
        for name in [&entry.current_name, &entry.correct_name] {
            let mut components = Path::new(name).components();
            if !matches!(components.next(), Some(Component::Normal(_)))
                || components.next().is_some()
                || name.contains(['/', '\\', ':'])
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "placement requires a filename within the working directory",
                ));
            }
        }
        if entry.current_name == entry.correct_name {
            continue;
        }
        if let Some(previous) = entries.iter().find(|previous| {
            paths_equivalent_for_placement(
                Path::new(&previous.current_name),
                Path::new(&entry.current_name),
            )
        }) {
            if previous.correct_name == entry.correct_name {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "placement maps one source to multiple destinations",
            ));
        }
        if entries.iter().any(|previous| {
            paths_equivalent_for_placement(
                Path::new(&previous.correct_name),
                Path::new(&entry.correct_name),
            )
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "placement maps multiple sources to one destination",
            ));
        }
        entries.push(entry);
    }
    if entries.is_empty() {
        return Ok(0);
    }

    // Reject a collision or missing source before changing any file. Recheck
    // at each move with the shared no-overwrite operation as well.
    for entry in &entries {
        if !std::fs::symlink_metadata(dir.join(&entry.current_name))?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "placement source is not a regular file",
            ));
        }
        let destination = dir.join(&entry.correct_name);
        match std::fs::symlink_metadata(&destination) {
            Ok(_)
                if !entries.iter().any(|source| {
                    paths_equivalent_for_placement(&dir.join(&source.current_name), &destination)
                }) =>
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!(
                        "placement destination is occupied by an unrelated file: {}",
                        destination.display()
                    ),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }

    // Never let TempDir's destructor delete the only surviving copy if an I/O
    // error prevents rollback. Original filenames identify retained bytes.
    let staging = tempfile::Builder::new()
        .prefix(".weaver-placement-")
        .tempdir_in(dir)?
        .keep();
    let mut staged = 0;
    let mut installed = 0;
    let result = (|| {
        for entry in &entries {
            move_file(
                &dir.join(&entry.current_name),
                &staging.join(&entry.current_name),
            )?;
            staged += 1;
        }
        for entry in &entries {
            move_file(
                &staging.join(&entry.current_name),
                &dir.join(&entry.correct_name),
            )?;
            installed += 1;
        }
        Ok::<_, io::Error>(())
    })();

    if let Err(error) = result {
        // Move installed destinations out of the way before restoring sources;
        // rolling each pair back directly would collide inside a cycle again.
        let mut rollback_errors = Vec::new();
        for entry in entries[..installed].iter().rev() {
            if let Err(rollback) = move_file(
                &dir.join(&entry.correct_name),
                &staging.join(&entry.current_name),
            ) {
                rollback_errors.push(rollback.to_string());
            }
        }
        for entry in entries[..staged].iter().rev() {
            if let Err(rollback) = move_file(
                &staging.join(&entry.current_name),
                &dir.join(&entry.current_name),
            ) {
                rollback_errors.push(rollback.to_string());
            }
        }
        if rollback_errors.is_empty() {
            match std::fs::remove_dir(&staging) {
                Ok(()) => return Err(error),
                // A failed no-overwrite move can leave both copies if source
                // removal fails. Never silently abandon retained staged bytes.
                Err(cleanup) => rollback_errors.push(cleanup.to_string()),
            }
        }
        return Err(io::Error::other(format!(
            "{error}; placement rollback incomplete; retained files at {}: {}",
            staging.display(),
            rollback_errors.join("; ")
        )));
    }
    // An empty staging directory failing to disappear does not invalidate the
    // installed mapping. It must not prevent the caller from rebinding it.
    let _ = std::fs::remove_dir(&staging);
    Ok(entries.len())
}

#[cfg(test)]
mod tests;

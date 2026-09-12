use std::io;
use std::path::{Component, Path};

use crate::runtime::fs::{paths_equivalent_for_placement, rename_no_overwrite};

mod journal;
pub(crate) use journal::{Binding, Transaction, begin, recover};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ApplyOutcome {
    Applied,
    Reverify,
}

#[cfg(test)]
pub(super) fn apply_complete_plan(dir: &Path, plan: &par2_rs::PlacementPlan) -> io::Result<usize> {
    apply_complete_plan_with_move(dir, plan, rename_no_overwrite)
}

#[cfg(test)]
fn apply_complete_plan_with_move(
    dir: &Path,
    plan: &par2_rs::PlacementPlan,
    move_file: impl FnMut(&Path, &Path) -> io::Result<()>,
) -> io::Result<usize> {
    let Some(mut transaction) = journal::prepare(dir, plan, Vec::new())? else {
        return Ok(0);
    };
    transaction.run_with_move(move_file)?;
    let count = transaction.len();
    transaction.finish()?;
    Ok(count)
}

fn validate_plan<'a>(
    dir: &Path,
    plan: &'a par2_rs::PlacementPlan,
) -> io::Result<Vec<&'a par2_rs::PlacementEntry>> {
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
        return Ok(entries);
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

    Ok(entries)
}

#[cfg(test)]
mod tests;

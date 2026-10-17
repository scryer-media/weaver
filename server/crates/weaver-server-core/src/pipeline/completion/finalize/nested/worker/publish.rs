//! Places validated namespace outputs at their published names without
//! rewriting their bytes.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::pipeline::extraction::output::channel::StreamOwner;
use crate::pipeline::extraction::runtime::{NamespaceWork, RuntimeError};
use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget};

pub(super) type Completion = (StreamOwner, NamespaceWork, Vec<String>);

/// How a member's private backing reaches its published name.
///
/// A decoded namespace is retained after publication, and three readers still
/// open its member backings by their private paths: the restart probe that
/// re-verifies a direct-store graph, the rearm of a restored graph, and the
/// repair inventory that offers members as recovery siblings. Such a backing
/// is hardlinked rather than moved. A hardlink shares its inode, so a write in
/// place to the published file changes the backing those readers see.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    /// Nothing reads the private backing after publication.
    Move,
    /// A retained decoded namespace still reads the backing.
    Share,
}

struct Member {
    name: String,
    backing: PathBuf,
    metadata: cap_std::fs::Metadata,
}

struct Namespace {
    owner: StreamOwner,
    source: ExtractionRoot,
    placement: Placement,
    members: Vec<Member>,
}

/// Publishes every completion under `destination`, returning each published
/// name with its owner. Nothing is placed until every sibling has validated.
pub(super) fn publish(
    destination: &Path,
    completed: Vec<Completion>,
    budget: &Arc<JobExtractionBudget>,
) -> Result<Vec<(String, StreamOwner)>, RuntimeError> {
    let destination =
        ExtractionRoot::open(destination).map_err(RuntimeError::refused("publication root"))?;
    let namespaces = validate(&destination, completed)?;
    let mut published = Vec::new();
    let mut directories = Vec::new();
    for namespace in namespaces {
        for member in namespace.members {
            let relative = Path::new(&member.name);
            if member.metadata.is_dir() {
                destination.create_dir(relative, budget)?;
                directories.push((member.name.clone(), member.metadata));
            } else {
                place_file(
                    &namespace.source,
                    &member.backing,
                    &destination,
                    relative,
                    namespace.placement,
                    budget,
                )?;
                apply_metadata(&destination, relative, &member.metadata)?;
            }
            published.push((member.name, namespace.owner));
        }
    }
    // Deepest first, so stamping a child never moves its parent's times.
    directories.sort_by_key(|(name, _)| std::cmp::Reverse(Path::new(name).components().count()));
    for (name, metadata) in directories {
        apply_metadata(&destination, Path::new(&name), &metadata)?;
    }
    Ok(published)
}

fn validate(
    destination: &ExtractionRoot,
    completed: Vec<Completion>,
) -> Result<Vec<Namespace>, RuntimeError> {
    let mut names = HashSet::new();
    for ordinary in completed.iter().flat_map(|(_, work, _)| &work.ordinary) {
        claim(&mut names, ordinary)?;
    }
    completed
        .into_iter()
        .filter(|(_, _, members)| !members.is_empty())
        .map(|(owner, work, members)| {
            validate_namespace(destination, &mut names, owner, &work, members)
        })
        .collect()
}

fn validate_namespace(
    destination: &ExtractionRoot,
    names: &mut HashSet<String>,
    owner: StreamOwner,
    work: &NamespaceWork,
    members: Vec<String>,
) -> Result<Namespace, RuntimeError> {
    let source = ExtractionRoot::open_existing(&work.output)
        .map_err(RuntimeError::refused("namespace backing root"))?;
    let inventory: BTreeMap<_, _> = source
        .list_entries_no_links(&[])
        .map_err(RuntimeError::io("namespace inventory"))?
        .into_iter()
        .collect();
    let members = members
        .into_iter()
        .map(|name| {
            claim(names, &name)?;
            if destination.open_input(Path::new(&name)).is_ok() {
                return Err(RuntimeError::OutputCollision(name));
            }
            let backing = backing(work, &name)?;
            let metadata = inventory
                .get(&backing)
                .cloned()
                .ok_or_else(|| RuntimeError::MissingOutput(name.clone()))?;
            Ok(Member {
                name,
                backing,
                metadata,
            })
        })
        .collect::<Result<_, RuntimeError>>()?;
    Ok(Namespace {
        owner,
        source,
        placement: if work.outputs.is_empty() {
            Placement::Move
        } else {
            Placement::Share
        },
        members,
    })
}

fn claim(names: &mut HashSet<String>, name: &str) -> Result<(), RuntimeError> {
    if names.insert(name.to_owned()) {
        Ok(())
    } else {
        Err(RuntimeError::OutputCollision(name.to_owned()))
    }
}

// A decoded namespace stores members under private names; an engine that
// wrote straight into the namespace used the member's own name.
fn backing(work: &NamespaceWork, member: &str) -> Result<PathBuf, RuntimeError> {
    match work.outputs.get(member) {
        Some(path) => path
            .strip_prefix(&work.output)
            .map(Path::to_owned)
            .map_err(|_| RuntimeError::Invariant("namespace backing escaped root")),
        None => Ok(PathBuf::from(member)),
    }
}

fn place_file(
    source: &ExtractionRoot,
    backing: &Path,
    destination: &ExtractionRoot,
    relative: &Path,
    placement: Placement,
    budget: &Arc<JobExtractionBudget>,
) -> Result<(), RuntimeError> {
    destination.create_parents(relative, budget)?;
    let placed = match placement {
        Placement::Move => source.rename_input_to(backing, destination, relative),
        Placement::Share => source.link_input_to(backing, destination, relative),
    };
    or_copy_across_devices(
        placed,
        || budget.note_entry(relative).map_err(RuntimeError::from),
        || copy_file(source, backing, destination, relative, budget),
    )
}

// Only a filesystem boundary justifies writing the bytes a second time; any
// other refusal is a real publication failure.
fn or_copy_across_devices(
    placed: io::Result<()>,
    account: impl FnOnce() -> Result<(), RuntimeError>,
    copy: impl FnOnce() -> Result<(), RuntimeError>,
) -> Result<(), RuntimeError> {
    match placed {
        Ok(()) => account(),
        Err(error) if error.kind() == io::ErrorKind::CrossesDevices => copy(),
        Err(error) => Err(RuntimeError::io("publication placement")(error)),
    }
}

fn copy_file(
    source: &ExtractionRoot,
    backing: &Path,
    destination: &ExtractionRoot,
    relative: &Path,
    budget: &Arc<JobExtractionBudget>,
) -> Result<(), RuntimeError> {
    let mut input = source
        .open_input(backing)
        .map_err(RuntimeError::io("publication copy source"))?;
    let mut output = destination.create_file(relative, budget)?;
    io::copy(&mut input, &mut output).map_err(RuntimeError::io("publication copy"))?;
    io::Write::flush(&mut output).map_err(RuntimeError::io("publication copy"))
}

fn apply_metadata(
    destination: &ExtractionRoot,
    relative: &Path,
    metadata: &cap_std::fs::Metadata,
) -> Result<(), RuntimeError> {
    #[cfg(unix)]
    let mode = {
        use cap_std::fs::PermissionsExt;
        Some(metadata.permissions().mode())
    };
    #[cfg(not(unix))]
    let mode = None;
    destination
        .set_directory_metadata(
            relative,
            metadata.modified().ok().map(|time| time.into_std()),
            metadata.accessed().ok().map(|time| time.into_std()),
            mode,
        )
        .map_err(RuntimeError::io("published metadata"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(root: &Path) -> Arc<JobExtractionBudget> {
        let limits = Arc::new(crate::pipeline::extraction::ExtractionLimits {
            max_job_bytes: 1 << 40,
            max_member_bytes: 1 << 40,
            max_entries: 1_000,
            max_ratio: 100,
            max_seconds: 3_600,
            min_free_bytes: 1,
            max_memory_bytes: 1 << 30,
        });
        JobExtractionBudget::new(
            limits,
            root.to_path_buf(),
            1 << 30,
            0,
            0,
            crate::operations::metrics::PipelineMetrics::new(),
        )
        .unwrap()
    }

    fn roots(temp: &Path) -> (ExtractionRoot, ExtractionRoot) {
        (
            ExtractionRoot::open(&temp.join("private").join("namespace")).unwrap(),
            ExtractionRoot::open(&temp.join("public").join("destination")).unwrap(),
        )
    }

    #[cfg(unix)]
    fn inode(path: &Path) -> u64 {
        std::os::unix::fs::MetadataExt::ino(&std::fs::metadata(path).unwrap())
    }

    #[cfg(unix)]
    #[test]
    fn same_device_move_keeps_the_member_inode() {
        let temp = tempfile::tempdir().unwrap();
        let (source, destination) = roots(temp.path());
        let backing = temp.path().join("private/namespace/member-0");
        std::fs::write(&backing, b"payload").unwrap();
        let before = inode(&backing);
        place_file(
            &source,
            Path::new("member-0"),
            &destination,
            Path::new("dir/payload.bin"),
            Placement::Move,
            &budget(temp.path()),
        )
        .unwrap();
        let published = temp.path().join("public/destination/dir/payload.bin");
        assert_eq!(inode(&published), before);
        assert!(!backing.exists());
    }

    #[cfg(unix)]
    #[test]
    fn shared_backing_stays_readable_beside_the_published_name() {
        let temp = tempfile::tempdir().unwrap();
        let (source, destination) = roots(temp.path());
        let backing = temp.path().join("private/namespace/member-0");
        std::fs::write(&backing, b"payload").unwrap();
        place_file(
            &source,
            Path::new("member-0"),
            &destination,
            Path::new("payload.bin"),
            Placement::Share,
            &budget(temp.path()),
        )
        .unwrap();
        let published = temp.path().join("public/destination/payload.bin");
        assert_eq!(inode(&published), inode(&backing));
        assert_eq!(std::fs::read(&backing).unwrap(), b"payload");
    }

    #[cfg(unix)]
    #[test]
    fn refused_move_across_devices_copies_the_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let (source, destination) = roots(temp.path());
        let backing = temp.path().join("private/namespace/member-0");
        std::fs::write(&backing, b"payload").unwrap();
        let budget = budget(temp.path());
        let relative = Path::new("payload.bin");
        or_copy_across_devices(
            Err(io::Error::from(io::ErrorKind::CrossesDevices)),
            || unreachable!("a refused move accounts through the copy"),
            || {
                copy_file(
                    &source,
                    Path::new("member-0"),
                    &destination,
                    relative,
                    &budget,
                )
            },
        )
        .unwrap();
        let published = temp.path().join("public/destination/payload.bin");
        assert_eq!(std::fs::read(&published).unwrap(), b"payload");
        assert_ne!(inode(&published), inode(&backing));
    }

    #[test]
    fn other_refusals_fail_publication_without_copying() {
        let copied = std::cell::Cell::new(false);
        let result = or_copy_across_devices(
            Err(io::Error::from(io::ErrorKind::PermissionDenied)),
            || Ok(()),
            || {
                copied.set(true);
                Ok(())
            },
        );
        assert!(result.is_err());
        assert!(!copied.get());
    }
}

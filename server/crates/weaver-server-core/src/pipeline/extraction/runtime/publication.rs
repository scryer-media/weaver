use super::*;
use crate::pipeline::extraction::ExtractionRoot;
use crate::pipeline::extraction::safety::JobExtractionBudget;
use crate::pipeline::*;
use std::io::Write;
use std::path::{Path, PathBuf};

// Bookkeeping directories that never belong to the published tree.
const UNPUBLISHED: [&str; 5] = [
    ".weaver-checkpoints",
    crate::pipeline::direct_store::nested::NESTED_DIR,
    ".weaver-chunks",
    ".weaver-staging",
    crate::jobs::working_dir::WORKING_DIR_MARKER,
];

type Selection = BTreeMap<String, (PathBuf, cap_std::fs::Metadata)>;

fn open_root(path: &Path) -> Result<ExtractionRoot, RuntimeError> {
    ExtractionRoot::open(path).map_err(RuntimeError::refused("publication root"))
}

fn collect(
    root: &Path,
    excluded: &HashSet<String>,
    files: &mut Selection,
) -> Result<(), RuntimeError> {
    if !root.exists() {
        return Ok(());
    }
    let pinned = open_root(root)?;
    for (relative, metadata) in pinned
        .list_entries_no_links(&UNPUBLISHED)
        .map_err(RuntimeError::io("publication listing"))?
    {
        let name = relative
            .to_str()
            .ok_or(RuntimeError::Invariant("publication path encoding"))?
            .replace('\\', "/");
        if excluded.contains(&name) {
            continue;
        }
        ExtractionRoot::parse_relative_path(&name)
            .map_err(RuntimeError::refused("publication path"))?;
        files
            .entry(name)
            .or_insert_with(|| (root.join(relative), metadata));
    }
    Ok(())
}

fn permission_mode(metadata: &cap_std::fs::Metadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use cap_std::fs::PermissionsExt;
        Some(metadata.permissions().mode())
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

fn apply_metadata(
    target: &ExtractionRoot,
    relative: &Path,
    metadata: &cap_std::fs::Metadata,
) -> Result<(), RuntimeError> {
    target
        .set_directory_metadata(
            relative,
            metadata.modified().ok().map(|time| time.into_std()),
            metadata.accessed().ok().map(|time| time.into_std()),
            permission_mode(metadata),
        )
        .map_err(RuntimeError::io("publication metadata"))
}

// The private roots a publication reads from and the one it fills.
struct PublicationRoots {
    working: PathBuf,
    working_root: ExtractionRoot,
    staging: Option<(PathBuf, ExtractionRoot)>,
    target: ExtractionRoot,
}

impl PublicationRoots {
    fn source<'a>(&self, path: &'a Path) -> Result<(&ExtractionRoot, &'a Path), RuntimeError> {
        if let Some((staging, root)) = &self.staging
            && let Ok(relative) = path.strip_prefix(staging)
        {
            return Ok((root, relative));
        }
        let relative = path
            .strip_prefix(&self.working)
            .map_err(|_| RuntimeError::Invariant("publication source outside its roots"))?;
        Ok((&self.working_root, relative))
    }

    // Links the file into the publication, and copies it only when the two
    // roots sit on different devices.
    fn publish_file(
        &self,
        path: &Path,
        relative: &Path,
        metadata: &cap_std::fs::Metadata,
        budget: &Arc<JobExtractionBudget>,
    ) -> Result<(), RuntimeError> {
        let (source, source_path) = self.source(path)?;
        let Err(error) = source.link_input_to(source_path, &self.target, relative) else {
            return Ok(());
        };
        if error.kind() != std::io::ErrorKind::CrossesDevices {
            return Err(RuntimeError::io("publication link")(error));
        }
        let mut input = source
            .open_input(source_path)
            .map_err(RuntimeError::io("publication source"))?;
        let mut output = self.target.create_file(relative, budget)?;
        std::io::copy(&mut input, &mut output).map_err(RuntimeError::io("publication copy"))?;
        output
            .flush()
            .map_err(RuntimeError::io("publication flush"))?;
        drop(output);
        apply_metadata(&self.target, relative, metadata)
    }
}

fn publish_selection(
    working: PathBuf,
    staging: Option<PathBuf>,
    publication: &Path,
    excluded: &HashSet<String>,
    budget: &Arc<JobExtractionBudget>,
) -> Result<PathBuf, RuntimeError> {
    let mut selected = Selection::new();
    if let Some(staging) = &staging {
        collect(staging, excluded, &mut selected)?;
    }
    collect(&working, excluded, &mut selected)?;
    let _parent = open_root(publication)?;
    let publication = tempfile::Builder::new()
        .prefix("publication-")
        .tempdir_in(publication)
        .map_err(RuntimeError::io("publication directory"))?
        .keep();
    let roots = PublicationRoots {
        target: open_root(&publication)?,
        working_root: open_root(&working)?,
        staging: staging
            .filter(|path| path.exists())
            .map(|path| open_root(&path).map(|root| (path, root)))
            .transpose()?,
        working,
    };
    let mut directories = Vec::new();
    for (name, (path, metadata)) in selected {
        let relative = Path::new(&name);
        budget.note_entry(relative)?;
        if metadata.is_dir() {
            roots.target.create_dir(relative, budget)?;
            directories.push((name, metadata));
        } else {
            roots.publish_file(&path, relative, &metadata, budget)?;
        }
    }
    // Children first, so filling a directory never moves its own times.
    for (name, metadata) in directories.into_iter().rev() {
        apply_metadata(&roots.target, Path::new(&name), &metadata)?;
    }
    Ok(publication)
}

impl ArchiveRuntime {
    fn publication_unresolved(&self) -> bool {
        !self.publication_ready()
            || self.staged_replacement.is_some()
            || !self.replay.is_empty()
            || self
                .decoded
                .iter()
                .any(|state| !self.sessions.ancestry_verified(state.generation.owner()))
    }

    // Consumed archive names and every input a retained namespace read,
    // relative to whichever root holds it.
    fn publication_exclusions(&self, roots: &[&PathBuf]) -> HashSet<String> {
        let mut excluded = self.consumed.clone();
        for (_, work) in &self.retained_work {
            excluded.extend(work.input_names.iter().cloned());
            for path in work.inputs.iter().chain(&work.original_inputs) {
                for root in roots {
                    if let Ok(relative) = path.strip_prefix(root)
                        && let Some(name) = relative.to_str()
                    {
                        excluded.insert(name.replace('\\', "/"));
                    }
                }
            }
        }
        excluded
    }
}

impl Pipeline {
    pub(crate) async fn prepare_scoped_publication(
        &mut self,
        job_id: JobId,
        working: PathBuf,
        staging: Option<PathBuf>,
    ) -> Result<Option<PathBuf>, RuntimeError> {
        let Some(runtime) = self.archive_runtime.get(&job_id) else {
            return Ok(None);
        };
        if runtime.consumed.is_empty() && runtime.decoded.is_empty() {
            return Ok(None);
        }
        if runtime.publication_unresolved() {
            return Err(RuntimeError::Invariant(
                "archive namespace publication remains unresolved",
            ));
        }
        let roots: Vec<&PathBuf> = std::iter::once(&working).chain(staging.iter()).collect();
        let excluded = runtime.publication_exclusions(&roots);
        let publication = staging
            .as_ref()
            .unwrap_or(&working)
            .join(crate::pipeline::direct_store::manifest::DIRECTORY);
        let budget = self.extraction_budget(job_id, &publication)?;
        let result = tokio::task::spawn_blocking(move || {
            publish_selection(working, staging, &publication, &excluded, &budget)
        })
        .await
        .map_err(RuntimeError::join("archive publication"))??;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_inventory_keeps_empty_directories_and_excludes_exact_sources() {
        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("empty")).unwrap();
        std::fs::create_dir(temp.path().join("nested")).unwrap();
        std::fs::write(temp.path().join("nested/source.7z"), b"source").unwrap();
        std::fs::write(temp.path().join("nested/payload.bin"), b"payload").unwrap();
        std::fs::create_dir(temp.path().join(".weaver-checkpoints")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            "missing",
            temp.path().join(".weaver-checkpoints/private-link"),
        )
        .unwrap();
        let mut selected = BTreeMap::new();
        collect(
            temp.path(),
            &HashSet::from(["nested/source.7z".into()]),
            &mut selected,
        )
        .unwrap();
        assert!(selected["empty"].1.is_dir());
        assert!(selected.contains_key("nested/payload.bin"));
        assert!(!selected.contains_key("nested/source.7z"));
        assert!(
            selected
                .keys()
                .all(|name| !name.starts_with(".weaver-checkpoints"))
        );
    }
}

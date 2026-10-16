use super::*;
use crate::pipeline::extraction::ExtractionRoot;
use crate::pipeline::*;
use std::io::Write;
use std::path::{Path, PathBuf};

fn collect(
    root: &Path,
    current: &Path,
    excluded: &HashSet<String>,
    files: &mut BTreeMap<String, (PathBuf, cap_std::fs::Metadata)>,
) -> Result<(), String> {
    let _ = current;
    if !root.exists() {
        return Ok(());
    }
    let pinned = ExtractionRoot::open(root)?;
    for (relative, metadata) in pinned
        .list_entries_no_links(&[
            ".weaver-checkpoints",
            ".weaver-nested",
            ".weaver-chunks",
            ".weaver-staging",
            crate::jobs::working_dir::WORKING_DIR_MARKER,
        ])
        .map_err(|e| e.to_string())?
    {
        let name = relative
            .to_str()
            .ok_or("publication path encoding")?
            .replace('\\', "/");
        if excluded.contains(&name) {
            continue;
        }
        ExtractionRoot::parse_relative_path(&name)?;
        files
            .entry(name)
            .or_insert_with(|| (root.join(relative), metadata));
    }
    Ok(())
}

impl Pipeline {
    pub(crate) async fn prepare_scoped_publication(
        &mut self,
        job_id: JobId,
        working: PathBuf,
        staging: Option<PathBuf>,
    ) -> Result<Option<PathBuf>, String> {
        let Some(runtime) = self.archive_runtime.get(&job_id) else {
            return Ok(None);
        };
        if runtime.consumed.is_empty() && runtime.decoded.is_empty() {
            return Ok(None);
        }
        if !runtime.publication_ready()
            || runtime.staged_replacement.is_some()
            || !runtime.replay.is_empty()
            || runtime
                .decoded
                .iter()
                .any(|state| !runtime.sessions.ancestry_verified(state.generation.owner()))
        {
            return Err("archive namespace publication remains unresolved".into());
        }
        let mut excluded = runtime.consumed.clone();
        for (_, work) in &runtime.retained_work {
            excluded.extend(work.input_names.iter().cloned());
            for path in work.inputs.iter().chain(&work.original_inputs) {
                for root in std::iter::once(&working).chain(staging.iter()) {
                    if let Ok(relative) = path.strip_prefix(root)
                        && let Some(name) = relative.to_str()
                    {
                        excluded.insert(name.replace('\\', "/"));
                    }
                }
            }
        }
        let publication = staging
            .as_ref()
            .unwrap_or(&working)
            .join(crate::pipeline::direct_store::manifest::DIRECTORY);
        let budget = self
            .extraction_budget(job_id, &publication)
            .map_err(|e| e.to_string())?;
        let result = tokio::task::spawn_blocking(move || -> Result<PathBuf, String> {
            let mut selected = BTreeMap::new();
            if let Some(staging) = &staging {
                collect(staging, staging, &excluded, &mut selected)?;
            }
            collect(&working, &working, &excluded, &mut selected)?;
            let _parent = ExtractionRoot::open(&publication)?;
            let publication = tempfile::Builder::new()
                .prefix("publication-")
                .tempdir_in(&publication)
                .map_err(|e| e.to_string())?
                .keep();
            let target = ExtractionRoot::open(&publication)?;
            let working_root = ExtractionRoot::open(&working)?;
            let staging_root = staging
                .as_ref()
                .filter(|path| path.exists())
                .map(|path| ExtractionRoot::open(path))
                .transpose()?;
            let mut directories = Vec::new();
            for (name, (path, metadata)) in selected {
                let relative = Path::new(&name);
                budget.note_entry(relative).map_err(|e| e.to_string())?;
                if metadata.is_dir() {
                    target
                        .create_dir(relative, &budget)
                        .map_err(|e| e.to_string())?;
                    directories.push((name, metadata));
                    continue;
                }
                let (source, source_path) = if let Some(staging) = &staging
                    && let Ok(relative) = path.strip_prefix(staging)
                {
                    (
                        staging_root.as_ref().ok_or("staging source absent")?,
                        relative,
                    )
                } else {
                    (
                        &working_root,
                        path.strip_prefix(&working).map_err(|e| e.to_string())?,
                    )
                };
                if let Err(error) = source.link_input_to(source_path, &target, relative) {
                    if error.kind() != std::io::ErrorKind::CrossesDevices {
                        return Err(error.to_string());
                    }
                    let mut input = source.open_input(source_path).map_err(|e| e.to_string())?;
                    let mut output = target
                        .create_file(relative, &budget)
                        .map_err(|e| e.to_string())?;
                    std::io::copy(&mut input, &mut output).map_err(|e| e.to_string())?;
                    output.flush().map_err(|e| e.to_string())?;
                    drop(output);
                    #[cfg(unix)]
                    let mode = {
                        use cap_std::fs::PermissionsExt;
                        Some(metadata.permissions().mode())
                    };
                    #[cfg(not(unix))]
                    let mode = None;
                    target
                        .set_directory_metadata(
                            relative,
                            metadata.modified().ok().map(|time| time.into_std()),
                            metadata.accessed().ok().map(|time| time.into_std()),
                            mode,
                        )
                        .map_err(|e| e.to_string())?;
                }
            }
            for (name, metadata) in directories.into_iter().rev() {
                #[cfg(unix)]
                let mode = {
                    use cap_std::fs::PermissionsExt;
                    Some(metadata.permissions().mode())
                };
                #[cfg(not(unix))]
                let mode = None;
                target
                    .set_directory_metadata(
                        Path::new(&name),
                        metadata.modified().ok().map(|time| time.into_std()),
                        metadata.accessed().ok().map(|time| time.into_std()),
                        mode,
                    )
                    .map_err(|e| e.to_string())?;
            }
            Ok(publication)
        })
        .await
        .map_err(|e| e.to_string())??;
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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::RunEpoch;
use crate::pipeline::extraction::safety::PositionalOutput;
use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget, MemoryPermit};

pub(crate) struct MemberSpec {
    pub(crate) name: PathBuf,
    pub(crate) len: u64,
    pub(crate) directory: bool,
    pub(crate) times: Option<std::fs::FileTimes>,
    pub(crate) directory_times: Option<(std::time::SystemTime, Option<std::time::SystemTime>)>,
}

struct Member {
    spec: MemberSpec,
    partial: Option<PathBuf>,
    output: Option<PositionalOutput>,
}

// Ownership is per set, even when every set writes under the shared final
// staging root. No cleanup walks that root or removes an unowned member.
#[cfg(test)]
type WriteObserver = Arc<dyn Fn(usize, u64) + Send + Sync>;

pub(crate) struct PositionalManifest {
    root: Arc<ExtractionRoot>,
    workspace: ExtractionRoot,
    root_path: PathBuf,
    budget: Arc<JobExtractionBudget>,
    _memory: MemoryPermit,
    members: Vec<Option<Member>>,
    published: Mutex<bool>,
    #[cfg(test)]
    write_observer: Mutex<Option<WriteObserver>>,
}

impl PositionalManifest {
    pub(crate) fn create(
        root: Arc<ExtractionRoot>,
        root_path: PathBuf,
        budget: Arc<JobExtractionBudget>,
        owner: &str,
        specs: Vec<Option<MemberSpec>>,
    ) -> Result<Arc<Self>, String> {
        // The handoff outlives the parsed archive and its header reservation.
        // Charge retained names, member/partial records and temporary collision
        // tables until this manifest is published or discarded.
        let fixed = (specs.len() as u64)
            .checked_mul(1024)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or("positional manifest memory overflow")?;
        let retained = specs.iter().flatten().try_fold(fixed, |bytes, spec| {
            (spec.name.as_os_str().as_encoded_bytes().len() as u64)
                .checked_mul(8)
                .and_then(|name| bytes.checked_add(name))
                .ok_or("positional manifest memory overflow")
        })?;
        if retained
            > budget
                .max_memory_bytes()
                .saturating_sub(budget.memory_reserved_bytes())
        {
            return Err("positional manifest exceeds remaining memory policy".to_string());
        }
        let memory = budget.reserve_more_memory_wait(retained)?;
        // Archive names cannot alias each other or generated partial names,
        // including on case-insensitive destination filesystems.
        let mut names = std::collections::HashSet::new();
        names
            .try_reserve(
                specs
                    .len()
                    .checked_mul(2)
                    .ok_or("positional name count overflow")?,
            )
            .map_err(|error| error.to_string())?;
        for spec in specs.iter().flatten() {
            root.validate_relative_path(&spec.name.to_string_lossy())
                .map_err(|error| budget.reject_unsafe_path(error))?;
            budget.check_member_metadata(&spec.name.to_string_lossy(), spec.len)?;
            if spec.name.components().next().is_some_and(|part| {
                part.as_os_str()
                    .to_string_lossy()
                    .eq_ignore_ascii_case(".weaver-chunks")
            }) {
                return Err(budget
                    .reject_unsafe_path("archive member aliases internal workspace")
                    .into());
            }
            if !names.insert(spec.name.to_string_lossy().to_lowercase()) {
                return Err("duplicate positional member name".to_string());
            }
        }
        if owner.len() != 64 || !owner.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("invalid positional output owner".to_string());
        }
        // A restart re-decodes covered packed bytes into a fresh generation.
        // Crash-era partials remain excluded from delivery in the internal
        // workspace; the ordinary final move reclaims them. No old CRC is trusted.
        static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let generation = GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos();
        let workspace = root.positional_workspace()?;
        let mut partials = Vec::new();
        partials
            .try_reserve_exact(specs.len())
            .map_err(|error| error.to_string())?;
        for (index, spec) in specs.iter().enumerate() {
            let partial = spec.as_ref().filter(|spec| !spec.directory).map(|_| {
                PathBuf::from(format!(
                    "positional-{owner}-{stamp:x}-{generation:x}-{index}.partial"
                ))
            });
            if let Some(partial) = &partial
                && !names.insert(partial.to_string_lossy().to_lowercase())
            {
                return Err("archive member aliases a positional partial".to_string());
            }
            partials.push(partial);
        }
        let mut manifest = Self {
            root,
            workspace,
            root_path,
            budget,
            _memory: memory,
            members: Vec::new(),
            published: Mutex::new(false),
            #[cfg(test)]
            write_observer: Mutex::new(None),
        };
        manifest
            .members
            .try_reserve_exact(specs.len())
            .map_err(|error| error.to_string())?;
        for (spec, partial) in specs.into_iter().zip(partials) {
            let Some(spec) = spec else {
                manifest.members.push(None);
                continue;
            };
            let output = partial
                .as_ref()
                .map(|partial| {
                    manifest.workspace.create_positional_partial(
                        partial,
                        spec.len,
                        &manifest.budget,
                    )
                })
                .transpose()?;
            manifest.members.push(Some(Member {
                spec,
                partial,
                output,
            }));
        }
        Ok(Arc::new(manifest))
    }

    #[cfg(test)]
    pub(crate) fn observe_writes(&self, observer: WriteObserver) {
        *self.write_observer.lock().unwrap() = Some(observer);
    }

    pub(crate) fn write_at(
        &self,
        epoch: &RunEpoch,
        file: usize,
        offset: u64,
        bytes: &[u8],
    ) -> io::Result<usize> {
        epoch.check()?;
        let member = self
            .members
            .get(file)
            .and_then(Option::as_ref)
            .and_then(|member| member.output.as_ref())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "run names no output member")
            })?;
        let written = member.write_at(offset, bytes)?;
        #[cfg(test)]
        {
            let observer = self.write_observer.lock().unwrap().clone();
            if let Some(observer) = observer {
                observer(file, offset);
            }
        }
        epoch.check()?;
        Ok(written)
    }

    pub(crate) fn finish_files(&self, epoch: &RunEpoch) -> io::Result<()> {
        for member in self.members.iter().flatten() {
            epoch.check()?;
            if let Some(output) = &member.output {
                output.sync_data()?;
                if let Some(times) = member.spec.times {
                    // The ordinary extractor treats timestamp failure as
                    // nonfatal once all payload bytes were verified.
                    if let Err(error) = output.set_times(times) {
                        tracing::debug!(%error, "positional member times not restored");
                    }
                }
            }
        }
        epoch.check()
    }

    // Called only after all header CRCs and current-epoch completions have
    // passed, and after the controller hands the set to final extraction.
    pub(crate) fn publish(&self, epoch: &RunEpoch, expected_root: &Path) -> io::Result<()> {
        if expected_root != self.root_path {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "positional destination changed",
            ));
        }
        epoch.commit(|| {
            let mut published = self.published.lock().unwrap_or_else(|p| p.into_inner());
            if *published {
                return Ok(());
            }
            let mut linked: Vec<&Member> = Vec::new();
            linked.try_reserve_exact(self.members.len()).map_err(io::Error::other)?;
            let result = (|| {
                for member in self.members.iter().flatten() {
                    if member.spec.directory {
                        self.root.create_dir(&member.spec.name, &self.budget).map_err(io::Error::other)?;
                    } else {
                        let partial = member.partial.as_ref().expect("regular partial");
                        let output = member.output.as_ref().expect("regular output");
                        if !self.workspace.positional_alias_matches(partial, output)? {
                            return Err(io::Error::other("positional partial ownership changed"));
                        }
                        self.root.publish_positional_partial(&self.workspace, partial,
                            &member.spec.name, &self.budget).map_err(io::Error::other)?;
                        linked.push(member);
                    }
                }
                Ok::<_, io::Error>(())
            })();
            if let Err(error) = result {
                for member in linked.into_iter().rev() {
                    if let Err(cleanup) = self.root.remove_positional_partial(
                        &member.spec.name, member.output.as_ref().expect("regular output")) {
                        tracing::warn!(%cleanup, "positional publication rollback retained an alias");
                    }
                }
                return Err(error);
            }
            for member in linked {
                let _ = self.workspace.remove_positional_partial(
                    member.partial.as_ref().expect("regular partial"),
                    member.output.as_ref().expect("regular output"));
            }
            for member in self.members.iter().flatten().rev() {
                if member.spec.directory
                    && let Some((modified, accessed)) = member.spec.directory_times
                    && let Err(error) = self.root.set_directory_metadata(
                        &member.spec.name,
                        Some(modified),
                        accessed,
                        None,
                    )
                {
                    tracing::debug!(%error, "positional directory times not restored");
                }
            }
            *published = true;
            Ok(())
        })
    }
}

impl Drop for PositionalManifest {
    fn drop(&mut self) {
        for member in self.members.iter().flatten() {
            if let (Some(partial), Some(output)) = (&member.partial, &member.output) {
                let _ = self.workspace.remove_positional_partial(partial, output);
            }
        }
    }
}

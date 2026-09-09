//! Content identity discovery. A match proposes a name; it is not repair evidence.

use super::*;
use par3_rs::layout::ExtentKind;

struct Digest {
    snapshot: SourceSnapshot,
    revision: u64,
    fingerprint: par3_rs::Fingerprint,
    _reservation: assessment::ViewReservation,
}

#[derive(Default)]
pub(super) struct NameSearch {
    digests: BTreeMap<SourceId, Digest>,
    found: Option<NameMatch>,
    read_bytes: u64,
}

impl NameSearch {
    pub(super) fn source(&self) -> Option<SourceId> {
        self.found.as_ref().map(|found| found.source)
    }
}

pub(super) struct NameMatch {
    pub source: SourceId,
    pub snapshot: SourceSnapshot,
    pub path: Option<PathBuf>,
    pub name: String,
    _reservation: assessment::ViewReservation,
}

impl Par3Job {
    pub(super) fn discover_name(&mut self) -> EngineResult<()> {
        self.name_search.found = None;
        // Layouts retain their own native leases. Temporary references share
        // the host-state budget; the ordinary exact-name path reads no bytes.
        let _layouts = assessment::ViewReservation::acquire(self.sets.len() * 32)?;
        let mut layouts = Vec::with_capacity(self.sets.len());
        for set in self.sets.values_mut() {
            if let Some(layout) = set.native.layout()? {
                layouts.push(layout);
            }
        }
        let files = || layouts.iter().flat_map(|layout| layout.files());
        if !files().any(|file| !self.bindings.contains_key(&file.path)) {
            return Ok(());
        }
        self.name_search
            .digests
            .retain(|source, _| self.bindings.values().any(|bound| bound == source));
        for (name, &source) in &self.bindings {
            self.options.cancel.check()?;
            // A correctly named source already belongs to its description.
            // Do not steal its identity to satisfy a second, shared alias.
            if files().any(|file| file.path == *name) {
                continue;
            }
            let Some(snapshot) = self.sources.snapshot(source)? else {
                continue;
            };
            let eligible = |file: &&par3_rs::layout::FileLayout| {
                file.len == snapshot.len
                    && file.len != 0
                    && file.fingerprint != [0; 16]
                    && !self.bindings.contains_key(&file.path)
                    && !file
                        .extents
                        .iter()
                        .any(|extent| matches!(extent.kind, ExtentKind::Unprotected))
            };
            if !files().any(|file| eligible(&file)) {
                continue;
            }
            let revision = self
                .sources
                .revision(source)?
                .ok_or(EngineError::SourceChanged(source))?;
            if !self
                .name_search
                .digests
                .get(&source)
                .is_some_and(|digest| digest.snapshot == snapshot && digest.revision == revision)
            {
                // Holes have no implied value. Wait for complete published
                // coverage before attempting a whole-file identity hash.
                let mut offset = 0;
                while offset < snapshot.len {
                    self.options.cancel.check()?;
                    let Some(range) = self.sources.next_available(source, offset)? else {
                        break;
                    };
                    if range.start != offset {
                        break;
                    }
                    offset = range.end;
                }
                if offset != snapshot.len {
                    continue;
                }
                let reservation = assessment::ViewReservation::acquire(160)?;
                let fingerprint = fingerprint_source(
                    &self.sources,
                    source,
                    snapshot,
                    &self.options,
                    &mut self.name_search.read_bytes,
                )?;
                self.name_search.digests.insert(
                    source,
                    Digest {
                        snapshot,
                        revision,
                        fingerprint,
                        _reservation: reservation,
                    },
                );
            }
            let digest = &self.name_search.digests[&source];
            let mut matches = files()
                .filter(eligible)
                .filter(|file| file.fingerprint == digest.fingerprint);
            let Some(file) = matches.next() else {
                continue;
            };
            if matches.any(|other| other.path != file.path) {
                return Err(EngineError::InvalidState(
                    "ambiguous PAR3 content aliases require separate output placement",
                ));
            }
            let path = self
                .disk_publications
                .get(&source)
                .map(|publication| &publication.path);
            let reservation = assessment::ViewReservation::acquire(
                160 + file.path.len() + path.map_or(0, |path| path.as_os_str().len()),
            )?;
            self.name_search.found = Some(NameMatch {
                source,
                snapshot,
                path: path.cloned(),
                name: file.path.clone(),
                _reservation: reservation,
            });
            break;
        }
        Ok(())
    }

    pub(super) fn take_name_match(&mut self) -> EngineResult<Option<NameMatch>> {
        let Some(found) = self.name_search.found.take() else {
            return Ok(None);
        };
        if self.sources.snapshot(found.source)? != Some(found.snapshot) {
            return Err(EngineError::SourceChanged(found.source));
        }
        Ok(Some(found))
    }
}

fn fingerprint_source(
    access: &dyn SourceAccess,
    source: SourceId,
    snapshot: SourceSnapshot,
    options: &ExecutionOptions,
    read_bytes: &mut u64,
) -> EngineResult<par3_rs::Fingerprint> {
    let _reservation = assessment::ViewReservation::acquire(64 << 10)?;
    let mut bytes = vec![0; 64 << 10];
    let mut hash = par3_rs::hash::FingerprintHasher::new();
    let mut offset = 0;
    let mut reader = access.open_sequential(source)?;
    while offset < snapshot.len {
        options.cancel.check()?;
        let count = (snapshot.len - offset).min(bytes.len() as u64) as usize;
        let read = if let Some(reader) = reader.as_mut() {
            reader.read(&mut bytes[..count])?
        } else {
            access.read_at(source, offset, &mut bytes[..count])?
        };
        if read == 0 && reader.take().is_some() {
            continue;
        }
        if read == 0 {
            return Err(EngineError::Unavailable {
                source_id: source,
                offset,
            });
        }
        if read > count {
            return Err(EngineError::InvalidState("invalid placement read length"));
        }
        *read_bytes = read_bytes.saturating_add(read as u64);
        hash.update(&bytes[..read]);
        offset += read as u64;
    }
    if access.snapshot(source)? != Some(snapshot) {
        return Err(EngineError::SourceChanged(source));
    }
    Ok(hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{INDEX, RECOVERY, inputs};
    use super::*;

    #[test]
    fn content_match_accepts_pathless_sources_without_materializing_them() {
        let root = tempfile::tempdir().unwrap();
        let mut job = Par3Job::default();
        let mut memory = par3_rs::source::MemorySourceAccess::default();
        memory.insert(SourceId(1), 7, Arc::from(inputs()[0].1.clone()));
        job.publish_access(
            SourceId(1),
            Arc::new(memory),
            "unknown.dat".into(),
            std::iter::once(0..5000).collect(),
            None,
        )
        .unwrap();
        let path = root.path().join("set.par3");
        std::fs::write(&path, INDEX).unwrap();
        job.scan_file(SourceId(99), path, None).unwrap();
        job.assess().unwrap();
        let found = job.take_name_match().unwrap().unwrap();
        assert_eq!(found.name, "a.bin");
        assert_eq!(found.source, SourceId(1));
        assert!(found.path.is_none());
        assert!(!root.path().join("a.bin").exists());
        job.assess().unwrap();
        assert_eq!(job.name_search.read_bytes, 5000);
    }

    #[test]
    fn content_name_match_is_cached_but_never_verification_evidence() {
        let root = tempfile::tempdir().unwrap();
        let mut job = Par3Job::default();
        let bytes = &inputs()[0].1;
        let path = root.path().join("obfuscated.dat");
        std::fs::write(&path, bytes).unwrap();
        job.publish_file(
            SourceId(1),
            path.clone(),
            "obfuscated.dat".into(),
            vec![0..2000, 3000..5000],
        )
        .unwrap();
        let index = root.path().join("set.par3");
        std::fs::write(&index, INDEX).unwrap();
        job.scan_file(SourceId(99), index, None).unwrap();
        job.assess().unwrap();
        assert!(!job.name_search.source().is_some());
        assert_eq!(
            job.name_search.read_bytes, 0,
            "interior holes are not complete content"
        );
        job.publish_file(
            SourceId(1),
            path.clone(),
            "obfuscated.dat".into(),
            std::iter::once(0..5000).collect(),
        )
        .unwrap();
        job.assess().unwrap();
        let found = job.take_name_match().unwrap().unwrap();
        assert_eq!(found.name, "a.bin");
        assert_eq!(found.path, Some(path.clone()));
        assert_eq!(found.source, SourceId(1));
        assert_eq!(job.name_search.read_bytes, 5000);
        assert!(
            job.sets.values().all(|set| set
                .view
                .as_ref()
                .unwrap()
                .files
                .iter()
                .all(|file| !file.complete)),
            "identity hashes cannot manufacture native evidence"
        );
        for _ in 0..3 {
            job.assess().unwrap();
        }
        assert_eq!(job.name_search.read_bytes, 5000);
        let recovery = root.path().join("set.vol0+1.par3");
        std::fs::write(&recovery, RECOVERY).unwrap();
        job.scan_file(SourceId(98), recovery, None).unwrap();
        job.assess().unwrap();
        assert_eq!(
            job.name_search.read_bytes, 5000,
            "recovery-only arrival must not rerun content discovery"
        );
        job.sources.withdraw(SourceId(1)).unwrap();
        assert!(matches!(
            job.take_name_match(),
            Err(EngineError::SourceChanged(SourceId(1)))
        ));
        let mut changed = bytes.clone();
        changed[0] ^= 0x80;
        std::fs::write(&path, changed).unwrap();
        job.publish_file(
            SourceId(1),
            path,
            "obfuscated.dat".into(),
            std::iter::once(0..5000).collect(),
        )
        .unwrap();
        job.assess().unwrap();
        assert!(!job.name_search.source().is_some());
        assert_eq!(job.name_search.read_bytes, 10000);
        job.assess().unwrap();
        assert_eq!(
            job.name_search.read_bytes, 10000,
            "negative matches are cached too"
        );
    }

    #[test]
    fn content_discovery_honors_cancellation_before_reading() {
        let root = tempfile::tempdir().unwrap();
        let mut job = Par3Job::default();
        let path = root.path().join("unknown.bin");
        std::fs::write(&path, &inputs()[0].1).unwrap();
        job.publish_file(
            SourceId(1),
            path,
            "unknown.bin".into(),
            std::iter::once(0..5000).collect(),
        )
        .unwrap();
        let path = root.path().join("set.par3");
        std::fs::write(&path, INDEX).unwrap();
        job.scan_file(SourceId(99), path, None).unwrap();
        job.options.cancel.cancel();
        assert!(matches!(job.discover_name(), Err(EngineError::Cancelled)));
        assert_eq!(job.name_search.read_bytes, 0);
    }
}

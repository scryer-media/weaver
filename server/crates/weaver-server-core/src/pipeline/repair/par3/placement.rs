//! Content identity discovery. A match proposes a name; it is not repair evidence.

use super::*;
use par3_rs::layout::ExtentKind;

struct Digest {
    snapshot: SourceSnapshot,
    revision: u64,
    descriptions: Option<par3_rs::Fingerprint>,
    fingerprint: par3_rs::Fingerprint,
    damaged_name: Option<String>,
    _reservation: assessment::ViewReservation,
}

#[derive(Default)]
pub(super) struct NameSearch {
    digests: BTreeMap<SourceId, Digest>,
    embedded: embedded::Cache,
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
        // A negative or partial match is only valid for these authenticated
        // descriptions and destination bindings. New metadata can introduce an
        // alias or supply checksums that were previously unavailable.
        let mut descriptions = par3_rs::hash::FingerprintHasher::new();
        for layout in &layouts {
            descriptions.update(&layout.identity());
        }
        for (name, source) in &self.bindings {
            descriptions.update(&(name.len() as u64).to_le_bytes());
            descriptions.update(name.as_bytes());
            descriptions.update(&source.0.to_le_bytes());
        }
        let descriptions = descriptions.finalize();
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
                    // Nested outputs retain independent manifest ownership;
                    // extent donation preserves the original flat NZB source.
                    && !file.path.contains('/')
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
                let reservation = assessment::ViewReservation::acquire(
                    192 + files()
                        .filter(eligible)
                        .map(|file| file.path.len())
                        .max()
                        .unwrap_or(0),
                )?;
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
                        descriptions: None,
                        fingerprint,
                        damaged_name: None,
                        _reservation: reservation,
                    },
                );
            }
            if self.name_search.digests[&source].descriptions != Some(descriptions) {
                // Metadata arrivals invalidate candidate selection, not an
                // unchanged source's full hash. Keep optional rescans bounded.
                let reservation = assessment::ViewReservation::acquire(
                    192 + files()
                        .filter(eligible)
                        .map(|file| file.path.len())
                        .max()
                        .unwrap_or(0),
                )?;
                let fingerprint = self.name_search.digests[&source].fingerprint;
                let damaged_name = if files().any(|file| file.fingerprint == fingerprint) {
                    None
                } else {
                    unique_damaged_name(
                        files().filter(eligible),
                        &self.sources,
                        source,
                        snapshot,
                        &self.options,
                        &mut self.name_search.read_bytes,
                    )?
                };
                let digest = self
                    .name_search
                    .digests
                    .get_mut(&source)
                    .expect("hashed source");
                digest.descriptions = Some(descriptions);
                digest.damaged_name = damaged_name;
                digest._reservation = reservation;
            }
            let digest = &self.name_search.digests[&source];
            let mut matches = files().filter(eligible).filter(|file| {
                file.fingerprint == digest.fingerprint
                    || digest.damaged_name.as_deref() == Some(file.path.as_str())
            });
            let Some(file) = matches.next() else {
                continue;
            };
            if matches.any(|other| other.path != file.path) {
                // Compatible aliases need independent output identities. The
                // extent donor path can satisfy each without renaming this source.
                continue;
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
        if self.name_search.found.is_none() {
            self.discover_embedded_name(&layouts)?;
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

fn unique_damaged_name<'a>(
    files: impl Iterator<Item = &'a par3_rs::layout::FileLayout> + Clone,
    access: &dyn SourceAccess,
    source: SourceId,
    snapshot: SourceSnapshot,
    options: &ExecutionOptions,
    read_bytes: &mut u64,
) -> EngineResult<Option<String>> {
    const READ_LIMIT: u64 = 1 << 30;
    let required = files
        .clone()
        .try_fold(0u64, |total, file| total.checked_add(file.len));
    if required.is_none_or(|bytes| bytes > READ_LIMIT.saturating_sub(*read_bytes))
        || files.clone().any(|file| {
            file.extents.iter().any(|extent| {
                matches!(
                    extent.kind,
                    ExtentKind::Block {
                        fingerprint: None,
                        ..
                    }
                )
            })
        })
    {
        // An unexamined candidate is not a negative match. Never pick the first
        // name merely because checking another alias exhausted optional work.
        return Ok(None);
    }
    let mut matched: Option<&str> = None;
    for file in files {
        if matches_single_damaged_extent(access, source, snapshot, file, options, read_bytes)? {
            if matched.is_some_and(|name| name != file.path) {
                return Ok(None);
            }
            matched = Some(&file.path);
        }
    }
    Ok(matched.map(str::to_owned))
}

// A damaged name proposal still carries no verification evidence. Require an
// exact-length image with independently authenticated extents at both ends and
// every other extent matching except one interior block, with a majority of
// bytes authenticated. This is deliberately conservative. Ambiguous names are
// rejected by the caller; native verification and repair must follow the move.
fn matches_single_damaged_extent(
    access: &dyn SourceAccess,
    source: SourceId,
    snapshot: SourceSnapshot,
    file: &par3_rs::layout::FileLayout,
    options: &ExecutionOptions,
    read_bytes: &mut u64,
) -> EngineResult<bool> {
    const READ_LIMIT: u64 = 1 << 30;
    if file.extents.len() < 3
        || file.len != snapshot.len
        || file.len > READ_LIMIT.saturating_sub(*read_bytes)
        || file
            .extents
            .first()
            .is_none_or(|extent| extent.range.start != 0)
        || file
            .extents
            .last()
            .is_none_or(|extent| extent.range.end != file.len)
        || file
            .extents
            .windows(2)
            .any(|pair| pair[0].range.end != pair[1].range.start)
        || file.extents.iter().any(|extent| {
            !matches!(
                extent.kind,
                ExtentKind::Block {
                    fingerprint: Some(_),
                    ..
                }
            )
        })
    {
        return Ok(false);
    }
    let _reservation = assessment::ViewReservation::acquire(64 << 10)?;
    let mut bytes = vec![0; 64 << 10];
    let mut damaged = 0;
    let mut matched_bytes = 0;
    for (index, extent) in file.extents.iter().enumerate() {
        let ExtentKind::Block {
            fingerprint: Some(expected),
            ..
        } = extent.kind
        else {
            unreachable!()
        };
        let mut hash = par3_rs::hash::FingerprintHasher::new();
        let mut offset = extent.range.start;
        while offset < extent.range.end {
            options.cancel.check()?;
            let take = (extent.range.end - offset).min(bytes.len() as u64) as usize;
            let count = access.read_at(source, offset, &mut bytes[..take])?;
            if count == 0 || count > take {
                return Ok(false);
            }
            hash.update(&bytes[..count]);
            offset += count as u64;
            *read_bytes = read_bytes.saturating_add(count as u64);
        }
        if hash.finalize() != expected {
            damaged += 1;
            if damaged > 1 || index == 0 || index + 1 == file.extents.len() {
                return Ok(false);
            }
        } else {
            matched_bytes += extent.range.end - extent.range.start;
        }
    }
    if access.snapshot(source)? != Some(snapshot) {
        return Err(EngineError::SourceChanged(source));
    }
    Ok(damaged == 1 && matched_bytes > file.len / 2)
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
        job.scan_file(SourceId(98), recovery.clone(), None).unwrap();
        job.publish_file(
            SourceId(98),
            recovery,
            "set.vol0+1.par3".into(),
            std::iter::once(0..RECOVERY.len() as u64).collect(),
        )
        .unwrap();
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
        // The changed full-file hash also probes its first 2000-byte extent;
        // damage there rejects the partial identity without reading the rest.
        assert_eq!(job.name_search.read_bytes, 12000);
        job.assess().unwrap();
        assert_eq!(
            job.name_search.read_bytes, 12000,
            "negative matches are cached too"
        );
    }

    #[test]
    fn damaged_name_rejects_aliases_and_incomplete_candidate_searches() {
        let root = tempfile::tempdir().unwrap();
        let mut job = Par3Job::default();
        let index = root.path().join("set.par3");
        std::fs::write(&index, INDEX).unwrap();
        job.scan_file(SourceId(99), index, None).unwrap();
        let layout = job
            .sets
            .values_mut()
            .next()
            .unwrap()
            .native
            .layout()
            .unwrap()
            .unwrap();
        let file = layout
            .files()
            .iter()
            .find(|file| file.path == "a.bin")
            .unwrap();
        let mut alias = file.clone();
        alias.path = "alias.bin".into();
        let mut damaged = inputs()[0].1.clone();
        damaged[2200..2232].fill(0);
        let source = SourceId(1);
        let mut access = par3_rs::source::MemorySourceAccess::default();
        access.insert(source, 1, Arc::from(damaged));
        let snapshot = access.snapshot(source).unwrap().unwrap();
        let mut read = 0;
        assert_eq!(
            unique_damaged_name(
                [file].into_iter(),
                &access,
                source,
                snapshot,
                &job.options,
                &mut read
            )
            .unwrap()
            .as_deref(),
            Some("a.bin")
        );
        read = 0;
        assert!(
            unique_damaged_name(
                [file, &alias].into_iter(),
                &access,
                source,
                snapshot,
                &job.options,
                &mut read
            )
            .unwrap()
            .is_none()
        );
        read = (1 << 30) - file.len;
        assert!(
            unique_damaged_name(
                [file, &alias].into_iter(),
                &access,
                source,
                snapshot,
                &job.options,
                &mut read
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            read,
            (1 << 30) - file.len,
            "budget exhaustion cannot pick an untested alias"
        );
        if let ExtentKind::Block { fingerprint, .. } = &mut alias.extents[0].kind {
            *fingerprint = None;
        }
        read = 0;
        assert!(
            unique_damaged_name(
                [file, &alias].into_iter(),
                &access,
                source,
                snapshot,
                &job.options,
                &mut read
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(read, 0);
    }

    #[test]
    fn damaged_name_requires_authenticated_surrounding_content_and_a_free_destination() {
        for (damage, matched) in [(0..32, false), (2200..2232, true), (1000..4001, false)] {
            let root = tempfile::tempdir().unwrap();
            let mut job = Par3Job::default();
            let path = root.path().join("opaque.201");
            let mut bytes = inputs()[0].1.clone();
            bytes[damage].fill(0);
            std::fs::write(&path, bytes).unwrap();
            job.publish_file(
                SourceId(1),
                path.clone(),
                "opaque.201".into(),
                std::iter::once(0..5000).collect(),
            )
            .unwrap();
            let index = root.path().join("set.par3");
            std::fs::write(&index, INDEX).unwrap();
            job.scan_file(SourceId(99), index, None).unwrap();
            job.assess().unwrap();
            assert_eq!(job.take_name_match().unwrap().is_some(), matched);
            let read_bytes = job.name_search.read_bytes;
            job.assess().unwrap();
            assert_eq!(
                job.name_search.read_bytes, read_bytes,
                "cache repeated assessment"
            );
            assert!(
                job.sets.values().all(|set| set
                    .view
                    .as_ref()
                    .unwrap()
                    .files
                    .iter()
                    .all(|file| !file.complete)),
                "a partial identity is never verification evidence"
            );
            if matched {
                job.sources.withdraw(SourceId(1)).unwrap();
                assert!(matches!(
                    job.take_name_match(),
                    Err(EngineError::SourceChanged(SourceId(1)))
                ));
                job.publish_file(
                    SourceId(1),
                    path,
                    "opaque.201".into(),
                    std::iter::once(0..5000).collect(),
                )
                .unwrap();
                let target = root.path().join("a.bin");
                std::fs::write(&target, vec![0xff; 5000]).unwrap();
                job.publish_file(
                    SourceId(2),
                    target.clone(),
                    "a.bin".into(),
                    std::iter::once(0..5000).collect(),
                )
                .unwrap();
                job.assess().unwrap();
                assert!(
                    job.take_name_match().unwrap().is_none(),
                    "never take an owned destination"
                );
                assert_eq!(std::fs::read(target).unwrap(), vec![0xff; 5000]);
            }
        }
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

mod embedded;

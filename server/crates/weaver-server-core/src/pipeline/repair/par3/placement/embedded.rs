//! Identity matching for the protected stream of an embedded carrier.

use super::*;
use par3_rs::layout::BlockLayout;

type Key = (SourceId, par3_rs::Fingerprint, par3_rs::Fingerprint);

struct Candidate {
    snapshot: SourceSnapshot,
    revision: u64,
    matches: bool,
    _reservation: assessment::ViewReservation,
}

#[derive(Default)]
pub(super) struct Cache {
    entries: BTreeMap<Key, Candidate>,
}

impl Par3Job {
    pub(super) fn discover_embedded_name(
        &mut self,
        layouts: &[Arc<BlockLayout>],
    ) -> EngineResult<()> {
        self.name_search
            .embedded
            .entries
            .retain(|(source, _, _), _| self.carriers.contains_key(source));
        for layout in layouts {
            for (index, file) in layout.files().iter().enumerate() {
                if self.bindings.contains_key(&file.path)
                    || file.fingerprint == [0; 16]
                    || !file
                        .extents
                        .iter()
                        .any(|extent| matches!(extent.kind, ExtentKind::Unprotected))
                {
                    continue;
                }
                for (name, &source) in &self.bindings {
                    self.options.cancel.check()?;
                    let Some(path) = self
                        .carriers
                        .get(&source)
                        .and_then(|carrier| carrier.path.as_ref())
                    else {
                        continue;
                    };
                    if layouts
                        .iter()
                        .flat_map(|layout| layout.files())
                        .any(|file| file.path == *name)
                    {
                        continue;
                    }
                    let Some(snapshot) = self.sources.snapshot(source)? else {
                        continue;
                    };
                    if snapshot.len != file.len {
                        continue;
                    }
                    let revision = self
                        .sources
                        .revision(source)?
                        .ok_or(EngineError::SourceChanged(source))?;
                    let key = (source, layout.identity(), file.packet_hash);
                    if !self
                        .name_search
                        .embedded
                        .entries
                        .get(&key)
                        .is_some_and(|candidate| {
                            candidate.snapshot == snapshot && candidate.revision == revision
                        })
                    {
                        // Identity installation requires a complete physical
                        // carrier. Protection gaps are omitted from its PAR3
                        // fingerprint, but their availability is not invented.
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
                        let reservation = assessment::ViewReservation::acquire(224)?;
                        let before = self.options.diagnostics.source_io().read_bytes;
                        let evidence = par3_rs::evidence::verify_source(
                            Arc::clone(layout),
                            index,
                            &self.sources,
                            source,
                            &self.options,
                        );
                        self.name_search.read_bytes = self.name_search.read_bytes.saturating_add(
                            self.options
                                .diagnostics
                                .source_io()
                                .read_bytes
                                .saturating_sub(before),
                        );
                        let evidence = evidence?;
                        let matches =
                            evidence.whole_matches() == Some(true) && evidence.protected_complete();
                        self.name_search.embedded.entries.insert(
                            key,
                            Candidate {
                                snapshot,
                                revision,
                                matches,
                                _reservation: reservation,
                            },
                        );
                    }
                    if !self.name_search.embedded.entries[&key].matches {
                        continue;
                    }
                    if let Some(found) = &self.name_search.found {
                        if found.source == source && found.name != file.path {
                            return Err(EngineError::InvalidState(
                                "ambiguous embedded PAR3 content aliases",
                            ));
                        }
                        continue;
                    }
                    let reservation = assessment::ViewReservation::acquire(
                        160 + file.path.len() + path.as_os_str().len(),
                    )?;
                    self.name_search.found = Some(NameMatch {
                        source,
                        snapshot,
                        path: Some(path.clone()),
                        name: file.path.clone(),
                        _reservation: reservation,
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_identity_waits_for_holes_and_rejects_stale_proposals() {
        let bytes = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../e2e/internal/weaver/testdata/par3-inside/archive.zip"
        ));
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("opaque.dat");
        std::fs::write(&path, bytes).unwrap();
        let mut job = Par3Job::default();
        let source = SourceId(0);
        job.scan_embedded(
            source,
            path.clone(),
            "opaque.dat".into(),
            Some(vec![0..1024, 1025..bytes.len() as u64]),
            0,
        )
        .unwrap();
        job.assess().unwrap();
        assert!(job.name_search.source().is_none());
        assert_eq!(job.name_search.read_bytes, 0);
        job.scan_embedded(source, path.clone(), "opaque.dat".into(), None, 0)
            .unwrap();
        job.assess().unwrap();
        assert_eq!(job.name_search.source(), Some(source));
        let read = job.name_search.read_bytes;
        assert!(read > 0);
        for _ in 0..3 {
            job.scan_embedded(source, path.clone(), "opaque.dat".into(), None, 0)
                .unwrap();
            job.assess().unwrap();
        }
        assert_eq!(job.name_search.read_bytes, read);
        assert!(
            job.sets
                .values()
                .all(|set| !set.view.as_ref().unwrap().files[0].complete)
        );
        job.sources.withdraw(source).unwrap();
        assert!(
            matches!(job.take_name_match(), Err(EngineError::SourceChanged(id)) if id == source)
        );
    }
}

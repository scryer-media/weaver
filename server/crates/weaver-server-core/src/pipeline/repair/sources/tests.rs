use super::*;
use par3_rs::source::MemorySourceAccess;

fn backing(bytes: &[u8]) -> Arc<dyn SourceAccess> {
    let mut memory = MemorySourceAccess::default();
    memory.insert(SourceId(0), 1, Arc::from(bytes));
    Arc::new(memory)
}

#[test]
fn unchanged_backing_arrival_rechecks_both_generations_before_publication() {
    let sources = PublishedSources::default();
    let access = backing(b"abcdefgh");
    let physical = access.snapshot(SourceId(0)).unwrap().unwrap();
    let published = sources
        .replace(SourceId(0), Arc::clone(&access), 8, vec![0..2, 6..8])
        .unwrap();
    let ranges: Vec<_> = std::iter::once(0..8).collect();
    assert!(
        sources
            .can_extend_publication(SourceId(0), published, physical, &ranges)
            .unwrap()
    );
    let revision = sources.revision(SourceId(0)).unwrap();
    let mut changed = MemorySourceAccess::default();
    changed.insert(SourceId(0), 2, Arc::from(&b"ABcdefgh"[..]));
    assert!(matches!(
        sources.arrive_unchanged(
            SourceId(0),
            Arc::new(changed),
            8,
            ranges.clone(),
            published,
            physical
        ),
        Err(EngineError::SourceChanged(SourceId(0)))
    ));
    assert_eq!(sources.revision(SourceId(0)).unwrap(), revision);
    assert_eq!(sources.next_available(SourceId(0), 2).unwrap(), Some(6..8));
    assert_eq!(
        sources
            .arrive_unchanged(
                SourceId(0),
                Arc::clone(&access),
                8,
                ranges.clone(),
                published,
                physical
            )
            .unwrap(),
        published
    );
    sources.withdraw(SourceId(0)).unwrap();
    assert!(matches!(
        sources.arrive_unchanged(SourceId(0), access, 8, ranges, published, physical),
        Err(EngineError::SourceChanged(SourceId(0)))
    ));
    assert_eq!(sources.next_available(SourceId(0), 0).unwrap(), None);
}

#[test]
fn withdrawing_coverage_fences_open_readers_and_keeps_generation_history() {
    let sources = PublishedSources::default();
    let before = sources
        .replace(
            SourceId(0),
            backing(b"abcd"),
            4,
            std::iter::once(0..4).collect(),
        )
        .unwrap();
    let mut reader = sources.open_sequential(SourceId(0)).unwrap().unwrap();
    sources.withdraw(SourceId(0)).unwrap();
    assert_eq!(sources.next_available(SourceId(0), 0).unwrap(), None);
    assert_eq!(sources.read_at(SourceId(0), 0, &mut [0; 4]).unwrap(), 0);
    let error = reader.read(&mut [0; 4]).unwrap_err();
    assert!(matches!(
        EngineError::from(error),
        EngineError::SourceChanged(SourceId(0))
    ));
    let after = sources
        .replace(
            SourceId(0),
            backing(b"efgh"),
            4,
            std::iter::once(0..4).collect(),
        )
        .unwrap();
    assert!(after.generation > before.generation);
    assert!(
        reader.read(&mut [0; 4]).is_err(),
        "new publication cannot revive an old reader"
    );
    let mut bytes = [0; 4];
    assert_eq!(sources.read_at(SourceId(0), 0, &mut bytes).unwrap(), 4);
    assert_eq!(&bytes, b"efgh");
}

#[test]
fn sparse_disk_image_never_turns_uncommitted_bytes_into_data() {
    let sources = PublishedSources::default();
    let bytes = backing(b"abcd0000ijkl");
    sources
        .replace(SourceId(0), bytes, 12, vec![0..4, 8..12])
        .unwrap();
    let mut buffer = [0xcc; 12];
    assert_eq!(sources.read_at(SourceId(0), 0, &mut buffer).unwrap(), 4);
    assert_eq!(&buffer[..4], b"abcd");
    assert_eq!(sources.read_at(SourceId(0), 4, &mut buffer).unwrap(), 0);
    assert_eq!(sources.next_available(SourceId(0), 4).unwrap(), Some(8..12));
    assert_eq!(sources.read_at(SourceId(0), 8, &mut buffer).unwrap(), 4);
    assert_eq!(&buffer[..4], b"ijkl");
    let mut sequential = sources
        .open_sequential(SourceId(0))
        .unwrap()
        .expect("honest prefix reader");
    let mut prefix = Vec::new();
    sequential.read_to_end(&mut prefix).unwrap();
    assert_eq!(prefix, b"abcd");
}

#[test]
fn hole_fills_preserve_generation_but_rewrites_and_rebindings_do_not() {
    let sources = PublishedSources::default();
    let first = sources
        .replace(SourceId(0), backing(b"abcd0000ijkl"), 12, vec![0..4, 8..12])
        .unwrap();
    let before = sources.revision(SourceId(0)).unwrap();
    let second = sources
        .arrive(
            SourceId(0),
            backing(b"abcdefghijkl"),
            12,
            std::iter::once(0..12).collect(),
        )
        .unwrap();
    assert_eq!(first, second);
    assert!(sources.revision(SourceId(0)).unwrap() > before);
    assert!(matches!(
        sources.arrive(
            SourceId(0),
            backing(b"abcdefghijkl"),
            12,
            std::iter::once(0..4).collect()
        ),
        Err(EngineError::SourceChanged(SourceId(0)))
    ));
    assert!(matches!(
        sources.arrive(
            SourceId(0),
            backing(b"abcd"),
            4,
            std::iter::once(0..4).collect()
        ),
        Err(EngineError::SourceChanged(SourceId(0)))
    ));
    let third = sources
        .replace(
            SourceId(0),
            backing(b"abcd"),
            4,
            std::iter::once(0..4).collect(),
        )
        .unwrap();
    assert!(third.generation > second.generation);
    assert_eq!(sources.next_available(SourceId(0), 4).unwrap(), None);
}

#[test]
fn invalid_coverage_is_rejected_without_replacing_the_publication() {
    let sources = PublishedSources::default();
    let initial = sources
        .replace(
            SourceId(0),
            backing(b"abcdefgh"),
            8,
            std::iter::once(0..4).collect(),
        )
        .unwrap();
    for ranges in [
        vec![3..6, 2..4],
        std::iter::once(0..9).collect(),
        std::iter::once(4..4).collect(),
    ] {
        assert!(
            sources
                .replace(SourceId(0), backing(b"abcdefgh"), 8, ranges)
                .is_err()
        );
        assert_eq!(sources.snapshot(SourceId(0)).unwrap(), Some(initial));
    }
    assert!(
        sources
            .replace(
                SourceId(0),
                backing(b"abcd"),
                8,
                std::iter::once(0..8).collect()
            )
            .is_err()
    );
    assert_eq!(sources.snapshot(SourceId(0)).unwrap(), Some(initial));
}

#[test]
fn adjacent_coverage_can_preserve_existing_publication() {
    assert!(covers(&[0..2, 2..4, 4..8], std::slice::from_ref(&(0..8))));
    assert!(!covers(&[0..2, 3..8], std::slice::from_ref(&(0..8))));
    assert!(covers(std::slice::from_ref(&(0..12)), &[0..4, 8..12]));
}

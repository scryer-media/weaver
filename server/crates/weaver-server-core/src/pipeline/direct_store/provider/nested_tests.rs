use super::*;

fn ranges(values: &[(u64, u64)]) -> ByteRanges {
    let mut ranges = ByteRanges::new();
    for &(start, end) in values {
        ranges.insert(start, end - start);
    }
    ranges
}

fn tree(root: &std::path::Path, hole: bool) -> VirtualVolume {
    let leaf = root.join("leaf");
    let inner = root.join("inner-envelope");
    let outer = root.join("outer-envelope");
    std::fs::write(&leaf, b"PAYLOAD").unwrap();
    std::fs::write(&inner, b"h.......t").unwrap();
    std::fs::write(&outer, b"AB.........YZ").unwrap();
    let child = VirtualVolume {
        backing_leases: Default::default(),
        volume_index: 0,
        envelope: inner,
        extents: vec![MemberExtent {
            member_id: 0,
            physical_offset: 1,
            logical_offset: 0,
            len: 7,
        }],
        partials: Arc::new(HashMap::from([(0, leaf)])),
        nested: Default::default(),
        covered: if hole {
            ranges(&[(0, 4), (6, 9)])
        } else {
            ranges(&[(0, 9)])
        },
        envelope_covered: ranges(&[(0, 1), (8, 9)]),
        held: Arc::default(),
        len: 9,
        ciphers: Arc::default(),
    };
    VirtualVolume {
        backing_leases: Default::default(),
        volume_index: 0,
        envelope: outer,
        extents: vec![MemberExtent {
            member_id: 0,
            physical_offset: 2,
            logical_offset: 0,
            len: 9,
        }],
        partials: Arc::default(),
        nested: HashMap::from([(0, child)]).into(),
        covered: ranges(&[(0, 2), (11, 13)]),
        envelope_covered: ranges(&[(0, 2), (11, 13)]),
        held: Arc::default(),
        len: 13,
        ciphers: Arc::default(),
    }
}

#[test]
fn nested_reader_reconstructs_original_bytes_without_intermediate_file() {
    let dir = tempfile::tempdir().unwrap();
    let image = tree(dir.path(), false);
    assert_eq!(image.backing_paths().len(), 3);
    assert_eq!(image.bounded_reader_levels(), 2);
    assert_eq!(image.readable_ranges(), vec![(0, 13)]);
    let mut reader = VirtualVolumeReader::<true>::new(image, Arc::default());
    let mut bytes = [0; 13];
    reader.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ABhPAYLOADtYZ");
}

#[test]
fn nested_reader_preserves_holes_even_when_sparse_backing_has_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let image = tree(dir.path(), true);
    assert_eq!(image.readable_ranges(), vec![(0, 6), (8, 13)]);
    let mut reader = VirtualVolumeReader::<true>::new(image, Arc::default());
    reader.seek(SeekFrom::Start(6)).unwrap();
    assert!(is_hole(&reader.read(&mut [0; 2]).unwrap_err()));
    reader.seek(SeekFrom::Start(8)).unwrap();
    let mut tail = [0; 5];
    reader.read_exact(&mut tail).unwrap();
    assert_eq!(&tail, b"ADtYZ");
}

#[test]
fn child_holds_remain_readable_and_charged_without_becoming_placed() {
    let dir = tempfile::tempdir().unwrap();
    let mut image = tree(dir.path(), true);
    image.nested.make_mut().unwrap().get_mut(&0).unwrap().held =
        Arc::new(vec![HeldRun::memory(4, Bytes::from_static(b"LO"), 0, 2)]);
    assert_eq!(image.retained_payloads().map(Bytes::len).sum::<usize>(), 2);
    assert_eq!(image.covered.ranges(), &[(0, 2), (11, 13)]);
    assert_eq!(image.readable_ranges(), vec![(0, 13)]);
    let mut reader = VirtualVolumeReader::<true>::new(image, Arc::default());
    let mut bytes = [0; 13];
    reader.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ABhPAYLOADtYZ");
}

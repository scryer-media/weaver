use super::*;
use crate::pipeline::direct_store::nested::seek::Seek;
use crate::pipeline::direct_store::set::DirectSet;

fn offset_of(haystack: &[u8], needle: &[u8]) -> u64 {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
        .unwrap() as u64
}

// Where a 7z container's map lies in it, as its start header states.
fn map_of(container: &[u8]) -> std::ops::Range<u64> {
    let field = |at: usize| u64::from_le_bytes(container[at..at + 8].try_into().unwrap());
    let start = 32 + field(12);
    start..start + field(20)
}

// Bytes no window of which repeats, so a part of them is found at one place.
fn unrepeating(len: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_u32;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect()
}

fn root_router(dir: &tempfile::TempDir, volumes: &[Vec<u8>]) -> DirectSetRouter {
    let count = volumes.len() as u32;
    let mut root = DirectSetRouter::new(DirectSetPlan {
        set_name: "outer".into(),
        format: SetFormat::Rar,
        volumes: (0..count).map(|volume| (volume, volume)).collect(),
        files: (0..count).map(|volume| (volume, volume)).collect(),
        identity: None,
        unposted: Default::default(),
        working_dir: dir.path().join("working"),
        destination_dir: dir.path().join("staging"),
    });
    for (volume, bytes) in volumes.iter().enumerate() {
        root.note_declared_volume_size(volume as u32, bytes.len() as u64)
            .unwrap();
    }
    root
}

fn deliver(
    root: &mut DirectSetRouter,
    graph: &mut NestedGraph,
    volume: u32,
    bytes: &[u8],
    range: std::ops::Range<usize>,
) {
    for (index, chunk) in bytes[range.clone()].chunks(769).enumerate() {
        let spans = root
            .route(
                volume,
                (range.start + index * 769) as u64,
                &[bytes::Bytes::copy_from_slice(chunk)],
            )
            .unwrap();
        graph.forward(&*root, ROOT_NODE, spans).unwrap();
    }
}

// A RAR store holding a RAR store holding a 7z store, with only the front of
// the outer volume delivered: enough for every layer's headers and the 7z
// start header, and nothing of the 7z map at the far end.
struct Layers {
    _dir: tempfile::TempDir,
    root: DirectSetRouter,
    graph: NestedGraph,
    outer: Vec<u8>,
    // Where the 7z container lies in the outer volume.
    inner: std::ops::Range<u64>,
    // Where its map lies in the outer volume.
    map: std::ops::Range<u64>,
}

fn layers() -> Layers {
    let dir = tempfile::tempdir().unwrap();
    let payload: Vec<u8> = (0..32_769).map(|i| (i % 251) as u8).collect();
    let inner = stored(SetFormat::SevenZip, "leaf.bin", &payload);
    let middle = stored(SetFormat::Rar, "inner.7z", &inner);
    let outer = stored(SetFormat::Rar, "middle.rar", &middle);
    let start = offset_of(&outer, &middle) + offset_of(&middle, &inner);
    let mut root = DirectSetRouter::new(DirectSetPlan {
        set_name: "outer".into(),
        format: SetFormat::Rar,
        volumes: BTreeMap::from([(0, 0)]),
        files: HashMap::from([(0, 0)]),
        identity: None,
        unposted: Default::default(),
        working_dir: dir.path().join("working"),
        destination_dir: dir.path().join("staging"),
    });
    root.note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut graph = NestedGraph::default();
    let front = (start + 64) as usize;
    for (index, chunk) in outer[..front].chunks(769).enumerate() {
        let spans = root
            .route(
                0,
                (index * 769) as u64,
                &[bytes::Bytes::copy_from_slice(chunk)],
            )
            .unwrap();
        graph.forward(&root, ROOT_NODE, spans).unwrap();
    }
    assert_eq!(graph.nodes.len(), 2);
    let map = map_of(&inner);
    Layers {
        _dir: dir,
        root,
        graph,
        outer,
        inner: start..start + inner.len() as u64,
        map: start + map.start..start + map.end,
    }
}

impl Layers {
    fn deliver(&mut self, range: std::ops::Range<usize>) {
        let spans = self
            .root
            .route(
                0,
                range.start as u64,
                &[bytes::Bytes::copy_from_slice(&self.outer[range])],
            )
            .unwrap();
        self.graph.forward(&self.root, ROOT_NODE, spans).unwrap();
    }
}

#[test]
fn a_seek_lifts_through_two_stored_layers_to_the_root_bytes() {
    let Layers {
        root, graph, inner, ..
    } = layers();
    assert_eq!(
        graph
            .lift(
                &root,
                2,
                Seek {
                    volume: 0,
                    range: 0..u64::MAX,
                },
            )
            .unwrap(),
        vec![Seek {
            volume: 0,
            range: inner.clone(),
        }]
    );
    assert_eq!(
        graph.lift(&root, 2, Seek::byte(0, 100)).unwrap(),
        vec![Seek::byte(0, inner.start + 100)]
    );
}

#[test]
fn a_stored_container_asks_for_its_whole_map_through_stored_parents_and_stays_routed() {
    let mut layers = layers();
    let map = layers.map.clone();
    // The middle RAR set reads its headers in order and asks for nothing; the
    // 7z container's start header placed its map, and it asks for all of it.
    assert_eq!(
        layers.graph.seeks(&layers.root),
        vec![Seek {
            volume: 0,
            range: map.clone(),
        }]
    );
    // What lands leaves the ask.
    layers.deliver(map.start as usize..map.start as usize + 1);
    assert_eq!(
        layers.graph.seeks(&layers.root),
        vec![Seek {
            volume: 0,
            range: map.start + 1..map.end,
        }]
    );
    for node in &layers.graph.nodes {
        assert_eq!(node.mode, NodeMode::Direct);
    }
}

#[test]
fn a_stored_container_under_an_engine_parent_is_refused_into_an_engine() {
    let mut layers = layers();
    // An engine reads the middle set in order, so nothing can be asked of it
    // ahead of that order.
    layers.graph.node_mut(1).mode = NodeMode::Engine;
    // Asking is not refusing: only the refusal turns the child.
    assert!(layers.graph.seeks(&layers.root).is_empty());
    assert_eq!(layers.graph.node(2).mode, NodeMode::Direct);
    layers.graph.refuse_unseekable(&layers.root);
    assert_eq!(layers.graph.node(2).mode, NodeMode::Engine);
    assert_eq!(
        layers.graph.node(2).router.demotion(),
        Some(DemotionReason::UnseekableParent)
    );
}

#[test]
fn an_engine_waiting_on_the_next_byte_in_order_asks_for_nothing() {
    let mut layers = layers();
    let start = layers.inner.start;
    layers.graph.node_mut(2).mode = NodeMode::Engine;
    // The fixture delivered the container's first 64 bytes.
    layers.graph.note_engine_want(2, Some((0, 64)));
    assert!(layers.graph.seeks(&layers.root).is_empty());
    layers.graph.note_engine_want(2, Some((0, 1_000)));
    assert_eq!(
        layers.graph.seeks(&layers.root),
        vec![Seek::byte(0, start + 1_000)]
    );
}

#[test]
fn every_ask_names_what_is_still_wanted() {
    let mut layers = layers();
    let map = layers.map.clone();
    let whole = Seek {
        volume: 0,
        range: map.clone(),
    };
    assert_eq!(layers.graph.seeks(&layers.root), vec![whole.clone()]);
    assert_eq!(layers.graph.seeks(&layers.root), vec![whole]);
    layers.deliver(map.start as usize..map.start as usize + 1);
    assert_eq!(
        layers.graph.seeks(&layers.root),
        vec![Seek {
            volume: 0,
            range: map.start + 1..map.end,
        }]
    );
    assert_eq!(layers.graph.node(2).mode, NodeMode::Direct);
}

// A container stored across three RAR volumes, with only the first of them
// delivered: the map is in the third, whose headers have not arrived, so
// nothing places it yet. The parent asks for that volume's front instead.
#[test]
fn a_rar_member_tail_in_an_unread_volume_lifts_to_that_volume_front() {
    let dir = tempfile::tempdir().unwrap();
    let inner = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(32_769));
    let volumes: Vec<Vec<u8>> = single_member_store_set("inner.7z", &inner, 3)
        .into_iter()
        .map(|(_, bytes)| bytes)
        .collect();
    let mut root = root_router(&dir, &volumes);
    let mut graph = NestedGraph::default();
    deliver(&mut root, &mut graph, 0, &volumes[0], 0..volumes[0].len());
    assert_eq!(graph.nodes.len(), 1);
    let end = inner.len() as u64;
    assert_eq!(
        graph.lift(&root, 1, Seek::byte(0, end - 1)).unwrap(),
        vec![Seek::byte(2, 0)]
    );
    assert_eq!(graph.seeks(&root), vec![Seek::byte(2, 0)]);
    // Once that front is read the layout places the map, and the ask is the
    // map itself.
    let map = map_of(&inner);
    let carried = offset_of(&volumes[2], &inner[map.start as usize..]);
    deliver(&mut root, &mut graph, 2, &volumes[2], 0..carried as usize);
    let asked = graph.seeks(&root);
    assert_eq!(asked.len(), 1);
    assert_eq!(asked[0].volume, 2);
    assert_eq!(asked[0].range.start, carried);
    assert_eq!(
        asked[0].range.end - asked[0].range.start,
        map.end - map.start
    );
    assert_eq!(graph.node(1).mode, NodeMode::Direct);
}

// A 7z split into three parts, each a member of one RAR volume. The geometry
// puts the map in the third part; until the parent's headers bind that part,
// nothing could be asked of it, and nothing is.
#[test]
fn a_split_container_wants_nothing_until_its_tail_volume_binds() {
    let dir = tempfile::tempdir().unwrap();
    let container = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(24_000));
    let part = container.len().div_ceil(3);
    let parts: Vec<Vec<u8>> = container.chunks(part).map(<[u8]>::to_vec).collect();
    assert_eq!(parts.len(), 3);
    let map = map_of(&container);
    assert!(map.start >= 2 * part as u64, "the map is in the last part");
    let members = [
        ("inner.7z.001", parts[0].clone()),
        ("inner.7z.002", parts[1].clone()),
        ("inner.7z.003", parts[2].clone()),
    ];
    let outer = multi_member_store_set(&members, 1).remove(0).1;
    let mut root = root_router(&dir, std::slice::from_ref(&outer));
    let mut graph = NestedGraph::default();
    let third = offset_of(&outer, &parts[2]) as usize;
    // Everything up to the third part's header.
    let before_third = offset_of(&outer, &parts[1]) as usize + parts[1].len();
    deliver(&mut root, &mut graph, 0, &outer, 0..before_third);
    assert_eq!(graph.nodes.len(), 1);
    assert!(!graph.node(1).roster_closed);
    assert!(graph.node(1).inputs.contains_key(&1));
    assert!(!graph.node(1).inputs.contains_key(&2));
    assert!(graph.seeks(&root).is_empty());
    // The header binds the third part; none of its bytes are here yet.
    deliver(&mut root, &mut graph, 0, &outer, before_third..third);
    assert!(graph.node(1).inputs.contains_key(&2));
    let base = third as u64 - 2 * part as u64;
    assert_eq!(
        graph.seeks(&root),
        vec![Seek {
            volume: 0,
            range: base + map.start..base + map.end,
        }]
    );
    assert_eq!(graph.node(1).mode, NodeMode::Direct);
}

// The decoded output of an engine reads in order only: a container inside it
// that asks for its map ahead of that order is refused into an engine, and
// says why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_container_in_decoded_output_is_refused_with_its_reason() {
    use crate::pipeline::direct_store::nested::catalog::DecodedNamespace;
    use crate::pipeline::direct_store::router::{RoutedSpan, RouterResources};
    use crate::pipeline::extraction::ExtractionLimits;
    use crate::pipeline::extraction::catalog::decoded::DecodedCatalog;
    use crate::pipeline::extraction::output::ArchiveOutput;
    use crate::pipeline::extraction::output::channel::{ChannelOutput, Event, StreamOwner};
    use crate::pipeline::extraction::safety::JobExtractionBudget;

    let dir = tempfile::tempdir().unwrap();
    let inner = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(4_096));
    let budget = JobExtractionBudget::new(
        Arc::new(ExtractionLimits {
            max_job_bytes: 1 << 20,
            max_member_bytes: 1 << 20,
            max_entries: 32,
            max_ratio: 100,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 64 * 1024 * 1024,
        }),
        dir.path().to_path_buf(),
        1 << 20,
        0,
        0,
        crate::operations::metrics::PipelineMetrics::new(),
    )
    .unwrap();
    let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
    let (output, generation) = ChannelOutput::new(
        StreamOwner {
            root: 7,
            node: 1,
            generation: 1,
        },
        budget.try_reserve_memory(64 * 1024).unwrap(),
        sender,
        tokio::runtime::Handle::current(),
        Arc::clone(&budget),
    )
    .unwrap();
    let mut catalog = DecodedCatalog::new(generation, budget.try_reserve_memory(4096).unwrap());
    // The decoder declares the container and writes its front, then holds
    // the member open: a member dropped unfinished revokes the generation.
    let (release, held) = std::sync::mpsc::channel::<()>();
    let front = inner[..64].to_vec();
    let length = inner.len() as u64;
    let worker = tokio::task::spawn_blocking(move || {
        let mut member = output
            .file(0, std::path::Path::new("inner.7z"), Some(length))
            .unwrap();
        member.write_all(&front).unwrap();
        held.recv().unwrap();
        drop(member);
    });
    let plan = DirectSetPlan {
        set_name: "decoded".into(),
        format: SetFormat::SevenZip,
        volumes: BTreeMap::from([(0, 0)]),
        files: HashMap::from([(0, 0)]),
        identity: None,
        unposted: Default::default(),
        working_dir: dir.path().join("working"),
        destination_dir: dir.path().join("staging"),
    };
    let resources = RouterResources {
        accountant: Arc::default(),
        holds_budget: 1 << 20,
        scratch_ceiling: 1 << 20,
        sparse: Default::default(),
        migration_ceiling_bytes: 1 << 20,
    };
    let mut destinations = BTreeMap::new();
    let mut graph = NestedGraph::default();
    for _ in 0..2 {
        let frame = receiver.recv().await.unwrap();
        let member_id = catalog.accept(&frame).unwrap();
        destinations.insert(member_id, dir.path().join("inner.7z.partial"));
        let spans = match frame.event {
            Event::Bytes { offset, bytes } => vec![RoutedSpan {
                owner: Default::default(),
                destination: DirectDestination::Member { member_id },
                destination_offset: offset,
                volume_index: 0,
                source_offset: offset,
                bytes: vec![bytes],
            }],
            _ => Vec::new(),
        };
        let namespace = DecodedNamespace {
            catalog: &catalog,
            plan: &plan,
            resources: &resources,
            destinations: &destinations,
        };
        graph.forward(&namespace, ROOT_NODE, spans).unwrap();
    }
    let namespace = DecodedNamespace {
        catalog: &catalog,
        plan: &plan,
        resources: &resources,
        destinations: &destinations,
    };
    assert_eq!(graph.nodes.len(), 1);
    assert_eq!(graph.node(1).mode, NodeMode::Direct);
    let map = map_of(&inner);
    assert_eq!(
        graph.lift(
            &namespace,
            1,
            Seek {
                volume: 0,
                range: map.clone(),
            },
        ),
        Err(DemotionReason::UnseekableParent)
    );
    graph.refuse_unseekable(&namespace);
    assert_eq!(graph.node(1).mode, NodeMode::Engine);
    assert_eq!(
        graph.node(1).router.demotion(),
        Some(DemotionReason::UnseekableParent)
    );
    release.send(()).unwrap();
    worker.await.unwrap();
}

// A 7z split into two parts whose cut falls inside its map, both parts stored
// in one RAR volume. The map is asked for on both sides of the cut, each
// piece where its part lies in the volume.
#[test]
fn a_map_cut_between_two_parts_is_asked_for_in_both() {
    let dir = tempfile::tempdir().unwrap();
    let container = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(24_000));
    let map = map_of(&container);
    let cut = (map.start + (map.end - map.start) / 2) as usize;
    let parts = [container[..cut].to_vec(), container[cut..].to_vec()];
    let members = [
        ("inner.7z.001", parts[0].clone()),
        ("inner.7z.002", parts[1].clone()),
    ];
    let outer = multi_member_store_set(&members, 1).remove(0).1;
    let mut root = root_router(&dir, std::slice::from_ref(&outer));
    let mut graph = NestedGraph::default();
    let first = offset_of(&outer, &parts[0]);
    let second = offset_of(&outer, &parts[1]);
    // The first part stops one byte short of the map, so the map's first
    // byte is not the next one the in-order flow brings anyway.
    let short = (first + map.start - 1) as usize;
    deliver(&mut root, &mut graph, 0, &outer, 0..short);
    // The second part's header binds it; none of its bytes are here yet.
    let first_end = first as usize + cut;
    deliver(&mut root, &mut graph, 0, &outer, first_end..second as usize);
    assert_eq!(graph.nodes.len(), 1);
    assert!(graph.node(1).inputs.contains_key(&1));
    assert_eq!(
        graph.seeks(&root),
        vec![
            Seek {
                volume: 0,
                range: first + map.start..first + cut as u64,
            },
            Seek {
                volume: 0,
                range: second..second + (map.end - cut as u64),
            },
        ]
    );
    assert_eq!(graph.node(1).mode, NodeMode::Direct);
}

// A 7z stored across three volumes of a RAR set, itself stored in one outer
// RAR volume. With the middle set's third volume bound but unread, nothing
// places the map yet: the middle asks for that volume's front, and the ask
// is lifted on to where that volume lies in the outer one.
#[test]
fn a_header_ask_lifts_through_two_stored_layers() {
    let dir = tempfile::tempdir().unwrap();
    let inner = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(32_769));
    let middle = single_member_store_set("inner.7z", &inner, 3);
    let members: Vec<(&str, Vec<u8>)> = middle
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = multi_member_store_set(&members, 1).remove(0).1;
    let mut root = root_router(&dir, std::slice::from_ref(&outer));
    let mut graph = NestedGraph::default();
    let third = offset_of(&outer, &middle[2].1);
    deliver(&mut root, &mut graph, 0, &outer, 0..third as usize);
    assert_eq!(graph.nodes.len(), 2);
    assert!(graph.node(1).inputs.contains_key(&2));
    assert_eq!(graph.seeks(&root), vec![Seek::byte(0, third)]);
    for node in &graph.nodes {
        assert_eq!(node.mode, NodeMode::Direct);
    }
}

// Through the real admission: a routed child's map is admitted one article
// at a time, the article that holds its first byte, and not while that
// article is already on the wire.
#[tokio::test]
async fn a_nested_want_admits_the_article_holding_its_first_byte_while_none_is_in_flight() {
    const ARTICLES: usize = 16;
    let directory = tempfile::tempdir().unwrap();
    let job = JobId(41985);
    let inner = stored(SetFormat::SevenZip, "leaf.bin", &unrepeating(32_769));
    let volumes = single_member_store_set("inner.7z", &inner, 1);
    let outer = volumes[0].1.clone();
    let spec = direct_store_job_spec_with_articles("nested pulls", &volumes, ARTICLES);
    let (mut pipeline, _, _) = new_direct_pipeline(&directory).await;
    insert_active_job(&mut pipeline, job, spec.clone()).await;
    let working = pipeline.jobs[&job].working_dir.clone();
    let destination = pipeline.deterministic_extraction_staging_dir(job);
    let plan = DirectSetPlan::discover(&spec, &working, &destination)
        .0
        .remove(0);
    let file_id = NzbFileId {
        job_id: job,
        file_index: plan.volumes[&0],
    };
    let mut root = DirectSetRouter::new(plan.clone());
    root.note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut graph = NestedGraph::default();
    // The first article carries the outer header and the 7z start header,
    // which places the map at the far end.
    let (_, first_end) = article_extent(outer.len(), 0, ARTICLES);
    let spans = root
        .route(0, 0, &[bytes::Bytes::copy_from_slice(&outer[..first_end])])
        .unwrap();
    graph.forward(&root, ROOT_NODE, spans).unwrap();
    assert_eq!(graph.nodes.len(), 1);
    pipeline
        .jobs
        .get_mut(&job)
        .unwrap()
        .assembly
        .file_mut(file_id)
        .unwrap()
        .record_placement(0, 0, first_end as u32);
    let map_start = offset_of(&outer, &inner) as usize + map_of(&inner).start as usize;
    let holder = (0..ARTICLES as u32)
        .find(|&segment| {
            let (start, end) = article_extent(outer.len(), segment, ARTICLES);
            (start..end).contains(&map_start)
        })
        .unwrap();
    let mut set = DirectSet::new(job, plan.clone());
    set.router = root;
    set.install_restored_nested(graph);
    set.enable_nested_checkpoint_authority();
    pipeline.direct_store.install_restored(job, vec![set]);
    pipeline.pull_nested_seeks(job, &plan.set_name);
    let holding = SegmentId {
        file_id,
        segment_number: holder,
    };
    assert_eq!(pipeline.direct_store_pulled_for_test(job), vec![holding]);
    pipeline.rate_limit_reservations.insert(holding, 1);
    assert!(pipeline.direct_store_pulled_for_test(job).is_empty());
}

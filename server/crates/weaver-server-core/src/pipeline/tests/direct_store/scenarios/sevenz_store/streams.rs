// A single-stream member inside a stored root is decoded as its bytes land,
// and whatever archive its one output holds chains to a further child.

use super::*;
use crate::pipeline::direct_store::nested::{NestedGraph, NodeMode};
use crate::pipeline::direct_store::plan::{SetFormat, StreamKind};
use std::io::Write;

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut writer = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    writer.write_all(bytes).unwrap();
    writer.finish().unwrap()
}

fn gz(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn bz2(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn zst(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 3).unwrap()
}

fn br(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
    encoder.write_all(bytes).unwrap();
    encoder.into_inner()
}

fn deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn leaf() -> Vec<u8> {
    (0..196_613u32)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect()
}

// Every node a graph holds, as its set name, format and mode.
fn nodes(graph: &NestedGraph) -> Vec<(String, SetFormat, NodeMode)> {
    graph
        .nodes
        .iter()
        .map(|node| {
            let plan = node.router.plan();
            (plan.set_name.clone(), plan.format, node.mode)
        })
        .collect()
}

// What one job made of a stored root holding `members`: the root graph's
// nodes once every article has been routed, every decoded namespace's
// nodes, and the job's published output.
struct StreamedRoot {
    born: Vec<(String, SetFormat, NodeMode)>,
    decoded: Vec<(String, SetFormat, NodeMode)>,
    output: PathBuf,
    _temp: tempfile::TempDir,
}

// What one job ended as: its status and every engine failure it reported on
// the way, as class and message.
struct StreamedJob {
    root: StreamedRoot,
    status: Option<JobStatus>,
    failures: Vec<(ExtractionErrorClass, String)>,
}

async fn stream_through_store_root(job: JobId, members: &[(&str, Vec<u8>)]) -> StreamedRoot {
    let volumes = multi_member_store_set(members, 1);
    let spec = direct_store_job_spec("Streamed member", &volumes);
    let run = run_job(job, &volumes, spec, Delivery::Articles).await;
    assert!(
        matches!(run.status, Some(JobStatus::Complete)),
        "{:?} {:?}",
        run.status,
        run.failures
    );
    run.root
}

// How a job's volumes arrive: as the two articles a direct root routes, or
// as whole files a root the direct store does not take is extracted from.
enum Delivery {
    Articles,
    Files,
}

// Delivers the job's volumes and drives it to its end. The graph a direct
// root built is read once every article has been routed.
async fn run_job(
    job: JobId,
    volumes: &[(String, Vec<u8>)],
    spec: JobSpec,
    delivery: Delivery,
) -> StreamedJob {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let output = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(&spec.name));
    insert_active_job(&mut pipeline, job, spec).await;
    match delivery {
        // The graph is promoted at the start of the article after the one
        // whose header named the member.
        Delivery::Articles => {
            submit_volume_article(&mut pipeline, job, volumes, 0, 0).await;
            submit_volume_article(&mut pipeline, job, volumes, 0, 1).await;
        }
        Delivery::Files => {
            for (index, (name, bytes)) in volumes.iter().enumerate() {
                write_and_complete_file(&mut pipeline, job, index as u32, name, bytes).await;
            }
        }
    }
    let born = pipeline
        .direct_store
        .set(job, 0)
        .and_then(|set| set.nested_graph())
        .map(nodes)
        .unwrap_or_default();
    pipeline.check_job_completion(job).await;
    drain_rar_refreshes(&mut pipeline).await;
    let mut decoded = Vec::new();
    let mut failures = Vec::new();
    drive_extractions_observing(&mut pipeline, job, |done| {
        let ExtractionDone::Scoped {
            decoded: state,
            result,
            ..
        } = done
        else {
            return;
        };
        if let Some(state) = state {
            decoded.extend(nodes(&state.graph));
        }
        match result {
            Ok(outcome) => failures.extend(
                outcome
                    .failed
                    .iter()
                    .map(|(_, error)| (error.class(), error.message().to_owned())),
            ),
            Err(error) => failures.push((error.class(), error.message().to_owned())),
        }
    })
    .await;
    StreamedJob {
        root: StreamedRoot {
            born,
            decoded,
            output,
            _temp: temp,
        },
        status: job_status_for_assert(&pipeline, job),
        failures,
    }
}

async fn assert_stream_decodes_live(
    job: JobId,
    suffix: &str,
    kind: StreamKind,
    compressed: Vec<u8>,
) {
    let leaf = leaf();
    let name = format!("leaf.bin{suffix}");
    let streamed = stream_through_store_root(job, &[(&name, compressed)]).await;
    assert_eq!(
        streamed.born,
        [(name.clone(), SetFormat::Stream(kind), NodeMode::Engine)]
    );
    assert_eq!(
        std::fs::read(streamed.output.join("leaf.bin")).unwrap(),
        leaf
    );
    assert!(!streamed.output.join(&name).exists());
}

#[tokio::test]
async fn xz_member_of_a_store_root_is_an_engine_from_birth_and_decodes_live() {
    assert_stream_decodes_live(JobId(862001), ".xz", StreamKind::Xz, xz(&leaf())).await;
}

#[tokio::test]
async fn gz_member_of_a_store_root_decodes_live() {
    assert_stream_decodes_live(JobId(862002), ".gz", StreamKind::Gz, gz(&leaf())).await;
}

#[tokio::test]
async fn zstd_member_of_a_store_root_decodes_live() {
    assert_stream_decodes_live(JobId(862003), ".zst", StreamKind::Zstd, zst(&leaf())).await;
}

#[tokio::test]
async fn bzip2_member_of_a_store_root_decodes_live() {
    assert_stream_decodes_live(JobId(862004), ".bz2", StreamKind::Bzip2, bz2(&leaf())).await;
}

#[tokio::test]
async fn brotli_member_of_a_store_root_decodes_live() {
    assert_stream_decodes_live(JobId(862005), ".br", StreamKind::Brotli, br(&leaf())).await;
}

#[tokio::test]
async fn deflate_member_of_a_store_root_decodes_live() {
    assert_stream_decodes_live(
        JobId(862006),
        ".deflate",
        StreamKind::Deflate,
        deflate(&leaf()),
    )
    .await;
}

// The xz output is a RAR store, which needs no tail to route: the chase that
// feeds it from the decoder keeps it direct. The root also stores a plain
// sibling, so a root that installs output of its own beside its engines stays
// covered too.
#[tokio::test]
async fn xz_wrapping_a_rar_store_chains_to_a_direct_rar_child() {
    const NOTES: &[u8] = b"the stored sibling\n";
    let leaf = leaf();
    let inner = single_member_store_set("leaf.bin", &leaf, 1).remove(0).1;
    let streamed = stream_through_store_root(
        JobId(862007),
        &[("notes.txt", NOTES.to_vec()), ("inner.rar.xz", xz(&inner))],
    )
    .await;
    assert_eq!(
        streamed.born,
        [(
            "inner.rar.xz".to_owned(),
            SetFormat::Stream(StreamKind::Xz),
            NodeMode::Engine
        )]
    );
    assert!(
        streamed
            .decoded
            .contains(&("inner".to_owned(), SetFormat::Rar, NodeMode::Direct)),
        "{:?}",
        streamed.decoded
    );
    assert_eq!(
        std::fs::read(streamed.output.join("leaf.bin")).unwrap(),
        leaf
    );
    assert_eq!(
        std::fs::read(streamed.output.join("notes.txt")).unwrap(),
        NOTES
    );
    assert!(!streamed.output.join("inner.rar").exists());
}

// A 7z store needs its length and its tail before it can route, and a decoded
// stream states neither until it ends.
#[tokio::test]
async fn xz_wrapping_a_7z_store_chains_to_a_7z_child() {
    let leaf = leaf();
    let inner = build_7z(
        &[Entry::file("leaf.bin", leaf.clone())],
        EncoderMethod::COPY,
        None,
    );
    let streamed = stream_through_store_root(JobId(862008), &[("inner.7z.xz", xz(&inner))]).await;
    // A 7z under a stream has no length to seek by, so it is born an engine.
    assert!(
        streamed
            .decoded
            .contains(&("inner.7z".to_owned(), SetFormat::SevenZip, NodeMode::Engine)),
        "{:?}",
        streamed.decoded
    );
    assert_eq!(
        std::fs::read(streamed.output.join("leaf.bin")).unwrap(),
        leaf
    );
    assert!(!streamed.output.join("inner.7z").exists());
}

// A RAR store inside the root stays direct, and the xz it holds is an engine
// two layers down: the root's articles reach it through the stored layer's
// member, and its output lands as the root's own would.
#[tokio::test]
async fn a_rar_store_inside_the_root_hands_its_xz_member_to_an_engine() {
    let leaf = leaf();
    let inner = single_member_store_set("leaf.bin.xz", &xz(&leaf), 1)
        .remove(0)
        .1;
    let streamed = stream_through_store_root(JobId(862031), &[("inner.rar", inner)]).await;
    assert_eq!(
        streamed.born,
        [
            ("inner".to_owned(), SetFormat::Rar, NodeMode::Direct),
            (
                "leaf.bin.xz".to_owned(),
                SetFormat::Stream(StreamKind::Xz),
                NodeMode::Engine
            ),
        ]
    );
    assert_eq!(
        std::fs::read(streamed.output.join("leaf.bin")).unwrap(),
        leaf
    );
    assert!(!streamed.output.join("inner.rar").exists());
    assert!(!streamed.output.join("leaf.bin.xz").exists());
}

// A 7z store inside the root routes until its headers name a compressed
// member, which nothing stored can serve. The child is refused into an
// engine, the bytes it routed are copied into the root's member, and the
// engine reads them from there.
#[tokio::test]
async fn a_7z_store_inside_the_root_with_a_compressed_member_is_refused_into_an_engine() {
    let leaf = leaf();
    let inner = build_7z(
        &[Entry::file("leaf.bin", leaf.clone())],
        EncoderMethod::LZMA2,
        None,
    );
    let streamed = stream_through_store_root(JobId(862032), &[("inner.7z", inner)]).await;
    assert_eq!(
        streamed.born,
        [("inner.7z".to_owned(), SetFormat::SevenZip, NodeMode::Engine)]
    );
    assert_eq!(
        std::fs::read(streamed.output.join("leaf.bin")).unwrap(),
        leaf
    );
    assert!(!streamed.output.join("inner.7z").exists());
}

// A stream keeps its directory, so two same-named streams in different
// directories are two outputs rather than one overwriting the other.
#[tokio::test]
async fn same_named_streams_in_two_directories_both_land() {
    let first = leaf();
    let second: Vec<u8> = first.iter().map(|byte| !byte).collect();
    let streamed = stream_through_store_root(
        JobId(862010),
        &[
            ("CD1/subs.srt.gz", gz(&first)),
            ("CD2/subs.srt.gz", gz(&second)),
        ],
    )
    .await;
    assert_eq!(
        std::fs::read(streamed.output.join("CD1/subs.srt")).unwrap(),
        first
    );
    assert_eq!(
        std::fs::read(streamed.output.join("CD2/subs.srt")).unwrap(),
        second
    );
    assert!(!streamed.output.join("subs.srt").exists());
}

// A stream that is a member of a compressed root is read from the root's
// decoder, and its leaf lands like any member's.
#[tokio::test]
async fn a_stream_inside_a_decoded_namespace_decodes_and_lands() {
    let leaf = leaf();
    let archive = build_7z(
        &[
            Entry::file("first.bin", leaf.clone()),
            Entry::file("inner.xz", xz(&leaf)),
        ],
        EncoderMethod::LZMA2,
        None,
    );
    let volumes = split_volumes(&archive, 1);
    let run = run_job(
        JobId(862011),
        &volumes,
        sevenz_job_spec(&volumes, 1),
        Delivery::Files,
    )
    .await;
    assert!(
        matches!(run.status, Some(JobStatus::Complete)),
        "{:?} {:?}",
        run.status,
        run.failures
    );
    assert!(
        run.root.decoded.contains(&(
            "inner.xz".to_owned(),
            SetFormat::Stream(StreamKind::Xz),
            NodeMode::Engine
        )),
        "{:?}",
        run.root.decoded
    );
    assert_eq!(std::fs::read(run.root.output.join("inner")).unwrap(), leaf);
    assert_eq!(
        std::fs::read(run.root.output.join("first.bin")).unwrap(),
        leaf
    );
    assert!(!run.root.output.join("inner.xz").exists());
}

// A stream cut short ends at the end of what was committed: the decoder
// reports the truncation and the job fails on it instead of waiting for
// bytes no article will bring.
#[tokio::test]
async fn a_truncated_xz_stream_fails_the_job_cleanly() {
    let mut compressed = xz(&leaf());
    compressed.truncate(compressed.len() / 2);
    let volumes = multi_member_store_set(&[("leaf.bin.xz", compressed)], 1);
    let run = run_job(
        JobId(862012),
        &volumes,
        direct_store_job_spec("Truncated stream", &volumes),
        Delivery::Articles,
    )
    .await;
    assert!(
        matches!(run.status, Some(JobStatus::Failed { .. })),
        "{:?} {:?}",
        run.status,
        run.failures
    );
    assert!(!run.failures.is_empty());
    assert!(!run.root.output.join("leaf.bin").exists());
}

// The codes a checkpoint stores a set's format as. A stream kind's code is
// its declaration order after the two archive formats, so reordering the
// kinds would reread every saved stream as another codec.
#[test]
fn set_format_codes_are_pinned() {
    assert_eq!(SetFormat::Rar.code(), 0);
    assert_eq!(SetFormat::SevenZip.code(), 1);
    let kinds = [
        StreamKind::Xz,
        StreamKind::Gz,
        StreamKind::Bzip2,
        StreamKind::Zstd,
        StreamKind::Brotli,
        StreamKind::Deflate,
    ];
    for (code, kind) in (2u8..).zip(kinds) {
        assert_eq!(SetFormat::Stream(kind).code(), code, "{kind:?}");
        assert_eq!(SetFormat::from_code(code), Some(SetFormat::Stream(kind)));
    }
    assert_eq!(SetFormat::from_code(8), None);
}

// Writes one routed span where its owner keeps it and records it, returning
// the receipt and file a restored set is seeded with.
fn place(
    root: &crate::pipeline::direct_store::router::DirectSetRouter,
    graph: &mut NestedGraph,
    write: &crate::pipeline::direct_store::nested::NodeSpan,
    covered: &mut crate::pipeline::direct_store::ByteRanges,
) -> (
    crate::pipeline::direct_store::router::PlacementReceipt,
    PathBuf,
) {
    use crate::pipeline::direct_store::router::DirectDestination;
    use std::io::{Seek, SeekFrom};
    let router = graph.router(root, write.owner);
    let path = match write.span.destination {
        DirectDestination::Envelope { volume_index } => router.plan().envelope_path(volume_index),
        DirectDestination::Member { member_id } => {
            let (_, _, partial) = router
                .member_partials()
                .into_iter()
                .find(|(id, _, _)| *id == member_id)
                .unwrap();
            router.plan().destination_path(partial)
        }
    };
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(write.span.destination_offset))
        .unwrap();
    for piece in &write.span.bytes {
        file.write_all(piece).unwrap();
    }
    for source in graph.record_placement(root, write.placement()).unwrap() {
        covered.insert(source.offset, source.len);
    }
    (write.placement(), path)
}

// A stream node is an engine from birth, and a checkpoint brings it back as
// one: the restart reopens the decoder over the member, never a file.
#[tokio::test]
async fn a_stream_node_survives_a_checkpoint_as_an_engine() {
    use crate::pipeline::direct_store::manifest::graph::JobGraph;
    use crate::pipeline::direct_store::nested::ROOT_NODE;
    use crate::pipeline::direct_store::plan::DirectSetPlan;
    use crate::pipeline::direct_store::restart::ArchiveRestoreRows;
    use crate::pipeline::direct_store::router::DirectSetRouter;
    use crate::pipeline::direct_store::set::DirectSet;
    let directory = tempfile::tempdir().unwrap();
    let job_id = JobId(862009);
    let volumes = single_member_store_set("leaf.bin.xz", &xz(&leaf()), 1);
    let outer = &volumes[0].1;
    let spec = direct_store_job_spec("stream checkpoint", &volumes);
    let (mut pipeline, _, _) = new_direct_pipeline(&directory).await;
    let working = directory.path().join("working");
    let destination = pipeline.deterministic_extraction_staging_dir(job_id);
    let plan = DirectSetPlan::discover(&spec, &working, &destination)
        .0
        .remove(0);

    let mut root = DirectSetRouter::new(plan.clone());
    root.note_declared_volume_size(0, outer.len() as u64)
        .unwrap();
    let mut graph = NestedGraph::default();
    let mut placements = Vec::new();
    let mut covered = crate::pipeline::direct_store::ByteRanges::new();
    let writes = root
        .route(0, 0, &[bytes::Bytes::copy_from_slice(outer)])
        .unwrap();
    for write in graph.forward(&root, ROOT_NODE, writes).unwrap() {
        placements.push(place(&root, &mut graph, &write, &mut covered));
    }
    let tail = root.note_volume_complete(0, outer.len() as u64).unwrap();
    for write in graph.forward(&root, ROOT_NODE, tail).unwrap() {
        placements.push(place(&root, &mut graph, &write, &mut covered));
    }
    assert_eq!(
        nodes(&graph),
        [(
            "leaf.bin.xz".to_owned(),
            SetFormat::Stream(StreamKind::Xz),
            NodeMode::Engine
        )]
    );

    let mut set = DirectSet::new(job_id, plan);
    set.router = root;
    set.install_restored_nested(graph);
    for (receipt, path) in placements {
        set.seed_qualified_placement(
            receipt.owner,
            receipt.destination,
            path,
            vec![(
                receipt.destination_offset,
                receipt.destination_offset + receipt.len,
            )],
        );
    }
    set.seed_root_source_coverage(0, covered.ranges().to_vec());
    let mut saved = JobGraph::empty(1);
    crate::pipeline::direct_store::graph_checkpoint::capture_direct_root(&mut saved, &set).unwrap();
    let saved = JobGraph::decode(&saved.encode().unwrap()).unwrap();
    let mut restored = pipeline
        .restore_direct_store_coverage(
            job_id,
            &spec,
            &working,
            &std::collections::HashMap::new(),
            ArchiveRestoreRows::Graph(Box::new(saved)),
        )
        .await
        .unwrap();
    let set = restored.sets.remove(0);
    let graph = set.nested_graph().unwrap();
    assert_eq!(
        nodes(graph),
        [(
            "leaf.bin.xz".to_owned(),
            SetFormat::Stream(StreamKind::Xz),
            NodeMode::Engine
        )]
    );
    let live: Vec<_> = graph
        .live_child_descriptions(&set.router)
        .iter()
        .map(|child| child.node)
        .collect();
    assert_eq!(live, [1]);
}

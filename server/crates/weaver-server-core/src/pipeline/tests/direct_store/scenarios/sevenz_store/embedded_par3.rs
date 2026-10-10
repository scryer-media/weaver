// A one-volume stored 7z that carries its own PAR3 recovery after the end
// header: `[7z][PAR3 packets]`, the shape an inside-insertion writes.
//
// The container states its own length in its start header, so the bytes past
// it are the recovery tail. The set is routed direct like any one-volume 7z;
// the tail is envelope, and the recovery it carries repairs the members in
// place when an article never arrives.

use super::*;
use par3_rs::creation::CreationOptions;
use par3_rs::inside::{ContainerLimits, InsertionPlan};
use par3_rs::source::{MemorySourceAccess, SourceId};

const ARTICLES: usize = 8;

// `archive` with a PAR3 recovery tail inserted after its end header, the way
// a poster's inside-insertion writes it.
pub(in super::super) fn with_embedded_par3(
    archive: &[u8],
    block_size: u64,
    recovery_count: u64,
) -> Vec<u8> {
    with_embedded_par3_named(
        archive,
        "silver.horizon.7z",
        CreationOptions {
            block_size,
            recovery_count,
            ..CreationOptions::default()
        },
    )
}

pub(in super::super) fn with_embedded_par3_named(
    archive: &[u8],
    name: &str,
    options: CreationOptions,
) -> Vec<u8> {
    let scratch = tempfile::tempdir().unwrap();
    let source = SourceId(0);
    let mut access = MemorySourceAccess::default();
    access.insert(source, 1, Arc::from(archive));
    let path = scratch.path().join(name);
    InsertionPlan::build(
        Arc::new(access),
        source,
        name,
        options,
        &ContainerLimits::default(),
    )
    .expect("an insertion plan over the fixture archive")
    .execute(&path, scratch.path())
    .expect("the recovery tail is written");
    let inserted = std::fs::read(&path).unwrap();
    assert_eq!(
        &inserted[..archive.len()],
        archive,
        "the insertion must leave the container's own bytes alone"
    );
    assert!(inserted.len() > archive.len());
    inserted
}

struct Embedded {
    member: Vec<u8>,
    container_len: usize,
    volumes: Vec<(String, Vec<u8>)>,
}

fn embedded_fixture(seed: u8, member_len: usize) -> Embedded {
    let member = payload(seed, member_len);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    let inserted = with_embedded_par3(&archive, 512, 24);
    Embedded {
        member,
        container_len: archive.len(),
        volumes: split_volumes(&inserted, 1),
    }
}

// The article that holds byte `offset` of the one volume.
fn article_holding(volume_len: usize, offset: usize) -> u32 {
    (0..ARTICLES as u32)
        .find(|&article| {
            let (start, end) = article_extent(volume_len, article, ARTICLES);
            (start..end).contains(&offset)
        })
        .expect("the offset is inside the volume")
}

// Drives the one-volume set through `arrivals`, never delivering `lost`, to
// whatever the job reaches. Articles the pipeline asks for again are
// answered unless they are lost, the way a server that never had them would.
async fn run_embedded(
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    arrivals: &[u32],
    lost: &[u32],
) -> Par3RepairOutcome {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = sevenz_job_spec(volumes, ARTICLES);
    let job_name = spec.name.clone();
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let mut sets = Vec::new();
    let observe = |pipeline: &Pipeline, sets: &mut Vec<String>| {
        let current = format!("{:?}", pipeline.direct_store.sets_for(job_id));
        if current != "[]" && sets.last() != Some(&current) {
            sets.push(current);
        }
    };
    let mut volume_file_seen = false;
    for &article in arrivals {
        if lost.contains(&article) {
            continue;
        }
        submit_volume_article_of(&mut pipeline, job_id, volumes, 0, article, ARTICLES).await;
        observe(&pipeline, &mut sets);
        volume_file_seen |= working_dir.join(&volumes[0].0).exists();
    }

    let lease_the_queue = |pipeline: &mut Pipeline| -> Vec<u32> {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.recovery_queue = crate::DownloadQueue::new();
        state
            .download_queue
            .drain_all()
            .into_iter()
            .filter(|work| work.segment_id.file_id.file_index == 0)
            .map(|work| work.segment_id.segment_number)
            .collect()
    };
    lease_the_queue(&mut pipeline);
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        for article in lease_the_queue(&mut pipeline) {
            if lost.contains(&article) {
                continue;
            }
            submit_volume_article_of(&mut pipeline, job_id, volumes, 0, article, ARTICLES).await;
        }
        drain_rar_refreshes(&mut pipeline).await;
        // A demoted partial volume can leave a short write behind once the
        // queue drains; the actor's quiescent flush is what lands it.
        pipeline.flush_quiescent_write_backlog().await;
        pipeline.check_job_completion(job_id).await;
        settle_par3_work(&mut pipeline, job_id).await;
        observe(&pipeline, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        settle_inflight_moves(&mut pipeline).await;
        observe(&pipeline, &mut sets);
        if let Some(done) = next_owed_extraction(&mut pipeline, job_id).await {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        volume_file_seen |= working_dir.join(&volumes[0].0).exists();
        observe(&pipeline, &mut sets);
    }

    Par3RepairOutcome {
        status: job_status_for_assert(&pipeline, job_id),
        sets,
        volume_file_seen,
        repair_scratch_left: direct_scratch_left(&working_dir),
        materialized: pipeline.direct_store.repair_materialized_volumes,
        finalized: pipeline.direct_store.finalized_sets,
        output_root: complete_dir.join(crate::jobs::working_dir::sanitize_dirname(&job_name)),
        working_dir,
        _temp_dir: temp_dir,
    }
}

fn in_order() -> Vec<u32> {
    (0..ARTICLES as u32).collect()
}

// The set stayed direct, published only its member, and finished.
fn assert_stayed_direct(label: &str, outcome: &Par3RepairOutcome, fixture: &Embedded) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "{label}: the job must complete, got {:?}\nsets: {}",
        outcome.status,
        outcome.shapes()
    );
    assert!(
        !outcome.demoted(),
        "{label}: the set must stay direct\nsets: {}",
        outcome.shapes()
    );
    assert_eq!(
        outcome.member(MEMBER).as_deref(),
        Some(fixture.member.as_slice()),
        "{label}: the member must be published whole\nsets: {}",
        outcome.shapes()
    );
    assert_eq!(
        outcome.finalized,
        1,
        "{label}: the set must commit its own partials\nsets: {}",
        outcome.shapes()
    );
    assert!(
        !outcome.volume_file_seen,
        "{label}: the volume may never appear under its own name\nsets: {}",
        outcome.shapes()
    );
    let published: Vec<String> = std::fs::read_dir(&outcome.output_root)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                // The output directory's ownership marker is not a delivery.
                .filter(|name| name != ".weaver-output-dir")
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(
        published,
        vec![MEMBER.to_string()],
        "{label}: only the member is published"
    );
    assert_eq!(
        outcome.repair_scratch_left, 0,
        "{label}: repair scratch must be cleaned up"
    );
}

#[tokio::test]
async fn sevenz_store_embedded_par3_routes_direct_without_loss() {
    let fixture = embedded_fixture(41, 40_000);
    let outcome = run_embedded(JobId(9_700), &fixture.volumes, &in_order(), &[]).await;
    assert!(
        outcome.sets.iter().any(|shape| shape.contains("Finalized")),
        "the set must be routed and finalized direct\nsets: {}",
        outcome.shapes()
    );
    assert_stayed_direct("no loss", &outcome, &fixture);
}

fn tail_first() -> Vec<u32> {
    (0..ARTICLES as u32).rev().collect()
}

fn start_header_last() -> Vec<u32> {
    (1..ARTICLES as u32).chain([0]).collect()
}

#[tokio::test]
async fn sevenz_store_embedded_par3_repairs_a_lost_member_article() {
    let fixture = embedded_fixture(43, 40_000);
    let lost = article_holding(fixture.volumes[0].1.len(), 32 + 10_000);
    for (label, arrivals) in [
        ("in order", in_order()),
        ("tail first", tail_first()),
        ("start header last", start_header_last()),
    ] {
        let outcome = run_embedded(JobId(9_701), &fixture.volumes, &arrivals, &[lost]).await;
        assert_stayed_direct(label, &outcome, &fixture);
    }
}

// The recovery packets are the only thing lost. The container is whole and
// every member passed its own checksum, so the set finalizes direct; the
// tail it never needed is envelope and is not delivered.
#[tokio::test]
async fn sevenz_store_embedded_par3_finalizes_direct_without_its_tail() {
    let fixture = embedded_fixture(47, 40_000);
    let len = fixture.volumes[0].1.len();
    let lost = ARTICLES as u32 - 1;
    assert!(
        article_extent(len, lost, ARTICLES).0 > fixture.container_len,
        "the fixture's last article must hold recovery packets only"
    );
    for (label, arrivals) in [("in order", in_order()), ("tail first", tail_first())] {
        let outcome = run_embedded(JobId(9_702), &fixture.volumes, &arrivals, &[lost]).await;
        assert_stayed_direct(label, &outcome, &fixture);
    }
}

// The start header states where everything else is, the tail included. A
// set that never receives it has no destination for any byte, so it
// demotes for an unreadable map; the conventional path then finds the
// recovery set in the volume's own tail and repairs the archive in place.
#[tokio::test]
async fn sevenz_store_embedded_par3_without_its_start_header_repairs_conventionally() {
    let fixture = embedded_fixture(53, 40_000);
    let outcome = run_embedded(JobId(9_703), &fixture.volumes, &in_order(), &[0]).await;
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}\nsets: {}",
        outcome.status,
        outcome.shapes()
    );
    assert!(
        outcome
            .sets
            .iter()
            .any(|shape| shape.contains("Demoted(SevenZip(UnreadableMap))")),
        "a set without its start header has no map\nsets: {}",
        outcome.shapes()
    );
    assert_eq!(
        outcome.member(MEMBER).as_deref(),
        Some(fixture.member.as_slice())
    );
}

// The end header shares its article with the recovery set's leading
// packets. Losing it costs the map and the set's own description at once:
// the set demotes for an unreadable map, and with no start packet left
// the recovery set cannot describe what it protects, so the job fails
// rather than delivering anything.
#[tokio::test]
async fn sevenz_store_embedded_par3_fails_when_the_end_header_takes_the_recovery_metadata() {
    let fixture = embedded_fixture(59, 40_000);
    let len = fixture.volumes[0].1.len();
    let lost = article_holding(len, fixture.container_len - 1);
    assert_eq!(
        lost,
        article_holding(len, fixture.container_len),
        "the fixture's end header must share an article with the tail's start"
    );
    let outcome = run_embedded(JobId(9_704), &fixture.volumes, &in_order(), &[lost]).await;
    assert!(
        matches!(&outcome.status, Some(JobStatus::Failed { error }) if error.contains("PAR3")),
        "got {:?}\nsets: {}",
        outcome.status,
        outcome.shapes()
    );
    assert!(outcome.demoted(), "sets: {}", outcome.shapes());
    assert_eq!(outcome.member(MEMBER), None);
}

// More member bytes lost than the tail carries recovery for.
#[tokio::test]
async fn sevenz_store_embedded_par3_starved_loss_fails() {
    let fixture = embedded_fixture(61, 40_000);
    let len = fixture.volumes[0].1.len();
    let lost: Vec<u32> = [8_000, 15_000, 24_000]
        .into_iter()
        .map(|offset| article_holding(len, 32 + offset))
        .collect();
    let outcome = run_embedded(JobId(9_705), &fixture.volumes, &in_order(), &lost).await;
    assert!(
        matches!(&outcome.status, Some(JobStatus::Failed { error }) if error.contains("PAR3 recovery exhausted")),
        "got {:?}\nsets: {}",
        outcome.status,
        outcome.shapes()
    );
    assert_eq!(outcome.finalized, 0);
    assert_eq!(outcome.member(MEMBER), None);
}

// Bytes after a one-volume container that are not a recovery set are the
// posting disagreeing with the archive about where the file ends. The tail
// is admitted on its signature alone, so anything else is still refused.
#[tokio::test]
async fn sevenz_store_one_volume_with_a_foreign_tail_is_refused() {
    let member = payload(67, 20_000);
    let mut archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    archive.extend_from_slice(&[0u8; 4_096]);
    let volumes = split_volumes(&archive, 1);
    let outcome = run_embedded(JobId(9_706), &volumes, &in_order(), &[]).await;
    assert!(
        outcome
            .sets
            .iter()
            .any(|shape| shape.contains("Demoted(SevenZip(VolumeSize))")),
        "sets: {}",
        outcome.shapes()
    );
    assert_eq!(outcome.finalized, 0);
}

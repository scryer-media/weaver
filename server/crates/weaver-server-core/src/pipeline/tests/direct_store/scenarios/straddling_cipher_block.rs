//! A cipher block straddling a held neighbour
//!
//! An encrypted member split across volumes has cipher blocks that straddle a
//! volume boundary. When the earlier volume's share of such a block is held —
//! its own first block chains from a predecessor lost on the wire — the later
//! volume's drain still resolves the block and places its own share, so the
//! coverage map claims those bytes. Serving them in posted space means
//! re-encrypting the whole block, and the held share's plaintext is not in the
//! partial. These sets lose the tail of the second volume, which holds the
//! whole of the third, and whether the fourth volume's head block is resolved
//! before or after that depends on nothing but the order the articles land in.
//! So the orders are listed as data, and every one of them must repair in
//! place, with the provider serving every byte the coverage map claims.

use super::*;

/// Articles per volume, in volume order. The last volume is one article.
const ARTICLES: [usize; 4] = [11, 11, 11, 1];

/// The articles the posting lost: the tail of the second volume.
const LOST: [(u32, u32); 3] = [(1, 8), (1, 9), (1, 10)];

/// Volume ordinal at each NZB file index after the PAR2 index at index 0: the
/// order the posting lists them in.
const NZB_ORDER: [usize; 4] = [0, 1, 3, 2];

const PASSWORD: &str = "weaver-e2e-direct-password";

/// `(volume ordinal, article)` arrival orders. Every order holds every article,
/// the lost ones included; those are skipped where they would have landed.
#[rustfmt::skip]
const ARRIVAL_ORDERS: [&[(u32, u32)]; 24] = [
    // order 0
    &[(2, 1), (0, 6), (2, 3), (1, 5), (3, 0), (2, 8), (1, 2), (2, 10), (1, 10), (1, 4), (1, 6), (1, 8), (1, 3), (2, 0), (0, 4), (2, 9), (0, 5), (0, 0), (2, 4), (0, 2), (2, 2), (2, 7), (0, 9), (1, 7), (0, 3), (0, 1), (1, 1), (2, 5), (0, 7), (1, 0), (0, 10), (1, 9), (2, 6), (0, 8)],
    // order 1
    &[(1, 2), (1, 8), (0, 9), (2, 10), (1, 10), (0, 4), (2, 2), (0, 0), (1, 0), (1, 9), (1, 3), (2, 6), (3, 0), (2, 4), (0, 7), (1, 7), (1, 5), (1, 6), (2, 7), (2, 3), (2, 0), (0, 2), (0, 8), (2, 5), (2, 8), (1, 1), (0, 3), (0, 10), (2, 9), (0, 5), (2, 1), (0, 6), (1, 4), (0, 1)],
    // order 2
    &[(0, 5), (3, 0), (0, 4), (0, 0), (1, 1), (2, 3), (1, 9), (1, 0), (0, 8), (0, 6), (2, 8), (1, 8), (1, 2), (2, 5), (2, 9), (0, 7), (2, 1), (1, 6), (1, 5), (1, 10), (1, 4), (1, 3), (0, 1), (0, 2), (1, 7), (2, 0), (0, 9), (0, 10), (2, 4), (2, 7), (2, 10), (2, 2), (0, 3), (2, 6)],
    // order 3
    &[(1, 1), (3, 0), (2, 3), (0, 2), (1, 0), (0, 0), (1, 4), (0, 3), (2, 8), (2, 0), (2, 9), (0, 6), (1, 3), (0, 4), (1, 8), (0, 8), (2, 2), (1, 9), (0, 9), (2, 5), (0, 1), (0, 5), (1, 6), (1, 7), (1, 2), (0, 7), (2, 6), (1, 10), (2, 4), (2, 10), (1, 5), (0, 10), (2, 1), (2, 7)],
    // order 4
    &[(1, 1), (0, 3), (2, 4), (0, 0), (3, 0), (0, 4), (0, 9), (1, 9), (1, 3), (1, 0), (0, 6), (1, 4), (1, 10), (1, 2), (0, 8), (2, 2), (1, 8), (0, 1), (2, 1), (0, 7), (2, 8), (2, 6), (1, 5), (0, 2), (2, 10), (1, 7), (2, 7), (0, 5), (2, 3), (1, 6), (2, 9), (2, 5), (0, 10), (2, 0)],
    // order 5
    &[(2, 0), (1, 8), (0, 2), (2, 2), (0, 0), (1, 10), (2, 4), (1, 4), (0, 1), (1, 3), (2, 3), (0, 6), (2, 7), (0, 8), (2, 6), (1, 9), (2, 8), (0, 3), (2, 10), (1, 6), (1, 1), (0, 7), (0, 9), (1, 7), (2, 5), (0, 4), (3, 0), (0, 5), (1, 5), (1, 0), (0, 10), (1, 2), (2, 9), (2, 1)],
    // order 6
    &[(2, 5), (2, 0), (2, 7), (0, 8), (0, 1), (0, 7), (1, 6), (1, 0), (1, 1), (2, 10), (1, 3), (2, 8), (0, 6), (1, 10), (1, 4), (2, 3), (0, 4), (0, 3), (0, 9), (2, 2), (1, 8), (1, 9), (0, 2), (0, 10), (0, 0), (2, 4), (1, 2), (2, 6), (3, 0), (0, 5), (2, 1), (2, 9), (1, 7), (1, 5)],
    // order 7
    &[(2, 5), (1, 1), (1, 0), (1, 10), (1, 8), (1, 2), (0, 5), (1, 3), (0, 8), (2, 4), (2, 6), (1, 5), (2, 9), (3, 0), (2, 0), (1, 4), (2, 10), (2, 3), (0, 1), (0, 2), (2, 1), (0, 10), (1, 7), (2, 8), (1, 9), (2, 2), (0, 4), (0, 0), (0, 7), (2, 7), (0, 3), (1, 6), (0, 6), (0, 9)],
    // order 8
    &[(2, 8), (0, 8), (2, 6), (2, 0), (2, 3), (0, 7), (2, 2), (1, 3), (0, 4), (0, 2), (1, 7), (1, 9), (2, 7), (2, 9), (2, 5), (0, 0), (1, 10), (0, 5), (1, 2), (1, 1), (2, 10), (0, 1), (1, 6), (1, 8), (1, 4), (0, 9), (3, 0), (1, 0), (0, 6), (2, 1), (1, 5), (0, 3), (2, 4), (0, 10)],
    // order 9
    &[(2, 6), (0, 9), (2, 1), (0, 1), (3, 0), (2, 9), (1, 4), (0, 4), (0, 8), (1, 2), (2, 0), (0, 6), (0, 7), (1, 5), (1, 8), (0, 2), (2, 3), (1, 7), (0, 5), (2, 10), (2, 7), (1, 1), (2, 2), (2, 8), (0, 10), (1, 10), (0, 0), (1, 0), (2, 4), (1, 6), (2, 5), (1, 9), (1, 3), (0, 3)],
    // order 10
    &[(0, 8), (1, 5), (0, 0), (2, 8), (2, 5), (1, 1), (1, 7), (1, 0), (1, 9), (1, 10), (2, 9), (0, 10), (1, 2), (1, 4), (0, 2), (2, 3), (1, 3), (2, 6), (1, 6), (2, 2), (0, 1), (2, 0), (0, 3), (2, 7), (0, 5), (3, 0), (0, 7), (2, 1), (2, 4), (1, 8), (0, 9), (0, 6), (2, 10), (0, 4)],
    // order 11
    &[(1, 5), (2, 8), (1, 10), (0, 1), (1, 3), (1, 7), (0, 9), (2, 6), (2, 5), (1, 1), (0, 3), (0, 2), (2, 4), (2, 7), (0, 4), (0, 5), (2, 10), (2, 0), (0, 10), (0, 7), (0, 8), (2, 3), (1, 8), (1, 0), (0, 0), (1, 4), (3, 0), (0, 6), (1, 6), (1, 2), (2, 1), (2, 2), (1, 9), (2, 9)],
    // order 12
    &[(1, 2), (1, 10), (0, 4), (2, 1), (0, 0), (0, 9), (2, 0), (2, 3), (0, 8), (0, 5), (2, 6), (1, 1), (2, 9), (2, 4), (3, 0), (1, 8), (1, 0), (2, 7), (0, 2), (1, 6), (1, 4), (2, 5), (1, 7), (2, 8), (1, 9), (0, 1), (1, 3), (0, 6), (1, 5), (2, 10), (0, 3), (0, 10), (0, 7), (2, 2)],
    // order 13
    &[(0, 0), (2, 10), (0, 7), (3, 0), (1, 4), (0, 10), (0, 4), (2, 0), (0, 3), (2, 7), (0, 6), (1, 2), (2, 8), (2, 9), (0, 2), (2, 2), (0, 5), (0, 9), (1, 3), (2, 4), (1, 9), (2, 1), (1, 0), (1, 8), (1, 6), (1, 1), (1, 10), (1, 7), (0, 8), (0, 1), (1, 5), (2, 5), (2, 6), (2, 3)],
    // order 14
    &[(2, 6), (0, 0), (2, 9), (1, 3), (2, 2), (0, 3), (3, 0), (2, 0), (2, 1), (2, 7), (0, 2), (1, 6), (2, 8), (2, 10), (0, 5), (1, 8), (1, 5), (0, 9), (1, 9), (0, 10), (0, 6), (0, 8), (0, 4), (2, 4), (1, 1), (1, 10), (1, 0), (0, 1), (0, 7), (2, 3), (2, 5), (1, 2), (1, 4), (1, 7)],
    // order 15
    &[(0, 0), (0, 4), (0, 10), (2, 2), (1, 5), (2, 3), (1, 3), (0, 5), (1, 4), (1, 2), (2, 8), (2, 4), (1, 6), (2, 1), (0, 8), (2, 7), (0, 2), (1, 0), (1, 7), (2, 0), (1, 1), (1, 10), (1, 9), (2, 10), (0, 7), (0, 6), (2, 6), (0, 1), (2, 5), (3, 0), (0, 9), (2, 9), (0, 3), (1, 8)],
    // order 16
    &[(0, 1), (2, 2), (0, 0), (0, 5), (0, 8), (0, 4), (0, 3), (1, 9), (1, 4), (2, 8), (2, 4), (0, 9), (0, 6), (2, 6), (2, 3), (1, 5), (2, 7), (0, 10), (1, 10), (1, 3), (2, 9), (2, 10), (1, 8), (1, 0), (0, 2), (1, 7), (0, 7), (3, 0), (2, 5), (1, 2), (2, 0), (1, 6), (2, 1), (1, 1)],
    // order 17
    &[(2, 7), (2, 5), (0, 9), (2, 10), (1, 8), (1, 6), (0, 5), (0, 0), (0, 8), (1, 3), (2, 6), (0, 1), (1, 10), (0, 6), (3, 0), (1, 5), (1, 4), (0, 10), (1, 9), (2, 1), (2, 4), (0, 2), (1, 1), (0, 4), (2, 0), (2, 8), (2, 3), (2, 2), (1, 7), (0, 7), (2, 9), (0, 3), (1, 0), (1, 2)],
    // order 18
    &[(1, 0), (3, 0), (1, 8), (2, 5), (0, 4), (1, 10), (0, 0), (2, 8), (0, 3), (2, 7), (2, 1), (2, 0), (2, 2), (1, 3), (2, 4), (0, 1), (0, 9), (2, 3), (1, 7), (0, 7), (0, 10), (1, 2), (0, 5), (2, 10), (2, 6), (1, 1), (1, 4), (0, 8), (1, 6), (0, 2), (1, 5), (1, 9), (2, 9), (0, 6)],
    // order 19
    &[(1, 9), (0, 5), (2, 6), (1, 3), (0, 0), (0, 8), (1, 5), (0, 3), (1, 7), (1, 2), (2, 10), (0, 1), (2, 9), (2, 8), (1, 0), (0, 2), (2, 0), (2, 1), (0, 10), (1, 6), (1, 4), (0, 4), (2, 7), (2, 3), (1, 1), (2, 2), (1, 10), (0, 7), (0, 9), (2, 4), (2, 5), (0, 6), (1, 8), (3, 0)],
    // order 20
    &[(3, 0), (2, 3), (2, 9), (2, 4), (2, 6), (0, 10), (1, 10), (1, 0), (2, 0), (1, 4), (1, 2), (0, 5), (1, 7), (0, 2), (1, 6), (2, 1), (2, 10), (1, 3), (0, 3), (2, 5), (0, 1), (1, 5), (2, 7), (0, 4), (0, 7), (2, 8), (1, 1), (1, 8), (0, 8), (1, 9), (0, 9), (2, 2), (0, 6), (0, 0)],
    // order 21
    &[(1, 6), (2, 0), (2, 2), (0, 4), (2, 1), (1, 10), (1, 0), (1, 7), (0, 5), (1, 1), (0, 9), (0, 1), (1, 5), (1, 8), (2, 4), (0, 6), (0, 0), (2, 7), (1, 4), (2, 9), (0, 8), (0, 7), (1, 2), (2, 3), (0, 3), (2, 8), (2, 10), (0, 2), (2, 6), (1, 3), (1, 9), (0, 10), (3, 0), (2, 5)],
    // order 22
    &[(2, 1), (0, 1), (0, 9), (1, 5), (2, 0), (1, 8), (2, 5), (2, 2), (2, 4), (0, 10), (1, 0), (1, 10), (1, 4), (1, 6), (0, 5), (1, 7), (2, 3), (2, 10), (0, 7), (1, 1), (1, 2), (1, 9), (0, 6), (0, 4), (0, 0), (0, 3), (2, 7), (2, 8), (2, 9), (0, 8), (0, 2), (3, 0), (1, 3), (2, 6)],
    // order 23
    &[(0, 6), (2, 0), (1, 7), (0, 7), (1, 6), (2, 3), (1, 1), (0, 10), (0, 3), (2, 6), (2, 9), (2, 10), (0, 4), (1, 0), (0, 8), (3, 0), (2, 5), (2, 8), (0, 0), (2, 7), (2, 1), (0, 9), (0, 5), (2, 2), (1, 9), (1, 4), (2, 4), (1, 3), (1, 8), (0, 2), (1, 5), (1, 2), (0, 1), (1, 10)],
];

/// One encryption shape: the posted volumes, the PAR2 set that describes
/// them, and the digest of the member they carry. Built here, over bytes the
/// test posts itself, so a straddling block is a straddling block of *these*
/// volumes.
struct StraddleFixture {
    name: &'static str,
    member: &'static str,
    volumes: Vec<(String, Vec<u8>)>,
    par2_bytes: Vec<u8>,
    expected_blake3: String,
}

/// Enough recovery to rebuild the lost tail of the second volume, with slack
/// for the slices it only partly covers.
const RECOVERY_BLOCKS: usize = 16;

fn straddle_fixture(
    name: &'static str,
    member: &'static str,
    volumes_of: impl FnOnce(&[u8]) -> Vec<(String, Vec<u8>)>,
) -> StraddleFixture {
    let payload: Vec<u8> = (0..12_000u32).map(|index| (index % 241) as u8).collect();
    let volumes = volumes_of(&payload);
    assert_eq!(volumes.len(), ARTICLES.len());
    let par2_bytes = repairable_par2_index(&volumes, RECOVERY_BLOCKS);
    StraddleFixture {
        name,
        member,
        volumes,
        par2_bytes,
        expected_blake3: blake3::hash(&payload).to_hex().to_string(),
    }
}

/// Runs one arrival order to its end. `Err` names what went wrong.
async fn run_straddle_order(
    fixture: &StraddleFixture,
    order_index: usize,
    job: u64,
) -> Result<(), String> {
    let volumes: &[(String, Vec<u8>)] = &fixture.volumes;
    let par2_bytes: &[u8] = &fixture.par2_bytes;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let job_id = JobId(job);
    let mut spec = direct_store_job_spec_with_articles("Violet Cascade", volumes, ARTICLES[0]);
    spec.files[3].segments = vec![segment_spec! {
        number: 0,
        bytes: yenc_declared_bytes(volumes[3].1.len() as u32),
        message_id: "direct-3-0@example.com".to_string(),
    }];
    spec.files = NZB_ORDER
        .iter()
        .map(|&ordinal| spec.files[ordinal].clone())
        .collect();
    append_par2_index(&mut spec, par2_bytes);
    let index = spec.files.pop().unwrap();
    spec.files.insert(0, index);
    spec.password = Some(PASSWORD.to_owned());
    let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

    let file_index_of = |ordinal: u32| {
        1 + NZB_ORDER
            .iter()
            .position(|&at| at == ordinal as usize)
            .unwrap() as u32
    };
    for &(ordinal, article) in ARRIVAL_ORDERS[order_index] {
        if LOST.contains(&(ordinal, article)) {
            continue;
        }
        submit_volume_article_indexed_of(
            &mut pipeline,
            job_id,
            volumes,
            ordinal,
            file_index_of(ordinal),
            article,
            ARTICLES[ordinal as usize],
        )
        .await;
    }
    submit_decoded_segment(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: 0,
        },
        0,
        0,
        par2_bytes,
        "violet.cascade.par2",
        None,
    )
    .await;

    // Every byte the coverage map claims is a byte the provider serves, and
    // serves as posted.
    {
        let set = pipeline
            .direct_store
            .set(job_id, 0)
            .ok_or("the set was not admitted")?;
        let lengths: std::collections::BTreeMap<u32, u64> = volumes
            .iter()
            .enumerate()
            .map(|(ordinal, (_, bytes))| (ordinal as u32, bytes.len() as u64))
            .collect();
        let provider = crate::pipeline::direct_store::provider::HybridVolumeProvider::new(
            set.virtual_volumes(&lengths),
        );
        for ordinal in 0..4u32 {
            let posted = &volumes[ordinal as usize].1;
            let mut reader = provider
                .open(ordinal)
                .ok_or_else(|| format!("volume {ordinal} has no reader"))?;
            for &(start, end) in set.volume_coverage(ordinal).ranges() {
                let mut at = start;
                let mut buffer = vec![0u8; 1 << 20];
                while at < end {
                    let want = ((end - at) as usize).min(buffer.len());
                    std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(at))
                        .map_err(|error| format!("volume {ordinal} seek {at}: {error}"))?;
                    std::io::Read::read_exact(&mut reader, &mut buffer[..want]).map_err(
                        |error| {
                            format!("volume {ordinal} claims [{at}, {end}) but refuses it: {error}")
                        },
                    )?;
                    if buffer[..want] != posted[at as usize..at as usize + want] {
                        return Err(format!(
                            "volume {ordinal} serves [{at}, {}) unlike the posted bytes",
                            at + want as u64
                        ));
                    }
                    at += want as u64;
                }
            }
        }
    }

    let mut sets = format!("{:?}", pipeline.direct_store.sets_for(job_id));
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    // Bounded by the work there is, not by time: each pass settles what the
    // last one produced, and a job that has not finished after this many has
    // stopped making progress.
    for _ in 0..48 {
        if matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete) | Some(JobStatus::Failed { .. })
        ) {
            break;
        }
        drain_rar_refreshes(&mut pipeline).await;
        pipeline.check_job_completion(job_id).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        pump_pipeline_runtime_queues(&mut pipeline).await;
        sample_direct_sets(&pipeline, job_id, &mut sets);
        settle_inflight_moves(&mut pipeline).await;
        if let Some(done) = next_owed_extraction(&mut pipeline, job_id).await {
            pipeline.handle_extraction_done(done).await;
            pump_pipeline_runtime_queues(&mut pipeline).await;
            settle_inflight_moves(&mut pipeline).await;
        }
        sample_direct_sets(&pipeline, job_id, &mut sets);
    }

    let status = job_status_for_assert(&pipeline, job_id);
    if !matches!(status, Some(JobStatus::Complete)) {
        return Err(format!("job ended {status:?}; sets {sets}"));
    }
    if pipeline.direct_store.repair_materialized_volumes != 1 {
        return Err(format!(
            "the set did not repair in place (materialized {}); sets {sets}",
            pipeline.direct_store.repair_materialized_volumes
        ));
    }
    let output = complete_dir
        .join(crate::jobs::working_dir::sanitize_dirname("Violet Cascade"))
        .join(fixture.member);
    let member = std::fs::read(&output)
        .ok()
        .or_else(|| staging_member(&complete_dir, fixture.member))
        .ok_or("the member was not delivered")?;
    let digest = blake3::hash(&member).to_hex().to_string();
    if digest != fixture.expected_blake3 {
        return Err(format!(
            "the member was delivered with the wrong bytes ({digest})"
        ));
    }
    Ok(())
}

async fn every_order_repairs_in_place(fixture: StraddleFixture, job_base: u64) {
    let mut failures = Vec::new();
    for order_index in 0..ARRIVAL_ORDERS.len() {
        if let Err(reason) =
            run_straddle_order(&fixture, order_index, job_base + order_index as u64).await
        {
            failures.push(format!("order {order_index}: {reason}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} arrival order(s) of {} failed:\n{}",
        failures.len(),
        fixture.name,
        failures.join("\n")
    );
}

#[tokio::test]
async fn a_straddling_block_behind_a_held_volume_repairs_in_place_rar5_file_encryption() {
    let member = "obsidian.current.s01e08.mkv";
    every_order_repairs_in_place(
        straddle_fixture("rar5 file encryption", member, |payload| {
            encrypted_store_set(member, payload, 4, PASSWORD, Some(PASSWORD), true)
        }),
        52_000,
    )
    .await;
}

#[tokio::test]
async fn a_straddling_block_behind_a_held_volume_repairs_in_place_rar5_header_encryption() {
    let member = "umber.tideline.s01e12.mkv";
    every_order_repairs_in_place(
        straddle_fixture("rar5 header encryption", member, |payload| {
            header_encrypted_store_set(member, payload, 4, PASSWORD, HeaderCheck::For(PASSWORD))
        }),
        52_100,
    )
    .await;
}

#[tokio::test]
async fn a_straddling_block_behind_a_held_volume_repairs_in_place_rar4_encryption() {
    let member = "cobalt.lantern.s01e10.mkv";
    every_order_repairs_in_place(
        straddle_fixture("rar4 encryption", member, |payload| {
            encrypted_rar4_store_set(member, payload, 4, PASSWORD, Some(TEST_RAR4_SALT))
        }),
        52_200,
    )
    .await;
}

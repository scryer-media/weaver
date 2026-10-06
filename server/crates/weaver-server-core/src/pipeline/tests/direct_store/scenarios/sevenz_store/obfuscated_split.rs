//! An obfuscated split 7z: hex names, and parts that carry nothing saying
//! which part they are.
//!
//! Only the recovery set's descriptions name the parts, so the set is admitted
//! from them and holds every byte until each part is bound to its real name
//! and number. Only then is the container whole, its map readable, and the
//! held bytes routed. Every way that naming can fail leaves the set
//! conventional.

use super::*;

const VOLUMES: usize = 3;

fn obfuscated_split_volumes(seed: u8) -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
    // Each part longer than the 16 KiB window its description fingerprints.
    let member = payload(seed, 120_000);
    let archive = build_7z(
        &[Entry::file(MEMBER, member.clone())],
        EncoderMethod::COPY,
        None,
    );
    (member, split_volumes(&archive, VOLUMES))
}

/// One obfuscated split job with its index parsed, before any part arrives.
struct HeldSplit {
    _temp: tempfile::TempDir,
    pipeline: Pipeline,
    job_id: JobId,
    complete_dir: PathBuf,
    working_dir: PathBuf,
    volumes: Vec<(String, Vec<u8>)>,
    obfuscated: Vec<(String, Vec<u8>)>,
}

impl HeldSplit {
    async fn new(
        job_id: JobId,
        volumes: Vec<(String, Vec<u8>)>,
        holds: Option<(u64, u64)>,
    ) -> Self {
        let par2_bytes = par2_index_over_volumes(&volumes);
        let obfuscated = obfuscate_volumes(&volumes);
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        if let Some((budget, ceiling)) = holds {
            pipeline.direct_store.set_holds_budget(budget);
            pipeline.direct_store.set_holds_scratch_ceiling(ceiling);
        }
        let (mut spec, index_file_index) =
            par2_bearing_job_spec("Silver Horizon", &obfuscated, &par2_bytes);
        spec.password = None;
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        deliver_par2_index(&mut pipeline, job_id, index_file_index, &par2_bytes).await;
        Self {
            _temp: temp,
            pipeline,
            job_id,
            complete_dir,
            working_dir,
            volumes,
            obfuscated,
        }
    }

    async fn submit(&mut self, file_index: u32, segment_number: u32) {
        submit_volume_article(
            &mut self.pipeline,
            self.job_id,
            &self.obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }

    fn sets(&self) -> String {
        format!("{:?}", self.pipeline.direct_store.sets_for(self.job_id))
    }

    /// A source volume file under either name: the conventional path writes
    /// one, a routed set never does.
    fn volume_file_seen(&self) -> bool {
        self.obfuscated
            .iter()
            .chain(self.volumes.iter())
            .any(|(filename, _)| self.working_dir.join(filename).exists())
    }

    /// Runs the job to its end and returns the member it produced.
    async fn finish(&mut self) -> (Option<Vec<u8>>, Option<JobStatus>, String) {
        if let Some(state) = self.pipeline.jobs.get_mut(&self.job_id) {
            state.download_queue = crate::DownloadQueue::new();
            state.recovery_queue = crate::DownloadQueue::new();
        }
        self.pipeline.check_job_completion(self.job_id).await;
        let sets = self.sets();
        drain_rar_refreshes(&mut self.pipeline).await;
        drive_extractions_to_terminal(&mut self.pipeline, self.job_id, 64).await;
        let output_root = self
            .complete_dir
            .join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
        let member = std::fs::read(output_root.join(MEMBER))
            .ok()
            .or_else(|| staging_member(&self.complete_dir, MEMBER));
        (
            member,
            job_status_for_assert(&self.pipeline, self.job_id),
            sets,
        )
    }
}

#[tokio::test]
async fn an_obfuscated_split_sevenz_set_holds_until_every_part_is_named_then_routes() {
    let (member, volumes) = obfuscated_split_volumes(41);
    let mut job = HeldSplit::new(JobId(9_740), volumes, None).await;

    // The first part binds and the set is admitted, but two parts are still
    // unnamed: the container is not whole, so nothing routes and the first
    // part's bytes are held.
    job.submit(0, 0).await;
    job.submit(0, 1).await;
    let set = job
        .pipeline
        .direct_store
        .set(job.job_id, 0)
        .expect("the first named part admits the set");
    assert!(
        !set.plan().is_whole() && !set.is_demoted(),
        "a set with unnamed parts must wait, got {}",
        job.sets()
    );
    let held_before = set.router.staged_bytes();
    assert!(held_before > 0, "the first part's bytes must be held");

    // The second part binds; the third is still unnamed, so the set waits.
    job.submit(1, 0).await;
    job.submit(1, 1).await;
    let set = job.pipeline.direct_store.set(job.job_id, 0).unwrap();
    assert!(
        !set.plan().is_whole() && !set.is_demoted(),
        "one unnamed part still holds the set, got {}",
        job.sets()
    );

    // The last part's front names it: the container is whole, its map is
    // read at the tail, and everything held routes.
    job.submit(2, 0).await;
    job.submit(2, 1).await;
    let set = job.pipeline.direct_store.set(job.job_id, 0).unwrap();
    assert!(
        set.plan().is_whole() && !set.is_demoted(),
        "every part named must make the set whole, got {}",
        job.sets()
    );
    assert_eq!(
        set.router.staged_bytes(),
        0,
        "every held byte must have routed once the map was read"
    );
    assert!(
        !job.volume_file_seen(),
        "a routed set must never write a source volume"
    );

    let (produced, status, sets) = job.finish().await;
    assert!(
        sets.contains("Finalized"),
        "the set must finalize, got {sets}"
    );
    assert_eq!(produced.as_deref(), Some(member.as_slice()));
    assert!(
        matches!(status, Some(JobStatus::Complete)),
        "got {status:?}"
    );
}

#[tokio::test]
async fn an_obfuscated_split_sevenz_set_admits_from_par2_and_matches_the_conventional_output() {
    let (member, volumes) = obfuscated_split_volumes(43);
    let arrivals = in_order_arrivals(volumes.len());
    let conventional = run_obfuscated_par2_gate(
        DirectStoreGate::Disabled,
        JobId(9_741),
        MEMBER,
        &volumes,
        true,
        &arrivals,
    )
    .await;
    // Scrambled, so a later part's bytes arrive before an earlier part is
    // named and the holds carry them across.
    let direct = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(9_742),
        MEMBER,
        &volumes,
        true,
        &scrambled(volumes.len()),
    )
    .await;

    assert!(direct.admitted, "got {}", direct.demotions);
    assert!(
        !direct.volume_file_seen,
        "a routed set must never write a source volume, got {}",
        direct.demotions
    );
    assert!(
        direct.demotions.contains("Finalized"),
        "got {}",
        direct.demotions
    );
    assert_eq!(conventional.member.as_deref(), Some(member.as_slice()));
    assert_eq!(
        (direct.member, direct.member_location, direct.status),
        (
            conventional.member,
            conventional.member_location,
            conventional.status
        ),
    );
}

/// An obfuscated split with no recovery set at all, run under `gate`: whether
/// a set was admitted, whether every part landed under its hex name, and what
/// the job produced.
async fn run_unnamed_split(
    gate: DirectStoreGate,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) -> (bool, bool, Option<Vec<u8>>, Option<JobStatus>) {
    let obfuscated = obfuscate_volumes(volumes);
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    pipeline.direct_store.set_gate(gate);
    let working_dir = insert_active_job(
        &mut pipeline,
        job_id,
        direct_store_job_spec("Silver Horizon", &obfuscated),
    )
    .await;
    for (file_index, segment_number) in in_order_arrivals(obfuscated.len()) {
        submit_volume_article(
            &mut pipeline,
            job_id,
            &obfuscated,
            file_index,
            segment_number,
        )
        .await;
    }
    let admitted = !pipeline.direct_store.sets_for(job_id).is_empty();
    let conventional = obfuscated
        .iter()
        .all(|(filename, _)| working_dir.join(filename).exists());
    if let Some(state) = pipeline.jobs.get_mut(&job_id) {
        state.download_queue = crate::DownloadQueue::new();
        state.recovery_queue = crate::DownloadQueue::new();
    }
    pipeline.check_job_completion(job_id).await;
    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    let output_root =
        complete_dir.join(crate::jobs::working_dir::sanitize_dirname("Silver Horizon"));
    let produced = std::fs::read(output_root.join(MEMBER))
        .ok()
        .or_else(|| staging_member(&complete_dir, MEMBER));
    (
        admitted,
        conventional,
        produced,
        job_status_for_assert(&pipeline, job_id),
    )
}

#[tokio::test]
async fn an_obfuscated_split_sevenz_set_without_a_recovery_set_stays_conventional() {
    // Nothing names a part, and the first part's own start header points past
    // its end, so it is no whole one-volume container either. The job ends
    // exactly as it does with direct routing off.
    let (_, volumes) = obfuscated_split_volumes(47);
    let (_, _, off_member, off_status) =
        run_unnamed_split(DirectStoreGate::Disabled, JobId(9_743), &volumes).await;
    let (admitted, conventional, member, status) =
        run_unnamed_split(DirectStoreGate::Enabled, JobId(9_746), &volumes).await;

    assert!(!admitted, "nothing may admit an unnamed split");
    assert!(conventional, "every part must land under its own name");
    assert_eq!((member, status), (off_member, off_status));
}

#[tokio::test]
async fn an_obfuscated_split_sevenz_set_named_after_its_body_stays_conventional() {
    // The descriptions arrive after every part's bytes have landed under
    // their hex names: a routed set owns all of a part's bytes or none, so
    // nothing is admitted and the job extracts from the volumes.
    let (member, volumes) = obfuscated_split_volumes(53);
    let arrivals = in_order_arrivals(volumes.len());
    let outcome = run_obfuscated_par2_gate(
        DirectStoreGate::Enabled,
        JobId(9_744),
        MEMBER,
        &volumes,
        false,
        &arrivals,
    )
    .await;

    assert!(!outcome.admitted, "got {}", outcome.demotions);
    assert!(outcome.volume_file_seen);
    assert_eq!(outcome.member.as_deref(), Some(member.as_slice()));
    assert!(
        matches!(outcome.status, Some(JobStatus::Complete)),
        "got {:?}",
        outcome.status
    );
}

#[tokio::test]
async fn an_obfuscated_split_sevenz_set_whose_holds_overflow_demotes_with_the_ceiling_reason() {
    // The bytes held while parts are still unnamed are ordinary holds, under
    // the ordinary limits: past the scratch ceiling the set demotes with that
    // reason, and the job still extracts from the volumes.
    let (member, volumes) = obfuscated_split_volumes(59);
    let mut job = HeldSplit::new(JobId(9_745), volumes, Some((64, 16))).await;
    job.submit(0, 0).await;
    job.submit(0, 1).await;
    assert!(
        job.sets().contains("Demoted(HoldsScratchCeiling)"),
        "an overflow while waiting for names must demote on the ceiling, got {}",
        job.sets()
    );
    for (file_index, segment_number) in [(1, 0), (1, 1), (2, 0), (2, 1)] {
        job.submit(file_index, segment_number).await;
    }

    let (produced, status, _) = job.finish().await;
    assert_eq!(produced.as_deref(), Some(member.as_slice()));
    assert!(
        matches!(status, Some(JobStatus::Complete)),
        "got {status:?}"
    );
}

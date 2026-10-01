//! Exhaust every publication subset around a placement's write and actor
//! commit. Semaphores control I/O; receipt application is a separate event.
use super::*;
use crate::pipeline::repair::par3::work::Coordinator;
use par3_rs::source::SourceId;

#[derive(Clone, Copy, Debug)]
enum Encryption {
    Plain,
    Member,
    KeyedChecksum,
}

struct Harness {
    pipeline: Pipeline,
    job: JobId,
    volumes: Vec<(String, Vec<u8>)>,
    hold: Arc<Semaphore>,
    trace: Vec<&'static str>,
    _root: TempDir,
}

impl Harness {
    async fn new(encryption: Encryption, sibling: bool) -> Self {
        let root = TempDir::new().unwrap();
        let payload: Vec<u8> = (0..120_000u32).map(|i| (i * 7 + 3) as u8).collect();
        let password = "moonlit-harbour";
        let volumes = match encryption {
            Encryption::Plain => single_member_store_set("feature.mkv", &payload, 3),
            Encryption::Member | Encryption::KeyedChecksum => encrypted_store_set(
                "feature.mkv",
                &payload,
                3,
                password,
                Some(password),
                matches!(encryption, Encryption::KeyedChecksum),
            ),
        };
        let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
        pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
        let job = JobId(41995);
        let mut spec = direct_store_job_spec_with_articles("Placement schedules", &volumes, 3);
        if !matches!(encryption, Encryption::Plain) {
            spec.password = Some(password.into());
        }
        insert_active_job(&mut pipeline, job, spec).await;
        for article in 0..if sibling { 3 } else { 1 } {
            route_article(&mut pipeline, job, &volumes, 0, article).await;
            settle_direct_placement_work(&mut pipeline).await;
        }
        let mut coordinator = Coordinator::new(
            pipeline.repair_work_done_tx.clone(),
            Arc::clone(&pipeline.metrics),
        );
        coordinator.admit(job).unwrap();
        pipeline.par3_runtime = Some(Box::new(coordinator));
        let hold = Arc::new(Semaphore::new(0));
        pipeline.direct_placement_hold = Some(Arc::clone(&hold));
        let mut result = Self {
            pipeline,
            job,
            volumes,
            hold,
            trace: vec![],
            _root: root,
        };
        result.publish().await;
        result
    }

    async fn route(&mut self, file: u32, article: u32) {
        self.trace.push("route");
        route_article(&mut self.pipeline, self.job, &self.volumes, file, article).await;
        assert!(
            !committed(&self.pipeline, segment(self.job, file, article)),
            "{:?}",
            self.trace
        );
        assert!(
            self.pipeline.has_direct_placements(self.job),
            "{:?}",
            self.trace
        );
    }

    async fn write(&mut self) {
        self.trace.push("write, retain unapplied receipt");
        self.hold.add_permits(1);
        self.pipeline.await_direct_placement_io(self.job, 0).await;
        assert!(
            self.pipeline.has_direct_placements(self.job),
            "{:?}",
            self.trace
        );
    }

    async fn apply(&mut self) {
        self.trace.push("apply receipt");
        settle_direct_placement_work(&mut self.pipeline).await;
        assert!(
            !self.pipeline.has_direct_placements(self.job),
            "{:?}",
            self.trace
        );
    }

    async fn publish(&mut self) {
        self.trace.push("publish");
        self.pipeline.refresh_par3_sources(self.job).unwrap();
        if self.pipeline.has_direct_placements(self.job) {
            assert!(
                !self
                    .pipeline
                    .par3_runtime
                    .as_ref()
                    .unwrap()
                    .has_worker_in_flight(self.job),
                "publication escaped a pending placement: {:?}",
                self.trace
            );
        }
        settle_par3_work(&mut self.pipeline, self.job).await;
        if !self.pipeline.has_direct_placements(self.job) {
            self.assert_sources();
        }
    }

    fn assert_sources(&self) {
        let runtime = self.pipeline.par3_runtime.as_ref().unwrap();
        let set = self.pipeline.direct_store.set(self.job, 0).unwrap();
        for (index, (_, expected)) in self.volumes.iter().enumerate() {
            let source = SourceId(index as u64);
            let ranges = runtime.source_ranges(self.job, source).unwrap();
            let actual = runtime.published_source_bytes(self.job, source).unwrap();
            // Independent bytes catch an incorrectly routed or decrypted range
            // even if both coverage accounting layers agree on its bounds.
            for (start, bytes) in actual {
                assert_eq!(
                    bytes,
                    expected[start as usize..start as usize + bytes.len()],
                    "source={source:?} trace={:?}",
                    self.trace
                );
            }
            let volume =
                set.virtual_volumes(&BTreeMap::from([(index as u32, expected.len() as u64)]));
            if let Some(volume) = volume
                .iter()
                .find(|volume| volume.volume_index == index as u32)
            {
                assert_eq!(
                    ranges,
                    volume.readable_ranges(),
                    "coverage source={source:?} trace={:?}",
                    self.trace
                );
            }
        }
    }
}

async fn campaign(encryption: Encryption) {
    for sibling in [false, true] {
        for mask in 0..8 {
            let mut harness = Harness::new(encryption, sibling).await;
            let (file, article) = if sibling { (1, 0) } else { (0, 1) };
            harness.route(file, article).await;
            if mask & 1 != 0 {
                harness.publish().await;
            }
            harness.write().await;
            if mask & 2 != 0 {
                harness.publish().await;
            }
            assert!(!committed(
                &harness.pipeline,
                segment(harness.job, file, article)
            ));
            harness.apply().await;
            if mask & 4 != 0 {
                harness.publish().await;
            }
            assert!(committed(
                &harness.pipeline,
                segment(harness.job, file, article)
            ));
            harness.publish().await;
            // Repeating refresh must preserve both coverage and actual bytes.
            harness.publish().await;
        }
    }
}

#[tokio::test]
async fn plain_placement_schedules() {
    campaign(Encryption::Plain).await;
}
#[tokio::test]
async fn encrypted_placement_schedules() {
    campaign(Encryption::Member).await;
}
#[tokio::test]
async fn keyed_checksum_encrypted_placement_schedules() {
    campaign(Encryption::KeyedChecksum).await;
}

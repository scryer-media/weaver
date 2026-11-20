// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::model::SchedulingMemoryEstimate;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::jobs::repository::JobsRepositoryQueries;
use crate::pipeline::activation::JobActivated;

// Add a job through admission as a submitted NZB is added, so it waits its
// turn with its NZB stored. Returns the job's spec as read from that NZB.
pub(in crate::pipeline::tests) async fn add_queued_job(
    pipeline: &mut Pipeline,
    job_id: JobId,
    articles: usize,
) -> JobSpec {
    let segments: String = (1..=articles)
        .map(|number| {
            format!(
                r#"<segment bytes="512" number="{number}">{}-{number}@queued.test</segment>"#,
                job_id.0
            )
        })
        .collect();
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="&quot;payload.bin&quot; yEnc (1/{articles})">
            <groups><group>alt.binaries.test</group></groups>
            <segments>{segments}</segments>
          </file>
        </nzb>"#
    );
    let nzb_zstd = crate::ingest::compress_nzb_bytes(xml.as_bytes()).unwrap();
    let nzb_path = PathBuf::from(format!("queued-{}.nzb", job_id.0));
    let nzb = crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd).unwrap();
    let spec = crate::ingest::nzb_to_spec(&nzb, &nzb_path, None, vec![]);
    let resources = pipeline.admission_resources();
    let prepared = Pipeline::prepare_add(
        &resources,
        job_id,
        spec.clone(),
        nzb_path,
        nzb_zstd,
        crate::jobs::AddJobOptions::default(),
    )
    .await
    .unwrap();
    pipeline.install_prepared_add(job_id, prepared);
    spec
}

pub struct ActivationReply(JobActivated);
impl ActivationReply {
    pub fn generation(&self) -> u64 {
        self.0.activation
    }
}
impl DispatchFixture {
    pub async fn add_queued_job(&mut self, id: JobId, articles: usize) -> JobSpec {
        add_queued_job(&mut self.0, id, articles).await
    }
    pub fn request_job_activation(&mut self, id: JobId) {
        self.0.request_job_activation(id);
    }
    pub async fn next_activation(&mut self) -> ActivationReply {
        ActivationReply(
            self.0
                .job_activated_rx
                .recv()
                .await
                .expect("the activation channel stays open"),
        )
    }
    pub fn handle_activation(&mut self, reply: ActivationReply) {
        self.0.handle_job_activated(reply.0);
    }
    pub async fn deliver_activation(&mut self) {
        let reply = self.next_activation().await;
        self.handle_activation(reply);
    }
    pub fn has_repeated_articles(&self, id: JobId) -> bool {
        self.0.repeated_articles.contains_key(&id)
    }
    pub fn has_article_memory(&self, id: JobId) -> bool {
        self.0.job_article_memory.contains_key(&id)
    }
    pub fn reserved_memory(&self) -> u64 {
        self.0.process_memory_budget.reserved_bytes()
    }
    pub fn set_memory_budget(&mut self, bytes: u64) {
        self.0.process_memory_budget = Arc::new(crate::pipeline::ProcessMemoryBudget::new(bytes));
    }
    pub fn estimate_scheduling_memory(spec: &JobSpec) -> u64 {
        spec.scheduling_memory_estimate()
    }
    pub fn persist_admitted(&self, id: JobId) {
        self.0.db.set_nzb_admitted(id, true).unwrap();
    }
    pub fn delete_active_job(&self, id: JobId) {
        self.0.db.delete_active_job(id).unwrap();
    }
    pub fn purge_terminal_runtime(&mut self, id: JobId) {
        self.0.purge_terminal_job_runtime(id);
    }
    pub async fn cancel_job(&mut self, id: JobId) -> Result<(), crate::SchedulerError> {
        cancel_one_job(
            &mut self.0,
            id,
            crate::jobs::handle::CancellationOrigin::User,
        )
        .await
    }
}

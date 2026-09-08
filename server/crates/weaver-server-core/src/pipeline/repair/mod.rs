use super::*;

pub(in crate::pipeline) mod backend;
pub(in crate::pipeline) mod par2;
pub(in crate::pipeline) mod par3;
mod sources;
pub(crate) use par2::PROMOTED_RECOVERY_PRIORITY;

impl Pipeline {
    pub(in crate::pipeline) async fn handle_repair_work_done(&mut self, done: RepairWorkDone) {
        match done {
            RepairWorkDone::Par2(done) => self.handle_par2_analysis_done(done).await,
            RepairWorkDone::Par3(done) => self.handle_par3_work_done(*done),
        }
    }
}

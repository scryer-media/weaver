// A restart keeps member bytes but not the running checksums over them. A
// rearm re-reads each surviving run once so the router can verify the member
// when its last bytes arrive, exactly as if it had never stopped.

use super::{RuntimeError, limits};
use crate::pipeline::direct_store::provider::{NestedMembers, VirtualVolumeReader};
use crate::pipeline::direct_store::router::RestartReadRun;
use crate::pipeline::extraction::{ExtractionRoot, JobExtractionBudget};
use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

trait Input: Read + Seek {}
impl<T: Read + Seek> Input for T {}

pub(crate) struct RearmRead {
    pub(crate) runs: Vec<RestartReadRun>,
    // Members backed by a file under `base`.
    pub(crate) physical: Arc<HashMap<u32, PathBuf>>,
    // Members backed by a nested archive's virtual volume.
    pub(crate) nested: NestedMembers,
    pub(crate) root: Arc<ExtractionRoot>,
    pub(crate) base: PathBuf,
    pub(crate) budget: Arc<JobExtractionBudget>,
}

impl RearmRead {
    // The runs, with one CRC-32 per run in run order.
    pub(crate) async fn checksum(self) -> Result<(Vec<RestartReadRun>, Vec<u32>), RuntimeError> {
        let memory = self
            .budget
            .try_reserve_memory(limits::MATERIALIZATION_PERMIT_BYTES)
            .ok_or(RuntimeError::Admission("graph rearm"))?;
        let task = self.budget.task_permit_for_root(Arc::clone(&self.root))?;
        tokio::task::spawn_blocking(move || {
            let (_memory, _task) = (memory, task);
            let checksums = self.checksums()?;
            Ok((self.runs, checksums))
        })
        .await
        .map_err(RuntimeError::join("graph rearm"))?
        .map_err(RuntimeError::io("graph rearm"))
    }

    fn checksums(&self) -> io::Result<Vec<u32>> {
        let mut buffer = vec![0; limits::MATERIALIZATION_PERMIT_BYTES as usize];
        self.runs
            .iter()
            .map(|run| self.checksum_run(run, &mut buffer))
            .collect()
    }

    fn checksum_run(&self, run: &RestartReadRun, buffer: &mut [u8]) -> io::Result<u32> {
        let mut input = self.open(run.member_id)?;
        input.seek(SeekFrom::Start(run.logical_offset))?;
        let mut hash = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        let mut remaining = run.len;
        while remaining != 0 {
            self.budget.check_active_io()?;
            let count = remaining.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..count])?;
            hash.update(&buffer[..count]);
            remaining -= count as u64;
        }
        // CRC-32 occupies the low 32 bits of the digest.
        Ok(hash.finalize() as u32)
    }

    fn open(&self, member: u32) -> io::Result<Box<dyn Input>> {
        if let Some(image) = self.nested.get(&member) {
            return Ok(Box::new(VirtualVolumeReader::<true>::new(
                image.clone(),
                Arc::default(),
            )));
        }
        let path = self.physical.get(&member).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "rearm member backing absent")
        })?;
        let relative = path.strip_prefix(&self.base).map_err(io::Error::other)?;
        Ok(Box::new(self.root.open_input(relative)?.into_std()))
    }
}

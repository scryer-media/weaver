use std::io;
use std::sync::{Arc, mpsc};

use super::{PositionalManifest, RunEpoch, SetCoverage};
use crate::pipeline::extraction::{JobExtractionBudget, MemoryPermit};
use sevenz_turbo::{Lzma2RunCompletion, PositionalLzma2Folder, ReadAt};

struct Record {
    completion: Lzma2RunCompletion,
    _memory: MemoryPermit,
}

// Records stay provisional and charged until all folder/member CRCs pass.
// Repair joins old workers before replacing records from a prior epoch.
#[derive(Default)]
pub(crate) struct RunJournal {
    folders: Vec<Vec<Option<Record>>>,
}

struct FolderOutput {
    files: Vec<(u64, u64, usize)>,
    len: u64,
    manifest: Arc<PositionalManifest>,
    _memory: MemoryPermit,
}

impl FolderOutput {
    fn new(
        folder: &PositionalLzma2Folder,
        manifest: Arc<PositionalManifest>,
        memory: MemoryPermit,
    ) -> Result<Self, String> {
        let mut files = Vec::new();
        files
            .try_reserve_exact(folder.entries().count())
            .map_err(|error| error.to_string())?;
        let mut offset = 0u64;
        for (index, entry) in folder.entries() {
            let end = offset
                .checked_add(entry.size())
                .ok_or("folder file map overflow")?;
            files.push((offset, end, index));
            offset = end;
        }
        if offset != folder.output_len() {
            return Err("folder file map length mismatch".to_string());
        }
        Ok(Self {
            files,
            len: offset,
            manifest,
            _memory: memory,
        })
    }

    fn write_at(&self, epoch: &RunEpoch, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        epoch.check()?;
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|end| end > self.len)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "run exceeds folder output",
            ));
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        let index = self.files.partition_point(|(_, end, _)| *end <= offset);
        let &(start, end, file) = self.files.get(index).ok_or(io::ErrorKind::InvalidInput)?;
        if offset < start {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let count = bytes
            .len()
            .min(usize::try_from(end - offset).unwrap_or(usize::MAX));
        self.manifest
            .write_at(epoch, file, offset - start, &bytes[..count])
    }
}

struct Completed {
    block: usize,
    index: usize,
    result: Result<Lzma2RunCompletion, String>,
    record_memory: MemoryPermit,
}

fn take_completed(
    receiver: &mpsc::Receiver<Completed>,
    records: &mut [Vec<Option<Record>>],
    epoch: &RunEpoch,
) -> Result<(), String> {
    let completed = receiver
        .recv()
        .map_err(|_| "positional run worker disappeared".to_string())?;
    let completion = completed.result?;
    epoch
        .commit(|| {
            let record = records
                .get_mut(completed.block)
                .and_then(|records| records.get_mut(completed.index))
                .ok_or(io::ErrorKind::InvalidData)?;
            if record.is_some() {
                return Err(io::ErrorKind::InvalidData.into());
            }
            *record = Some(Record {
                completion,
                _memory: completed.record_memory,
            });
            Ok(())
        })
        .map_err(|error| error.to_string())
}

fn reserve_growth(
    bytes: u64,
    budget: &Arc<JobExtractionBudget>,
    in_flight: &mut usize,
    receiver: &mpsc::Receiver<Completed>,
    journal: &mut RunJournal,
    epoch: &RunEpoch,
) -> Result<MemoryPermit, String> {
    loop {
        epoch.check().map_err(|error| error.to_string())?;
        if let Some(memory) = budget.try_reserve_memory(bytes) {
            return Ok(memory);
        }
        if *in_flight != 0 {
            *in_flight -= 1;
            take_completed(receiver, &mut journal.folders, epoch)?;
            continue;
        }
        // The remaining reservations belong to this coordinator and its
        // provisional journal. Waiting for itself to release them cannot help.
        if bytes
            > budget
                .max_memory_bytes()
                .saturating_sub(budget.memory_reserved_bytes())
        {
            return Err("positional run and retained metadata exceed memory policy".to_string());
        }
        return budget.reserve_more_memory_wait(bytes).map_err(String::from);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_pass(
    folders: &[PositionalLzma2Folder],
    source: &dyn ReadAt,
    coverage: &SetCoverage,
    epoch: &RunEpoch,
    output: &Arc<PositionalManifest>,
    budget: &Arc<JobExtractionBudget>,
    pool: &Arc<rayon::ThreadPool>,
    journal: &mut RunJournal,
) -> Result<(), String> {
    if journal.folders.len() > folders.len() {
        return Err("positional folder count changed".to_string());
    }
    journal
        .folders
        .try_reserve(folders.len() - journal.folders.len())
        .map_err(|error| error.to_string())?;
    journal.folders.resize_with(folders.len(), Vec::new);
    // The coordinator is a blocking task outside this pool. Coverage waits
    // therefore never occupy the workers required to finish in-flight runs.
    let width = pool.current_num_threads().max(1);
    let (send, receive) = mpsc::sync_channel(width);
    let mut in_flight = 0usize;
    let _scan_memory = reserve_growth(64 << 10, budget, &mut in_flight, &receive, journal, epoch)?;
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(64 << 10)
        .map_err(|error| error.to_string())?;
    buffer.resize(64 << 10, 0);
    let result = (|| -> Result<(), String> {
        for (block, folder) in folders.iter().enumerate() {
            let map_bytes = (folder.entries().count() as u64)
                .checked_mul(std::mem::size_of::<(u64, u64, usize)>() as u64)
                .ok_or("file map memory overflow")?;
            let map_memory =
                reserve_growth(map_bytes, budget, &mut in_flight, &receive, journal, epoch)?;
            let map = Arc::new(FolderOutput::new(folder, Arc::clone(output), map_memory)?);
            let mut scanner = folder.scanner();
            let range = folder.packed_range();
            let mut position = range.offset;
            let mut seen = 0usize;
            while position < range.end() {
                epoch.check().map_err(|error| error.to_string())?;
                let wanted = buffer
                    .len()
                    .min(usize::try_from(range.end() - position).unwrap_or(usize::MAX));
                let count = source
                    .read_at(&mut buffer[..wanted], position)
                    .map_err(|error| error.to_string())?;
                if count == 0 {
                    return Err("truncated positional scan".to_string());
                }
                let mut consumed = 0;
                while consumed < count {
                    let used = scanner
                        .feed(&buffer[consumed..count])
                        .map_err(|error| error.to_string())?;
                    consumed += used;
                    if let Some(run) = scanner.next_run() {
                        let index =
                            usize::try_from(run.index()).map_err(|_| "run index overflow")?;
                        if index != seen {
                            return Err("non-contiguous run indexes".to_string());
                        }
                        seen += 1;
                        if let Some(Some(old)) = journal.folders[block].get(index) {
                            let check = &old.completion;
                            let packed = run.packed_range();
                            if epoch.vouches_for(packed.offset, packed.size)
                                && check.index == run.index()
                                && check.packed == packed
                                && check.output_offset == run.output_offset()
                                && check.output_len == run.output_len()
                            {
                                continue;
                            }
                        }
                        if let Some(old) = journal.folders[block].get_mut(index) {
                            *old = None;
                        }
                        while in_flight >= width {
                            in_flight -= 1;
                            take_completed(&receive, &mut journal.folders, epoch)?;
                        }
                        let packed = run.packed_range();
                        coverage
                            .wait_for_run_range(epoch, packed.offset, packed.size)
                            .map_err(|error| error.to_string())?;
                        let required = run.memory_required().map_err(|error| error.to_string())?;
                        // Includes CRC records, the journal slot, flat verification
                        // views and checksum-fold bookkeeping. Decoder/ring and
                        // conversion storage have their own worker reservation.
                        let retained = (folder.entries().count() as u64 + 1)
                            .checked_mul(256)
                            .and_then(|bytes| {
                                bytes.checked_add(4 * std::mem::size_of::<Option<Record>>() as u64)
                            })
                            .ok_or("run journal memory overflow")?;
                        let record_memory = reserve_growth(
                            retained,
                            budget,
                            &mut in_flight,
                            &receive,
                            journal,
                            epoch,
                        )?;
                        let worker_memory = reserve_growth(
                            required,
                            budget,
                            &mut in_flight,
                            &receive,
                            journal,
                            epoch,
                        )?;
                        if index == journal.folders[block].len() {
                            journal.folders[block]
                                .try_reserve_exact(1)
                                .map_err(|error| error.to_string())?;
                            journal.folders[block].push(None);
                        }
                        let send = send.clone();
                        let epoch = epoch.clone();
                        let map = Arc::clone(&map);
                        pool.spawn(move || {
                            let _worker_memory = worker_memory;
                            let result =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    run.decode(epoch.cancellation(), &mut |offset, bytes| {
                                        map.write_at(&epoch, offset, bytes)
                                    })
                                    .map_err(|error| error.to_string())
                                }))
                                .unwrap_or_else(|_| {
                                    Err("positional run worker panicked".to_string())
                                });
                            // Release decoded working storage before reporting
                            // completion; admission can immediately reuse it.
                            drop(_worker_memory);
                            drop(map);
                            let _ = send.send(Completed {
                                block,
                                index,
                                result,
                                record_memory,
                            });
                        });
                        in_flight += 1;
                    }
                    if used == 0 {
                        return Err("positional scanner made no progress".to_string());
                    }
                }
                position += count as u64;
            }
            scanner.finish().map_err(|error| error.to_string())?;
            journal.folders[block].truncate(seen);
        }
        // Fully covered folders share the same bounded worker frontier; a previous
        // folder need not finish before the next folder's independent jobs start.
        while in_flight != 0 {
            in_flight -= 1;
            take_completed(&receive, &mut journal.folders, epoch)?;
        }
        for (block, folder) in folders.iter().enumerate() {
            let records = &mut journal.folders[block];
            let mut completed = Vec::new();
            // Cloning checksum payload is avoided: the verification API borrows
            // a flat slice, so move records out only after all workers join.
            completed
                .try_reserve_exact(records.len())
                .map_err(|error| error.to_string())?;
            let mut permits = Vec::new();
            permits
                .try_reserve_exact(records.len())
                .map_err(|error| error.to_string())?;
            for record in records.iter_mut() {
                let record = record.take().ok_or("missing positional run completion")?;
                completed.push(record.completion);
                permits.push(record._memory);
            }
            let verified = folder
                .verify_runs(&completed)
                .map_err(|error| error.to_string());
            for (slot, (completion, memory)) in
                records.iter_mut().zip(completed.into_iter().zip(permits))
            {
                *slot = Some(Record {
                    completion,
                    _memory: memory,
                });
            }
            let verified = verified?;
            if verified.len() != folder.entries().count() {
                return Err("positional output requires member CRCs".to_string());
            }
            epoch.check().map_err(|error| error.to_string())?;
        }
        Ok(())
    })();
    // A repair epoch may reuse output only after every old writer has left.
    // Drain even on scanner/sink/CRC errors; no detached writer survives.
    while in_flight != 0 {
        in_flight -= 1;
        let _ = take_completed(&receive, &mut journal.folders, epoch);
    }
    result?;
    output
        .finish_files(epoch)
        .map_err(|error| error.to_string())
}

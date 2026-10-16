use super::*;
use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::extraction::{BudgetedWriter, ExtractionLimits};
use std::sync::Mutex;

pub(super) fn budget(root: &Path) -> Arc<JobExtractionBudget> {
    JobExtractionBudget::new(
        Arc::new(ExtractionLimits {
            max_job_bytes: 8192,
            max_member_bytes: 8192,
            max_entries: 32,
            max_ratio: 100,
            max_seconds: 60,
            min_free_bytes: 1,
            max_memory_bytes: 64 * 1024 * 1024,
        }),
        root.to_path_buf(),
        8192,
        0,
        0,
        PipelineMetrics::new(),
    )
    .unwrap()
}

struct ProbeWriter {
    pointer: usize,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Write for ProbeWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(
            bytes.as_ptr() as usize,
            self.pointer,
            "the wrapper copied payload"
        );
        self.events.lock().unwrap().push("write");
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for ProbeWriter {
    fn drop(&mut self) {
        self.events.lock().unwrap().push("closed");
    }
}

struct Completion {
    events: Arc<Mutex<Vec<&'static str>>>,
    revoked: bool,
}

impl OutputCompletion for Completion {
    fn data_complete(&mut self, _: u64) -> io::Result<()> {
        self.events.lock().unwrap().push("data");
        Ok(())
    }
    fn integrity(&mut self, integrity: Integrity) -> io::Result<()> {
        if self.revoked {
            return Err(io::Error::other("stale generation"));
        }
        self.events.lock().unwrap().push(match integrity {
            Integrity::Verified => "verified",
            Integrity::NoChecksum => "no-checksum",
        });
        Ok(())
    }
    fn abort(&mut self) {
        self.events.lock().unwrap().push("abort");
    }
}

fn stream(
    budget: Arc<JobExtractionBudget>,
    bytes: &[u8],
    events: &Arc<Mutex<Vec<&'static str>>>,
    revoked: bool,
) -> MemberOutput<ProbeWriter> {
    MemberOutput::new(
        BudgetedWriter::new(
            ProbeWriter {
                pointer: bytes.as_ptr() as usize,
                events: Arc::clone(events),
            },
            budget,
        ),
        Some(Box::new(Completion {
            events: Arc::clone(events),
            revoked,
        })),
        Some(bytes.len() as u64),
    )
}

#[test]
fn stream_passes_original_buffer_and_closes_before_delayed_integrity() {
    for integrity in [Integrity::Verified, Integrity::NoChecksum] {
        let dir = tempfile::tempdir().unwrap();
        let bytes = [19u8; 4096];
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut output = stream(budget(dir.path()), &bytes, &events, false);
        output.write_all(&bytes).unwrap();
        output.flush().unwrap();
        assert_eq!(*events.lock().unwrap(), ["write"]);
        let pending = output.finish_data().unwrap();
        assert_eq!(*events.lock().unwrap(), ["write", "data", "closed"]);
        pending.integrity(integrity).unwrap();
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 4);
        assert_eq!(
            events[3],
            if integrity == Integrity::Verified {
                "verified"
            } else {
                "no-checksum"
            }
        );
    }
}

#[test]
fn incomplete_or_revoked_outputs_abort_once() {
    for scenario in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let bytes = [31u8; 8];
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut output = stream(budget(dir.path()), &bytes, &events, scenario == 3);
        match scenario {
            0 => drop(output),
            1 => assert!(output.finish_data().is_err()),
            2 => {
                output.write_all(&bytes).unwrap();
                drop(output.finish_data().unwrap());
            }
            3 => {
                output.write_all(&bytes).unwrap();
                assert!(
                    output
                        .finish_data()
                        .unwrap()
                        .integrity(Integrity::Verified)
                        .is_err()
                );
            }
            _ => unreachable!(),
        }
        let events = events.lock().unwrap();
        assert_eq!(events.iter().filter(|event| **event == "abort").count(), 1);
        assert!(!events.contains(&"verified"));
    }
}

#[test]
fn non_filesystem_output_obeys_job_cancellation_and_byte_limits() {
    for cancelled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let budget = budget(dir.path());
        let bytes = [13u8; 8193];
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut output = stream(Arc::clone(&budget), &bytes, &events, false);
        if cancelled {
            budget.stop_for_rejection();
        }
        assert!(output.write_all(&bytes).is_err());
        drop(output);
        assert!(!events.lock().unwrap().contains(&"write"));
    }
}

#[cfg(unix)]
#[test]
fn file_metadata_stays_on_open_member_when_path_is_replaced_by_symlink() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let root = ExtractionRoot::open(dir.path()).unwrap();
    let budget = budget(dir.path());
    let output = FileOutput {
        root: &root,
        budget: &budget,
    };
    let mut file = output.file(0, Path::new("member.bin"), Some(1)).unwrap();
    file.write_all(&[9]).unwrap();
    let target = dir.path().join("unrelated.bin");
    std::fs::write(&target, [1]).unwrap();
    let before = std::fs::metadata(&target).unwrap().modified().unwrap();
    std::fs::remove_file(dir.path().join("member.bin")).unwrap();
    symlink(&target, dir.path().join("member.bin")).unwrap();
    file.metadata(Metadata {
        modified: Some(SystemTime::UNIX_EPOCH),
        ..Metadata::default()
    })
    .unwrap();
    assert_eq!(
        std::fs::metadata(&target).unwrap().modified().unwrap(),
        before
    );
}

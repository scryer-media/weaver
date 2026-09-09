use super::{Par3RepairRequest, RepairBackend};
use par3_rs::ingest::{PacketScanner, ScanEvent};
use par3_rs::runtime::{EngineError, ExecutionOptions};
use par3_rs::session::RepairStatus;
use par3_rs::source::{MemorySourceAccess, SourceAccess, SourceId, SourceSnapshot};
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const INDEX: &[u8] = include_bytes!("fixtures/set.par3");
const RECOVERY: &[u8] = include_bytes!("fixtures/set.vol0+1.par3");
const SET: par3_rs::InputSetId =
    par3_rs::InputSetId([0x24, 0xa1, 0xad, 0x60, 0x1a, 0xe5, 0xbc, 0x72]);

struct CountedSources {
    inner: MemorySourceAccess,
    reads: AtomicU64,
}

impl SourceAccess for CountedSources {
    fn snapshot(&self, id: SourceId) -> io::Result<Option<SourceSnapshot>> {
        self.inner.snapshot(id)
    }

    fn read_at(&self, id: SourceId, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read_at(id, offset, out)?;
        self.reads.fetch_add(count as u64, Ordering::Relaxed);
        Ok(count)
    }

    fn next_available(&self, id: SourceId, offset: u64) -> io::Result<Option<Range<u64>>> {
        self.inner.next_available(id, offset)
    }
}

fn merge(session: &mut par3_rs::Par3RepairSession, bytes: &[u8], options: &ExecutionOptions) {
    let mut carrier = MemorySourceAccess::default();
    carrier.insert(SourceId(99), 0, Arc::from(bytes));
    let mut scanner = PacketScanner::new(
        Arc::new(carrier),
        SourceId(99),
        options.clone(),
        par3_rs::ScanLimits::default(),
    )
    .unwrap();
    loop {
        match scanner.poll().unwrap() {
            ScanEvent::Packet(packet) => {
                session.merge(packet).unwrap();
            }
            ScanEvent::End => break,
            ScanEvent::NeedData { .. } => panic!("complete official carrier"),
        }
    }
}

#[test]
fn par3_backend_retains_native_evidence_and_stages_only_damage() {
    let original: Vec<u8> = (0..5000u32).map(|i| (i * 7 + 3) as u8).collect();
    let mut damaged = original.clone();
    damaged[2300] ^= 1;
    let mut inner = MemorySourceAccess::default();
    inner.insert(SourceId(0), 1, damaged.into());
    inner.insert(SourceId(1), 1, Arc::from(&b"qrstuvwxyz"[..]));
    inner.insert(
        SourceId(2),
        1,
        (0..4000u32)
            .map(|i| (i * 13 + 1) as u8)
            .collect::<Vec<_>>()
            .into(),
    );
    let access = Arc::new(CountedSources {
        inner,
        reads: AtomicU64::new(0),
    });
    let options = ExecutionOptions::default();
    let mut session =
        par3_rs::Par3RepairSession::new(SET, access.clone(), options.clone()).unwrap();
    for (name, id) in [("a.bin", 0), ("b.txt", 1), ("sub/c.bin", 2)] {
        session.bind_file(name, SourceId(id)).unwrap();
    }
    assert_eq!(
        RepairBackend::assess(&mut session).unwrap().status,
        RepairStatus::IncompleteMetadata
    );
    merge(&mut session, INDEX, &options);
    assert_eq!(
        RepairBackend::assess(&mut session).unwrap().status,
        RepairStatus::NeedRecovery
    );
    let reads = access.reads.load(Ordering::Relaxed);
    assert!(reads > 0);
    assert_eq!(
        RepairBackend::assess(&mut session).unwrap().status,
        RepairStatus::NeedRecovery
    );
    assert_eq!(access.reads.load(Ordering::Relaxed), reads);
    merge(&mut session, RECOVERY, &options);
    merge(&mut session, RECOVERY, &options);
    let assessment = RepairBackend::assess(&mut session).unwrap();
    assert_eq!(assessment.status, RepairStatus::Ready);
    assert_eq!(assessment.lost_blocks, [1]);
    assert_eq!(assessment.requirements[0].available.len(), 1);
    assert_eq!(
        access.reads.load(Ordering::Relaxed),
        reads,
        "recovery merges must reuse source evidence"
    );
    assert_eq!(
        RepairBackend::retained_bytes(&session),
        session.retained_bytes()
    );
    let output = tempfile::tempdir().unwrap();
    let repaired = session
        .execute(Par3RepairRequest {
            output: output.path(),
            backup: false,
        })
        .unwrap();
    assert_eq!(repaired.installed.len(), 1);
    assert_eq!(
        std::fs::read(output.path().join("a.bin")).unwrap(),
        original
    );
    assert!(!output.path().join("b.txt").exists());
    assert!(!output.path().join("sub/c.bin").exists());
    // Native source invalidation must still discard the old strong evidence.
    let reads = access.reads.load(Ordering::Relaxed);
    session.invalidate(SourceId(0));
    assert_eq!(
        RepairBackend::assess(&mut session).unwrap().status,
        RepairStatus::Ready
    );
    assert!(access.reads.load(Ordering::Relaxed) > reads);
    drop(session);
    assert_eq!(options.memory.used(), 0);
    assert_eq!(options.handles.used(), 0);
}

#[test]
fn par3_backend_preserves_typed_cancellation() {
    let options = ExecutionOptions::default();
    let mut session = par3_rs::Par3RepairSession::new(
        SET,
        Arc::new(MemorySourceAccess::default()),
        options.clone(),
    )
    .unwrap();
    options.cancel.cancel();
    assert!(matches!(
        RepairBackend::assess(&mut session),
        Err(EngineError::Cancelled)
    ));
}
